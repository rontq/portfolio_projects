import os
import pandas as pd
import joblib
import xgboost as xgb
import numpy as np
import gc
from sklearn.metrics import mean_squared_error
from dotenv import load_dotenv
import psycopg2

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT")
}

def fetch_company_data(symbol):
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT * FROM stock_market_table
        WHERE symbol = %s AND future_return_1d IS NOT NULL
        ORDER BY date
    """
    df = pd.read_sql(query, conn, params=(symbol,))
    conn.close()
    return df

def assign_symbol_id(df, symbol_id=100):
    df['symbol_id'] = symbol_id
    return df

def preprocess(df):
    df = df.sort_values("date")
    df = assign_symbol_id(df)

    features = [
        'open', 'high', 'low', 'close', 'volume', 'adj_close',
        'sma_5', 'sma_20', 'sma_50', 'sma_125', 'sma_200', 'sma_200_weekly',
        'ema_5', 'ema_20', 'ema_50', 'ema_125', 'ema_200',
        'macd', 'dma', 'rsi',
        'bollinger_upper', 'bollinger_middle', 'bollinger_lower', 'obv',
        'market_cap', 'market_cap_proxy', 'vix_close', 'symbol_id'
    ]

    df = df.dropna(subset=features + ['future_return_1d'])
    X = df[features]
    y = df['future_return_1d']
    return X, y


def train_model(X, y, xgb_params, max_retries=50, early_stop_window=10):
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.3, shuffle=False)

    best_rmse = float("inf")
    best_model = None
    no_improve_count = 0

    print("📈 Starting model training with retry logic...")

    for i in range(1, max_retries + 1):
        print(f"   🔁 Attempt {i}/{max_retries}")

        model = xgb.XGBRegressor(**xgb_params)
        model.fit(X_train, y_train)

        preds = model.predict(X_val)
        rmse = mean_squared_error(y_val, preds, squared=False)
        print(f"   RMSE = {rmse:.4f}")

        if rmse < best_rmse:
            best_rmse = rmse
            best_model = model
            no_improve_count = 0
            print("   ✅ New best model found.")
        else:
            no_improve_count += 1
            print(f"   ⚠️ No improvement ({no_improve_count}/{early_stop_window})")

        if no_improve_count >= early_stop_window:
            print(f"⛔ Early stopping: no improvement after {early_stop_window} rounds.")
            break

    print(f"🏁 Final best RMSE: {best_rmse:.4f}")
    return best_model, best_rmse, i


def save_model(model, symbol):
    model_dir = "models"
    os.makedirs(model_dir, exist_ok=True)
    file_path = os.path.join(model_dir, f"xgb_model_{symbol.lower()}.joblib")
    joblib.dump(model, file_path)
    print(f"✅ Model saved: {file_path}")

def clear_resources():
    gc.collect()
    xgb.Booster().free()
    print("🧹 Cleared memory and XGBoost cache.")

def run_pipeline(symbol, symbol_id=100):
    print(f"\n\U0001f4ca Starting training for company: {symbol}")
    df = fetch_company_data(symbol)
    df = assign_symbol_id(df, symbol_id)
    X, y = preprocess(df)
    model = train_model(X, y)
    save_model(model, symbol)
    clear_resources()

if __name__ == "__main__":
    # Example run: NVDA assigned ID 100
    run_pipeline("NVDA", symbol_id=100)
