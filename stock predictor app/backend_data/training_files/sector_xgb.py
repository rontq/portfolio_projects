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

def train_model(X, y):
    model = xgb.XGBRegressor(
        n_estimators=500,
        learning_rate=0.05,
        max_depth=6,
        subsample=0.8,
        colsample_bytree=0.8,
        tree_method='hist',
        random_state=42
    )
    model.fit(X, y)
    return model

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
