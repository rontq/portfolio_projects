import os
import json
import psycopg2
import pandas as pd
import xgboost as xgb
import numpy as np
import gc  # For clearing memory
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from dotenv import load_dotenv
import joblib
from pathlib import Path

# === CONFIG ===
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../credentials/.env'))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

MODEL_DIR = "trained_total_model"
SAVE_DIR = Path(MODEL_DIR)
SAVE_DIR.mkdir(exist_ok=True)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_all_data(conn):
    query = """
    SELECT * FROM stock_market_table
    ORDER BY symbol, date;
    """
    return pd.read_sql(query, conn)

def preprocess(df):
    df = df.copy()

    ohlcv_cols = ['open', 'close', 'high', 'low', 'volume']
    df = df.dropna(subset=ohlcv_cols)

    df = df.drop(columns=['date', 'symbol', 'sector', 'subsector'], errors='ignore')

    # Convert pe_ratio safely to float
    if 'pe_ratio' in df.columns:
        df['pe_ratio'] = pd.to_numeric(df['pe_ratio'], errors='coerce')  # Turn anything non-numeric into NaN
        if df['pe_ratio'].isna().all():
            df.drop(columns=['pe_ratio'], inplace=True)

    # Drop any other non-numeric columns silently
    df = df.select_dtypes(include=[np.number])

    # Prepare the target variable (next day's close)
    target = df['close'].shift(-1)
    df = df.iloc[:-1]
    target = target.iloc[:-1]

    if df.empty or target.empty:
        raise ValueError("No valid data left after preprocessing.")

    return df, target

def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, shuffle=False)

    y_train = y_train.dropna()
    y_test = y_test.dropna()

    model = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=100, enable_categorical=True)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    min_len = min(len(y_test), len(preds))
    y_test = y_test.iloc[:min_len]
    preds = preds[:min_len]

    mask = ~np.isnan(y_test) & ~np.isnan(preds)
    y_test = y_test[mask]
    preds = preds[mask]

    return model, y_test, preds

def save_results(model, feature_names):
    save_path = SAVE_DIR / "all_market_model.joblib"

    # Prepare data to save
    data_to_save = {
        "model": model,
        "feature_names": feature_names,
    }

    # Save the entire bundle using joblib
    joblib.dump(data_to_save, save_path)
    print(f"📦 Model and metadata saved to {save_path}")


def main():
    conn = get_db_connection()
    gc.collect()  # Clear memory cache

    print("\n--- Training model for the total stock market trend ---")
    df = fetch_all_data(conn)
    if df.empty:
        print("⚠️ No data found, skipping.")
        return

    X, y = preprocess(df)
    if X.empty or y.isnull().all():
        print("⚠️ Not enough data after preprocessing, skipping.")
        return

    model, y_test, preds = train_model(X, y)
    print(f"✅ Finished training")

    save_results(model, X.columns.tolist())  # Save model and feature names

    conn.close()
    print("\n🎉 All stock market data model trained and saved.")

if __name__ == "__main__":
    main()
