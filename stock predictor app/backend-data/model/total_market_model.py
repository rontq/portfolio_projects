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
    df = df.dropna()
    
    drop_cols = ['date', 'symbol', 'sector', 'subsector']

    if 'target' in df.columns:
        target = df['target']
        df = df.drop(columns=drop_cols + ['target'])
    else:
        target = df['close'].shift(-1)  # Shift close price to create the target variable
        df = df.drop(columns=drop_cols)

    df = df.select_dtypes(include=['number', 'bool', 'category'])
    target = target.dropna()
    df = df.loc[target.index]

    # Check if data is still valid after preprocessing (i.e., no empty DataFrames)
    if df.empty or target.empty:
        raise ValueError("No valid data left after preprocessing. Ensure the data has enough non-NaN entries.")

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

def save_results(model, metrics, feature_names):
    save_path = SAVE_DIR / "all_market_model.joblib"
    
    # Combine model and metadata into a single dictionary
    data_to_save = {
        "model": model,
        "features": feature_names
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

    model, y_test, preds, metrics = train_model(X, y)
    print(f"✅ Finished training — RMSE: {metrics['RMSE']:.4f}, R2: {metrics['R2']:.4f}")

    save_results(model, metrics, X.columns.tolist())

    conn.close()
    print("\n🎉 All stock market data model trained and saved.")

if __name__ == "__main__":
    main()
