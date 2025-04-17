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

import argparse

# === CONFIG ===
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../credentials/.env'))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

SUBSECTORS = [
    #Information Technology
    "Semiconductors",
    "System Software",
    "IT Services & Consulting",
    "Hardware & Peripherals",

    #Financials
    "Banks",
    "Investment Management",
    "Insurance",
    "Exchanges & Financial Services",

    #Healthcare
    "Pharmaceuticals",
    "Healthcare Equipment",
    "Healthcare Services",
    "Biotech & Research",

    #Consumer Discretionary
    "Retail",
    "Automotive",
    "Restaurants",
    "Travel & Leisure",

    #Industrial
    "Aerospace & Defense",
    "Machinery",
    "Transportation",
    "Construction & Engineering",

    #Consumer Staples
    "Food & Beverage",
    "Retail & Distribution",
    "Household Products",
    "Tobacco & Alcohol",

    #Communications
    "Internet Services",
    "Media & Entertainment",
    "Telecom",
    "Gaming & Interactive Media",

    #Utilities
    "Electric Utilities",
    "Gas Utilities",
    "Renewables",
    "Water Utilities"
]


MODEL_DIR = "trained_subsector_models"
SAVE_DIR = Path(MODEL_DIR)
SAVE_DIR.mkdir(exist_ok=True)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_sector_data(conn, sector):
    query = """
    SELECT * FROM stock_market_table
    WHERE subsector = %s
    ORDER BY symbol, date;
    """
    return pd.read_sql(query, conn, params=(sector,))

def preprocess(df):
    df = df.copy()
    df = df.dropna()
    drop_cols = ['date', 'symbol', 'sector', 'subsector']

    if 'target' in df.columns:
        target = df['target']
        df = df.drop(columns=drop_cols + ['target'])
    else:
        target = df['close'].shift(-1)
        df = df.drop(columns=drop_cols)

    df = df.select_dtypes(include=['number', 'bool', 'category'])
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

    rmse = mean_squared_error(y_test, preds, squared=False)
    mae = mean_absolute_error(y_test, preds)
    r2 = r2_score(y_test, preds)
    mape = np.mean(np.abs((y_test - preds) / y_test)) * 100

    metrics = {
        "RMSE": rmse,
        "MAE": mae,
        "R2": r2,
        "MAPE": mape
    }

    return model, y_test, preds, metrics

def save_results(subsector, model, metrics, feature_names):
    save_path = SAVE_DIR / f"{subsector}_model.joblib"
    
    # Combine model and metadata into a single dictionary
    data_to_save = {
        "subsector": subsector,
        "model": model,
        "metrics": metrics,
        "features": feature_names
    }

    # Save the entire bundle using joblib
    joblib.dump(data_to_save, save_path)
    print(f"📦 Model and metadata saved to {save_path}")

def main():
    conn = get_db_connection()
    trained_subsectors = []

    # Always train through all subsectors
    for subsector in SUBSECTORS:
        print(f"\n--- Training model for subsector: {subsector} ---")
        gc.collect()  # Clear memory cache between training
        print("Clearing cache")
        df = fetch_sector_data(conn, subsector)
        if df.empty:
            print(f"⚠️ No data found for {subsector}, skipping.")
            continue

        X, y = preprocess(df)
        if X.empty or y.isnull().all():
            print(f"⚠️ Not enough data after preprocessing for {subsector}, skipping.")
            continue

        model, y_test, preds, metrics = train_model(X, y)
        print(f"✅ Finished training {subsector} — RMSE: {metrics['RMSE']:.4f}, R2: {metrics['R2']:.4f}")

        save_results(subsector, model, metrics, X.columns.tolist())
        trained_subsectors.append(subsector)

    conn.close()
    print(f"\n✅ Trained subsectors: {trained_subsectors}")
    print("\n🎉 All subsector models trained and saved.")

if __name__ == "__main__":
    main()