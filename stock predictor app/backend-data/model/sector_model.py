import os
import psycopg2
import pandas as pd
import xgboost as xgb
import numpy as np
import gc  # For clearing memory
from sklearn.model_selection import train_test_split
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

SECTORS = [
    "Information Technology",
    "Financials",
    "Healthcare",
    "Consumer Discretionary",
    "Industrials",
    "Consumer Staples",
    "Communications",
    "Utilities"
]

MODEL_DIR = "trained_sector_models"
SAVE_DIR = Path(MODEL_DIR)
SAVE_DIR.mkdir(exist_ok=True)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_sector_data(conn, sector):
    query = """
    SELECT * FROM stock_market_table
    WHERE sector = %s
    ORDER BY symbol, date;
    """
    return pd.read_sql(query, conn, params=(sector,))

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

import xgboost as xgb
from sklearn.model_selection import train_test_split
import numpy as np

def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, shuffle=False)

    # Remove NaNs from target values in the training and test sets
    y_train = y_train.dropna()
    y_test = y_test.dropna()

    # Remove NaNs in feature sets (X_train, X_test)
    X_train = X_train.loc[y_train.index]
    X_test = X_test.loc[y_test.index]

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


def save_results(sector, model, feature_names):
    save_path = SAVE_DIR / f"{sector}_model.joblib"
    
    # Combine model and metadata into a single dictionary
    data_to_save = {
        "sector": sector,
        "model": model,
        "features": feature_names
    }

    # Save the entire bundle using joblib
    joblib.dump(data_to_save, save_path)
    print(f"📦 Model and metadata saved to {save_path}")





def main():
    conn = get_db_connection()
    trained_sectors = []

    for sector in SECTORS:
        print(f"\n--- Training model for sector: {sector} ---")
        gc.collect()  # Clear memory cache between training
        print("Clearing cache")
        df = fetch_sector_data(conn, sector)
        if df.empty:
            print(f"⚠️ No data found for {sector}, skipping.")
            continue

        X, y = preprocess(df)
        if X.empty or y.isnull().all():
            print(f"⚠️ Not enough data after preprocessing for {sector}, skipping.")
            continue

        model, y_test, preds = train_model(X, y)
        print(f"✅ Finished training {sector}")

        save_results(sector, model, X.columns.tolist())
        trained_sectors.append(sector)

    conn.close()
    print(f"\n✅ Trained sectors: {trained_sectors}")
    print("\n🎉 All sector models trained and saved.")

if __name__ == "__main__":
    main()
