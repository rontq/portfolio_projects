import os
import pandas as pd
import joblib
import xgboost as xgb
import numpy as np
import gc
from sklearn.metrics import mean_squared_error
import psycopg2
from db_params import DB_CONFIG

import sys, os

current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../.."))
sys.path.append(project_root)

try:
    from data_fetch_store.stock_list import SECTORS
    print("SECTORS imported successfully!")
except ModuleNotFoundError as e:
    print(f"Error: {e}")

xgb_params = {
    "n_estimators": 500,
    "learning_rate": 0.05,
    "max_depth": 6,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "tree_method": "hist",
    "random_state": 42
}

input_params = {
    "features": [
        'open', 'high', 'low', 'close', 'volume', 'adj_close',
        'sma_5', 'sma_20', 'sma_50', 'sma_125', 'sma_200', 'sma_200_weekly',
        'ema_5', 'ema_20', 'ema_50', 'ema_125', 'ema_200',
        'macd', 'dma', 'rsi',
        'bollinger_upper', 'bollinger_middle', 'bollinger_lower', 'obv',
        'market_cap', 'market_cap_proxy', 'vix_close', 'symbol_id'
    ],
    "target_column": "index_value"
}



def fetch_data_for_sector(sector_id):
    conn = psycopg2.connect(**DB_CONFIG)

    # Step 1: Get all stock data (no is_subsector filter here)
    stock_query = """
        SELECT * FROM stock_market_table
        WHERE sector_id = %s AND future_return_1d IS NOT NULL
        ORDER BY date;
    """
    stock_df = pd.read_sql(stock_query, conn, params=(sector_id,))

    # Step 2: Get sector index data (exclude subsectors)
    index_query = """
        SELECT date, sector, sector_id, index_value
        FROM sector_index_table
        WHERE sector_id = %s AND is_subsector = FALSE
        ORDER BY date;
    """
    index_df = pd.read_sql(index_query, conn, params=(sector_id,))

    conn.close()

    # Step 3: Merge index value into stock data
    merged_df = pd.merge(stock_df, index_df, on=["sector", "date"], how="left")

    return merged_df


def assign_symbol_id(df, symbol_id=100):
    df['symbol_id'] = symbol_id
    return df


def preprocess(df, features, target_col):
    df = df.sort_values("date")
    df = assign_symbol_id(df)
    df = df.dropna(subset=features + [target_col])
    X = df[features]
    y = df[target_col]
    return X, y


def train_model(X, y, xgb_params):
    model = xgb.XGBRegressor(**xgb_params)
    model.fit(X, y)
    return model


def save_model(model, sector_name, model_dir="trained_models"):
    """Save trained model using the sector name."""
    os.makedirs(model_dir, exist_ok=True)
    safe_name = sector_name.lower().replace(" ", "_")
    path = os.path.join(model_dir, f"xgb_model_{safe_name}.joblib")
    joblib.dump(model, path)
    print(f"✅ Saved model: {path}")

def clear_resources():
    gc.collect()
    xgb.Booster().free()
    print("🧹 Cleared memory and XGBoost cache.")


def run_pipeline_for_sector(sector_id, sector_name, input_params, xgb_params):
    """Run the full pipeline for one sector."""
    print(f"\n📊 Training sector: {sector_name} (ID: {sector_id})")
    df = fetch_data_for_sector(sector_id)
    X, y = preprocess(df, input_params["features"], input_params["target_column"])
    model = train_model(X, y, xgb_params)
    save_model(model, sector_id)
    clear_resources()


def train_selected_sectors(sectors, input_params, xgb_params):
    """Train models for a fixed list of sector names."""
    conn = psycopg2.connect(**DB_CONFIG)
    sector_map = {}

    for name in sectors:
        query = """
            SELECT DISTINCT sector_id FROM sector_index_table
            WHERE sector = %s AND is_subsector = FALSE;
        """
        df = pd.read_sql(query, conn, params=(name,))
        if not df.empty:
            sector_map[name] = df.iloc[0]["sector_id"]
        else:
            print(f"⚠️ Skipping unknown or invalid sector: {name}")

    conn.close()

    for name, sid in sector_map.items():
        try:
            run_pipeline_for_sector(sid, name, input_params, xgb_params)
        except Exception as e:
            print(f"❌ Error training {name} (ID: {sid}): {e}")


if __name__ == "__main__":
    train_selected_sectors(SECTORS, input_params, xgb_params)
