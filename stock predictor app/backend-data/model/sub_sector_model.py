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

def fetch_subsector_data(sector, subsector):
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT * FROM stock_market_table
        WHERE sector = %s AND subsector = %s AND future_return_1d IS NOT NULL
        ORDER BY symbol, date
    """
    df = pd.read_sql(query, conn, params=(sector, subsector))

    index_query = """
        SELECT * FROM sector_index_table
        WHERE sector = %s AND subsector = %s
    """
    index_df = pd.read_sql(index_query, conn, params=(sector, subsector))
    conn.close()
    return df, index_df

def assign_symbol_ids(df, start_id=100, symbol_col="symbol", id_col="symbol_id"):
    current_symbol = None
    current_id = start_id
    id_map = {}
    ids = []

    for symbol in df[symbol_col]:
        if symbol != current_symbol:
            current_symbol = symbol
            if symbol not in id_map:
                id_map[symbol] = current_id
                current_id += 1
        ids.append(id_map[symbol])

    df[id_col] = ids
    return df, id_map

def enrich_with_subsector_features(company_df, index_df):
    subsector_level = index_df[index_df["is_subsector"] == True].copy()

    enriched = company_df.merge(
        subsector_level.add_prefix("subsector_"),
        left_on=["sector", "subsector", "date"],
        right_on=["subsector_sector", "subsector_subsector", "subsector_date"],
        how="left"
    )

    return enriched

def preprocess(df, index_df):
    df = df.sort_values(["symbol", "date"])
    df, symbol_map = assign_symbol_ids(df)
    df = enrich_with_subsector_features(df, index_df)

    features = [
        'open', 'high', 'low', 'close', 'volume', 'adj_close',
        'sma_5', 'sma_20', 'sma_50', 'sma_125', 'sma_200', 'sma_200_weekly',
        'ema_5', 'ema_20', 'ema_50', 'ema_125', 'ema_200',
        'macd', 'dma', 'rsi',
        'bollinger_upper', 'bollinger_middle', 'bollinger_lower', 'obv',
        'market_cap', 'market_cap_proxy', 'sector_id', 'subsector_id',
        'sector_weight', 'subsector_weight', 'vix_close', 'symbol_id',
        'subsector_index_value', 'subsector_return_vs_previous', 'subsector_market_cap',
        'subsector_influence_weight'
    ]

    df = df.dropna(subset=features + ['future_return_1d'])
    X = df[features]
    y = df['future_return_1d']
    return X, y, symbol_map

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

def save_model(model, sector_name, subsector_name):
    model_dir = "models"
    os.makedirs(model_dir, exist_ok=True)
    file_path = os.path.join(
        model_dir,
        f"xgb_model_{sector_name.replace(' ', '_').lower()}__{subsector_name.replace(' ', '_').lower()}.joblib"
    )
    joblib.dump(model, file_path)
    print(f"✅ Model saved: {file_path}")

def clear_resources():
    gc.collect()
    xgb.Booster().free()
    print("🧹 Cleared memory and XGBoost cache.")

def run_pipeline(sector, subsector):
    print(f"\n\U0001f4ca Starting training for subsector: {sector} → {subsector}")
    df, index_df = fetch_subsector_data(sector, subsector)
    X, y, _ = preprocess(df, index_df)
    model = train_model(X, y)
    save_model(model, sector, subsector)
    clear_resources()

if __name__ == "__main__":
    # Example run: update with real sector/subsector
    run_pipeline("Information Technology", "Semiconductors")
