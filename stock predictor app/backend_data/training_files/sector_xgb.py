import os
import pandas as pd
import joblib
import xgboost as xgb
import numpy as np
import gc
from sklearn.metrics import mean_squared_error
import psycopg2

from data_fetch_store.db_params import DB_CONFIG

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



def fetch_sector_data_for_symbol(symbol):
    conn = psycopg2.connect(**DB_CONFIG)

    # Step 1: Get sector_id for the symbol
    sector_id_query = "SELECT DISTINCT sector_id FROM stock_market_table WHERE symbol = %s;"
    sector_id_df = pd.read_sql(sector_id_query, conn, params=(symbol,))

    if sector_id_df.empty:
        conn.close()
        raise ValueError(f"No sector_id found for symbol: {symbol}")

    sector_id_val = int(sector_id_df.iloc[0, 0])

    # Step 2: Fetch all companies in that sector
    stock_query = """
        SELECT * FROM stock_market_table
        WHERE sector_id = %s AND future_return_1d IS NOT NULL
        ORDER BY date;
    """
    stock_df = pd.read_sql(stock_query, conn, params=(sector_id_val,))

    # Step 3: Fetch sector index data
    index_query = """
        SELECT * FROM sector_index_table
        WHERE sector_id = %s
        ORDER BY date;
    """
    index_df = pd.read_sql(index_query, conn, params=(sector_id_val,))

    conn.close()

    # Step 4: Merge index data into stock data
    merged_df = pd.merge(stock_df, index_df, on=["sector", "date"], how="left", suffixes=("", "_sector"))

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


def run_pipeline(symbol, input_params, xgb_params):
    print(f"\n📊 Starting training for company: {symbol}")
    df = fetch_sector_data_for_symbol(symbol)
    df = assign_symbol_id(df, input_params["symbol_id"])
    X, y = preprocess(df, input_params["features"], input_params["target_column"])
    model = train_model(X, y, xgb_params)
    save_model(model, symbol)
    clear_resources()


if __name__ == "__main__":
    run_pipeline("NVDA", input_params, xgb_params)
