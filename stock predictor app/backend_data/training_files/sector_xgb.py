import os
import pandas as pd
import joblib
import xgboost as xgb
import gc
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error
import sys

current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../.."))
sys.path.append(project_root)
# Ensure DB_CONFIG and SECTORS are correctly imported
try:
    from db_params import DB_CONFIG
except ModuleNotFoundError as e:
    print(f"Error: {e} - DB_CONFIG is missing.")
    sys.exit(1)

try:
    from data_fetch_store.stock_list import SECTORS
    print("SECTORS imported successfully!")
except ModuleNotFoundError as e:
    print(f"Error: {e} - SECTORS is missing.")
    sys.exit(1)

# XGBoost configuration
xgb_params = {
    "n_estimators": 1000,
    "learning_rate": 0.01,
    "max_depth": 8,
    "subsample": 0.8,
    "colsample_bytree": 0.8,
    "tree_method": "hist",
    "eval_metric": "rmse",
    "objective": "reg:squarederror",
    "random_state": 42,
    "booster": "gbtree",
    "gamma": 0.1,
    "min_child_weight": 1,
    "max_delta_step": 1,
    "scale_pos_weight": 1,
    "n_jobs": -1,
    "lambda": 1,
    "alpha": 0,
}

# Features and target setup
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

# Set up SQLAlchemy engine
db_uri = f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
engine = create_engine(db_uri)

def fetch_data_for_sector(sector_id):
    """Fetch merged stock and index data for a given sector ID."""
    stock_query = """
        SELECT * FROM stock_market_table
        WHERE sector_id = %s 
        ORDER BY date;
    """
    index_query = """
        SELECT date, sector, index_value
        FROM sector_index_table
        WHERE is_subsector = false AND sector = (
            SELECT sector FROM stock_market_table WHERE sector_id = %s LIMIT 1
        )
        ORDER BY date;
    """

    # Fetch stock and index data
    print(f"Executing stock query for sector_id {sector_id}...")
    stock_df = pd.read_sql(stock_query, engine, params=(sector_id,))
    print(f"Fetched {len(stock_df)} rows for stock data")

    print(f"Executing index query for sector_id {sector_id}...")
    index_df = pd.read_sql(index_query, engine, params=(sector_id,))
    print(f"Fetched {len(index_df)} rows for index data")

    if stock_df.empty or index_df.empty:
        print(f"⚠️ No data found for sector_id {sector_id}, skipping...")
        return pd.DataFrame()  # Return an empty DataFrame if no data

    # Merge dataframes on date and sector
    merged_df = pd.merge(stock_df, index_df, on=["sector", "date"], how="left")
    return merged_df

def assign_symbol_id(df, symbol_id=100):
    if 'symbol_id' not in df.columns:
        df['symbol_id'] = symbol_id
    return df

def preprocess(df, features, target_col):
    df = df.sort_values("date").copy()
    df = assign_symbol_id(df)

    missing = [col for col in features + [target_col] if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df[features + [target_col]] = df[features + [target_col]].apply(pd.to_numeric, errors="coerce")
    df = df.dropna(subset=features + [target_col])

    if df.empty:
        print("⚠️ No valid training rows after preprocessing.")
        return pd.DataFrame(), pd.Series(dtype=float)

    X = df[features]
    y = df[target_col]
    return X, y

def train_model(X, y, xgb_params, max_retries=100, early_stop_window=50):
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, shuffle=False)

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

def save_model(model, sector_name, model_dir="trained_models"):
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
    print(f"\n📊 Training sector: {sector_name} (ID: {sector_id})")
    df = fetch_data_for_sector(sector_id)

    if df.empty:
        print(f"⚠️ No data found for {sector_name}, skipping...")
        return None

    if df['index_value'].isnull().all():
        print(f"⚠️ No index_value found for {sector_name}, skipping...")
        return None

    try:
        X, y = preprocess(df, input_params["features"], input_params["target_column"])
    except ValueError as ve:
        print(f"❌ Preprocessing failed for {sector_name}: {ve}")
        return None

    if X.empty or y.empty:
        print(f"⚠️ No valid training data for {sector_name}, skipping...")
        return None

    model, rmse, rounds = train_model(X, y, xgb_params)
    save_model(model, sector_name)
    clear_resources()

    return {
        "sector": sector_name,
        "rmse": rmse,
        "rounds": rounds
    }

def train_selected_sectors(sectors, input_params, xgb_params):
    sector_map = {}

    for name in sectors:
        query = """
            SELECT DISTINCT sector_id FROM stock_market_table
            WHERE sector = %s;
        """
        sector_df = pd.read_sql(query, engine, params=(name,))
        if not sector_df.empty:
            sector_map[name] = int(sector_df.iloc[0]["sector_id"])
        else:
            print(f"⚠️ Skipping unknown or invalid sector: {name}")

    summary = []
    for name, sid in sector_map.items():
        try:
            result = run_pipeline_for_sector(sid, name, input_params, xgb_params)
            if result:
                summary.append(result)
        except Exception as e:
            print(f"❌ Error training {name} (ID: {sid}): {e}")

    if summary:
        print("\n\n📊 Training Summary:")
        print(pd.DataFrame(summary).sort_values(by="rmse"))
    else:
        print("⚠️ No models were successfully trained.")

if __name__ == "__main__":
    train_selected_sectors(SECTORS, input_params, xgb_params)
