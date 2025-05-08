import os
import re
import pandas as pd
import joblib
import xgboost as xgb
import gc
from sklearn.metrics import mean_squared_error
from sklearn.model_selection import train_test_split
from db_params import DB_CONFIG
import psycopg2


# XGBoost Parameters
xgb_params = {
    'max_depth': 5,
    'learning_rate': 0.03,
    'subsample': 0.8,
    'colsample_bytree': 0.8,
    'min_child_weight': 5,
    'gamma': 0.1,
    'lambda': 1.0,
    'alpha': 0.0,
    'objective': 'reg:squarederror',
    'eval_metric': 'rmse',
    'tree_method': 'hist',
    'verbosity': 1
}

# Feature groups
macro_features = [
    'cpi_inflation', 'core_cpi_inflation', 'pce_inflation', 'core_pce_inflation',
    'breakeven_inflation_rate', 'realized_inflation', 'us_10y_bond_rate',
    'retail_sales', 'consumer_confidence_index', 'nfp', 'unemployment_rate',
    'effective_federal_funds_rate'
]

sub_techs = [
    'volatility_5d', 'volatility_10d', 'volatility_20d', 'volatility_40d',
    'momentum_14d', 'sma_5', 'sma_20', 'sma_50', 'sma_125', 'sma_200', 'sma_200_weekly',
    'ema_5', 'ema_10', 'ema_20', 'ema_50', 'ema_125', 'ema_200'
]
subsector_features = [f"sub_{f}" for f in sub_techs]

core_stock_features = [
    'open', 'high', 'low', 'close', 'volume', 'adj_close',
    'sma_5', 'sma_20', 'sma_50', 'sma_125', 'sma_200', 'sma_200_weekly',
    'ema_5', 'ema_20', 'ema_50', 'ema_125', 'ema_200',
    'macd', 'dma', 'rsi',
    'bollinger_upper', 'bollinger_middle', 'bollinger_lower', 'obv',
    'market_cap', 'market_cap_proxy', 'vix_close', 'symbol_id'
]

FEATURES = macro_features + subsector_features + core_stock_features

def normalize_subsector_name(name):
    return re.sub(r'\W+', '_', name.strip()).lower()

def fetch_subsector_ids_and_names():
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT DISTINCT subsector_id, subsector
        FROM stock_market_table
        WHERE subsector_id IS NOT NULL AND subsector IS NOT NULL
        ORDER BY subsector_id
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df.to_dict(orient="records")

def fetch_data(subsector_id):
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT *
        FROM stock_market_table
        WHERE subsector_id = %s AND future_return_1d IS NOT NULL
        ORDER BY date
    """
    df = pd.read_sql(query, conn, params=(subsector_id,))
    conn.close()
    return df

def preprocess(df):
    df = df.sort_values("date").copy()
    df['symbol_id'] = 100  # dummy ID

    missing = [col for col in FEATURES + ['future_return_1d'] if col not in df.columns]
    if missing:
        raise ValueError(f"Missing required columns: {missing}")

    df[FEATURES + ['future_return_1d']] = df[FEATURES + ['future_return_1d']].apply(pd.to_numeric, errors='coerce')
    df = df.dropna(subset=FEATURES + ['future_return_1d'])

    if df.empty:
        raise ValueError("No valid data after preprocessing.")

    X = df[FEATURES]
    y = df['future_return_1d']
    return X, y

def train_model(X, y, max_retries=50, early_stop_window=10):
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.3, shuffle=False)

    best_rmse = float("inf")
    best_model = None
    no_improve_count = 0

    for i in range(1, max_retries + 1):
        model = xgb.XGBRegressor(**xgb_params)
        model.fit(X_train, y_train)
        preds = model.predict(X_val)
        rmse = mean_squared_error(y_val, preds, squared=False)

        if rmse < best_rmse:
            best_rmse = rmse
            best_model = model
            no_improve_count = 0
        else:
            no_improve_count += 1

        if no_improve_count >= early_stop_window:
            break

    return best_model, best_rmse

def save_model(model, subsector_name):
    model_dir = "models"
    os.makedirs(model_dir, exist_ok=True)
    normalized = normalize_subsector_name(subsector_name)
    file_path = os.path.join(model_dir, f"xgb_model_{normalized}.joblib")
    joblib.dump(model, file_path)
    print(f"✅ Model saved: {file_path}")

def clear_resources():
    gc.collect()
    xgb.Booster().free()

def train_all_subsector_models():
    print("🔍 Fetching subsector IDs and names...")
    subsectors = fetch_subsector_ids_and_names()
    print(f"📊 Found {len(subsectors)} subsectors.")

    summary = []

    for sub in subsectors:
        sid = sub["subsector_id"]
        name = sub["subsector"]
        print(f"\n📈 Training: {name} (ID: {sid})")

        try:
            df = fetch_data(sid)
            if df.empty:
                print(f"⚠️ No data for {name}, skipping.")
                continue

            X, y = preprocess(df)
            model, rmse = train_model(X, y)
            save_model(model, name)
            summary.append({"subsector_id": sid, "subsector": name, "rmse": round(rmse, 5)})

        except Exception as e:
            print(f"❌ Error training {name} (ID: {sid}): {e}")

        finally:
            clear_resources()

    # Final summary table
    if summary:
        print("\n📊 Training Summary:")
        print(pd.DataFrame(summary).sort_values(by="rmse"))
    else:
        print("⚠️ No models were successfully trained.")

if __name__ == "__main__":
    train_all_subsector_models()
