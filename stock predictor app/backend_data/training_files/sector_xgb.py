import os
import sys
import gc
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from datetime import datetime
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score
from xgboost import callback, DMatrix

# === Path Setup ===
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../.."))
sys.path.append(project_root)

try:
    from db_params import DB_CONFIG
    from data_fetch_store.stock_list import SECTORS
except ModuleNotFoundError as e:
    print(f"Module not found: {e}")
    sys.exit(1)

def create_db_engine():
    db_uri = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(db_uri)

engine = create_db_engine()

xgb_params = {
    "learning_rate": 0.01,
    "max_depth": 7,
    "subsample": 0.8,
    "colsample_bytree": 0.7,
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
    "alpha": 0.1,
}

input_params = {
    "features": [
        "volatility_10d", "volatility_20d", "sma_20", "sma_125",
        "ema_10", "ema_50", "momentum_14d", "return_vs_previous",
        "us_10y_bond_rate", "cpi_inflation", "pce_inflation",
        "consumer_confidence_index", "unemployment_rate",
        "return_1d_mean", "rsi_std", "pe_ratio_mean", "volume_std",
        "top1_return", "top1_rsi", "top1_pe",
        "top2_return", "top2_rsi", "top2_pe"
    ],
    "target_column": "future_index_value"
}

def fetch_data_for_sector(sector_id):
    query = '''
        SELECT 
            idx.date, idx.sector, idx.index_value,
            idx.volatility_10d, idx.volatility_20d,
            idx.sma_20, idx.sma_125,
            idx.ema_10, idx.ema_50,
            idx.momentum_14d, idx.return_vs_previous,
            sm.us_10y_bond_rate, sm.cpi_inflation, sm.pce_inflation,
            sm.consumer_confidence_index, sm.unemployment_rate
        FROM sector_index_table idx
        JOIN (
            SELECT 
                date,
                AVG(us_10y_bond_rate) AS us_10y_bond_rate,
                AVG(cpi_inflation) AS cpi_inflation,
                AVG(pce_inflation) AS pce_inflation,
                AVG(consumer_confidence_index) AS consumer_confidence_index,
                AVG(unemployment_rate) AS unemployment_rate
            FROM stock_market_table
            WHERE sector_id = %s
            GROUP BY date
        ) sm ON idx.date = sm.date
        WHERE idx.is_subsector = false 
        AND idx.sector = (
            SELECT sector FROM stock_market_table WHERE sector_id = %s LIMIT 1
        )
        ORDER BY idx.date;
    '''
    return pd.read_sql(query, engine, params=(sector_id, sector_id))

def generate_company_distribution_features(sector_id):
    query = '''
        SELECT 
            date, sector, symbol, 
            open, high, low, close, volume, adj_close,
            rsi, pe_ratio, market_cap
        FROM stock_market_table
        WHERE sector_id = %s AND date IS NOT NULL
    '''
    df = pd.read_sql(query, engine, params=(sector_id,))
    if df.empty:
        print(f"⚠️ No company data for sector_id {sector_id}")
        return pd.DataFrame()

    # Compute 1-day return
    df.sort_values(["symbol", "date"], inplace=True)
    df["return_1d"] = df.groupby("symbol")["close"].pct_change()

    # Drop NaNs and compute market cap weights
    df.dropna(subset=["return_1d", "market_cap"], inplace=True)
    df["weight"] = df.groupby(["sector", "date"])["market_cap"].transform(lambda x: x / x.sum())

    agg_list = []

    for (sector, date), group in df.groupby(["sector", "date"]):
        record = {"sector": sector, "date": date}
        record["pe_ratio_mean"] = group["pe_ratio"].mean()
        record["rsi_std"] = group["rsi"].std()


        # Simple aggregates: OHLCV + returns
        for col in ["open", "high", "low", "close", "adj_close", "volume", "return_1d"]:
            record[f"{col}_mean"] = group[col].mean()
            record[f"{col}_std"] = group[col].std()

        # Market-cap weighted aggregates
        for col in ["rsi", "return_1d", "pe_ratio"]:
            group[f"weighted_{col}"] = group[col] * group["weight"]
            record[f"{col}_weighted_avg"] = group[f"weighted_{col}"].sum()
            

        # Participation metrics
        record["percent_positive"] = (group["return_1d"] > 0).mean()
        record["num_companies"] = len(group)

        # Top-K company signals by market cap
        top_k = group.sort_values("market_cap", ascending=False).head(5)
        for idx, row in enumerate(top_k.itertuples(), 1):
            record[f"top{idx}_return"] = row.return_1d
            record[f"top{idx}_rsi"] = row.rsi
            record[f"top{idx}_pe"] = row.pe_ratio
            record[f"top{idx}_market_cap"] = row.market_cap

        agg_list.append(record)

    return pd.DataFrame(agg_list)



def preprocess(df, features, target_col):
    df = df.sort_values("date").copy()
    df["future_index_value"] = df["index_value"].shift(-1)
    df["index_value_lag1"] = df["index_value"].shift(1)
    df["return_vs_previous_lag1"] = df["return_vs_previous"].shift(1)

    df.dropna(inplace=True)
    features = list(set(features + ["index_value_lag1", "return_vs_previous_lag1"]))

    # Validate all features exist
    missing_feats = [f for f in features if f not in df.columns]
    if missing_feats:
        raise ValueError(f"Missing features in data: {missing_feats}")

    df[features + [target_col]] = df[features + [target_col]].apply(pd.to_numeric, errors="coerce")
    df.dropna(subset=features + [target_col], inplace=True)

    return df[features].reset_index(drop=True), df[target_col].reset_index(drop=True), features

# === Training ===
def train_model(X, y, xgb_params, early_stop_window=20):
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.2, shuffle=False)
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dval = xgb.DMatrix(X_val, label=y_val)

    print("🚀 Training model...")
    model = xgb.train(
        xgb_params,
        dtrain,
        num_boost_round=1000,
        evals=[(dval, "eval")],
        early_stopping_rounds=early_stop_window,
        verbose_eval=True
    )

    preds = model.predict(dval)
    rmse = mean_squared_error(y_val, preds, squared=False)
    mae = mean_absolute_error(y_val, preds)
    r2 = r2_score(y_val, preds)
    corr = np.corrcoef(y_val, preds)[0, 1]

    print(f"\n📊 Metrics:\nRMSE={rmse:.4f}, MAE={mae:.4f}, R²={r2:.4f}, Corr={corr:.4f}")
    print(f"🧠 Validation Set Size: {len(y_val)}")

    importance = model.get_score(importance_type="gain")
    sorted_feats = sorted(importance.items(), key=lambda x: x[1], reverse=True)[:10]
    print("\n🔥 Top 10 Features by Gain:")
    for feat, val in sorted_feats:
        print(f"{feat}: {val:.2f}")

    return model, rmse, mae, r2


def save_model(model, sector_name, model_dir="trained_models"):
    os.makedirs(model_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"xgb_model_{sector_name.lower().replace(' ', '_')}_{timestamp}.joblib"
    filepath = os.path.join(model_dir, filename)
    joblib.dump(model, filepath)
    print(f"💾 Model saved: {filepath}")


def run_pipeline_for_sector(sector_id, sector_name, input_params, xgb_params):
    print(f"\n📌 Processing: {sector_name}")
    df_sector = fetch_data_for_sector(sector_id)
    if df_sector.empty or df_sector['index_value'].isnull().all():
        print(f"⚠️ Skipping {sector_name}: no sector-level data.")
        return None

    df_company_agg = generate_company_distribution_features(sector_id)
    if df_company_agg.empty:
        print(f"⚠️ Skipping {sector_name}: no company-level features.")
        return None

    df = pd.merge(df_sector, df_company_agg, on=["sector", "date"], how="inner")
    
    try:
        X, y, updated_features = preprocess(df, input_params["features"], input_params["target_column"])
    except ValueError as e:
        print(f"⚠️ Preprocessing error for {sector_name}: {e}")
        return None

    model, rmse, mae, r2 = train_model(X, y, xgb_params)
    
    save_model(model, sector_name)
    gc.collect()

    return {
        "sector": sector_name,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "n_samples": len(X),
        "n_features": X.shape[1]
    }


def train_selected_sectors(sectors, input_params, xgb_params):
    summary = []
    for name in sectors:
        df = pd.read_sql("SELECT DISTINCT sector_id FROM stock_market_table WHERE sector = %s;", engine, params=(name,))
        if df.empty:
            print(f"⚠️ No sector_id for '{name}', skipping.")
            continue

        sector_id = int(df.iloc[0]["sector_id"])
        result = run_pipeline_for_sector(sector_id, name, input_params, xgb_params)
        if result:
            summary.append(result)

    return pd.DataFrame(summary).sort_values("rmse")


if __name__ == "__main__":
    training_summary = train_selected_sectors(SECTORS, input_params, xgb_params)
    if not training_summary.empty:
        print("\n📊 Training Summary:")
        print(training_summary)
    else:
        print("⚠️ No models were trained.")
