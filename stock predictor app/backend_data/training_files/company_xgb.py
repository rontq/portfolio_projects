import os
import sys
import gc
import argparse
import pandas as pd
import numpy as np
import joblib
import xgboost as xgb
from datetime import datetime
from sqlalchemy import create_engine
from sklearn.model_selection import train_test_split
from sklearn.metrics import mean_squared_error, mean_absolute_error, r2_score

# === Path Setup ===
current_file = os.path.abspath(__file__)
project_root = os.path.abspath(os.path.join(current_file, "../../.."))
sys.path.append(project_root)

try:
    from db_params import DB_CONFIG
except ModuleNotFoundError as e:
    print(f"Module not found: {e}")
    sys.exit(1)

# === DB Engine ===
def create_db_engine():
    db_uri = (
        f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
        f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
    )
    return create_engine(db_uri)

engine = create_db_engine()

# === Feature & Target Config ===
input_params = {
    "features": [
        "open", "high", "low", "volume", "adj_close",
        "sma_5", "sma_20", "sma_50", "sma_125", "sma_200_weekly",
        "ema_5", "ema_20", "ema_50",
        "rsi", "bollinger_upper", "bollinger_middle", "bollinger_lower", "obv",
        "pe_ratio", "forward_pe", "price_to_book",
        "volatility_5d", "volatility_10d", "volatility_20d", "volatility_40d",
        "market_cap", "market_cap_proxy", "vix_close",
        "day_of_week", "week_of_year",
        "return_1d", "return_3d", "return_5d",
        "price_vs_sma_20", "price_vs_ema_20"
    ],
    "target_column": "future_return_5d"
}

xgb_params = {
    "objective": "reg:squarederror",
    "eval_metric": "rmse",
    "learning_rate": 0.01,
    "max_depth": 5,
    "min_child_weight": 10,
    "subsample": 0.7,
    "colsample_bytree": 0.6,
    "lambda": 2,
    "alpha": 0.3,
    "gamma": 1,
    "tree_method": "hist",
    "random_state": 42,
    "n_jobs": -1,
}

# === Fetch company data ===
def fetch_company_data(symbol):
    query = '''
        SELECT date, symbol,
               open, high, low, close, volume, adj_close,
               sma_5, sma_20, sma_50, sma_125, sma_200_weekly,
               ema_5, ema_20, ema_50, ema_125,
               macd, dma, rsi,
               bollinger_upper, bollinger_middle, bollinger_lower,
               obv,
               pe_ratio, forward_pe, price_to_book,
               volatility_5d, volatility_10d, volatility_20d, volatility_40d,
               market_cap, market_cap_proxy,
               vix_close,
               day_of_week, week_of_year
        FROM stock_market_table
        WHERE symbol = %s AND date IS NOT NULL
        ORDER BY date
    '''
    return pd.read_sql(query, engine, params=(symbol,))

# === Preprocess Data ===
def preprocess(df, features, target_col):
    df = df.sort_values("date").copy()

    if target_col == "future_return_5d":
        df[target_col] = df["close"].shift(-5) / df["close"] - 1
    else:
        raise ValueError(f"Unsupported target column: {target_col}")

    df["return_1d"] = df["close"].pct_change().shift(1)
    df["return_3d"] = df["close"].pct_change(3).shift(1)
    df["return_5d"] = df["close"].pct_change(5).shift(1)
    df["price_vs_sma_20"] = df["close"] / df["sma_20"]
    df["price_vs_ema_20"] = df["close"] / df["ema_20"]

    available_features = [f for f in features if f in df.columns]
    required_columns = available_features + [target_col]

    df = df[required_columns].apply(pd.to_numeric, errors="coerce")
    df.dropna(inplace=True)

    return df[available_features], df[target_col]

# === Train Model ===
def train_model(X, y, xgb_params, early_stop_window=100):
    X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=0.3, shuffle=False)
    dtrain = xgb.DMatrix(X_train, label=y_train)
    dval = xgb.DMatrix(X_val, label=y_val)

    print("🚀 Training model...")
    model = xgb.train(
        xgb_params,
        dtrain,
        num_boost_round=3000,
        evals=[(dval, "eval")],
        early_stopping_rounds=early_stop_window,
        verbose_eval=100
    )

    preds = model.predict(dval)
    rmse = mean_squared_error(y_val, preds, squared=False)
    mae = mean_absolute_error(y_val, preds)
    r2 = r2_score(y_val, preds)
    corr = np.corrcoef(y_val, preds)[0, 1]

    return model, rmse, mae, r2, corr

# === Save Model ===
def save_model(model, symbol, model_dir="trained_company_models"):
    os.makedirs(model_dir, exist_ok=True)
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"xgb_model_{symbol.lower()}_{timestamp}.joblib"
    filepath = os.path.join(model_dir, filename)
    joblib.dump(model, filepath)
    print(f"💾 Model saved: {filepath}")

# === Run Pipeline for One Company ===
def run_pipeline_for_company(symbol, input_params, xgb_params):
    print(f"\n📌 Processing: {symbol}")
    df = fetch_company_data(symbol)
    if df.empty or df["close"].isnull().all():
        print(f"⚠️ Skipping {symbol}: no usable price data.")
        return None

    try:
        X, y = preprocess(df, input_params["features"], input_params["target_column"])
    except ValueError as e:
        print(f"⚠️ Preprocessing error for {symbol}: {e}")
        return None

    if len(X) < 30:
        print(f"⚠️ Skipping {symbol}: not enough data ({len(X)} rows).")
        return None

    model, rmse, mae, r2, corr = train_model(X, y, xgb_params)
    save_model(model, symbol)
    gc.collect()

    importance = model.get_score(importance_type="gain")
    importance_series = pd.Series(importance).sort_values(ascending=False)
    top_features = importance_series.head(10).to_dict()

    print(f"📊 {symbol} - RMSE: {rmse:.4f}, MAE: {mae:.4f}, R²: {r2:.4f}, Corr: {corr:.4f}")
    print("🔥 Top Feature Importances (by gain):")
    for f, score in top_features.items():
        print(f"   {f}: {score:.4f}")

    return {
        "symbol": symbol,
        "rmse": rmse,
        "mae": mae,
        "r2": r2,
        "corr": corr,
        "n_samples": len(X),
        "n_features": X.shape[1],
        "top_features": top_features
    }

# === Train Selected Companies ===
def train_selected_companies(symbols, input_params, xgb_params):
    summary = []
    for symbol in symbols:
        result = run_pipeline_for_company(symbol, input_params, xgb_params)
        if result:
            summary.append(result)
    return pd.DataFrame(summary).sort_values("rmse")

# === CLI Entry ===
def main():
    parser = argparse.ArgumentParser(description="Train XGBoost model for company/companies.")
    parser.add_argument("--symbol", type=str, help="Train only this symbol (e.g., AAPL)")
    args = parser.parse_args()

    if args.symbol:
        symbol = args.symbol.strip().upper()
        print(f"🔍 Training model for: {symbol}")
        run_pipeline_for_company(symbol, input_params, xgb_params)
    else:
        print("📚 No symbol provided. Training all available companies.")
        symbols_df = pd.read_sql("SELECT DISTINCT symbol FROM stock_market_table;", engine)
        symbols = symbols_df["symbol"].tolist()

        training_summary = train_selected_companies(symbols, input_params, xgb_params)
        if not training_summary.empty:
            print("\n📊 Training Summary:")
            print(training_summary[["symbol", "rmse", "mae", "r2", "corr"]])
        else:
            print("⚠️ No models were trained.")

if __name__ == "__main__":
    main()
