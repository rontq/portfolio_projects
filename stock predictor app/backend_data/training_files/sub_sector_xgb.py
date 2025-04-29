import xgboost as xgb
import pandas as pd
import psycopg2
import os
import joblib
from datetime import datetime
from sklearn.model_selection import train_test_split
from data_fetch_store.db_params import DB_CONFIG

# ---------------------------
# XGBoost Parameters
# ---------------------------

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

# Boosting rounds (epochs-like)
num_boost_round = 500
early_stopping_rounds = 25

# Model storage path
MODEL_DIR = "models"
os.makedirs(MODEL_DIR, exist_ok=True)

# ---------------------------
# Load Data Function
# ---------------------------

def load_subsector_data(subsector: str) -> pd.DataFrame:
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT *
        FROM stock_market_table
        WHERE subsector = %s AND future_return_1d IS NOT NULL
        ORDER BY date
    """
    df = pd.read_sql(query, conn, params=(subsector,))
    conn.close()
    return df

# ---------------------------
# Train Model Function
# ---------------------------

def train_subsector_model(subsector: str):
    df = load_subsector_data(subsector)

    if df.empty:
        raise ValueError(f"❌ No data found for subsector: {subsector}")

    # Clean old model if exists
    model_path = os.path.join(MODEL_DIR, f"xgb_model_{subsector.replace(' ', '_')}.joblib")
    if os.path.exists(model_path):
        os.remove(model_path)
        print(f"🧹 Cleared previous model cache: {model_path}")

    # Define features
    drop_cols = ['symbol', 'sector', 'subsector', 'date', 'future_return_1d']
    feature_cols = [col for col in df.columns if col not in drop_cols]

    X = df[feature_cols]
    y = df['future_return_1d']

    # Train-validation split
    X_train, X_val, y_train, y_val = train_test_split(
        X, y, test_size=0.2, random_state=42, shuffle=False
    )

    dtrain = xgb.DMatrix(X_train, label=y_train, feature_names=feature_cols)
    dvalid = xgb.DMatrix(X_val, label=y_val, feature_names=feature_cols)

    evals = [(dtrain, 'train'), (dvalid, 'eval')]

    model = xgb.train(
        params=xgb_params,
        dtrain=dtrain,
        num_boost_round=num_boost_round,
        evals=evals,
        early_stopping_rounds=early_stopping_rounds,
        verbose_eval=50
    )

    # Save model to joblib
    joblib.dump(model, model_path)
    print(f"✅ Model trained and saved to: {model_path}")

    return model, feature_cols
