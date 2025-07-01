import joblib
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import xgboost as xgb
from datetime import datetime, timedelta
from company_xgb import fetch_company_data, preprocess, input_params, xgb_params

def plot_candlestick_with_returns(symbol, model_path):
    # Load model
    model = joblib.load(model_path)

    # Fetch & preprocess data
    df = fetch_company_data(symbol)
    if df.empty:
        print(f"No data for {symbol}")
        return

    X, y = preprocess(df, input_params["features"], input_params["target_column"])
    dmatrix = xgb.DMatrix(X)
    preds = model.predict(dmatrix)

    df_trimmed = df.iloc[-len(X):].copy()
    df_trimmed["actual_return"] = y.values
    df_trimmed["predicted_return"] = preds
    df_trimmed["date"] = pd.to_datetime(df_trimmed["date"])

    # Filter to last year
    one_year_ago = datetime.now() - timedelta(days=180)
    df_trimmed = df_trimmed[df_trimmed["date"] >= one_year_ago].copy()

    # Plotting
    fig, ax1 = plt.subplots(figsize=(15, 8))

    candle_width = 0.6
    wick_width = 0.1
    dates = pd.to_datetime(df_trimmed["date"]).dt.date
    date_range = range(len(dates))
    df_trimmed["index"] = date_range

    for idx, row in df_trimmed.iterrows():
        color = "green" if row["close"] >= row["open"] else "red"
        # Body
        ax1.add_patch(mpatches.Rectangle(
            (row["index"] - candle_width / 2, min(row["open"], row["close"])),
            candle_width,
            abs(row["close"] - row["open"]),
            color=color
        ))
        # Wick
        ax1.vlines(
            x=row["index"],
            ymin=row["low"],
            ymax=row["high"],
            color=color,
            linewidth=1
        )

    ax1.set_xticks(df_trimmed["index"][::20])
    ax1.set_xticklabels(df_trimmed["date"].dt.strftime('%Y-%m-%d')[::20], rotation=45)
    ax1.set_ylabel("Price (USD)")
    ax1.set_title(f"{symbol} Candlestick + 1-Day Predicted vs Actual Return")

    # Plot returns on secondary axis
    ax2 = ax1.twinx()
    ax2.plot(df_trimmed["index"], df_trimmed["actual_return"], label="Actual Return", color="blue", alpha=0.4)
    ax2.plot(df_trimmed["index"], df_trimmed["predicted_return"], label="Predicted Return", color="orange", alpha=0.6)
    ax2.set_ylabel("1-Day Return")

    # Legend and layout
    ax2.legend(loc="upper left")
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    model_file = "trained_company_models/xgb_model_wmt_20250515_205858.joblib"
    plot_candlestick_with_returns("AAPL", model_file)
