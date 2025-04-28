import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
from dotenv import load_dotenv
from db_params import DB_CONFIG, test_database_connection

load_dotenv()

def calculate_rolling_std(series, window):
    return series.rolling(window=window).std()

def calculate_sma(series, window):
    return series.rolling(window=window).mean()

def calculate_ema(series, window):
    return series.ewm(span=window, adjust=False).mean()

def validate_columns(df, expected_columns):
    """
    Validate if expected columns exist in dataframe.
    Raise KeyError if missing.
    """
    missing = [col for col in expected_columns if col not in df.columns]
    if missing:
        raise KeyError(f"Missing columns in dataframe: {missing}")

def update_subsector_indicators():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # Fetch all subsector index data
    cur.execute("""
        SELECT id, subsector, date, index_value
        FROM sector_index_table
        WHERE is_subsector = TRUE
        ORDER BY subsector, date
    """)
    rows = cur.fetchall()

    if not rows:
        print("⚠️ No subsector data found.")
        return

    df = pd.DataFrame(rows, columns=["id", "subsector", "date", "index_value"])

    updates = []

    for subsector, group in df.groupby("subsector"):
        group = group.sort_values("date").reset_index(drop=True)

        # Volatility (rolling std)
        group["volatility_5d"] = calculate_rolling_std(group["index_value"], 5)
        group["volatility_10d"] = calculate_rolling_std(group["index_value"], 10)
        group["volatility_20d"] = calculate_rolling_std(group["index_value"], 20)
        group["volatility_40d"] = calculate_rolling_std(group["index_value"], 40)

        # SMA
        group["sma_5"] = calculate_sma(group["index_value"], 5)
        group["sma_20"] = calculate_sma(group["index_value"], 20)
        group["sma_50"] = calculate_sma(group["index_value"], 50)
        group["sma_125"] = calculate_sma(group["index_value"], 125)
        group["sma_200"] = calculate_sma(group["index_value"], 200)

        # EMA (including future-proofed higher windows)
        group["ema_5"] = calculate_ema(group["index_value"], 5)
        group["ema_10"] = calculate_ema(group["index_value"], 10)
        group["ema_20"] = calculate_ema(group["index_value"], 20)
        group["ema_40"] = calculate_ema(group["index_value"], 40)
        group["ema_50"] = calculate_ema(group["index_value"], 50)
        group["ema_125"] = calculate_ema(group["index_value"], 125)
        group["ema_200"] = calculate_ema(group["index_value"], 200)

        # Validate columns
        expected_cols = [
            "volatility_5d", "volatility_10d", "volatility_20d", "volatility_40d",
            "sma_5", "sma_20", "sma_50", "sma_125", "sma_200",
            "ema_5", "ema_10", "ema_20", "ema_50", "ema_125", "ema_200"
        ]
        validate_columns(group, expected_cols)

        for _, row in group.iterrows():
            updates.append((
                row["volatility_5d"], row["volatility_10d"], row["volatility_20d"], row["volatility_40d"],
                row["sma_5"], row["sma_20"], row["sma_50"], row["sma_125"], row["sma_200"],
                row["ema_5"], row["ema_10"], row["ema_20"], row["ema_50"], row["ema_125"], row["ema_200"],
                row["id"]
            ))

            print(f"✅ {subsector} - {row['date']}: Vol5d={row['volatility_5d']:.2f}, SMA5={row['sma_5']:.2f}, EMA5={row['ema_5']:.2f}")

    if updates:
        # Batch update
        execute_batch(cur, """
            UPDATE sector_index_table SET
                volatility_5d = %s,
                volatility_10d = %s,
                volatility_20d = %s,
                volatility_40d = %s,
                sma_5 = %s,
                sma_20 = %s,
                sma_50 = %s,
                sma_125 = %s,
                sma_200 = %s,
                ema_5 = %s,
                ema_10 = %s,
                ema_20 = %s,
                ema_50 = %s,
                ema_125 = %s,
                ema_200 = %s
            WHERE id = %s
        """, updates)

        conn.commit()

    cur.close()
    conn.close()
    print(f"\n📊 Updated {len(updates)} rows successfully.")

if __name__ == "__main__":
    if test_database_connection():
        update_subsector_indicators()
    else:
        print("❌ Failed database connection.")
