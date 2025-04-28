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
        print("No subsector data found.")
        return

    df = pd.DataFrame(rows, columns=["id", "subsector", "date", "index_value"])

    updates = []

    for subsector, group in df.groupby("subsector"):
        group = group.sort_values("date").reset_index(drop=True)
        group["volatility_5d"] = calculate_rolling_std(group["index_value"], 5)
        group["volatility_10d"] = calculate_rolling_std(group["index_value"], 10)
        group["volatility_20d"] = calculate_rolling_std(group["index_value"], 20)
        group["volatility_40d"] = calculate_rolling_std(group["index_value"], 40)

        group["sma_5"] = calculate_sma(group["index_value"], 5)
        group["sma_10"] = calculate_sma(group["index_value"], 10)
        group["sma_20"] = calculate_sma(group["index_value"], 20)
        group["sma_40"] = calculate_sma(group["index_value"], 40)

        for _, row in group.iterrows():
            updates.append((
                row["volatility_5d"], row["volatility_10d"], row["volatility_20d"], row["volatility_40d"],
                row["sma_5"], row["sma_10"], row["sma_20"], row["sma_40"],
                row["id"]
            ))

            print(f"✅ {subsector} - {row['date']}: Volatility5d={row['volatility_5d']}, SMA5={row['sma_5']}")

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
            sma_125 = %s
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
