import os
import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from dotenv import load_dotenv
from db_params import DB_CONFIG, test_database_connection

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

def calculate_rolling_metrics():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("\u23f3 Fetching sector index data...")
    cur.execute("""
        SELECT sector, subsector, date, index_value
        FROM sector_index_table
        WHERE is_subsector = TRUE
        ORDER BY subsector, date
    """)
    rows = cur.fetchall()

    if not rows:
        print("⚠️ No subsector data found!")
        return

    df = pd.DataFrame(rows, columns=["sector", "subsector", "date", "index_value"])

    updates = []

    for (sector, subsector), group in df.groupby(["sector", "subsector"]):
        group = group.sort_values("date").reset_index(drop=True)

        group["volatility_5d"] = group["index_value"].rolling(window=5).std()
        group["volatility_10d"] = group["index_value"].rolling(window=10).std()
        group["volatility_20d"] = group["index_value"].rolling(window=20).std()
        group["volatility_40d"] = group["index_value"].rolling(window=40).std()

        group["sma_5"] = group["index_value"].rolling(window=5).mean()
        group["sma_20"] = group["index_value"].rolling(window=20).mean()
        group["sma_50"] = group["index_value"].rolling(window=50).mean()
        group["sma_125"] = group["index_value"].rolling(window=125).mean()
        group["sma_200"] = group["index_value"].rolling(window=200).mean()

        for _, row in group.iterrows():
            updates.append((
                row["volatility_5d"], row["volatility_10d"], row["volatility_20d"], row["volatility_40d"],
                row["sma_5"], row["sma_20"], row["sma_50"], row["sma_125"], row["sma_200"],
                sector, subsector, row["date"]
            ))

    if updates:
        print(f"✍️ Updating {len(updates)} subsector rolling metrics...")

        execute_values(cur, """
            UPDATE sector_index_table AS t
            SET
                volatility_5d = v.volatility_5d,
                volatility_10d = v.volatility_10d,
                volatility_20d = v.volatility_20d,
                volatility_40d = v.volatility_40d,
                sma_5 = v.sma_5,
                sma_20 = v.sma_20,
                sma_50 = v.sma_50,
                sma_125 = v.sma_125,
                sma_200 = v.sma_200
            FROM (VALUES %s) AS v(
                volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                sma_5, sma_20, sma_50, sma_125, sma_200,
                sector, subsector, date
            )
            WHERE t.sector = v.sector AND t.subsector = v.subsector AND t.date = v.date
        """, updates)


        conn.commit()

    print("✅ Subsector volatility and SMAs updated!")

    cur.close()
    conn.close()

if __name__ == "__main__":
    if test_database_connection():
        calculate_rolling_metrics()
    else:
        print("\u274c Failed DB connection.")
