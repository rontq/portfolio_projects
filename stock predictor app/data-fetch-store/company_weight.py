import os
import psycopg2
import pandas as pd
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

def calculate_and_update_weights():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print("⏳ Fetching stock data from database...")
    cur.execute("""
        SELECT id, symbol, date, sector, subsector, market_cap, market_cap_proxy
        FROM stock_market_table
        WHERE market_cap_proxy IS NOT NULL OR market_cap IS NOT NULL
    """)

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    print("✅ Data loaded. Calculating synthetic caps and weights...")
    df["synthetic_cap"] = 0.3 * df["market_cap"].fillna(0) + 0.7 * df["market_cap_proxy"].fillna(0)

    df_grouped_sub = df.groupby(["date", "subsector"])['synthetic_cap'].transform('sum')
    df_grouped_sec = df.groupby(["date", "sector"])['synthetic_cap'].transform('sum')

    df["subsector_weight"] = df["synthetic_cap"] / df_grouped_sub.replace(0, pd.NA)
    df["sector_weight"] = df["synthetic_cap"] / df_grouped_sec.replace(0, pd.NA)

    df["company_sector_influence"] = df["sector_weight"]
    df["company_subsector_influence"] = df["subsector_weight"]

    print("✉️ Updating weights and influence in the database...")
    updates = 0
    for _, row in df.iterrows():
        cur.execute("""
            UPDATE stock_market_table
            SET sector_weight = %s, 
                subsector_weight = %s
            WHERE id = %s
        """, (
            row["company_sector_influence"], 
            row["company_subsector_influence"], 
            row["id"]
        ))

        print(f"✅ {row['symbol']} - {row['date']}: Sector Weight = {row['company_sector_influence']:.6f}, Subsector Weight = {row['company_subsector_influence']:.6f}")

        updates += 1
        if updates % 10000 == 0:
            print(f"{updates} rows updated...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"📈 Sector and subsector weights and company influence updated for {updates} rows.")

if __name__ == "__main__":
    calculate_and_update_weights()
