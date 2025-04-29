import os
import psycopg2
import pandas as pd
import time
from dotenv import load_dotenv
from db_params import DB_CONFIG, test_database_connection
from datetime import datetime, timedelta

load_dotenv()

def get_latest_stock_date():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(date) FROM stock_market_table;")
        result = cur.fetchone()
        return result[0] if result and result[0] else None
    finally:
        cur.close()
        conn.close()

def calculate_and_update_weights(start_date):
    print(f"📌 Starting weight update for company records on {start_date}")
    time.sleep(1)  # Pause before calculating

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print(f"⏳ Fetching stock data from database starting from {start_date}...")
    cur.execute("""
        SELECT id, symbol, date, sector, subsector, market_cap, market_cap_proxy
        FROM stock_market_table
        WHERE (market_cap_proxy IS NOT NULL OR market_cap IS NOT NULL)
          AND date >= %s
    """, (start_date,))

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        print("⚠️ No new stock data found for updating weights.")
        cur.close()
        conn.close()
        return

    print("✅ Data loaded. Calculating synthetic caps and weights...")
    df["synthetic_cap"] = 0.3 * df["market_cap"].fillna(0) + 0.7 * df["market_cap_proxy"].fillna(0)

    df_grouped_sub = df.groupby(["date", "subsector"])['synthetic_cap'].transform('sum')
    df_grouped_sec = df.groupby(["date", "sector"])['synthetic_cap'].transform('sum')

    df["subsector_weight"] = df["synthetic_cap"] / df_grouped_sub.replace(0, pd.NA)
    df["sector_weight"] = df["synthetic_cap"] / df_grouped_sec.replace(0, pd.NA)

    updates = 0

    print("✉️ Updating weights and influence in the database...")
    for _, row in df.iterrows():
        cur.execute("""
            UPDATE stock_market_table
            SET sector_weight = %s, 
                subsector_weight = %s
            WHERE id = %s
        """, (
            row["sector_weight"], 
            row["subsector_weight"], 
            row["id"]
        ))

        print(f"✅ {row['symbol']} - {row['date']}: Sector Weight = {row['sector_weight']:.6f}, Subsector Weight = {row['subsector_weight']:.6f}")

        updates += 1
        if updates % 10000 == 0:
            print(f"{updates} rows updated...")

    conn.commit()
    cur.close()
    conn.close()
    print(f"\n📈 Sector and subsector weights updated for {updates} rows.")

def calculate_company_weight():
    if test_database_connection():
        latest_date = get_latest_stock_date()
        today = datetime.today().date()

        if latest_date is None:
            print("❌ No existing stock data found! Cannot proceed with updating.")
            return

        if latest_date >= today:
            print(f"⚠️ Latest date in database ({latest_date}) is up to today ({today}). No automatic updates possible.")
            start_date_input = input("Please manually enter a start date in format YYYY-MM-DD (or press Enter to fallback to yesterday): ")

            if start_date_input.strip() == "":
                fallback_start_date = today - timedelta(days=1)
                print(f"⏩ No manual date provided. Falling back to yesterday: {fallback_start_date}")
                start_date = fallback_start_date
            else:
                try:
                    start_date = datetime.strptime(start_date_input.strip(), "%Y-%m-%d").date()
                except ValueError:
                    print("❌ Invalid date format. Please use YYYY-MM-DD format.")
                    return

            calculate_and_update_weights(start_date)
        else:
            start_date = latest_date + timedelta(days=1)
            calculate_and_update_weights(start_date)
    else:
        print("❌ Failed database connection.")

if __name__ == "__main__":
    calculate_company_weight()
