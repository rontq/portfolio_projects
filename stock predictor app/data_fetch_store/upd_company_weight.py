import os
import psycopg2
import pandas as pd
import time
from db_params import DB_CONFIG, test_database_connection
from datetime import datetime, timedelta
import sys
from stock_list import SECTOR_STOCKS  # 🧩 Sector-subsector-symbol mapping

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
    time.sleep(1)

    # Step 1: Build symbol map and ordering
    ordered_tickers = []
    for sector, subsectors in SECTOR_STOCKS.items():
        for subsector, tickers in subsectors.items():
            for symbol in tickers:
                ordered_tickers.append((symbol, sector, subsector))

    symbols = [symbol for symbol, _, _ in ordered_tickers]
    symbol_map = {symbol: (sector, subsector) for symbol, sector, subsector in ordered_tickers}

    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    print(f"⏳ Fetching stock data from database starting from {start_date}...")
    placeholders = ','.join(['%s'] * len(symbols))
    cur.execute(f"""
        SELECT id, symbol, date, sector, subsector, market_cap, market_cap_proxy
        FROM stock_market_table
        WHERE (market_cap_proxy IS NOT NULL OR market_cap IS NOT NULL)
          AND date >= %s
          AND symbol IN ({placeholders})
    """, (start_date, *symbols))

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        print("⚠️ No new stock data found for updating weights.")
        cur.close()
        conn.close()
        return

    print("✅ Data loaded. Overwriting sector/subsector using SECTOR_STOCKS...")
    df["sector"] = df["symbol"].map(lambda s: symbol_map.get(s, (None, None))[0])
    df["subsector"] = df["symbol"].map(lambda s: symbol_map.get(s, (None, None))[1])

    print("🧮 Calculating synthetic caps and weights...")
    df["synthetic_cap"] = 0.3 * df["market_cap"].fillna(0) + 0.7 * df["market_cap_proxy"].fillna(0)

    df_grouped_sub = df.groupby(["date", "subsector"])['synthetic_cap'].transform('sum')
    df_grouped_sec = df.groupby(["date", "sector"])['synthetic_cap'].transform('sum')

    df["subsector_weight"] = df["synthetic_cap"] / df_grouped_sub.replace(0, pd.NA)
    df["sector_weight"] = df["synthetic_cap"] / df_grouped_sec.replace(0, pd.NA)

    # Force order to match SECTOR_STOCKS
    df["symbol_order"] = df["symbol"].apply(lambda s: symbols.index(s))
    df.sort_values(by=["symbol_order", "date"], inplace=True)

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
    print(f"📈 Sector and subsector weights updated for {updates} rows.")

def main(force_update: bool = False, start_date: datetime.date = None):
    if not test_database_connection():
        print("❌ Failed database connection.")
        return

    latest_date = get_latest_stock_date()
    today = datetime.today().date()

    if latest_date is None:
        print("❌ No existing stock data found! Cannot proceed with updating.")
        return

    if start_date is None:
        if latest_date >= today:
            print(f"⚠️ Latest date in DB ({latest_date}) is up to or after today ({today})")

            if force_update:
                fallback = today - timedelta(days=1)
                while fallback.weekday() >= 5:
                    fallback -= timedelta(days=1)
                start_date = fallback
                print(f"⏩ Forced update fallback to: {start_date}")
            else:
                start_date_input = input("Enter start date (YYYY-MM-DD) or press Enter to fallback to yesterday: ").strip()
                if start_date_input == "":
                    fallback = today - timedelta(days=1)
                    while fallback.weekday() >= 5:
                        fallback -= timedelta(days=1)
                    start_date = fallback
                    print(f"⏩ Fallback to last weekday: {start_date}")
                else:
                    try:
                        start_date = datetime.strptime(start_date_input, "%Y-%m-%d").date()
                    except ValueError:
                        print("❌ Invalid date format.")
                        return
        else:
            start_date = latest_date + timedelta(days=1)

    calculate_and_update_weights(start_date)

if __name__ == "__main__":
    force = "--force" in sys.argv
    date_arg = None
    for arg in sys.argv[1:]:
        if arg != "--force":
            try:
                date_arg = datetime.strptime(arg, "%Y-%m-%d").date()
            except ValueError:
                pass

    main(force_update=force, start_date=date_arg)
