import psycopg2
import pandas as pd
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTOR_STOCKS  # ⬅️ Your sector/subsector-ticker mapping

def calculate_and_update_weights():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 🔁 Step 1: Flatten SECTOR_STOCKS into ordered list of (symbol, sector, subsector)
    ordered_tickers = []
    for sector, subsectors in SECTOR_STOCKS.items():
        for subsector, tickers in subsectors.items():
            for symbol in tickers:
                ordered_tickers.append((symbol, sector, subsector))

    symbols = [symbol for symbol, _, _ in ordered_tickers]
    symbol_map = {symbol: (sector, subsector) for symbol, sector, subsector in ordered_tickers}

    print("⏳ Fetching stock data from database...")
    placeholders = ','.join(['%s'] * len(symbols))
    cur.execute(f"""
        SELECT id, symbol, date, sector, subsector, market_cap, market_cap_proxy
        FROM stock_market_table
        WHERE (market_cap_proxy IS NOT NULL OR market_cap IS NOT NULL)
        AND symbol IN ({placeholders})
    """, symbols)

    rows = cur.fetchall()
    cols = [desc[0] for desc in cur.description]
    df = pd.DataFrame(rows, columns=cols)

    if df.empty:
        print("⚠️ No matching stock data found in the database.")
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

    df["company_sector_influence"] = df["sector_weight"]
    df["company_subsector_influence"] = df["subsector_weight"]

    # 🔁 Step 4: Sort to match order in SECTOR_STOCKS
    df["symbol_order"] = df["symbol"].apply(lambda s: symbols.index(s))
    df.sort_values(by=["symbol_order", "date"], inplace=True)

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
    if test_database_connection():
        calculate_and_update_weights()
