import psycopg2
import pandas as pd
from psycopg2.extras import execute_batch
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTOR_STOCKS  

def calculate_and_update_weights():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    total_updates = 0

    for sector, subsectors in SECTOR_STOCKS.items():
        print(f"\n📊 Processing sector: {sector}")
        sector_symbols = []
        symbol_map = {}

        for subsector, tickers in subsectors.items():
            for symbol in tickers:
                sector_symbols.append(symbol)
                symbol_map[symbol] = (sector, subsector)

        if not sector_symbols:
            print(f"⚠️ No symbols found for sector: {sector}")
            continue

        symbol_order_map = {symbol: i for i, symbol in enumerate(sector_symbols)}

        placeholders = ','.join(['%s'] * len(sector_symbols))
        cur.execute(f"""
            SELECT id, symbol, date, sector, subsector, market_cap, market_cap_proxy
            FROM stock_market_table
            WHERE (market_cap_proxy IS NOT NULL OR market_cap IS NOT NULL)
            AND symbol IN ({placeholders})
        """, sector_symbols)

        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description]
        df = pd.DataFrame(rows, columns=cols)

        if df.empty:
            print(f"⚠️ No matching stock data for sector: {sector}")
            continue

        df["sector"] = df["symbol"].map(lambda s: symbol_map.get(s, (None, None))[0])
        df["subsector"] = df["symbol"].map(lambda s: symbol_map.get(s, (None, None))[1])
        df["symbol_order"] = df["symbol"].map(symbol_order_map)

        df["market_cap"] = df["market_cap"].fillna(0)
        df["market_cap_proxy"] = df["market_cap_proxy"].fillna(0)
        df["synthetic_cap"] = 0.4 * df["market_cap"] + 0.6 * df["market_cap_proxy"]

        df_grouped_sub = df.groupby(["date", "subsector"])['synthetic_cap'].transform('sum')
        df_grouped_sec = df.groupby(["date", "sector"])['synthetic_cap'].transform('sum')

        df["subsector_weight"] = df["synthetic_cap"] / df_grouped_sub.replace(0, pd.NA)
        df["sector_weight"] = df["synthetic_cap"] / df_grouped_sec.replace(0, pd.NA)

        df["company_sector_influence"] = df["sector_weight"]
        df["company_subsector_influence"] = df["subsector_weight"]

        df.sort_values(by=["symbol_order", "date"], inplace=True)

        # Group by subsector for per-subsector commit
        for subsector in df["subsector"].dropna().unique():
            sub_df = df[df["subsector"] == subsector]

            update_data = [
                (
                    row["company_sector_influence"],
                    row["company_subsector_influence"],
                    row["id"]
                )
                for _, row in sub_df.iterrows()
            ]

            if not update_data:
                continue

            execute_batch(cur, """
                UPDATE stock_market_table
                SET sector_weight = %s, 
                    subsector_weight = %s
                WHERE id = %s
            """, update_data, page_size=1000)

            conn.commit()
            total_updates += len(update_data)
            print(f"📦 Committed updates for subsector: {subsector} ({len(update_data)} rows)")

    cur.close()
    conn.close()
    print(f"\n✅ All sectors processed. Total rows updated: {total_updates}")

if __name__ == "__main__":
    if test_database_connection():
        calculate_and_update_weights()
    else:
        print("❌ Failed to connect to the database.")
