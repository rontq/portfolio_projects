import psycopg2
import os
from dotenv import load_dotenv
from collections import defaultdict
from datetime import datetime

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

# DB connect credentials
DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

SECTORS = [
    "Information Technology",
    "Financials",
    "Healthcare",
    "Consumer Discretionary",
    "Industrials",
    "Consumer Staples",
    "Communications",
    "Utilities"
]

def ensure_unique_constraint(cur):
    cur.execute("""
        DO $$
        BEGIN
            IF NOT EXISTS (
                SELECT 1 FROM information_schema.table_constraints
                WHERE table_name = 'sector_index_table'
                AND constraint_name = 'unique_sector_date'
            ) THEN
                ALTER TABLE sector_index_table
                ADD CONSTRAINT unique_sector_date UNIQUE (sector_index, date);
            END IF;
        END $$;
    """)

def get_existing_baselines(cur):
    cur.execute("""
        SELECT sector_index, market_cap
        FROM sector_index_table
        WHERE date = (SELECT MIN(date) FROM sector_index_table);
    """)
    rows = cur.fetchall()
    return {row[0]: row[1] for row in rows}

def calculate_sector_indexes():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    ensure_unique_constraint(cur)
    conn.commit()

    for sector in SECTORS:
        sector_index_name = f"{sector} Index"
        print(f"Processing sector: {sector_index_name}")

        cur.execute("""
            SELECT symbol, date, close, market_cap
            FROM stock_market_table
            WHERE sector = %s AND close IS NOT NULL
            ORDER BY date
        """, (sector,))
        rows = cur.fetchall()

        # Organize prices and baseline prices
        prices_by_symbol = defaultdict(dict)
        market_cap_at_baseline = {}
        all_dates = set()

        for symbol, date, close, market_cap in rows:
            prices_by_symbol[symbol][date] = close
            all_dates.add(date)

        sorted_dates = sorted(all_dates)
        if not sorted_dates:
            continue
        baseline_date = sorted_dates[0]

        # Calculate weights using baseline date
        total_baseline_market_cap = 0
        for symbol in prices_by_symbol:
            base_price = prices_by_symbol[symbol].get(baseline_date)
            if base_price:
                cur.execute("""
                    SELECT market_cap FROM stock_market_table
                    WHERE symbol = %s AND date = %s
                """, (symbol, baseline_date))
                result = cur.fetchone()
                if result and result[0]:
                    market_cap_at_baseline[symbol] = result[0]
                    total_baseline_market_cap += result[0]

        if total_baseline_market_cap == 0:
            continue

        weights = {
            symbol: cap / total_baseline_market_cap
            for symbol, cap in market_cap_at_baseline.items()
        }

        # Calculate index values per date
        for date in sorted_dates:
            index_val = 0
            for symbol in weights:
                base_price = prices_by_symbol[symbol].get(baseline_date)
                current_price = prices_by_symbol[symbol].get(date)

                if base_price and current_price:
                    price_change_ratio = current_price / base_price
                    index_val += weights[symbol] * price_change_ratio

            final_index_val = round(index_val * 10000, 2)

            # Optional: store total current market cap just for DB completeness
            cur.execute("""
                SELECT SUM(market_cap)
                FROM stock_market_table
                WHERE sector = %s AND date = %s
            """, (sector, date))
            result = cur.fetchone()
            current_total_market_cap = result[0] if result and result[0] else None

            cur.execute("""
                INSERT INTO sector_index_table (sector_index, subsector_index, date, market_cap, index_val)
                VALUES (%s, %s, %s, %s, %s)
                ON CONFLICT (sector_index, date)
                DO UPDATE SET market_cap = EXCLUDED.market_cap, index_val = EXCLUDED.index_val
            """, (sector_index_name, None, date, current_total_market_cap, final_index_val))
            print(f"{sector_index_name}'s index value is {final_index_val} at {date}")

        conn.commit()
    cur.close()
    conn.close()
    print("✅ Sector indexes updated successfully.")

if __name__ == "__main__":
    calculate_sector_indexes()
