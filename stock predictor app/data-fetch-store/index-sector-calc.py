import psycopg2
import os
from dotenv import load_dotenv
from collections import defaultdict

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

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

def calculate_sector_indexes():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for sector in SECTORS:
        print(f"\U0001f4ca Processing sector: {sector}")

        # Step 1: Load all daily stock data with market_cap_proxy
        cur.execute(""" 
            SELECT symbol, date, close, market_cap_proxy, volume, future_return_1d
            FROM stock_market_table
            WHERE sector = %s AND close IS NOT NULL AND market_cap_proxy IS NOT NULL
            ORDER BY date
        """, (sector,))
        rows = cur.fetchall()

        if not rows:
            print(f"⚠️ No data for {sector}")
            continue

        data_by_date = defaultdict(list)
        prices_by_symbol = defaultdict(dict)
        proxy_cap_baseline = {}
        all_dates = set()

        for symbol, date, close, cap_proxy, volume, future_ret in rows:
            data_by_date[date].append((symbol, close, cap_proxy, volume, future_ret))
            prices_by_symbol[symbol][date] = close
            all_dates.add(date)

        sorted_dates = sorted(all_dates)
        baseline_date = sorted_dates[0]
        baseline_data = data_by_date[baseline_date]

        total_baseline_cap = 0
        for symbol, close, cap_proxy, *_ in baseline_data:
            if cap_proxy:
                proxy_cap_baseline[symbol] = cap_proxy
                total_baseline_cap += cap_proxy

        if total_baseline_cap == 0:
            print(f"⚠️ Skipping {sector}: baseline market cap is zero.")
            continue

        weights = {
            symbol: cap / total_baseline_cap
            for symbol, cap in proxy_cap_baseline.items()
        }

        previous_index = None

        for date in sorted_dates:
            daily_data = data_by_date[date]
            index_val = 0
            total_volume = 0
            total_return = 0
            weighted_return = 0
            constituent_count = 0

            for symbol, close, cap_proxy, volume, future_ret in daily_data:
                base_price = prices_by_symbol[symbol].get(baseline_date)
                if symbol in weights and base_price and close:
                    ratio = close / base_price
                    index_val += weights[symbol] * ratio

                    if future_ret is not None:
                        weighted_return += weights[symbol] * future_ret
                        total_return += future_ret
                        constituent_count += 1

                    total_volume += volume or 0

            final_index_value = round(index_val * 1000, 2)

            return_vs_previous = None
            if previous_index is not None:
                return_vs_previous = round(((final_index_value - previous_index) / previous_index) * 100, 2)

            weighted_ret = round(weighted_return, 5) if constituent_count else None

            cur.execute(""" 
                SELECT 
                SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
                FROM stock_market_table
                WHERE sector = %s AND date = %s
                """, (sector, date))
            cap_result = cur.fetchone()
            current_cap = cap_result[0] if cap_result and cap_result[0] else None

            cur.execute(""" 
                INSERT INTO sector_index_table (
                    sector, subsector, is_subsector, date,
                    index_value, market_cap, total_volume,
                    average_return, weighted_return, return_vs_previous,
                    num_constituents
                )
                VALUES (%s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s)
                ON CONFLICT (sector, subsector, date)
                DO UPDATE SET
                    index_value = EXCLUDED.index_value,
                    market_cap = EXCLUDED.market_cap,
                    total_volume = EXCLUDED.total_volume,
                    average_return = EXCLUDED.average_return,
                    weighted_return = EXCLUDED.weighted_return,
                    return_vs_previous = EXCLUDED.return_vs_previous,
                    num_constituents = EXCLUDED.num_constituents
            """, (
                sector, None, False, date,
                final_index_value, current_cap, total_volume,
                return_vs_previous, weighted_ret, return_vs_previous,
                constituent_count
            ))

            print(f"✅ {sector} - {date}: Index = {final_index_value}, Return = {return_vs_previous}%")
            previous_index = final_index_value

        conn.commit()

    cur.close()
    conn.close()
    print("\U0001f3c1 Sector index calculation completed.")


if __name__ == "__main__":
    calculate_sector_indexes()
