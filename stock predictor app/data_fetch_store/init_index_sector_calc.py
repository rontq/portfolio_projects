import psycopg2
from collections import defaultdict
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTORS

def calculate_sector_indexes():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for sector in SECTORS:
        print(f"\U0001F4CA Processing sector: {sector}")

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
        first_valid_price = {}
        proxy_cap_baseline = {}
        prices_by_symbol = defaultdict(dict)
        all_dates = set()

        for symbol, date, close, cap_proxy, volume, future_ret in rows:
            data_by_date[date].append((symbol, close, cap_proxy, volume, future_ret))
            all_dates.add(date)
            prices_by_symbol[symbol][date] = close

            if symbol not in first_valid_price and close:
                first_valid_price[symbol] = close
            if symbol not in proxy_cap_baseline and cap_proxy:
                proxy_cap_baseline[symbol] = cap_proxy

        total_baseline_cap = sum(proxy_cap_baseline.values())
        if total_baseline_cap == 0:
            print(f"⚠️ Skipping {sector}: baseline market cap is zero.")
            continue

        weights = {
            symbol: cap / total_baseline_cap
            for symbol, cap in proxy_cap_baseline.items()
        }

        sorted_dates = sorted(all_dates)
        previous_index = None

        for i, date in enumerate(sorted_dates):
            daily_data = data_by_date[date]
            index_val = 0
            daily_weights_used = 0
            total_volume = 0
            total_return = 0
            weighted_return = 0
            tickers_used_today = set()

            for symbol, close, cap_proxy, volume, future_ret in daily_data:
                base_price = first_valid_price.get(symbol)
                if symbol in weights and base_price and close:
                    ratio = close / base_price
                    index_val += weights[symbol] * ratio
                    daily_weights_used += weights[symbol]
                    tickers_used_today.add(symbol)

                    if future_ret is not None:
                        weighted_return += weights[symbol] * future_ret
                        total_return += future_ret

                    total_volume += volume or 0

            final_index_value = round((index_val / daily_weights_used) * 1000, 2) if daily_weights_used else None
            constituent_count = len(tickers_used_today)

            return_vs_previous = (
                round(((final_index_value - previous_index) / previous_index) * 100, 2)
                if previous_index and final_index_value else None
            )
            weighted_ret = round(weighted_return, 5) if constituent_count else None
            avg_ret = round(total_return / constituent_count, 5) if constituent_count else None

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
                avg_ret, weighted_ret, return_vs_previous,
                constituent_count
            ))

            print(f"✅ {sector} - {date}: Index = {final_index_value}, Return = {return_vs_previous}%, Tickers Used = {constituent_count}")
            previous_index = final_index_value

        conn.commit()

    cur.close()
    conn.close()
    print("\U0001F3C1 Sector index calculation completed.")

if __name__ == "__main__":
    if test_database_connection():
        calculate_sector_indexes()
    else:
        print("❌ Database connection failed.")
