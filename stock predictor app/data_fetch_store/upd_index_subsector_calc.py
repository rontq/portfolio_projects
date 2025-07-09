# updated_subsector_index_updater.py

import psycopg2
from psycopg2.extras import execute_values
from collections import defaultdict
from datetime import datetime, timedelta
import time

from db_params import DB_CONFIG, test_database_connection, get_latest_stock_date
from stock_list import SUBSECTOR_TO_SECTOR

BATCH_INSERT_THRESHOLD = 2
ROLLING_WINDOW_BUFFER = 250

def get_subsector_index_at_date(sector, subsector, date):
    with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT index_value
            FROM sector_index_table
            WHERE sector = %s AND subsector = %s AND date = %s
            LIMIT 1
        """, (sector, subsector, date))
        result = cur.fetchone()
        return result[0] if result else None

def process_subsector(subsector, start_date):
    preload_start = start_date - timedelta(days=ROLLING_WINDOW_BUFFER)
    sector_name = SUBSECTOR_TO_SECTOR[subsector]
    baseline_index = get_subsector_index_at_date(sector_name, subsector, start_date)

    if baseline_index is None:
        print(f"❌ [{subsector}] No baseline index on {start_date}. Skipping.")
        return

    print(f"📌 [{subsector}] Baseline ({start_date}): {baseline_index}")
    time.sleep(1)

    with psycopg2.connect(**DB_CONFIG) as conn, conn.cursor() as cur:
        cur.execute("""
            SELECT symbol, date, close, market_cap_proxy, volume
            FROM stock_market_table
            WHERE subsector = %s AND close IS NOT NULL AND market_cap_proxy IS NOT NULL AND date >= %s
            ORDER BY date
        """, (subsector, preload_start))
        rows = cur.fetchall()

        if not rows:
            print(f"⚠️ [{subsector}] No data from {preload_start}.")
            return

        data_by_date = defaultdict(list)
        prices_by_symbol = defaultdict(dict)
        all_dates = set()

        for symbol, date, close, cap_proxy, volume in rows:
            data_by_date[date].append((symbol, close, cap_proxy, volume))
            prices_by_symbol[symbol][date] = close
            all_dates.add(date)

        sorted_dates = sorted(all_dates)
        if not sorted_dates:
            return

        baseline_data = data_by_date.get(start_date)
        if not baseline_data:
            print(f"⚠️ [{subsector}] No baseline data on {start_date}. Skipping.")
            return

        total_cap = sum(cap for _, _, cap, _ in baseline_data if cap)
        if total_cap == 0:
            print(f"⚠️ [{subsector}] Zero baseline market cap. Skipping.")
            return

        cap_weights = {symbol: cap / total_cap for symbol, _, cap, _ in baseline_data if cap}
        symbol_set = set(cap_weights)

        insert_buffer = []
        previous_index = baseline_index

        for i, date in enumerate(sorted_dates):
            if date < start_date:
                continue

            prev_date = sorted_dates[i - 1] if i > 0 else None
            daily_data = data_by_date[date]

            index_return = total_return = weighted_return = total_volume = 0
            constituent_count = 0

            for symbol, close, _, volume in daily_data:
                if symbol not in symbol_set:
                    continue
                prev_close = prices_by_symbol[symbol].get(prev_date)
                if close and prev_close:
                    ret = (close / prev_close) - 1
                    weight = cap_weights[symbol]
                    index_return += weight * ret
                    total_return += ret
                    weighted_return += weight * ret
                    total_volume += volume or 0
                    constituent_count += 1

            final_index_value = round(previous_index * (1 + index_return), 2)
            avg_ret = round(total_return / constituent_count, 5) if constituent_count else None
            w_ret = round(weighted_return, 5) if constituent_count else None
            ret_pct = round(index_return * 100, 2) if previous_index else None
            previous_index = final_index_value

            cur.execute("""
                SELECT market_cap FROM sector_index_table
                WHERE sector = %s AND subsector IS NULL AND date = %s
            """, (sector_name, date))
            sector_cap = cur.fetchone()
            sector_cap = sector_cap[0] if sector_cap else 0

            cur.execute("""
                SELECT SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
                FROM stock_market_table
                WHERE subsector = %s AND date = %s
            """, (subsector, date))
            sub_cap = cur.fetchone()[0] or 0

            influence = round(sub_cap / sector_cap, 5) if sector_cap else None

            insert_buffer.append((
                sector_name, subsector, True, date,
                final_index_value, sub_cap, total_volume,
                constituent_count, avg_ret, w_ret, ret_pct, influence
            ))

            print(f"✅ [{date}] {subsector} | Index: {final_index_value} | Influence: {influence}")

        if insert_buffer:
            insert_query = """
                INSERT INTO sector_index_table (
                    sector, subsector, is_subsector, date,
                    index_value, market_cap, total_volume,
                    num_constituents, average_return, weighted_return,
                    return_vs_previous, influence_weight
                ) VALUES %s
                ON CONFLICT (sector, subsector, date)
                DO UPDATE SET
                    index_value = EXCLUDED.index_value,
                    market_cap = EXCLUDED.market_cap,
                    total_volume = EXCLUDED.total_volume,
                    num_constituents = EXCLUDED.num_constituents,
                    average_return = EXCLUDED.average_return,
                    weighted_return = EXCLUDED.weighted_return,
                    return_vs_previous = EXCLUDED.return_vs_previous,
                    influence_weight = EXCLUDED.influence_weight
            """
            if len(insert_buffer) >= BATCH_INSERT_THRESHOLD:
                execute_values(cur, insert_query, insert_buffer)
            else:
                for row in insert_buffer:
                    cur.execute(insert_query, row)
            conn.commit()

def main():
    if not test_database_connection():
        print("❌ DB connection failed.")
        return

    latest_date = get_latest_stock_date()
    today = datetime.today().date()

    if not latest_date:
        print("❌ No stock data found.")
        return

    start_date = latest_date + timedelta(days=1)
    while start_date.weekday() >= 5:
        start_date += timedelta(days=1)
    
    if start_date >= today:
        print(f"⛔ Aborted: start_date ({start_date}) is today or in the future.")
        return

    print(f"\n🚀 Starting subsector index update from: {start_date}\n")
    for subsector in SUBSECTOR_TO_SECTOR.keys():
        process_subsector(subsector, start_date)

if __name__ == "__main__":
    main()
