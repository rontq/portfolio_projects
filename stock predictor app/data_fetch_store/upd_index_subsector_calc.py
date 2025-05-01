import psycopg2
import time
from collections import defaultdict
from psycopg2.extras import execute_values
from db_params import DB_CONFIG, test_database_connection
from stock_list import SUBSECTOR_TO_SECTOR
from datetime import datetime, timedelta
import sys

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_latest_stock_date():
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(date) FROM stock_market_table;")
        result = cur.fetchone()
        return result[0] if result and result[0] else None
    finally:
        cur.close()
        conn.close()

def get_subsector_index_at_date(sector, subsector, date):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT index_value
            FROM sector_index_table
            WHERE sector = %s AND subsector = %s AND date = %s
            LIMIT 1
        """, (sector, subsector, date))
        result = cur.fetchone()
        return result[0] if result else None
    finally:
        cur.close()
        conn.close()

def process_subsector(subsector, start_date):
    conn = get_db_connection()
    cur = conn.cursor()
    sector_name = SUBSECTOR_TO_SECTOR[subsector]

    baseline_index = get_subsector_index_at_date(sector_name, subsector, start_date)
    if baseline_index is None:
        print(f"❌ No baseline index found for {subsector} on {start_date}. Skipping.")
        cur.close()
        conn.close()
        return

    print(f"📌 Starting baseline for {subsector} on {start_date}: {baseline_index}")
    time.sleep(1)

    cur.execute("""
        SELECT symbol, date, close, market_cap_proxy, volume
        FROM stock_market_table
        WHERE subsector = %s AND close IS NOT NULL AND market_cap_proxy IS NOT NULL AND date >= %s
        ORDER BY date
    """, (subsector, start_date))
    rows = cur.fetchall()

    if not rows:
        print(f"⚠️ No data for {subsector} from {start_date}")
        cur.close()
        conn.close()
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
        cur.close()
        conn.close()
        return

    baseline_data = data_by_date[sorted_dates[0]]
    proxy_cap_baseline = {}
    total_baseline_cap = 0
    for symbol, close, cap_proxy, *_ in baseline_data:
        if cap_proxy:
            proxy_cap_baseline[symbol] = cap_proxy
            total_baseline_cap += cap_proxy

    if total_baseline_cap == 0:
        print(f"⚠️ Skipping {subsector}: baseline market cap is zero.")
        cur.close()
        conn.close()
        return

    weights = {symbol: cap / total_baseline_cap for symbol, cap in proxy_cap_baseline.items()}

    insert_buffer = []
    previous_index = baseline_index

    for i, date in enumerate(sorted_dates):
        if date == start_date:
            continue

        daily_data = data_by_date[date]
        index_return = total_return = weighted_return = total_volume = 0
        constituent_count = 0
        prev_date = sorted_dates[i - 1] if i > 0 else None

        for symbol, close, cap_proxy, volume in daily_data:
            prev_close = prices_by_symbol[symbol].get(prev_date) if prev_date else None
            if symbol in weights and prev_close and close:
                daily_ret = (close / prev_close) - 1
                weight = weights.get(symbol)
                index_return += weight * daily_ret
                total_return += daily_ret
                weighted_return += weight * daily_ret
                constituent_count += 1
                total_volume += volume or 0

        final_index_value = round(previous_index * (1 + index_return), 2)
        avg_return = round(total_return / constituent_count, 5) if constituent_count else None
        weighted_ret = round(weighted_return, 5) if constituent_count else None
        return_vs_prev = round(index_return * 100, 2) if previous_index else None
        previous_index = final_index_value

        cur.execute("""
            SELECT market_cap
            FROM sector_index_table
            WHERE sector = %s AND subsector IS NULL AND date = %s
        """, (sector_name, date))
        sector_cap_result = cur.fetchone()
        current_sector_cap = sector_cap_result[0] if sector_cap_result else 0

        cur.execute("""
            SELECT SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
            FROM stock_market_table
            WHERE subsector = %s AND date = %s
        """, (subsector, date))
        current_subsector_cap = cur.fetchone()[0] or 0

        influence_weight = round(current_subsector_cap / current_sector_cap, 5) if current_sector_cap else None

        insert_buffer.append((
            sector_name, subsector, True, date,
            final_index_value, current_subsector_cap, total_volume,
            constituent_count, avg_return, weighted_ret, return_vs_prev,
            influence_weight
        ))

        print(f"✅ {date} | {subsector}: Index = {final_index_value}, Influence Weight = {influence_weight}")

    if insert_buffer:
        execute_values(cur, """
            INSERT INTO sector_index_table (
                sector, subsector, is_subsector, date,
                index_value, market_cap, total_volume,
                num_constituents, average_return, weighted_return,
                return_vs_previous, influence_weight
            )
            VALUES %s
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
        """, insert_buffer)
        conn.commit()

    cur.close()
    conn.close()

def main(force_update=False, start_date=None):
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

    print(f"⏩ Starting subsector index calculation from {start_date}")
    for subsector in SUBSECTOR_TO_SECTOR.keys():
        process_subsector(subsector, start_date)

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