import psycopg2
import time
from collections import defaultdict
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTORS
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

def get_sector_index_at_date(sector, date):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT index_value
            FROM sector_index_table
            WHERE sector = %s AND subsector IS NULL AND date = %s
            LIMIT 1
        """, (sector, date))
        result = cur.fetchone()
        return result[0] if result else None
    finally:
        cur.close()
        conn.close()

def get_previous_day_closes(sector, previous_date):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT symbol, close
            FROM stock_market_table
            WHERE sector = %s AND date = %s
        """, (sector, previous_date))
        rows = cur.fetchall()
        return {symbol: close for symbol, close in rows if close is not None}
    finally:
        cur.close()
        conn.close()

def get_baseline_weights(sector, baseline_date):
    conn = get_db_connection()
    cur = conn.cursor()
    try:
        cur.execute("""
            SELECT symbol, market_cap_proxy
            FROM stock_market_table
            WHERE sector = %s AND date = %s
        """, (sector, baseline_date))
        rows = cur.fetchall()
        caps = {symbol: cap for symbol, cap in rows if cap is not None}
        total_cap = sum(caps.values())
        return {symbol: cap / total_cap for symbol, cap in caps.items()} if total_cap > 0 else {}
    finally:
        cur.close()
        conn.close()

def calculate_sector_indexes(start_date):
    conn = get_db_connection()
    cur = conn.cursor()

    for sector in SECTORS:
        print(f"📈 Processing sector: {sector}")

        baseline_index = get_sector_index_at_date(sector, start_date)
        if baseline_index is None:
            print(f"❌ No sector index found for {sector} on {start_date}. Skipping.")
            continue

        print(f"📌 Starting baseline for {sector} on {start_date}: {baseline_index}")
        time.sleep(1)

        previous_day = start_date
        previous_closes = get_previous_day_closes(sector, previous_day)
        weights = get_baseline_weights(sector, previous_day)

        if not previous_closes or not weights:
            print(f"⚠️ Missing previous day data or weights for {sector} at {previous_day}. Skipping.")
            continue

        previous_index = baseline_index

        cur.execute("""
            SELECT symbol, date, close, market_cap_proxy, volume, future_return_1d
            FROM stock_market_table
            WHERE sector = %s AND close IS NOT NULL AND market_cap_proxy IS NOT NULL AND date > %s
            ORDER BY date
        """, (sector, start_date))
        rows = cur.fetchall()
        if not rows:
            print(f"⚠️ No new data for {sector} after {start_date}")
            continue

        data_by_date = defaultdict(list)
        for symbol, date, close, cap_proxy, volume, future_ret in rows:
            data_by_date[date].append((symbol, close, cap_proxy, volume, future_ret))

        for date in sorted(data_by_date):
            index_return = weighted_return = total_return = total_volume = 0
            constituent_count = 0
            for symbol, close, cap_proxy, volume, future_ret in data_by_date[date]:
                prev_close = previous_closes.get(symbol)
                if prev_close and prev_close > 0:
                    daily_ret = (close / prev_close) - 1
                    weight = weights.get(symbol)
                    if weight is not None:
                        index_return += weight * daily_ret
                        if future_ret is not None:
                            weighted_return += weight * future_ret
                            total_return += future_ret
                            constituent_count += 1
                        total_volume += volume or 0
                else:
                    print(f"⚠️ {symbol} missing previous close on {date}, skipped.")

            final_index_value = round(previous_index * (1 + index_return), 2)
            return_vs_previous = round(index_return * 100, 2)
            weighted_ret = round(weighted_return, 5) if constituent_count else None

            cur.execute("""
                SELECT SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
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
            for symbol, close, *_ in data_by_date[date]:
                if close is not None:
                    previous_closes[symbol] = close

        conn.commit()

    cur.close()
    conn.close()
    print("🏁 Sector index calculation completed.")

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
            print(f"⚠️ Latest date in database ({latest_date}) is up to or after today ({today})")
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

    calculate_sector_indexes(start_date)

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

