import psycopg2
from collections import defaultdict
from psycopg2.extras import execute_values
from db_params import DB_CONFIG, test_database_connection
from stock_list import SUBSECTOR_TO_SECTOR


def process_subsector(subsector):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    sector_name = SUBSECTOR_TO_SECTOR[subsector]

    cur.execute("""
        SELECT symbol, date, close, market_cap_proxy, volume
        FROM stock_market_table
        WHERE subsector = %s AND close IS NOT NULL AND market_cap_proxy IS NOT NULL
        ORDER BY date
    """, (subsector,))
    rows = cur.fetchall()

    if not rows:
        print(f"⚠️ No data for {subsector}")
        return

    data_by_date = defaultdict(list)
    prices_by_symbol = defaultdict(dict)
    first_price = {}
    first_cap = {}
    all_dates = set()

    for symbol, date, close, cap_proxy, volume in rows:
        data_by_date[date].append((symbol, close, cap_proxy, volume))
        prices_by_symbol[symbol][date] = close
        all_dates.add(date)

        if symbol not in first_price:
            first_price[symbol] = close
            first_cap[symbol] = cap_proxy

    sorted_dates = sorted(all_dates)
    if not sorted_dates:
        return

    baseline_cap = sum(cap for cap in first_cap.values() if cap)
    if baseline_cap == 0:
        print(f"⚠️ Skipping {subsector}: baseline market cap is zero.")
        return

    weights = {
        symbol: cap / baseline_cap
        for symbol, cap in first_cap.items() if cap
    }

    insert_buffer = []
    previous_index = None

    for i, date in enumerate(sorted_dates):
        daily_data = data_by_date[date]
        index_raw = 0
        total_volume = 0
        total_return = 0
        weighted_return = 0
        tickers_today = set()

        prev_date = sorted_dates[i - 1] if i > 0 else None

        for symbol, close, cap_proxy, volume in daily_data:
            base_price = first_price.get(symbol)
            prev_close = prices_by_symbol[symbol].get(prev_date) if prev_date else None

            if symbol in weights and base_price and close:
                ratio = close / base_price
                index_raw += weights[symbol] * ratio
                tickers_today.add(symbol)

                if prev_close:
                    daily_ret = (close - prev_close) / prev_close
                    total_return += daily_ret
                    weighted_return += weights[symbol] * daily_ret

                total_volume += volume or 0

        active_weight_sum = sum(weights[s] for s in tickers_today)
        if not tickers_today or active_weight_sum == 0:
            continue

        final_index_value = round((index_raw / active_weight_sum) * 1000, 2)
        constituent_count = len(tickers_today)
        avg_return = round(total_return / constituent_count, 5) if constituent_count else None
        weighted_ret = round(weighted_return, 5) if constituent_count else None
        return_vs_prev = (
            round((final_index_value - previous_index) / previous_index * 100, 2)
            if previous_index else None
        )
        previous_index = final_index_value

        # Get current market cap
        cur.execute("""
            SELECT SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
            FROM stock_market_table
            WHERE subsector = %s AND date = %s
        """, (subsector, date))
        current_subsector_cap = cur.fetchone()[0] or 0

        # Get sector cap for influence weight
        cur.execute("""
            SELECT market_cap
            FROM sector_index_table
            WHERE sector = %s AND subsector IS NULL AND date = %s
        """, (sector_name, date))
        sector_cap_result = cur.fetchone()
        current_sector_cap = sector_cap_result[0] if sector_cap_result else 0

        influence_weight = round(current_subsector_cap / current_sector_cap, 5) if current_sector_cap else None

        insert_buffer.append((
            sector_name, subsector, True, date,
            final_index_value, current_subsector_cap, total_volume,
            constituent_count, avg_return, weighted_ret, return_vs_prev,
            influence_weight
        ))

        print(f"✅ {date} | {subsector}: Index = {final_index_value}, Constituents = {constituent_count}, Influence = {influence_weight}")

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


def calculate_subsector_indexes():
    for subsector in SUBSECTOR_TO_SECTOR:
        process_subsector(subsector)


if __name__ == "__main__":
    if test_database_connection():
        calculate_subsector_indexes()
    else:
        print("❌ Failed database connection.")
