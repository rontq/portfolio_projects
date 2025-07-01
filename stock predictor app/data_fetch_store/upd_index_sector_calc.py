# updated_sector_index_updater.py

import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
import time
from collections import defaultdict
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTORS
from datetime import datetime, timedelta

BATCH_INSERT_THRESHOLD = 2
ROLLING_WINDOW_BUFFER = 250  # buffer for SMA/EMA continuity

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_latest_stock_date():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date) FROM stock_market_table;")
            result = cur.fetchone()
            return result[0] if result and result[0] else None

def get_sector_index_at_date(sector, date):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT index_value
                FROM sector_index_table
                WHERE sector = %s AND subsector IS NULL AND date = %s
                LIMIT 1
            """, (sector, date))
            result = cur.fetchone()
            return result[0] if result else None

def get_previous_day_closes_and_weights(sector, previous_date):
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT symbol, close, market_cap_proxy
                FROM stock_market_table
                WHERE sector = %s AND date = %s
            """, (sector, previous_date))
            rows = cur.fetchall()
            closes = {}
            caps = {}
            for symbol, close, cap in rows:
                if close is not None:
                    closes[symbol] = close
                if cap is not None:
                    caps[symbol] = cap
            total_cap = sum(caps.values())
            weights = {s: c / total_cap for s, c in caps.items()} if total_cap > 0 else {}
            return closes, weights

def calculate_sector_indexes(start_date):
    preload_start = start_date - timedelta(days=ROLLING_WINDOW_BUFFER)

    with get_db_connection() as conn:
        with conn.cursor() as cur:
            for sector in SECTORS:
                print(f"📈 Processing sector: {sector}")

                baseline_index = get_sector_index_at_date(sector, start_date)
                if baseline_index is None:
                    print(f"❌ No sector index found for {sector} on {start_date}. Skipping.")
                    continue

                print(f"📌 Starting baseline for {sector} on {start_date}: {baseline_index}")
                time.sleep(1)

                prev_closes, weights = get_previous_day_closes_and_weights(sector, start_date)
                if not prev_closes or not weights:
                    print(f"⚠️ Missing data for {sector} on {start_date}. Skipping.")
                    continue

                symbol_set = set(weights)
                prev_index = baseline_index

                cur.execute("""
                    SELECT symbol, date, close, market_cap_proxy, volume, future_return_1d
                    FROM stock_market_table
                    WHERE sector = %s AND close IS NOT NULL AND market_cap_proxy IS NOT NULL AND date >= %s
                    ORDER BY date
                """, (sector, preload_start))
                rows = cur.fetchall()

                if not rows:
                    print(f"⚠️ No new data for {sector} after {preload_start}")
                    continue

                data_by_date = defaultdict(list)
                for symbol, date, close, cap_proxy, volume, future_ret in rows:
                    data_by_date[date].append((symbol, close, cap_proxy, volume, future_ret))

                raw_rows = []
                for date in sorted(data_by_date):
                    index_return = weighted_return = total_return = total_volume = 0
                    constituents = 0
                    for symbol, close, _, volume, future_ret in data_by_date[date]:
                        if symbol not in symbol_set:
                            continue
                        prev_close = prev_closes.get(symbol)
                        if not prev_close or prev_close <= 0:
                            continue
                        daily_ret = (close / prev_close) - 1
                        weight = weights[symbol]
                        index_return += weight * daily_ret
                        if future_ret is not None:
                            weighted_return += weight * future_ret
                            total_return += future_ret
                            constituents += 1
                        total_volume += volume or 0

                    final_index_value = round(prev_index * (1 + index_return), 2)
                    return_vs_prev = round(index_return * 100, 2)
                    weighted_ret = round(weighted_return, 5) if constituents else None
                    avg_ret = round(total_return / constituents, 5) if constituents else None

                    cur.execute("""
                        SELECT SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
                        FROM stock_market_table
                        WHERE sector = %s AND date = %s
                    """, (sector, date))
                    cap_result = cur.fetchone()
                    market_cap = cap_result[0] if cap_result and cap_result[0] else None

                    raw_rows.append((
                        sector, None, False, date,
                        final_index_value, market_cap, total_volume,
                        avg_ret, weighted_ret, return_vs_prev, constituents
                    ))

                    prev_index = final_index_value
                    for symbol, close, *_ in data_by_date[date]:
                        if close is not None:
                            prev_closes[symbol] = close

                index_df = pd.DataFrame(raw_rows, columns=[
                    "sector", "subsector", "is_subsector", "date",
                    "index_value", "market_cap", "total_volume",
                    "average_return", "weighted_return", "return_vs_previous",
                    "num_constituents"
                ]).set_index("date")

                returns = index_df["index_value"].pct_change()

                for w in [5, 10, 20, 40]:
                    index_df[f"volatility_{w}d"] = returns.rolling(w).std()

                index_df["momentum_14d"] = index_df["index_value"].pct_change(14)

                for w in [5, 20, 50, 125, 200]:
                    index_df[f"sma_{w}"] = index_df["index_value"].rolling(w).mean()

                index_df["sma_200_weekly"] = index_df["index_value"].rolling(1000).mean()

                for w in [5, 10, 20, 50, 125, 200]:
                    index_df[f"ema_{w}"] = index_df["index_value"].ewm(span=w, adjust=False).mean()

                index_df = index_df[index_df.index >= pd.to_datetime(start_date)]

                final_rows = []
                for date, row in index_df.iterrows():
                    final_rows.append((
                        row["sector"], row["subsector"], row["is_subsector"], date,
                        row["index_value"], row["market_cap"], row["total_volume"],
                        row["average_return"], row["weighted_return"], row["return_vs_previous"],
                        row["num_constituents"],
                        *[round(row.get(f"volatility_{w}d"), 5) if pd.notna(row.get(f"volatility_{w}d")) else None for w in [5, 10, 20, 40]],
                        round(row.get("momentum_14d"), 5) if pd.notna(row.get("momentum_14d")) else None,
                        *[round(row.get(f"sma_{w}"), 5) if pd.notna(row.get(f"sma_{w}")) else None for w in [5, 20, 50, 125, 200]],
                        round(row.get("sma_200_weekly"), 5) if pd.notna(row.get("sma_200_weekly")) else None,
                        *[round(row.get(f"ema_{w}"), 5) if pd.notna(row.get(f"ema_{w}")) else None for w in [5, 10, 20, 50, 125, 200]]
                    ))

                insert_query = """
                    INSERT INTO sector_index_table (
                        sector, subsector, is_subsector, date,
                        index_value, market_cap, total_volume,
                        average_return, weighted_return, return_vs_previous,
                        num_constituents,
                        volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                        momentum_14d,
                        sma_5, sma_20, sma_50, sma_125, sma_200, sma_200_weekly,
                        ema_5, ema_10, ema_20, ema_50, ema_125, ema_200
                    )
                    VALUES %s
                    ON CONFLICT (sector, subsector, date)
                    DO UPDATE SET
                        index_value = EXCLUDED.index_value,
                        market_cap = EXCLUDED.market_cap,
                        total_volume = EXCLUDED.total_volume,
                        average_return = EXCLUDED.average_return,
                        weighted_return = EXCLUDED.weighted_return,
                        return_vs_previous = EXCLUDED.return_vs_previous,
                        num_constituents = EXCLUDED.num_constituents,
                        volatility_5d = EXCLUDED.volatility_5d,
                        volatility_10d = EXCLUDED.volatility_10d,
                        volatility_20d = EXCLUDED.volatility_20d,
                        volatility_40d = EXCLUDED.volatility_40d,
                        momentum_14d = EXCLUDED.momentum_14d,
                        sma_5 = EXCLUDED.sma_5,
                        sma_20 = EXCLUDED.sma_20,
                        sma_50 = EXCLUDED.sma_50,
                        sma_125 = EXCLUDED.sma_125,
                        sma_200 = EXCLUDED.sma_200,
                        sma_200_weekly = EXCLUDED.sma_200_weekly,
                        ema_5 = EXCLUDED.ema_5,
                        ema_10 = EXCLUDED.ema_10,
                        ema_20 = EXCLUDED.ema_20,
                        ema_50 = EXCLUDED.ema_50,
                        ema_125 = EXCLUDED.ema_125,
                        ema_200 = EXCLUDED.ema_200
                """

                if final_rows:
                    if len(final_rows) >= BATCH_INSERT_THRESHOLD:
                        execute_values(cur, insert_query, final_rows)
                    else:
                        for row in final_rows:
                            cur.execute(insert_query, row)

                conn.commit()
                print(f"✅ Completed {sector}: {len(final_rows)} days updated")

    print("🏁 Sector index calculation completed.")

def main():
    if not test_database_connection():
        print("❌ Failed database connection.")
        return

    latest_date = get_latest_stock_date()
    today = datetime.today().date()

    if latest_date is None:
        print("❌ No existing stock data found! Cannot proceed with updating.")
        return

    start_date = latest_date + timedelta(days=1)
    while start_date.weekday() >= 5:
        start_date += timedelta(days=1)

    calculate_sector_indexes(start_date)

if __name__ == "__main__":
    main()
