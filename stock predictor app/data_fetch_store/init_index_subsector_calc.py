import psycopg2
import pandas as pd
from psycopg2.extras import execute_values
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTOR_STOCKS  # {sector: {subsector: [symbols]}}
from datetime import datetime

def process_all_subsectors(cutoff_date="LATEST"):
    """
    Calculates sector and subsector indices up to a specified cutoff date.
    :param cutoff_date: 'YYYY-MM-DD' string or "LATEST" for latest DB date.
    """
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # --- Handle cutoff argument ---
    global_cutoff = None
    if cutoff_date != "LATEST":
        try:
            global_cutoff = datetime.strptime(cutoff_date, "%Y-%m-%d").date()
            print(f"📅 Global cutoff date set to {global_cutoff}")
        except ValueError:
            raise ValueError("❌ Invalid cutoff date format. Use 'YYYY-MM-DD' or 'LATEST'.")

    for sector, subsectors in SECTOR_STOCKS.items():
        symbols = [symbol for syms in subsectors.values() for symbol in syms]
        if not symbols:
            print(f"⚠️ No symbols in sector {sector}")
            continue

        print(f"\n📊 Processing sector: {sector} (Total symbols: {len(symbols)})")

        with psycopg2.connect(**DB_CONFIG) as conn:
            with conn.cursor() as cur:
                placeholders = ','.join(['%s'] * len(symbols))

                # Fetch stock-level data
                cur.execute(f"""
                    SELECT symbol, date, close, market_cap, market_cap_proxy, volume, future_return_1d, subsector
                    FROM stock_market_table
                    WHERE symbol IN ({placeholders})
                    AND close IS NOT NULL AND market_cap_proxy IS NOT NULL
                    ORDER BY date
                """, symbols)
                rows = cur.fetchall()

                if not rows:
                    print(f"⚠️ No data found for sector {sector}")
                    continue

                df = pd.DataFrame(rows, columns=[
                    "symbol", "date", "close", "market_cap",
                    "market_cap_proxy", "volume", "future_return_1d", "subsector"
                ])
                df["date"] = pd.to_datetime(df["date"]).dt.date

                # --- Apply cutoff globally ---
                if global_cutoff:
                    df = df[df["date"] <= global_cutoff]

                df["blended_cap"] = 0.4 * df["market_cap"] + 0.6 * df["market_cap_proxy"]

                # Fetch existing sector caps
                cur.execute("""
                    SELECT date, market_cap FROM sector_index_table
                    WHERE sector = %s AND subsector IS NULL AND market_cap IS NOT NULL
                """, (sector,))
                sector_caps = dict(cur.fetchall())

        # --- Process subsectors ---
        for subsector in subsectors:
            sub_df = df[df["subsector"] == subsector].copy()
            if sub_df.empty:
                print(f"⚠️ No data for subsector {subsector}")
                continue

            grouped = sub_df.groupby("date")
            sorted_dates = sorted(grouped.groups.keys())

            # ✅ Resolve cutoff dynamically
            if cutoff_date == "LATEST":
                with psycopg2.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur:
                        cur.execute("""
                            SELECT MAX(date) FROM sector_index_table
                            WHERE sector = %s AND subsector = %s
                        """, (sector, subsector))
                        last_stored = cur.fetchone()[0]
                        if last_stored:
                            sector_cutoff = last_stored
                            sorted_dates = [d for d in sorted_dates if d > sector_cutoff]
                            print(f"⏩ Resuming {subsector} after {sector_cutoff}")
                        else:
                            sector_cutoff = None
            else:
                sorted_dates = [d for d in sorted_dates if d <= global_cutoff]

            if not sorted_dates:
                print(f"⚠️ No valid dates for subsector {subsector} after cutoff")
                continue

            print(f"🔹 Calculating subsector: {subsector} (dates {sorted_dates[0]} → {sorted_dates[-1]})")

            first_valid_price = {}
            proxy_cap_baseline = {}
            seen_symbols = set()

            for _, row in sub_df.iterrows():
                symbol = row["symbol"]
                if symbol not in seen_symbols:
                    if pd.notna(row["close"]):
                        first_valid_price[symbol] = row["close"]
                    if pd.notna(row["market_cap_proxy"]):
                        proxy_cap_baseline[symbol] = row["market_cap_proxy"]
                    seen_symbols.add(symbol)

            total_baseline_cap = sum(proxy_cap_baseline.values())
            if total_baseline_cap == 0:
                print(f"⚠️ Skipping {subsector}: zero baseline cap.")
                continue

            weights = {
                symbol: cap / total_baseline_cap
                for symbol, cap in proxy_cap_baseline.items()
            }

            # --- Build index series ---
            previous_index = None
            index_series = []
            returns_map = {}
            metadata_by_date = {}

            for date in sorted_dates:
                daily_df = grouped.get_group(date)

                index_val = 0
                daily_weights_used = 0
                total_volume = 0
                total_return = 0
                weighted_return = 0
                tickers_used_today = set()

                for _, row in daily_df.iterrows():
                    symbol = row["symbol"]
                    close = row["close"]
                    base_price = first_valid_price.get(symbol)
                    if symbol in weights and pd.notna(close) and base_price:
                        ratio = close / base_price
                        index_val += weights[symbol] * ratio
                        daily_weights_used += weights[symbol]
                        tickers_used_today.add(symbol)

                        if pd.notna(row["future_return_1d"]):
                            weighted_return += weights[symbol] * row["future_return_1d"]
                            total_return += row["future_return_1d"]

                        total_volume += row["volume"] or 0

                if not tickers_used_today or daily_weights_used == 0:
                    continue

                final_index_value = round((index_val / daily_weights_used) * 1000, 2)
                constituent_count = len(tickers_used_today)

                return_vs_previous = (
                    round(((final_index_value - previous_index) / previous_index) * 100, 2)
                    if previous_index else None
                )
                weighted_ret = round(weighted_return, 5) if constituent_count else None
                avg_ret = round(total_return / constituent_count, 5) if constituent_count else None

                previous_index = final_index_value

                blended_cap = float(daily_df["blended_cap"].sum())
                sector_cap = sector_caps.get(date, 0)
                influence_weight = round(blended_cap / sector_cap, 5) if sector_cap else None

                index_series.append((date, final_index_value))
                returns_map[date] = avg_ret

                metadata_by_date[date] = {
                    "market_cap": blended_cap,
                    "total_volume": float(total_volume),
                    "constituents": constituent_count,
                    "weighted_ret": weighted_ret,
                    "return_vs_prev": return_vs_previous,
                    "influence_weight": influence_weight
                }

            # --- Build DataFrame & indicators ---
            index_df = pd.DataFrame(index_series, columns=["date", "index_value"]).set_index("date")
            returns = index_df["index_value"].pct_change()

            # Volatility windows
            for w in [5, 10, 20, 40]:
                index_df[f"volatility_{w}d"] = returns.rolling(w).std()

            # Momentum
            index_df["momentum_14d"] = index_df["index_value"].pct_change(14)

            # SMA
            for w in [5, 20, 50, 125, 200]:
                index_df[f"sma_{w}"] = index_df["index_value"].rolling(w).mean()

            # Long SMA ~ weekly proxy
            index_df["sma_200_weekly"] = index_df["index_value"].rolling(1000).mean()

            # EMA
            for w in [5, 10, 20, 50, 125, 200]:
                index_df[f"ema_{w}"] = index_df["index_value"].ewm(span=w, adjust=False).mean()

            # --- Build DB insert records ---
            insert_records = []
            for date, row in index_df.iterrows():
                meta = metadata_by_date.get(date, {})

                insert_records.append((
                    sector, subsector, True, date,
                    row["index_value"], meta.get("market_cap"), meta.get("total_volume"),
                    meta.get("constituents"),
                    returns_map.get(date), meta.get("weighted_ret"), meta.get("return_vs_prev"),
                    meta.get("influence_weight"),
                    *[round(row.get(f"volatility_{w}d"), 5) if pd.notna(row.get(f"volatility_{w}d")) else None for w in [5,10,20,40]],
                    round(row.get("momentum_14d"), 5) if pd.notna(row.get("momentum_14d")) else None,
                    *[round(row.get(f"sma_{w}"), 5) if pd.notna(row.get(f"sma_{w}")) else None for w in [5,20,50,125,200]],
                    round(row.get("sma_200_weekly"), 5) if pd.notna(row.get("sma_200_weekly")) else None,
                    *[round(row.get(f"ema_{w}"), 5) if pd.notna(row.get(f"ema_{w}")) else None for w in [5,10,20,50,125,200]]
                ))

            # --- Commit to DB ---
            if insert_records:
                with psycopg2.connect(**DB_CONFIG) as conn:
                    with conn.cursor() as cur:
                        execute_values(cur, """
                            INSERT INTO sector_index_table (
                                sector, subsector, is_subsector, date,
                                index_value, market_cap, total_volume,
                                num_constituents, average_return, weighted_return,
                                return_vs_previous, influence_weight,
                                volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                                momentum_14d,
                                sma_5, sma_20, sma_50, sma_125, sma_200, sma_200_weekly,
                                ema_5, ema_10, ema_20, ema_50, ema_125, ema_200
                            ) VALUES %s
                            ON CONFLICT (sector, subsector, date) DO UPDATE SET
                                index_value = EXCLUDED.index_value,
                                market_cap = EXCLUDED.market_cap,
                                total_volume = EXCLUDED.total_volume,
                                num_constituents = EXCLUDED.num_constituents,
                                average_return = EXCLUDED.average_return,
                                weighted_return = EXCLUDED.weighted_return,
                                return_vs_previous = EXCLUDED.return_vs_previous,
                                influence_weight = EXCLUDED.influence_weight,
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
                        """, insert_records)
                        conn.commit()
                        print(f"✅ Committed: {subsector} | Rows inserted/updated: {len(insert_records)}")

def main():
    if test_database_connection():
        # Example usage:
        # CUTOFF_DATE = "2025-08-15"   # fixed date
        CUTOFF_DATE = "LATEST"          # use latest date in DB per sector
        process_all_subsectors(cutoff_date=CUTOFF_DATE)
    else:
        print("❌ Failed database connection.")

if __name__ == "__main__":
    main()
