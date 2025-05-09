import psycopg2
import pandas as pd
from collections import defaultdict
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTORS

def calculate_sector_indexes():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for sector in SECTORS:
        print(f"\U0001F4CA Processing sector: {sector}")

        cur.execute("""
            SELECT symbol, date, close, market_cap, market_cap_proxy, volume, future_return_1d,
                   0.4 * market_cap + 0.6 * market_cap_proxy AS blended_cap
            FROM stock_market_table
            WHERE sector = %s AND close IS NOT NULL AND market_cap_proxy IS NOT NULL
            ORDER BY date
        """, (sector,))
        rows = cur.fetchall()

        if not rows:
            print(f"⚠️ No data for {sector}")
            continue

        df = pd.DataFrame(rows, columns=["symbol", "date", "close", "market_cap", "market_cap_proxy", "volume", "future_return_1d", "blended_cap"])
        df["date"] = pd.to_datetime(df["date"])

        first_valid_price = {}
        proxy_cap_baseline = {}
        seen_symbols = set()

        for _, row in df.iterrows():
            symbol = row["symbol"]
            if symbol not in seen_symbols:
                if pd.notna(row["close"]):
                    first_valid_price[symbol] = row["close"]
                if pd.notna(row["market_cap_proxy"]):
                    proxy_cap_baseline[symbol] = row["market_cap_proxy"]
                seen_symbols.add(symbol)

        total_baseline_cap = sum(proxy_cap_baseline.values())
        if total_baseline_cap == 0:
            print(f"⚠️ Skipping {sector}: baseline market cap is zero.")
            continue

        weights = {
            symbol: cap / total_baseline_cap
            for symbol, cap in proxy_cap_baseline.items()
        }

        grouped = df.groupby("date")
        sorted_dates = sorted(grouped.groups.keys())

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

            index_series.append((date, final_index_value))
            returns_map[date] = avg_ret

            metadata_by_date[date] = {
                "market_cap": float(daily_df["blended_cap"].sum()),
                "total_volume": float(total_volume),
                "constituents": constituent_count,
                "weighted_ret": weighted_ret,
                "return_vs_prev": return_vs_previous
            }

        index_df = pd.DataFrame(index_series, columns=["date", "index_value"]).set_index("date")
        returns = index_df["index_value"].pct_change()

        for w in [5, 10, 20, 40]:
            index_df[f"volatility_{w}d"] = returns.rolling(w).std()

        index_df["momentum_14d"] = index_df["index_value"].pct_change(14)

        for w in [5, 20, 50, 125, 200]:
            index_df[f"sma_{w}"] = index_df["index_value"].rolling(w).mean()

        index_df["sma_200_weekly"] = index_df["index_value"].rolling(1000).mean()

        for w in [5, 10, 20, 50, 125, 200]:
            index_df[f"ema_{w}"] = index_df["index_value"].ewm(span=w, adjust=False).mean()

        for date, row in index_df.iterrows():
            meta = metadata_by_date.get(date, {})

            cur.execute("""
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
                VALUES (%s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s,
                        %s,
                        %s, %s, %s, %s,
                        %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s)
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
            """, (
                sector, None, False, date,
                row["index_value"], meta.get("market_cap"), meta.get("total_volume"),
                returns_map.get(date), meta.get("weighted_ret"), meta.get("return_vs_prev"),
                meta.get("constituents"),
                *[round(row.get(f"volatility_{w}d"), 5) if pd.notna(row.get(f"volatility_{w}d")) else None for w in [5, 10, 20, 40]],
                round(row.get("momentum_14d"), 5) if pd.notna(row.get("momentum_14d")) else None,
                *[round(row.get(f"sma_{w}"), 5) if pd.notna(row.get(f"sma_{w}")) else None for w in [5, 20, 50, 125, 200]],
                round(row.get("sma_200_weekly"), 5) if pd.notna(row.get("sma_200_weekly")) else None,
                *[round(row.get(f"ema_{w}"), 5) if pd.notna(row.get(f"ema_{w}")) else None for w in [5, 10, 20, 50, 125, 200]]
            ))

            print(f"✅ {sector} - {date}: Index = {row['index_value']}")

        conn.commit()

    cur.close()
    conn.close()
    print("\U0001F3C1 Sector index calculation completed.")

if __name__ == "__main__":
    if test_database_connection():
        calculate_sector_indexes()
    else:
        print("❌ Database connection failed.")
