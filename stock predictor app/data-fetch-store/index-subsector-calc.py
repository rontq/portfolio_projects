import psycopg2
import os
from dotenv import load_dotenv
from collections import defaultdict, deque
import math
from psycopg2.extras import execute_values

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

SUBSECTOR_TO_SECTOR = {
    # Information Technology
    "Semiconductors": "Information Technology",
    "System Software": "Information Technology",
    "IT Services & Consulting": "Information Technology",
    "Hardware & Peripherals": "Information Technology",
    
    # Financials
    "Banks": "Financials",
    "Investment Management": "Financials",
    "Insurance": "Financials",
    "Exchanges & Financial Services": "Financials",
    
    # Healthcare
    "Pharmaceuticals": "Healthcare",
    "Healthcare Equipment": "Healthcare",
    "Healthcare Services": "Healthcare",
    "Biotech & Research": "Healthcare",
    
    # Consumer Discretionary
    "Retail": "Consumer Discretionary",
    "Automotive": "Consumer Discretionary",
    "Restaurants": "Consumer Discretionary",
    "Travel & Leisure": "Consumer Discretionary",
    
    # Industrials
    "Aerospace & Defense": "Industrials",
    "Machinery": "Industrials",
    "Transportation": "Industrials",
    "Construction & Engineering": "Industrials",
    
    # Consumer Staples
    "Food & Beverage": "Consumer Staples",
    "Retail & Distribution": "Consumer Staples",
    "Household Products": "Consumer Staples",
    "Tobacco & Alcohol": "Consumer Staples",
    
    # Communications
    "Internet Services": "Communications",
    "Media & Entertainment": "Communications",
    "Telecom": "Communications",
    "Gaming & Interactive Media": "Communications",
    
    # Utilities
    "Electric Utilities": "Utilities",
    "Gas Utilities": "Utilities",
    "Renewables": "Utilities",
    "Water Utilities": "Utilities",
    
    # Real Estate
    "REITs": "Real Estate",
    "Real Estate Services": "Real Estate",
    
    # Materials
    "Chemicals": "Materials",
    "Construction Materials": "Materials",
    "Metals & Mining": "Materials",
    "Paper & Packaging": "Materials",
    
    # Energy
    "Oil & Gas Producers": "Energy",
    "Oil & Gas Equipment & Services": "Energy",
    "Midstream & Pipelines": "Energy",
    "Renewable & Integrated Energy": "Energy"
}


def calculate_rolling_std(values, window):
    if len(values) < window:
        return None
    slice_vals = values[-window:]
    mean = sum(slice_vals) / window
    variance = sum((x - mean) ** 2 for x in slice_vals) / window
    return round(math.sqrt(variance), 5)

def calculate_ema(previous_ema, current_value, period):
    alpha = 2 / (period + 1)
    if previous_ema is None:
        return current_value  # Initial EMA is the first value
    return round((current_value - previous_ema) * alpha + previous_ema, 5)

def calculate_subsector_indexes():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    for subsector in SUBSECTOR_TO_SECTOR:
        print(f"📊 Processing subsector: {subsector}")
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
            continue

        data_by_date = defaultdict(list)
        prices_by_symbol = defaultdict(dict)
        all_dates = set()

        for symbol, date, close, cap_proxy, volume in rows:
            data_by_date[date].append((symbol, close, cap_proxy, volume))
            prices_by_symbol[symbol][date] = close
            all_dates.add(date)

        sorted_dates = sorted(all_dates)
        if not sorted_dates:
            continue

        baseline_date = sorted_dates[0]
        baseline_data = data_by_date[baseline_date]

        proxy_cap_baseline = {}
        total_baseline_cap = 0
        for symbol, close, cap_proxy, *_ in baseline_data:
            if cap_proxy:
                proxy_cap_baseline[symbol] = cap_proxy
                total_baseline_cap += cap_proxy

        if total_baseline_cap == 0:
            print(f"⚠️ Skipping {subsector}: baseline market cap is zero.")
            continue

        weights = {
            symbol: cap / total_baseline_cap
            for symbol, cap in proxy_cap_baseline.items()
        }

        index_history = []
        insert_buffer = []
        ema_values = {5: None, 20: None, 50: None, 125: None, 200: None}

        previous_index = None

        for i, date in enumerate(sorted_dates):
            daily_data = data_by_date[date]
            index_val = 0
            total_volume = 0
            total_return = 0
            weighted_return = 0
            constituent_count = 0

            prev_date = sorted_dates[i - 1] if i > 0 else None

            for symbol, close, cap_proxy, volume in daily_data:
                base_price = prices_by_symbol[symbol].get(baseline_date)
                prev_close = prices_by_symbol[symbol].get(prev_date) if prev_date else None

                if symbol in weights and base_price and close:
                    ratio = close / base_price
                    index_val += weights[symbol] * ratio

                    if prev_close:
                        ret = (close - prev_close) / prev_close
                        total_return += ret
                        weighted_return += weights[symbol] * ret
                        constituent_count += 1

                    total_volume += volume or 0

            final_index_value = round(index_val * 1000, 2)
            index_history.append(final_index_value)

            # Rolling metrics
            rolling_metrics = {
                "volatility_5d": calculate_rolling_std(index_history, 5),
                "volatility_10d": calculate_rolling_std(index_history, 10),
                "volatility_20d": calculate_rolling_std(index_history, 20),
                "volatility_40d": calculate_rolling_std(index_history, 40),
                "momentum_14d": round((final_index_value - index_history[-15]) / index_history[-15] * 100, 5) if len(index_history) >= 15 else None,
                "sma_5": round(sum(index_history[-5:]) / 5, 5) if len(index_history) >= 5 else None,
                "sma_20": round(sum(index_history[-20:]) / 20, 5) if len(index_history) >= 20 else None,
                "sma_50": round(sum(index_history[-50:]) / 50, 5) if len(index_history) >= 50 else None,
                "sma_125": round(sum(index_history[-125:]) / 125, 5) if len(index_history) >= 125 else None,
                "sma_200": round(sum(index_history[-200:]) / 200, 5) if len(index_history) >= 200 else None,
            }

            for period in [5, 20, 50, 125, 200]:
                ema_values[period] = calculate_ema(ema_values[period], final_index_value, period)

            avg_return = round(total_return / constituent_count, 5) if constituent_count else None
            weighted_ret = round(weighted_return, 5) if constituent_count else None
            return_vs_prev = round((final_index_value - previous_index) / previous_index * 100, 2) if previous_index else None
            previous_index = final_index_value

            cur.execute("""
                SELECT SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
                FROM stock_market_table
                WHERE subsector = %s AND date = %s
            """, (subsector, date))
            current_subsector_cap = cur.fetchone()[0] or 0

            cur.execute("""
                SELECT SUM(0.3 * market_cap + 0.7 * market_cap_proxy)
                FROM stock_market_table
                WHERE sector = %s AND date = %s
            """, (sector_name, date))
            current_sector_cap = cur.fetchone()[0] or 0

            influence_weight = round(current_subsector_cap / current_sector_cap, 5) if current_sector_cap else None

            insert_buffer.append((
                sector_name, subsector, True, date,
                final_index_value, current_subsector_cap, total_volume,
                constituent_count, avg_return, weighted_ret, return_vs_prev,
                rolling_metrics["volatility_5d"], rolling_metrics["volatility_10d"],
                rolling_metrics["volatility_20d"], rolling_metrics["volatility_40d"],
                rolling_metrics["momentum_14d"],
                rolling_metrics["sma_5"], rolling_metrics["sma_20"],
                rolling_metrics["sma_50"], rolling_metrics["sma_125"],
                rolling_metrics["sma_200"],
                None,  # sma_200_weekly — optional placeholder
                ema_values[5], ema_values[20], ema_values[50],
                ema_values[125], ema_values[200],
                influence_weight
            ))

            print(f"✅ {subsector} - {date}: Index = {final_index_value}, SMA20 = {rolling_metrics['sma_20']}, EMA20 = {ema_values[20]}")

        # Final batch insert
        if insert_buffer:
            execute_values(cur, """
                INSERT INTO sector_index_table (
                    sector, subsector, is_subsector, date,
                    index_value, market_cap, total_volume,
                    num_constituents, average_return, weighted_return,
                    return_vs_previous,
                    volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                    momentum_14d,
                    sma_5, sma_20, sma_50, sma_125, sma_200,
                    sma_200_weekly,
                    ema_5, ema_20, ema_50, ema_125, ema_200,
                    influence_weight
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
                    ema_20 = EXCLUDED.ema_20,
                    ema_50 = EXCLUDED.ema_50,
                    ema_125 = EXCLUDED.ema_125,
                    ema_200 = EXCLUDED.ema_200,
                    influence_weight = EXCLUDED.influence_weight
            """, insert_buffer)
            conn.commit()

    cur.close()
    conn.close()
    print("🏍️ Subsector index calculation completed with SMA/EMA/Volatility.")

def main():
    calculate_subsector_indexes()

if __name__ == "__main__":
    main()