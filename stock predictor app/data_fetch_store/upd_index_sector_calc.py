import psycopg2
from psycopg2.extras import execute_values
import pandas as pd
from collections import defaultdict
from datetime import datetime, timedelta
from db_params import DB_CONFIG, test_database_connection
from stock_list import SECTORS

ROLL_BACK = 250  # Buffer window

def get_sector_index_history(sector):
    """Fetch existing sector index from DB."""
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql(
            """
            SELECT date, index_value, total_volume, market_cap,
                   volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                   momentum_14d, sma_5, sma_20, sma_50, sma_125, sma_200, sma_200_weekly,
                   ema_5, ema_10, ema_20, ema_50, ema_125, ema_200
            FROM sector_index_table
            WHERE sector = %s AND is_subsector = FALSE
            ORDER BY date
            """, conn, params=(sector,)
        )
    if df.empty:
        return None
    df['date'] = pd.to_datetime(df['date']).dt.date
    return df.set_index('date')


def get_stock_data(sector, last_date):
    """Fetch stock data from last_date - ROLL_BACK to today."""
    cutoff = last_date - timedelta(days=ROLL_BACK)
    with psycopg2.connect(**DB_CONFIG) as conn:
        df = pd.read_sql(
            """
            SELECT symbol, date, close, market_cap_proxy, volume
            FROM stock_market_table
            WHERE sector = %s AND date >= %s
              AND close IS NOT NULL AND market_cap_proxy IS NOT NULL
            ORDER BY date
            """, conn, params=(sector, cutoff)
        )
    df['date'] = pd.to_datetime(df['date']).dt.date
    stock_dict = defaultdict(list)
    for r in df.itertuples(index=False):
        stock_dict[r.date].append((r.symbol, r.close, r.market_cap_proxy, r.volume))
    return stock_dict


def update_sector(sector, cutoff_date=None):
    """Incrementally update a sector index using historical index and stock data."""
    hist = get_sector_index_history(sector)
    if hist is None or hist.empty:
        print(f"❌ No prior index data for {sector}, skipping.")
        return

    last_date = hist.index.max()
    last_index = hist.loc[last_date, 'index_value']

    # Determine the dates to calculate
    today = datetime.today().date()
    if cutoff_date:
        cutoff_date = pd.to_datetime(cutoff_date).date()
        end_date = min(today, cutoff_date)
    else:
        end_date = today

    stock_data = get_stock_data(sector, last_date + timedelta(days=1))
    trading_dates = sorted(d for d in stock_data.keys() if d > last_date and d <= end_date)

    if not trading_dates:
        print(f"⏭️ {sector}: No new trading days to process.")
        return

    working = hist.copy()

    # Precompute buffers
    last_known_prices = {}
    last_known_caps = {}

    for date in sorted(stock_data.keys()):
        daily = stock_data.get(date)
        if not daily:
            continue
        for s, close, cap_proxy, _ in daily:
            if pd.notna(close):
                last_known_prices[s] = close
            if pd.notna(cap_proxy):
                last_known_caps[s] = cap_proxy

    # Process new dates
    for date in trading_dates:
        daily = stock_data.get(date)
        if not daily:
            continue

        available_symbols = [s for s, _, _, _ in daily if s in last_known_prices and s in last_known_caps]
        if not available_symbols:
            print(f"⚠️ {sector} {date}: No symbols with valid prior prices, skipping.")
            continue

        # Compute weights
        caps = {s: last_known_caps[s] for s in available_symbols}
        total_cap = sum(caps.values())
        if total_cap == 0:
            print(f"⚠️ {sector} {date}: Zero total cap, skipping.")
            continue

        weights = {s: caps[s] / total_cap for s in available_symbols}

        # Index calc
        idx_ret = 0
        vol_sum = 0
        cap_sum = 0
        for s, close, cap_proxy, volume in daily:
            if s not in available_symbols:
                continue
            prev_close = last_known_prices[s]
            idx_ret += weights[s] * ((close / prev_close) - 1)
            cap_sum += cap_proxy or 0
            vol_sum += volume or 0

            last_known_prices[s] = close
            last_known_caps[s] = cap_proxy

        new_index = round(last_index * (1 + idx_ret), 4)

        working.loc[date, [
            'index_value', 'total_volume', 'market_cap', 'num_constituents'
        ]] = [new_index, vol_sum, cap_sum, len(available_symbols)]

        last_index = new_index
        last_date = date

    # Compute indicators
    working['return_vs_previous'] = working['index_value'].pct_change()
    returns = working['index_value'].pct_change()
    for w in [5, 10, 20, 40]:
        working[f'volatility_{w}d'] = returns.rolling(w).std()
    working['momentum_14d'] = working['index_value'].pct_change(14)
    for w in [5, 20, 50, 125, 200]:
        working[f'sma_{w}'] = working['index_value'].rolling(w).mean()
    working['sma_200_weekly'] = working['index_value'].rolling(1000).mean()
    for w in [5, 10, 20, 50, 125, 200]:
        working[f'ema_{w}'] = working['index_value'].ewm(span=w, adjust=False).mean()

    working.fillna(method="ffill", inplace=True)

    # Insert only new dates
    new_rows = working.loc[hist.index.max() + timedelta(days=1):].dropna(subset=['index_value'])
    if new_rows.empty:
        print(f"⏭️ {sector}: No new rows to insert.")
        return

    insert_values = []
    for d, row in new_rows.iterrows():
        insert_values.append([
            sector, None, False, d,
            row['index_value'], row['market_cap'], row['total_volume'],
            None, None, round(row['return_vs_previous'], 5) if pd.notna(row['return_vs_previous']) else None,
            int(row.get('num_constituents', 0)),
            *[round(row.get(f'volatility_{w}d'), 5) if pd.notna(row.get(f'volatility_{w}d')) else None for w in [5, 10, 20, 40]],
            round(row['momentum_14d'], 5) if pd.notna(row['momentum_14d']) else None,
            *[round(row.get(f'sma_{w}'), 5) if pd.notna(row.get(f'sma_{w}')) else None for w in [5, 20, 50, 125, 200]],
            round(row['sma_200_weekly'], 5) if pd.notna(row['sma_200_weekly']) else None,
            *[round(row.get(f'ema_{w}'), 5) if pd.notna(row.get(f'ema_{w}')) else None for w in [5, 10, 20, 50, 125, 200]]
        ])

    insert_sql = """
        INSERT INTO sector_index_table (
            sector, subsector, is_subsector, date,
            index_value, market_cap, total_volume,
            average_return, weighted_return, return_vs_previous,
            num_constituents,
            volatility_5d, volatility_10d, volatility_20d, volatility_40d,
            momentum_14d,
            sma_5, sma_20, sma_50, sma_125, sma_200, sma_200_weekly,
            ema_5, ema_10, ema_20, ema_50, ema_125, ema_200
        ) VALUES %s
        ON CONFLICT (sector, subsector, date) DO UPDATE SET
            index_value = EXCLUDED.index_value,
            market_cap = EXCLUDED.market_cap,
            total_volume = EXCLUDED.total_volume,
            return_vs_previous = EXCLUDED.return_vs_previous,
            num_constituents = EXCLUDED.num_constituents,
            volatility_5d = EXCLUDED.volatility_5d,
            volatility_10d = EXCLUDED.volatility_10d,
            volatility_20d = EXCLUDED.volatility_20d,
            volatility_40d = EXCLUDED.volatility_40d,
            momentum_14d = EXCLUDED.momentum_14d,
            sma_5 = EXCLUDED.sma_5, sma_20 = EXCLUDED.sma_20,
            sma_50 = EXCLUDED.sma_50, sma_125 = EXCLUDED.sma_125,
            sma_200 = EXCLUDED.sma_200, sma_200_weekly = EXCLUDED.sma_200_weekly,
            ema_5 = EXCLUDED.ema_5, ema_10 = EXCLUDED.ema_10,
            ema_20 = EXCLUDED.ema_20, ema_50 = EXCLUDED.ema_50,
            ema_125 = EXCLUDED.ema_125, ema_200 = EXCLUDED.ema_200
    """

    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            execute_values(cur, insert_sql, insert_values)
            conn.commit()

    print(f"✅ {sector}: Inserted {len(insert_values)} new row(s) with continuous sector index.")

def main():
    if not test_database_connection():
        print("❌ Cannot connect to DB.")
        return


    for sec in SECTORS:
        print(f"\n➡️ Updating {sec}...")
        update_sector(sec, cutoff_date=None)  # optionally provide a cutoff


if __name__ == "__main__":
    main()
