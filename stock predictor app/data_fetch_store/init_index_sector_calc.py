def update_sector(sector):
    hist = get_index_history(sector)
    if hist is None:
        print(f"❌ No prior index data for {sector}, skipping.")
        return

    latest_date = hist.index.max()
    latest_index = hist.loc[latest_date, 'index_value']
    stock_data = get_stock_data(sector, latest_date + timedelta(days=1))

    working = hist.copy()
    next_day = latest_date + timedelta(days=1)
    today = datetime.today().date()

    while next_day <= today:
        if next_day.weekday() >= 5:
            next_day += timedelta(days=1)
            continue

        daily = stock_data.get(next_day)
        if not daily:
            next_day += timedelta(days=1)
            continue

        prev_day = latest_date
        prev_data = stock_data.get(prev_day)
        if not prev_data:
            print(f"⚠️ Missing stock data for {sector} on {prev_day}. Aborting.")
            break

        closes = {s: c for s, c, *_ in prev_data}
        caps = {s: cp for s, _, cp, _ in prev_data}
        total_cap = sum(caps.values())
        if total_cap == 0:
            print(f"⚠️ Skipping {sector} on {prev_day}: Zero total cap.")
            next_day += timedelta(days=1)
            continue

        weights = {s: caps[s] / total_cap for s in caps}
        idx_ret, vol, cap_sum = 0, 0, 0
        constituents_used = 0

        for s, close, cap_proxy, volume in daily:
            if s in closes and s in weights:
                idx_ret += weights[s] * ((close / closes[s]) - 1)
                cap_sum += cap_proxy or 0
                constituents_used += 1
            vol += volume or 0

        new_index = round(latest_index * (1 + idx_ret), 4)
        working.loc[next_day, ['index_value', 'total_volume', 'market_cap', 'num_constituents']] = [
            new_index, vol, cap_sum, constituents_used
        ]

        returns = working['index_value'].pct_change()
        for w in [5, 10, 20, 40]:
            working[f'volatility_{w}d'] = returns.rolling(w).std()
        working['momentum_14d'] = working['index_value'].pct_change(14)
        for w in [5, 20, 50, 125, 200]:
            working[f'sma_{w}'] = working['index_value'].rolling(w).mean()
        working['sma_200_weekly'] = working['index_value'].rolling(1000).mean()
        for w in [5, 10, 20, 50, 125, 200]:
            working[f'ema_{w}'] = working['index_value'].ewm(span=w, adjust=False).mean()

        latest_date = next_day
        latest_index = new_index
        next_day += timedelta(days=1)

    # Prepare new rows
    new_rows = working.loc[hist.index.max() + timedelta(days=1):].dropna(subset=['index_value'])
    if new_rows.empty:
        print(f"⏭️ {sector}: No new data to insert.")
        return

    values = []
    for d, row in new_rows.iterrows():
        values.append([
            sector, None, False, d,
            row['index_value'], row['market_cap'], row['total_volume'], None, None,
            round((row['index_value'] / working.shift(1).loc[d, 'index_value'] - 1) * 100, 4),
            int(row.get('num_constituents')) if pd.notna(row.get('num_constituents')) else None,
            *[round(row.get(f'volatility_{w}d'), 5) for w in [5, 10, 20, 40]],
            round(row.get('momentum_14d'), 5),
            *[round(row.get(f'sma_{w}'), 5) for w in [5, 20, 50, 125, 200]],
            round(row.get('sma_200_weekly'), 5),
            *[round(row.get(f'ema_{w}'), 5) for w in [5, 10, 20, 50, 125, 200]]
        ])

    # Count rows before insert
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sector_index_table WHERE sector = %s AND subsector IS NULL", (sector,))
            before_count = cur.fetchone()[0]

    # Insert new rows
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
            execute_values(cur, insert_sql, values)
            conn.commit()

    # Count rows after insert
    with psycopg2.connect(**DB_CONFIG) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM sector_index_table WHERE sector = %s AND subsector IS NULL", (sector,))
            after_count = cur.fetchone()[0]

    inserted = after_count - before_count
    expected = len(values)
    if inserted == expected:
        print(f"✅ {sector}: Inserted {inserted} new row(s) using {constituents_used} tickers.")
    else:
        print(f"⚠️ {sector}: Expected {expected} inserts, but only {inserted} were confirmed.")
