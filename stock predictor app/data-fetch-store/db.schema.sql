DROP TABLE IF EXISTS stock_market_table;
DROP TABLE IF EXISTS sector_index_table;

CREATE TABLE stock_market_table (
    -- Company level data
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    sector TEXT,
    subsector TEXT,
    date DATE NOT NULL,

    -- OHLCV from yfinance
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    adj_close FLOAT,

    -- Core Technical Indicators
    sma_5 FLOAT,
    sma_20 FLOAT,
    sma_50 FLOAT,
    sma_125 FLOAT,
    sma_200 FLOAT,
    sma_200_weekly FLOAT,
    ema_5 FLOAT,
    ema_20 FLOAT,
    ema_50 FLOAT,
    ema_125 FLOAT,
    ema_200 FLOAT,
    macd FLOAT,
    dma FLOAT,
    rsi FLOAT,
    bollinger_upper FLOAT,
    bollinger_middle FLOAT,
    bollinger_lower FLOAT,
    obv BIGINT,
    pe_ratio FLOAT,
    forward_pe FLOAT, 
    price_to_book FLOAT,


    -- Influence Modeling
	market_cap BIGINT,
    market_cap_proxy FLOAT,
    sector_id INT,
    subsector_id INT,
    sector_weight FLOAT,
    subsector_weight FLOAT,
    vix_close FLOAT,

    -- Labels for ML
    future_return_1d FLOAT,
    future_movement_class INT,

    UNIQUE(symbol, date)
);

CREATE TABLE sector_index_table (
    id SERIAL PRIMARY KEY,

    -- Hierarchy
    sector TEXT NOT NULL,
    subsector TEXT,                        -- NULL if row is a sector-level index
    is_subsector BOOLEAN DEFAULT FALSE,    -- True if this is a subsector record

    -- Temporal
    date DATE NOT NULL,

    -- Core market metrics
    index_value FLOAT,                     -- Synthetic index value (e.g. 1000 base)
    market_cap BIGINT,                     -- Aggregate market cap (∑ price * shares_outstanding)
    total_volume BIGINT,                   -- Total trading volume for the index
    num_constituents INT,                  -- Number of companies in index

    -- Return metrics
    average_return FLOAT,                 -- Mean % return of all constituents
    weighted_return FLOAT,                -- Market-cap weighted return %
    return_vs_previous FLOAT,             -- Return based on (index_value_t - index_value_t-1)

    -- Volatility / trend metrics
    volatility_30d FLOAT,                  -- Rolling std dev of index_value
    momentum_14d FLOAT,                    -- Optional: % change over past 14 days
    trend_direction TEXT,                  -- Optional: 'up', 'down', 'flat'

    -- Influence modeling (subsector only)
    influence_weight FLOAT,               -- Subsector influence on its parent sector (∑subsector_cap / ∑sector_cap)

    UNIQUE (sector, subsector, date)
);
