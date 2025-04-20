DROP TABLE IF EXISTS stock_market_table;
DROP TABLE IF EXISTS sector_index_table;

CREATE TABLE stock_market_table (
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

    -- Labels for ML
    future_return_1d FLOAT,
    future_movement_class INT,

    UNIQUE(symbol, date)
);

CREATE TABLE sector_index_table (
    id SERIAL PRIMARY KEY,

    sector TEXT NOT NULL,               -- e.g. 'Technology'
    subsector TEXT,                    -- Optional: only filled if it's a subsector index

    date DATE NOT NULL,

    -- Core metrics
    market_cap BIGINT,                 -- Total weighted market cap (∑ share_price * shares_outstanding)
    index_value FLOAT,                -- Synthetic index value (e.g. base 1000 style)
    total_volume BIGINT,              -- Sum of daily trading volume (liquidity proxy)
    
    -- Optional financial metrics
    average_return FLOAT,             -- Mean daily return of constituents
    volatility_30d FLOAT,             -- Rolling 30-day std dev (optional)
    weighted_return FLOAT,            -- Weighted daily return
    num_constituents INT,             -- Number of tickers in index

    UNIQUE (sector, subsector, date)
);
