DROP TABLE IF EXISTS stock_market_table;
DROP TABLE IF EXISTS sector_index_table;
CREATE TABLE stock_market_table (
    -- Note: All from yfinance for OCHLV and indicator values

    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    sector TEXT,
    subsector TEXT,
    date TIMESTAMP NOT NULL,

    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,

    -- Market valuation
    market_cap BIGINT,
    pe_ratio DOUBLE PRECISION,
    forward_pe DOUBLE PRECISION,
    price_to_book DOUBLE PRECISION,

    -- Indicators
    sma_50 FLOAT,
    ema_50 FLOAT,
    sma_200_weekly FLOAT,
    macd FLOAT,
    dma FLOAT,
    rsi FLOAT,
    bollinger_upper FLOAT,
    bollinger_middle FLOAT,
    bollinger_lower FLOAT,
    obv BIGINT
);

CREATE TABLE sector_index_table (
    id SERIAL PRIMARY KEY,
    sector_index TEXT,
    subsector_index TEXT,
    date TIMESTAMP NOT NULL,

    market_cap BIGINT, -- calculated as share price * outstanding shares
    index_val BIGINT,

     UNIQUE (sector_index, subsector_index, date)
);

