DROP TABLE IF EXISTS stock_market_table;

CREATE TABLE stock_market_table (
    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    sector TEXT,
    subsector TEXT,
    date DATE NOT NULL,
    day_of_week INT,
    week_of_year INT,
    is_ADR BOOLEAN NOT NULL,

    -- OHLCV
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    adj_close FLOAT,

    -- Technical Indicators (stock-level)
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

    -- Valuation + Influence Modeling
    pe_ratio FLOAT,
    forward_pe FLOAT,
    price_to_book FLOAT,
    volatility_5d FLOAT,
    volatility_10d FLOAT,
    volatility_20d FLOAT,
    volatility_40d FLOAT,
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
