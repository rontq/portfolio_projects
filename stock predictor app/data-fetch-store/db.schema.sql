CREATE TABLE stock_market_table (
    --Note all from yfinance for OCHLV and indicator values

    id SERIAL PRIMARY KEY,
    symbol TEXT NOT NULL,
    sector TEXT,
    date TIMESTAMP NOT NULL,
    
    open FLOAT,
    high FLOAT,
    low FLOAT,
    close FLOAT,
    volume BIGINT,
    
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
    obv BIGINT,
    
    -- Support/Resistance
    support_level FLOAT,
    resistance_level FLOAT,
    
    -- Metadata
    inserted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Optional: for fast querying
CREATE INDEX idx_stock_date ON stock_prices (symbol, date);
