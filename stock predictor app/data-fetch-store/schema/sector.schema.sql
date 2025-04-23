DROP TABLE IF EXISTS sector_index_table;

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
    market_cap BIGINT,
    total_volume BIGINT,
    num_constituents INT,

    -- Return metrics
    average_return FLOAT,
    weighted_return FLOAT,
    return_vs_previous FLOAT,

    -- Volatility / trend metrics
    volatility_5d FLOAT,
    volatility_10d FLOAT,
    volatility_20d FLOAT,
    volatility_40d FLOAT,
    momentum_14d FLOAT,
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

    -- Influence modeling (subsector only)
    influence_weight FLOAT,

    -- Unique identifier per row: prevents duplicate entries and ensures clean updates
    UNIQUE (sector, subsector, date)
);
