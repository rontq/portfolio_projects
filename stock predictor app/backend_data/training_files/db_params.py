import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../credentials/.env'))


DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

api_key = os.getenv("FRED_API_KEY")

def test_database_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("Successfully connected to PostgreSQL database.")
        return True
    except OperationalError as e:
        print("Could not connect to the database:", e)
        return False

def create_table():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        with open("schema/company.schema.sql", "r") as f:
            cur.execute(f.read())
        conn.commit()
        cur.close()
        conn.close()
        print("Table created or already exists.")
    except Exception as e:
        print("Error creating table:", e)

ALLOWED_COLUMNS = {
    "company": [
        "date", "symbol", "sector", "subsector",
        "open", "high", "low", "close", "volume", "adj_close",
        "sma_5", "sma_20", "sma_50", "sma_125", "sma_200", "sma_200_weekly",
        "ema_5", "ema_20", "ema_50", "ema_125", "ema_200",
        "macd", "dma", "rsi", "bollinger_upper", "bollinger_middle", "bollinger_lower", "obv",
        "pe_ratio", "forward_pe", "price_to_book",
        "volatility_5d", "volatility_10d", "volatility_20d", "volatility_40d",
        "market_cap", "market_cap_proxy", "sector_weight", "subsector_weight", "vix_close", "future_return_1d",
        "cpi_inflation", "core_cpi_inflation", "pce_inflation", "core_pce_inflation",
        "breakeven_inflation_rate", "realized_inflation", "us_10y_bond_rate",
        "retail_sales", "consumer_confidence_index", "nfp", "unemployment_rate", "effective_federal_funds_rate"
    ],
    "subsector": [
        "date", "subsector", "index_value", "market_cap", "total_volume", "num_constituents",
        "average_return", "weighted_return", "return_vs_previous",
        "volatility_5d", "volatility_10d", "volatility_20d", "volatility_40d", "momentum_14d",
        "sma_5", "sma_20", "sma_50", "sma_125", "sma_200", "sma_200_weekly",
        "ema_5", "ema_10", "ema_20", "ema_50", "ema_125", "ema_200",
        "influence_weight"
    ],
    "sector": [
        "date", "sector", "index_value", "market_cap", "total_volume", "num_constituents",
        "average_return", "weighted_return", "return_vs_previous",
        "volatility_5d", "volatility_10d", "volatility_20d", "volatility_40d", "momentum_14d",
        "sma_5", "sma_20", "sma_50", "sma_125", "sma_200", "sma_200_weekly",
        "ema_5", "ema_10", "ema_20", "ema_50", "ema_125", "ema_200"
    ]
}