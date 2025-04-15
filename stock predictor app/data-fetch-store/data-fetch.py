import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2 import sql

# DB connect credentials
DB_PARAMS = {
    "dbname": "stock_data",
    "user": "superuser",
    "password": "db123",
    "host": "localhost",
    "port": "5432"
}

# Define sectors and respective tickers. Modeled after S&P500
SECTOR_STOCKS = {
    "Information Technology": [
        "AAPL", "MSFT", "NVDA", "ADBE", "CRM", "AVGO", "INTC", "AMD", "ORCL", "TXN",
        "QCOM", "MU", "IBM", "HPQ", "ACN", "CDNS", "ADI", "KLAC", "SNPS", "PANW"
    ],
    "Financials": [
        "JPM", "BAC", "WFC", "C", "GS", "MS", "AXP", "USB", "TFC", "PNC",
        "BK", "BLK", "SCHW", "AIG", "CB", "MET", "PRU", "CME", "ICE", "TRV"
    ],
    "Healthcare": [
        "UNH", "JNJ", "PFE", "ABBV", "LLY", "MRK", "TMO", "MDT", "BMY", "AMGN",
        "CVS", "GILD", "CI", "HUM", "ISRG", "DHR", "ZBH", "BSX", "SYK", "BIIB"
    ],
    "Consumer Discretionary": [
        "AMZN", "TSLA", "HD", "MCD", "NKE", "LOW", "SBUX", "BKNG", "TJX", "ROST",
        "F", "GM", "EBAY", "LVS", "MAR", "RCL", "YUM", "DG", "AZO", "ORLY"
    ],
    "Consumer Staples": [
        "PG", "KO", "PEP", "WMT", "COST", "MDLZ", "CL", "KMB", "WBA", "KR",
        "TGT", "SYY", "STZ", "EL", "MO", "PM", "HSY", "K", "TSN", "CHD"
    ],
    "Communications": [
        "GOOGL", "META", "DIS", "NFLX", "CMCSA", "VZ", "T", "CHTR", "TMUS", "PARA",
        "FOXA", "WBD", "LUMN", "Z", "MTCH", "ATVI", "EA", "BIDU", "SPOT", "LYV"
    ],
    "Utilities": [
        "NEE", "DUK", "SO", "D", "EXC", "AEP", "XEL", "PEG", "ED", "WEC",
        "ES", "EIX", "FE", "ETR", "PPL", "AWK", "SRE", "CMS", "VST", "NRG"
    ]
}

def test_database_connection():
#   Test the PostgreSQL database connection before proceeding.
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.close()
        print("Successfully connected to PostgreSQL database.")
        return True
    except OperationalError as e:
        print("Could not connect to the database:")
        print(e)
        return False
    
def create_table():
#   Create stock_market_table if it doesn't exist, using schema.sql.
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        with open("db.schema.sql", "r") as f:
            cur.execute(f.read())
        conn.commit()
        cur.close()
        conn.close()
        print("✅ Table created or already exists.")
    except Exception as e:
        print("❌ Error creating table:")
        print(e)

def fetch_stock_data(symbol, start_date="2010-01-01"):
    stock = yf.download(symbol, start=start_date)
    stock = stock.reset_index()

    stock["sma_50"] = ta.trend.sma_indicator(stock["Close"], window=50)
    stock["ema_50"] = ta.trend.ema_indicator(stock["Close"], window=50)
    stock["macd"] = ta.trend.macd(stock["Close"])
    stock["dma"] = stock["Close"] - stock["sma_50"]
    stock["rsi"] = ta.momentum.rsi(stock["Close"])

    bb = ta.volatility.BollingerBands(stock["Close"], window=20)
    stock["bollinger_upper"] = bb.bollinger_hband()
    stock["bollinger_middle"] = bb.bollinger_mavg()
    stock["bollinger_lower"] = bb.bollinger_lband()

    stock["obv"] = ta.volume.on_balance_volume(stock["Close"], stock["Volume"])
    stock["sma_200_weekly"] = stock["Close"].rolling(window=200 * 5).mean()

    # Support/Resistance placeholders
    stock["support_level"] = None
    stock["resistance_level"] = None

    return stock

def insert_data(symbol, sector, df):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            sql.SQL("""
                INSERT INTO stock_market_table (
                    symbol, sector, date, open, high, low, close, volume,
                    sma_50, ema_50, sma_200_weekly, macd, dma, rsi,
                    bollinger_upper, bollinger_middle, bollinger_lower, obv,
                    support_level, resistance_level
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s
                )
            """),
            (
                symbol, sector, row["Date"], row["Open"], row["High"], row["Low"], row["Close"], row["Volume"],
                row["sma_50"], row["ema_50"], row["sma_200_weekly"], row["macd"], row["dma"], row["rsi"],
                row["bollinger_upper"], row["bollinger_middle"], row["bollinger_lower"], row["obv"],
                row["support_level"], row["resistance_level"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    if test_database_connection():
        create_table()
        for sector, stocks in SECTOR_STOCKS.items():
            for symbol in stocks:
                print(f"Fetching {symbol} ({sector})...")
                try:
                    df = fetch_stock_data(symbol)
                    if not df.empty:
                        insert_data(symbol, sector, df)
                except Exception as e:
                    print(f"⚠️ Failed to process {symbol}: {e}")
    else:
        print("Failed DB Connection.")
