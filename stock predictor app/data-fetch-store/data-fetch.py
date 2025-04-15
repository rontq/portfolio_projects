import yfinance as yf
import pandas as pd
import ta
import psycopg2
from psycopg2 import sql
from ta.trend import sma_indicator, ema_indicator, macd
from ta.momentum import rsi
from ta.volume import on_balance_volume
from ta.volatility import BollingerBands

# DB connect credentials
DB_PARAMS = {
    "dbname": "stock-market-table",
    "user": "postgres",
    "password": "db123",
    "host": "localhost",
    "port": "5432"
}

# Define sectors and respective tickers. Modeled after S&P500
SECTOR_STOCKS = {
    "Information Technology": [
        "AAPL", "MSFT", "NVDA", "ADBE", "CRM", "AVGO", "INTC", "AMD", "ORCL", "TXN",
        "QCOM", "MU", "IBM", "HPQ", "ACN", "CDNS", "ADI", "KLAC", "SNPS", "PANW",
        "NOW", "FTNT", "ANET", "MCHP", "MPWR", "NXPI", "AKAM", "PAYC", "TYL", "TER",
        "KEYS", "ENPH", "APH", "BR", "FLT", "CDW", "CTSH", "ZS", "TTD", "OKTA"
    ],
    "Financials": [
        "JPM", "BAC", "WFC", "C", "GS", "MS", "AXP", "USB", "TFC", "PNC",
        "BK", "BLK", "SCHW", "AIG", "CB", "MET", "PRU", "CME", "ICE", "TRV",
        "AFL", "ALL", "PGR", "MKTX", "DFS", "NDAQ", "MTB", "FITB", "CFG", "RF",
        "ZION", "HBAN", "CINF", "WRB", "RJF", "FRC", "HIG", "LNC", "STT", "IVZ"
    ],
    "Healthcare": [
        "UNH", "JNJ", "PFE", "ABBV", "LLY", "MRK", "TMO", "MDT", "BMY", "AMGN",
        "CVS", "GILD", "CI", "HUM", "ISRG", "DHR", "ZBH", "BSX", "SYK", "BIIB",
        "VRTX", "REGN", "EW", "ILMN", "IDXX", "RMD", "ALGN", "BAX", "BDX", "MTD",
        "TFX", "HCA", "ABC", "CAH", "MCK", "UHS", "INCY", "PKI", "DGX", "WST"
    ],
    "Consumer Discretionary": [
        "AMZN", "TSLA", "HD", "MCD", "NKE", "LOW", "SBUX", "BKNG", "TJX", "ROST",
        "F", "GM", "EBAY", "LVS", "MAR", "RCL", "YUM", "DG", "AZO", "ORLY",
        "TGT", "ULTA", "DPZ", "BBY", "ETSY", "DHI", "LEN", "TOL", "NVR", "POOL",
        "BURL", "KMX", "YETI", "WSM", "RH", "HOG", "MHK", "TPR", "HAS", "ROG"
    ],
    "Consumer Staples": [
        "PG", "KO", "PEP", "WMT", "COST", "MDLZ", "CL", "KMB", "WBA", "KR",
        "TGT", "SYY", "STZ", "EL", "MO", "PM", "HSY", "K", "TSN", "CHD",
        "GIS", "CAG", "KHC", "HRL", "CPB", "MKC", "TAP", "BF.B", "RAD", "ALCO",
        "USFD", "BRBR", "SFM", "POST", "PRGO", "MNST", "LW", "CELH", "ACI", "FLO"
    ],
    "Communications": [
        "GOOGL", "META", "DIS", "NFLX", "CMCSA", "VZ", "T", "CHTR", "TMUS", "PARA",
        "FOXA", "WBD", "LUMN", "Z", "MTCH", "ROKU", "EA", "BIDU", "SPOT", "LYV",
        "TTWO", "SIRI", "TTD", "BILI", "YY", "IQ", "IAC", "FWONA", "DISH", "CURI",
        "ATUS", "VG", "AMC", "IMAX", "CXM", "SBGI", "NXST", "SEAC", "EGHT", "IRDM"
    ],
    "Utilities": [
        "NEE", "DUK", "SO", "D", "EXC", "AEP", "XEL", "PEG", "ED", "WEC",
        "ES", "EIX", "FE", "ETR", "PPL", "AWK", "SRE", "CMS", "VST", "NRG",
        "CNP", "OGE", "NI", "ALE", "AEE", "PNW", "LNT", "IDU", "SWX", "AVA",
        "UGI", "OTTR", "ORA", "BKH", "WTRG", "EVRG", "HE", "POM", "TE", "AES"
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
        print(" Table created or already exists.")
    except Exception as e:
        print(" Error creating table:")
        print(e)

import time
import yfinance as yf
import ta

#Give time to yfinance to get each ticker properly
def fetch_stock_data(symbol, start_date="2010-01-01", retries=3, sleep_sec=2):
    import time
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date)
            if df.empty:
                raise ValueError(f"No data for {symbol}")
            df = df.reset_index()
            break
        except Exception as e:
            print(f"⏳ Retry {attempt + 1} for {symbol} due to error: {e}")
            time.sleep(sleep_sec)
    else:
        print(f"❌ Giving up on {symbol} after {retries} retries")
        return None

    try:
        df["sma_50"] = ta.trend.sma_indicator(df["Close"], window=50)
        df["ema_50"] = ta.trend.ema_indicator(df["Close"], window=50)
        df["macd"] = ta.trend.macd(df["Close"])
        df["dma"] = df["Close"] - df["sma_50"]
        df["rsi"] = ta.momentum.rsi(df["Close"])

        bb = ta.volatility.BollingerBands(df["Close"])
        df["bollinger_upper"] = bb.bollinger_hband()
        df["bollinger_middle"] = bb.bollinger_mavg()
        df["bollinger_lower"] = bb.bollinger_lband()

        df["obv"] = ta.volume.on_balance_volume(df["Close"], df["Volume"])
        df["sma_200_weekly"] = df["Close"].rolling(window=200 * 5).mean()
        return df

    except Exception as e:
        print(f"⚠️ Indicator calc failed for {symbol}: {e}")
        return None




def insert_data(symbol, sector, df):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            sql.SQL("""
                INSERT INTO stock_market_table (
                    symbol, sector, date, open, high, low, close, volume,
                    sma_50, ema_50, sma_200_weekly, macd, dma, rsi,
                    bollinger_upper, bollinger_middle, bollinger_lower, obv
                ) VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """),
            (
                symbol, sector, row["Date"], row["Open"], row["High"], row["Low"], row["Close"], row["Volume"],
                row["sma_50"], row["ema_50"], row["sma_200_weekly"], row["macd"], row["dma"], row["rsi"],
                row["bollinger_upper"], row["bollinger_middle"], row["bollinger_lower"], row["obv"]
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
