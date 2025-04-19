import os
import yfinance as yf
import pandas as pd
import ta
import time
import psycopg2
from psycopg2 import OperationalError, sql
from ta.trend import sma_indicator, ema_indicator, macd
from ta.momentum import rsi
from ta.volume import on_balance_volume
from ta.volatility import BollingerBands
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

# DB connect credentials
DB_PARAMS = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

# Define sectors and respective tickers. Modeled after S&P500
SECTOR_STOCKS = {
    "Information Technology": {
        "Semiconductors": [
            "NVDA", "AMD", "TSM", "QCOM", "TXN", "AVGO", "MU", "ADI", "KLAC", "MRVL"
        ],
        "System Software": [
            "MSFT", "NOW", "ORCL", "PANW", "FTNT", "ADBE", "CRM", "SNOW", "WDAY", "DDOG"
        ],
        "IT Services & Consulting": [
            "ACN", "IBM", "CDW", "EPAM", "CTSH", "INFY", "DXC", "GLOB", "GEN", "NTCT"
        ],
        "Hardware & Peripherals": [
            "AAPL", "HPQ", "DELL", "LOGI", "ANET", "ZBRA", "HPE", "NTAP", "STX", "WDC"
        ]
    },

    "Financials": {
        "Banks": [
            "JPM", "BAC", "WFC", "C", "USB", "PNC", "TFC", "FITB", "KEY", "RF"
        ],
        "Investment Management": [
            "GS", "MS", "BLK", "SCHW", "AMP", "TROW", "IVZ", "BX", "KKR", "APO"
        ],
        "Insurance": [
            "AIG", "CB", "MET", "PRU", "TRV", "ALL", "PGR", "HIG", "LNC", "CINF"
        ],
        "Exchanges & Financial Services": [
            "CME", "ICE", "NDAQ", "MKTX", "COIN", "INTU", "FIS", "GPN", "PYPL", "DFS"
        ]
    },

    "Healthcare": {
        "Pharmaceuticals": [
            "PFE", "MRK", "LLY", "BMY", "ABBV", "AMGN", "GILD", "VRTX", "ZTS", "REGN"
        ],
        "Healthcare Equipment": [
            "MDT", "SYK", "BSX", "ISRG", "ZBH", "EW", "STE", "BAX", "TFX", "PKI"
        ],
        "Healthcare Services": [
            "UNH", "CI", "HUM", "CNC", "MCK", "CAH", "HCA", "ELV", "MOH", "HCA"
        ],
        "Biotech & Research": [
            "BIIB", "ILMN", "INCY", "NVAX", "EXEL", "CRSP", "BLUE", "ALNY", "GILD"
        ]
    },

    "Consumer Discretionary": {
        "Retail": [
            "AMZN", "HD", "LOW", "TGT", "BBY", "ROST", "TJX", "DG", "FIVE", "WSM"
        ],
        "Automotive": [
            "TSLA", "F", "GM", "HOG", "LCID", "RIVN", "NIO", "XPEV", "STLA", "TM"
        ],
        "Restaurants": [
            "MCD", "SBUX", "YUM", "CMG", "DPZ", "QSR", "WEN", "SHAK", "DNUT", "CAKE"
        ],
        "Travel & Leisure": [
            "BKNG", "MAR", "RCL", "LVS", "CCL", "H", "NCLH", "EXPE", "HLT", "TRIP"
        ]
    },

    "Industrials": {
        "Aerospace & Defense": [
            "BA", "LMT", "GD", "NOC", "RTX", "HII", "SPR", "TDG", "COL", "HEI"
        ],
        "Machinery": [
            "CAT", "DE", "PCAR", "SAND", "HON", "ITT", "CMI", "AOS", "MAN", "MTW"
        ],
        "Transportation": [
            "UPS", "FDX", "CSX", "NSC", "WAB", "UNP", "LSTR", "ODFL", "JBHT", "UBER"
        ],
        "Construction & Engineering": [
            "FLR", "KBR", "HIT", "TTEK", "STRL", "MTZ", "MTRX", "ACM", "PWR"
        ]
    },

    "Consumer Staples": {
        "Food & Beverage": [
            "KO", "PEP", "MDLZ", "K", "GIS", "CPB", "KHC", "HSY", "TSN", "CAG"
        ],
        "Retail & Distribution": [
            "WMT", "COST", "KR", "TGT", "ACI", "SFM", "BJ", "WBA", "CVS", "CASY"
        ],
        "Household Products": [
            "PG", "CL", "KMB", "CHD", "ECL", "NWL", "ENR", "SPB", "UL", "REYN"
        ],
        "Tobacco & Alcohol": [
            "MO", "PM", "STZ", "BUD", "TAP", "DEO", "DEO", "SAM", "HEINY", "CCEP"
        ]
    },

    "Communications": {
        "Internet Services": [
            "GOOGL", "META", "NFLX", "ZM", "TWLO", "DDOG", "DOCN", "ABNB", "DUOL", "YELP"
        ],
        "Media & Entertainment": [
            "DIS", "PARA", "FOXA", "WBD", "ROKU", "LYV", "IMAX", "SIRI", "SPOT", "CURI"
        ],
        "Telecom": [
            "VZ", "T", "TMUS", "CHTR", "LUMN", "USM", "SHEN", "ATEX", "WOW"
        ],
        "Gaming & Interactive Media": [
            "EA", "TTWO", "RBLX", "HUYA", "BILI", "PLTK", "U", "SKLZ", "NTES"
        ]
    },

    "Utilities": {
        "Electric Utilities": [
            "NEE", "DUK", "SO", "D", "EXC", "AEP", "ED", "XEL", "FE", "EIX"
        ],
        "Gas Utilities": [
            "SRE", "NI", "UGI", "OKE", "ATO", "SWX", "NWN", "SR", "WMB", "CNP"
        ],
        "Renewables": [
            "RUN", "ENPH", "SEDG", "FSLR", "CWEN", "ORA", "TPIC"
        ],
        "Water Utilities": [
            "AWK", "WTRG", "SJW", "YORW", "MSEX", "AWR", "CWCO", "ARTNA", "SBS"
        ]
    }
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


#Give time to yfinance to get each symbol properly
def fetch_stock_data(symbol, start_date="2010-01-01", retries=3, sleep_sec=2):
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date)
            if df.empty:
                raise ValueError(f"No data for {symbol}")
            df = df.reset_index()
            info = ticker.info

            # Store valuation metrics (same for all rows)
            market_data = {
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "price_to_book": info.get("priceToBook"),
            }

            break
        except Exception as e:
            print(f"⏳ Retry {attempt + 1} for {symbol} due to error: {e}")
            time.sleep(sleep_sec)
    else:
        print(f"❌ Giving up on {symbol} after {retries} retries")
        return None, None

    try:
        df["sma_50"] = sma_indicator(df["Close"], window=50)
        df["ema_50"] = ema_indicator(df["Close"], window=50)
        df["macd"] = macd(df["Close"])
        df["dma"] = df["Close"] - df["sma_50"]
        df["rsi"] = rsi(df["Close"])

        bb = BollingerBands(df["Close"])
        df["bollinger_upper"] = bb.bollinger_hband()
        df["bollinger_middle"] = bb.bollinger_mavg()
        df["bollinger_lower"] = bb.bollinger_lband()

        df["obv"] = on_balance_volume(df["Close"], df["Volume"])
        df["sma_200_weekly"] = df["Close"].rolling(window=200 * 5).mean()

        return df, market_data

    except Exception as e:
        print(f"⚠️ Indicator calc failed for {symbol}: {e}")
        return None, None





def insert_data(symbol, sector, subsector, df, market_data):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    for _, row in df.iterrows():
        cur.execute(
            sql.SQL("""
                INSERT INTO stock_market_table (
                    symbol, sector, subsector, date,
                    open, high, low, close, volume,
                    sma_50, ema_50, sma_200_weekly, macd, dma, rsi,
                    bollinger_upper, bollinger_middle, bollinger_lower, obv,
                    market_cap, pe_ratio, forward_pe, price_to_book
                ) VALUES (
                    %s, %s, %s, %s,
                    %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s,
                    %s, %s, %s, %s
                )
            """),
            (
                symbol, sector, subsector, row["Date"],
                row["Open"], row["High"], row["Low"], row["Close"], row["Volume"],
                row["sma_50"], row["ema_50"], row["sma_200_weekly"], row["macd"], row["dma"], row["rsi"],
                row["bollinger_upper"], row["bollinger_middle"], row["bollinger_lower"], row["obv"],
                market_data["market_cap"], market_data["pe_ratio"], market_data["forward_pe"],
                market_data["price_to_book"]
            )
        )

    conn.commit()
    cur.close()
    conn.close()


if __name__ == "__main__":
    if test_database_connection():
        create_table()
        for sector, subsectors in SECTOR_STOCKS.items():
            for subsector, symbols in subsectors.items():
                for symbol in symbols:
                    print(f"📈 Fetching {symbol} ({sector} - {subsector})...")
                    try:
                        df, market_data = fetch_stock_data(symbol)
                        if df is not None and not df.empty:
                            insert_data(symbol, sector, subsector, df, market_data)
                    except Exception as e:
                        print(f"⚠️ Failed to process {symbol}: {e}")
    else:
        print("❌ Failed DB Connection.")