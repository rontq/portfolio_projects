import os
import time
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, sql
from dotenv import load_dotenv

from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator
from ta.volatility import BollingerBands

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

# Database connection parameters
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
            "NVDA", "AMD", "TSM", "QCOM", "TXN", "AVGO", "MU", "ADI", "KLAC", "MRVL",
            "NXPI", "LSCC", "ON", "ASML", "ACLS"
        ],
        "System Software": [
            "MSFT", "NOW", "ORCL", "PANW", "FTNT", "ADBE", "CRM", "SNOW", "WDAY", "DDOG",
            "ZS", "OKTA", "CRWD", "S", "TENB"
        ],
        "IT Services & Consulting": [
            "ACN", "IBM", "CDW", "EPAM", "CTSH", "INFY", "DXC", "GLOB", "GEN", "NTCT",
            "SAP", "HDB"
        ],
        "Hardware & Peripherals": [
            "AAPL", "HPQ", "DELL", "LOGI", "ANET", "ZBRA", "HPE", "NTAP", "STX", "WDC"
        ]
    },

    "Financials": {
        "Banks": [
            "JPM", "BAC", "WFC", "C", "USB", "PNC", "TFC", "FITB", "KEY", "RF",
            "TD", "RY", "BNS"
        ],
        "Investment Management": [
            "GS", "MS", "BLK", "SCHW", "AMP", "TROW", "IVZ", "BX", "KKR", "APO"
        ],
        "Insurance": [
            "AIG", "CB", "MET", "PRU", "TRV", "ALL", "PGR", "HIG", "LNC", "CINF"
        ],
        "Exchanges & Financial Services": [
            "CME", "ICE", "NDAQ", "MKTX", "COIN", "INTU", "FIS", "GPN", "PYPL", "DFS",
            "SOFI", "HOOD"
        ]
    },

    "Healthcare": {
        "Pharmaceuticals": [
            "PFE", "MRK", "LLY", "BMY", "ABBV", "AMGN", "GILD", "VRTX", "ZTS", "REGN"
        ],
        "Healthcare Equipment": [
            "MDT", "SYK", "BSX", "ISRG", "ZBH", "EW", "STE", "BAX", "TFX",
            "DXCM", "INSP", "NVCR"
        ],
        "Healthcare Services": [
            "UNH", "CI", "HUM", "CNC", "MCK", "CAH", "HCA", "ELV", "MOH"
        ],
        "Biotech & Research": [
            "BIIB", "ILMN", "INCY", "NVAX", "EXEL", "CRSP", "BLUE", "ALNY", "BNTX", "SAGE"
        ]
    },

    "Consumer Discretionary": {
        "Retail": [
            "AMZN", "HD", "LOW", "TGT", "BBY", "ROST", "TJX", "DG", "FIVE", "WSM",
            "ETSY", "CHWY", "FVRR", "SHOP"
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
            "BA", "LMT", "GD", "NOC", "RTX", "HII", "SPR", "TDG", "COL", "HEI",
            "GE", "SPCE"
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
            "MO", "PM", "STZ", "BUD", "TAP", "DEO", "SAM", "HEINY", "CCEP"
        ]
    },

    "Communications": {
        "Internet Services": [
            "GOOGL", "META", "NFLX", "ZM", "TWLO", "DDOG", "DOCN", "ABNB", "DUOL", "GOOG"
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
    },

    "Real Estate": {
        "REITs": [
            "O", "SPG", "PLD", "VTR", "AVB", "EXR", "DLR", "EQR", "MAA", "ARE"
        ],
        "Real Estate Services": [
            "CBRE", "JLL", "Z", "RDFN", "COMP"
        ]
    },

    "Materials": {
        "Chemicals": [
            "LIN", "DD", "DOW", "CE", "ALB", "MOS", "FMC"
        ],
        "Construction Materials": [
            "MLM", "VMC", "EXP"
        ],
        "Metals & Mining": [
            "NEM", "FCX", "X", "AA", "CLF", "ARCH", "HL", "SCCO", "VALE"
        ],
        "Paper & Packaging": [
            "IP", "PKG",
        ]
    },

    "Energy": {
        "Oil & Gas Producers": [
            "XOM", "CVX", "COP", "EOG", "OXY", "DVN", "FANG", "APA"
        ],
        "Oil & Gas Equipment & Services": [
            "SLB", "HAL", "BKR", "NOV", "CHX"
        ],
        "Midstream & Pipelines": [
            "KMI", "WMB", "ENB", "ET", "OKE"
        ],
        "Renewable & Integrated Energy": [
            "NEE", "DUK", "D", "BEP", "CWEN"
        ]
    }
}

# For masking purposes
SECTOR_IDS = {name: idx for idx, name in enumerate(SECTOR_STOCKS.keys(), 1)}
SUBSECTOR_IDS = {
    subsector: idx
    for sector in SECTOR_STOCKS
    for idx, subsector in enumerate(SECTOR_STOCKS[sector].keys(), 1)
}

def test_database_connection():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        conn.close()
        print("Successfully connected to PostgreSQL database.")
        return True
    except OperationalError as e:
        print("Could not connect to the database:", e)
        return False

def create_table():
    try:
        conn = psycopg2.connect(**DB_PARAMS)
        cur = conn.cursor()
        with open("db.schema.sql", "r") as f:
            cur.execute(f.read())
        conn.commit()
        cur.close()
        conn.close()
        print("Table created or already exists.")
    except Exception as e:
        print("Error creating table:", e)

def fetch_vix_data(start_date="2010-01-01"):
    try:
        vix = yf.Ticker("^VIX")
        df = vix.history(start=start_date).reset_index()
        df = df[["Date", "Close"]].rename(columns={"Date": "date", "Close": "vix_close"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
    except Exception as e:
        print("Failed to fetch VIX data:", e)
        return pd.DataFrame()

def fetch_stock_data(symbol, start_date="2010-01-01", retries=3, sleep_sec=2):
    for attempt in range(retries):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date)
            if df.empty:
                raise ValueError(f"No data for {symbol}")
            df = df.reset_index()
            df.columns = df.columns.str.lower()
            info = ticker.info

            market_data = {
                "market_cap": info.get("marketCap"),
                "pe_ratio": info.get("trailingPE"),
                "forward_pe": info.get("forwardPE"),
                "price_to_book": info.get("priceToBook"),
                "is_adr": info.get("quoteType") == "ADR"
            }
            break
        except Exception as e:
            print(f"\u23f3 Retry {attempt + 1} for {symbol} due to error: {e}")
            time.sleep(sleep_sec)
    else:
        print(f"\u274c Giving up on {symbol} after {retries} retries")
        return None, None

    try:
        close = df["close"]
        volume = df["volume"]

        for window in [5, 20, 50, 125, 200]:
            df[f"sma_{window}"] = SMAIndicator(close, window=window).sma_indicator()
            df[f"ema_{window}"] = EMAIndicator(close, window=window).ema_indicator()

        df["macd"] = MACD(close).macd_diff()
        df["dma"] = close - df["sma_50"]
        df["rsi"] = RSIIndicator(close).rsi()

        bb = BollingerBands(close)
        df["bollinger_upper"] = bb.bollinger_hband()
        df["bollinger_middle"] = bb.bollinger_mavg()
        df["bollinger_lower"] = bb.bollinger_lband()

        df["obv"] = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        df["sma_200_weekly"] = close.rolling(window=200 * 5).mean()

        df["adj_close"] = close
        df["market_cap_proxy"] = df["close"] * df["volume"]
        df["date"] = pd.to_datetime(df["date"])
        df["day_of_week"] = df["date"].dt.dayofweek + 1
        df["week_of_year"] = df["date"].dt.isocalendar().week

        for window in [5, 10, 20, 40]:
            df[f"volatility_{window}d"] = df["close"].rolling(window).std()

        df["date"] = df["date"].dt.date

        return df, market_data

    except Exception as e:
        print(f"\u26a0\ufe0f Indicator calc failed for {symbol}: {e}")
        return None, None

def insert_data(symbol, sector, subsector, df, market_data, vix_df):
    conn = psycopg2.connect(**DB_PARAMS)
    cur = conn.cursor()

    sector_id = SECTOR_IDS.get(sector, None)
    subsector_id = SUBSECTOR_IDS.get(subsector, None)

    df = df.merge(vix_df, on="date", how="left")

    for _, row in df.iterrows():
        if pd.isna(row["date"]):
            continue
        try:
            cur.execute(
                sql.SQL("""
                    INSERT INTO stock_market_table (
                        symbol, sector, subsector, date, day_of_week, week_of_year, is_adr,
                        open, high, low, close, volume, adj_close,
                        sma_5, sma_20, sma_50, sma_125, sma_200, sma_200_weekly,
                        ema_5, ema_20, ema_50, ema_125, ema_200,
                        macd, dma, rsi,
                        bollinger_upper, bollinger_middle, bollinger_lower, obv,
                        pe_ratio, forward_pe, price_to_book,
                        volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                        market_cap, market_cap_proxy,
                        sector_id, subsector_id,
                        sector_weight, subsector_weight, vix_close,
                        future_return_1d, future_movement_class
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s,
                        %s, %s
                    )
                    ON CONFLICT (symbol, date) DO NOTHING
                """),
                (
                    symbol, sector, subsector, row["date"], row["day_of_week"], row["week_of_year"], market_data.get("is_adr", False),
                    row["open"], row["high"], row["low"], row["close"], row["volume"], row["adj_close"],
                    row.get("sma_5"), row.get("sma_20"), row.get("sma_50"), row.get("sma_125"), row.get("sma_200"), row.get("sma_200_weekly"),
                    row.get("ema_5"), row.get("ema_20"), row.get("ema_50"), row.get("ema_125"), row.get("ema_200"),
                    row.get("macd"), row.get("dma"), row.get("rsi"),
                    row.get("bollinger_upper"), row.get("bollinger_middle"), row.get("bollinger_lower"), row.get("obv"),
                    market_data["pe_ratio"], market_data["forward_pe"], market_data["price_to_book"],
                    row.get("volatility_5d"), row.get("volatility_10d"), row.get("volatility_20d"), row.get("volatility_40d"),
                    market_data["market_cap"], row.get("market_cap_proxy"),
                    sector_id, subsector_id,
                    row.get("sector_weight"), row.get("subsector_weight"), row.get("vix_close"),
                    row.get("future_return_1d"), row.get("future_movement_class")
                )
            )
        except Exception as e:
            print(f"\u274c Failed to insert row for {symbol} on {row['date']}: {e}")
            print("Row contents (preview):")
            print(row.to_dict())
            conn.rollback()

    conn.commit()
    cur.close()
    conn.close()

if __name__ == "__main__":
    if test_database_connection():
        create_table()
        vix_df = fetch_vix_data()
        for sector, subsectors in SECTOR_STOCKS.items():
            for subsector, symbols in subsectors.items():
                for symbol in symbols:
                    print(f"\U0001f4c8 Fetching {symbol} ({sector} - {subsector})...")
                    try:
                        df, market_data = fetch_stock_data(symbol)
                        if df is not None and not df.empty:
                            insert_data(symbol, sector, subsector, df, market_data, vix_df)
                    except Exception as e:
                        print(f"\u26a0\ufe0f Failed to process {symbol}: {e}")
    else:
        print("\u274c Failed DB Connection.")
