import time
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2 import OperationalError
from psycopg2.extras import execute_values
from db_params import DB_CONFIG, test_database_connection, create_table, api_key
from stock_list import SECTOR_STOCKS, MACRO_CODES
from fredapi import Fred

from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator
from ta.volatility import BollingerBands

# --- Mapping Section ---
# Assign sector IDs (1,2,3...) from SECTOR_STOCKS
SECTOR_IDS = {sector: idx for idx, sector in enumerate(SECTOR_STOCKS.keys(), 1)}

# Assign globally unique subsector IDs (1,2,3,...) across all sectors
all_subsectors = []
for sector, subsectors in SECTOR_STOCKS.items():
    all_subsectors.extend(subsectors.keys())
SUBSECTOR_IDS = {subsector: idx for idx, subsector in enumerate(sorted(set(all_subsectors)), 1)}

# Assign symbol IDs (1,2,3...) sorted alphabetically
ALL_SYMBOLS = sorted({symbol for sector in SECTOR_STOCKS.values() for subsector in sector.values() for symbol in subsector})
SYMBOL_IDS = {symbol: idx for idx, symbol in enumerate(ALL_SYMBOLS, 1)}

# --- Helper Functions ---

def get_latest_date(symbol):
    """Fetch the latest date from the database for a given symbol."""
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("""
        SELECT MAX(date) FROM stock_market_table WHERE symbol = %s
    """, (symbol,))
    latest_date = cur.fetchone()[0]
    cur.close()
    conn.close()
    return latest_date

# --- Data Fetching Functions ---

def fetch_vix_data(start_date="2004-01-01"):
    try:
        vix = yf.Ticker("^VIX")
        df = vix.history(start=start_date).reset_index()
        df = df[["Date", "Close"]].rename(columns={"Date": "date", "Close": "vix_close"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
    except Exception as e:
        print("Failed to fetch VIX data:", e)
        return pd.DataFrame()

def fetch_macro_data(start_date="2004-01-01", end_date="2025-04-04"):
    fred = Fred(api_key=api_key)
    all_macro = []
    
    for macro_name, fred_code in MACRO_CODES.items():
        try:
            print(f"📈 Fetching {macro_name} ({fred_code})...")
            series = fred.get_series(fred_code, observation_start=start_date, observation_end=end_date)
            df = series.reset_index()
            df.columns = ["date", macro_name]
            all_macro.append(df)
        except Exception as e:
            print(f"⚠️ Error fetching {macro_name}: {e}")

    if all_macro:
        macro_df = all_macro[0]
        for df in all_macro[1:]:
            macro_df = pd.merge(macro_df, df, on="date", how="outer")

        macro_df["date"] = pd.to_datetime(macro_df["date"]).dt.date
        
        columns_to_forward_fill = [col for col in macro_df.columns if col != "date" and col != "breakeven_inflation_rate"]
        
        macro_df.sort_values("date", inplace=True)
        macro_df[columns_to_forward_fill] = macro_df[columns_to_forward_fill].ffill()

        return macro_df
    else:
        return pd.DataFrame()

def fetch_stock_data(symbol, start_date=None, retries=3, sleep_sec=2):
    # If start_date is None, get the latest date from the database
    if start_date is None:
        latest_date = get_latest_date(symbol)
        if latest_date is not None:
            start_date = latest_date + pd.Timedelta(days=1)  # Start from the day after the latest date in the DB
        else:
            start_date = "2004-01-01"  # Fallback if no data is found in DB

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
            print(f"⏳ Retry {attempt + 1} for {symbol} due to error: {e}")
            time.sleep(sleep_sec)
    else:
        print(f"❌ Giving up on {symbol} after {retries} retries")
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
        print(f"⚠️ Indicator calc failed for {symbol}: {e}")
        return None, None

# --- Data Insert Function ---

def insert_data(symbol, sector, subsector, df, market_data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    symbol_id = SYMBOL_IDS.get(symbol)
    sector_id = SECTOR_IDS.get(sector)
    subsector_id = SUBSECTOR_IDS.get(subsector)

    # Validate IDs
    if symbol_id is None:
        raise ValueError(f"❌ Symbol {symbol} not found in SYMBOL_IDS mapping.")
    if sector_id is None:
        raise ValueError(f"❌ Sector {sector} not found in SECTOR_IDS mapping.")
    if subsector_id is None:
        raise ValueError(f"❌ Subsector {subsector} not found in SUBSECTOR_IDS mapping.")

    insert_rows = []

    for _, row in df.iterrows():
        if pd.isna(row["date"]):
            continue
        insert_rows.append((...)) 

    if insert_rows:
        try:
            execute_values(cur, """
                INSERT INTO stock_market_table (
                    ...
                ) VALUES %s
                ON CONFLICT (symbol, date) DO NOTHING
            """, insert_rows)
            conn.commit()
        except Exception as e:
            print(f"❌ Failed batch insert {symbol}: {e}")
            conn.rollback()

    cur.close()
    conn.close()

def main():
    if test_database_connection():
        create_table()
        macro_df = fetch_macro_data()
        vix_df = fetch_vix_data()

        for sector, subsectors in SECTOR_STOCKS.items():
            for subsector, symbols in subsectors.items():
                for symbol in symbols:
                    print(f"📈 Fetching {symbol} ({sector} - {subsector})...")
                    try:
                        df, market_data = fetch_stock_data(symbol)
                        if df is not None and not df.empty:
                            df = df.merge(vix_df, on="date", how="left")
                            if not macro_df.empty:
                                df = df.merge(macro_df, on="date", how="left")
                            insert_data(symbol, sector, subsector, df, market_data)
                    except Exception as e:
                        print(f"⚠️ Failed processing {symbol}: {e}")
    else:
        print("❌ Database connection failed.")

if __name__ == "__main__":
    main()
