import time
import yfinance as yf
import pandas as pd
import psycopg2

from db_params import DB_CONFIG, test_database_connection, api_key
from stock_list import SECTOR_STOCKS, MACRO_CODES
from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator
from ta.volatility import BollingerBands

from datetime import datetime, timedelta

# Symbol mappings
SECTOR_IDS = {name: idx for idx, name in enumerate(SECTOR_STOCKS.keys(), 1)}
SUBSECTOR_IDS = {
    subsector: idx
    for sector in SECTOR_STOCKS
    for idx, subsector in enumerate(SECTOR_STOCKS[sector].keys(), 1)
}
ALL_SYMBOLS = sorted({symbol for sector in SECTOR_STOCKS.values() for subsector in sector.values() for symbol in subsector})
SYMBOL_IDS = {symbol: idx for idx, symbol in enumerate(ALL_SYMBOLS, 1)}

def fetch_vix_data(start_date):
    try:
        vix = yf.Ticker("^VIX")
        df = vix.history(start=start_date).reset_index()
        df = df[["Date", "Close"]].rename(columns={"Date": "date", "Close": "vix_close"})
        df["date"] = pd.to_datetime(df["date"]).dt.date
        return df
    except Exception as e:
        print("Failed to fetch VIX data:", e)
        return pd.DataFrame()

def fetch_macro_data(start_date, end_date=None):
    fred = api_key
    if end_date is None:
        end_date = pd.Timestamp.today().strftime("%Y-%m-%d")

    all_macro = []
    for field_name, fred_code in MACRO_CODES.items():
        try:
            print(f"📈 Fetching {fred_code} ({field_name})...")
            series = fred.get_series(fred_code, observation_start=start_date, observation_end=end_date)
            df = series.reset_index()
            df.columns = ["date", field_name]
            all_macro.append(df)
        except Exception as e:
            print(f"⚠️ Error fetching {fred_code}: {e}")

    if all_macro:
        macro_df = all_macro[0]
        for df in all_macro[1:]:
            macro_df = pd.merge(macro_df, df, on="date", how="outer")

        macro_df["date"] = pd.to_datetime(macro_df["date"]).dt.date
        macro_df = macro_df.sort_values("date").ffill()
        return macro_df
    else:
        print("⚠️ No macroeconomic data fetched.")
        return pd.DataFrame()

def get_latest_global_date(conn):
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT MAX(date) FROM stock_market_table;")
            result = cur.fetchone()
            if result and result[0]:
                return result[0]
            else:
                return None
    except Exception as e:
        print(f"❌ Error querying latest global date: {e}")
        return None

def fetch_stock_data(symbol, start_date, retries=3, sleep_sec=2):
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

def insert_data(symbol, sector, subsector, df, market_data):
    # --- your real insert logic will be filled here ---
    pass

def main():
    if test_database_connection():
        conn = psycopg2.connect(**DB_CONFIG)

        latest_date = get_latest_global_date(conn)
        today = datetime.today().date()

        if latest_date is None:
            print("❌ No data found in database! This updater script assumes prior full loading.")
            return

        if latest_date >= today:
            print(f"⚠️ Latest date in database ({latest_date}) is up to or after today ({today}). No automatic updates possible.")
            start_date_input = input("Please manually enter a start date in format YYYY-MM-DD: ")
            try:
                start_date = datetime.strptime(start_date_input.strip(), "%Y-%m-%d").date()
            except ValueError:
                print("❌ Invalid date format. Please use YYYY-MM-DD format.")
                return
        else:
            start_date = (latest_date + timedelta(days=1)).strftime("%Y-%m-%d")

        print(f"⏩ Global update starting from {start_date}")

        macro_df = fetch_macro_data(start_date=start_date)
        vix_df = fetch_vix_data(start_date=start_date)

        for sector, subsectors in SECTOR_STOCKS.items():
            for subsector, symbols in subsectors.items():
                for symbol in symbols:
                    print(f"📈 Updating {symbol} ({sector} - {subsector})...")
                    try:
                        df, market_data = fetch_stock_data(symbol, start_date=start_date)

                        if df is not None and not df.empty:
                            df = df.merge(vix_df, on="date", how="left")
                            if not macro_df.empty:
                                df = df.merge(macro_df, on="date", how="left")
                            insert_data(symbol, sector, subsector, df, market_data)
                        else:
                            print(f"⚠️ No new data for {symbol}")
                    except Exception as e:
                        print(f"⚠️ Failed to update {symbol}: {e}")

        conn.close()
    else:
        print("❌ Failed DB Connection.")

if __name__ == "__main__":
    main()
