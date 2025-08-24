import time
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from db_params import DB_CONFIG, test_database_connection, create_table, api_key
from stock_list import SECTOR_STOCKS, MACRO_CODES
from fredapi import Fred

from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator
from ta.volatility import BollingerBands
from datetime import datetime, timedelta


SECTOR_IDS = {sector: idx for idx, sector in enumerate(SECTOR_STOCKS.keys(), 1)}

all_subsectors = []
for sector, subsectors in SECTOR_STOCKS.items():
    all_subsectors.extend(subsectors.keys())
SUBSECTOR_IDS = {subsector: idx for idx, subsector in enumerate(sorted(set(all_subsectors)), 1)}

ALL_SYMBOLS = sorted({symbol for sector in SECTOR_STOCKS.values() for subsector in sector.values() for symbol in subsector})
SYMBOL_IDS = {symbol: idx for idx, symbol in enumerate(ALL_SYMBOLS, 1)}


# --- Database Helpers ---

def get_last_date_for_symbol(symbol):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    cur.execute("SELECT MAX(date) FROM stock_market_table WHERE symbol = %s", (symbol,))
    result = cur.fetchone()[0]
    cur.close()
    conn.close()
    return result  # returns None if no data yet


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


def fetch_macro_data(start_date="2004-01-01", end_date=None):
    if end_date is None:
        end_date = datetime.today().strftime("%Y-%m-%d")
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


def fetch_stock_data(symbol, start_date="2004-01-01", retries=3, sleep_sec=2):
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
                "country": info.get("country")
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

        # Technical indicators
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


# --- Incremental Stock Fetch ---

def fetch_stock_data_incremental(symbol, buffer_days=200):
    """
    Fetch new data for a symbol incrementally, recalculating indicators with a buffer window.
    """
    last_date = get_last_date_for_symbol(symbol)
    today = datetime.today().date()

    if last_date:
        # Start fetching buffer_days before last_date
        start_date = (pd.to_datetime(last_date) - pd.Timedelta(days=buffer_days)).strftime("%Y-%m-%d")
        print(f"↪️ {symbol}: last DB date = {last_date}, fetching from {start_date} to today")
    else:
        # DB has no data for this symbol
        start_date = "2004-01-01"
        print(f"🆕 {symbol}: no data in DB, fetching full history")

    # Fetch stock data and calculate indicators
    df, market_data = fetch_stock_data(symbol, start_date=start_date)
    if df is None or df.empty:
        return None, None

    # Only keep rows that are actually new for insertion
    if last_date:
        df_to_insert = df[df["date"] > last_date]
    else:
        df_to_insert = df

    # Guard: drop rows beyond today
    df_to_insert = df_to_insert[df_to_insert["date"] <= today]

    if df_to_insert.empty:
        print(f"⚠️ {symbol}: no new rows to insert")
        return None, None

    return df_to_insert, market_data

    
def insert_data(symbol, sector, subsector, df, market_data):
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    symbol_id = SYMBOL_IDS.get(symbol)
    sector_id = SECTOR_IDS.get(sector)
    subsector_id = SUBSECTOR_IDS.get(subsector)

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

        insert_rows.append((
            symbol, sector, subsector, row["date"], row.get("day_of_week"), row.get("week_of_year"),
            market_data.get("country"), symbol_id,
            row.get("open"), row.get("high"), row.get("low"), row.get("close"), row.get("volume"), row.get("adj_close"),
            row.get("sma_5"), row.get("sma_20"), row.get("sma_50"), row.get("sma_125"), row.get("sma_200"), row.get("sma_200_weekly"),
            row.get("ema_5"), row.get("ema_20"), row.get("ema_50"), row.get("ema_125"), row.get("ema_200"),
            row.get("macd"), row.get("dma"), row.get("rsi"),
            row.get("bollinger_upper"), row.get("bollinger_middle"), row.get("bollinger_lower"), row.get("obv"),
            market_data.get("pe_ratio"), market_data.get("forward_pe"), market_data.get("price_to_book"),
            row.get("volatility_5d"), row.get("volatility_10d"), row.get("volatility_20d"), row.get("volatility_40d"),
            market_data.get("market_cap"), row.get("market_cap_proxy"),
            sector_id, subsector_id,
            row.get("sector_weight"), row.get("subsector_weight"), row.get("vix_close"), row.get("future_return_1d"),
            row.get("cpi_inflation"), row.get("core_cpi_inflation"), row.get("pce_inflation"), row.get("core_pce_inflation"),
            row.get("breakeven_inflation_rate"), row.get("realized_inflation"), row.get("us_10y_bond_rate"),
            row.get("retail_sales"), row.get("consumer_confidence_index"), row.get("nfp"), row.get("unemployment_rate"),
            row.get("effective_federal_funds_rate")
        ))

    if insert_rows:
        try:
            execute_values(cur, """
                INSERT INTO stock_market_table (
                    symbol, sector, subsector, date, day_of_week, week_of_year, country_of_origin, symbol_id,
                    open, high, low, close, volume, adj_close,
                    sma_5, sma_20, sma_50, sma_125, sma_200, sma_200_weekly,
                    ema_5, ema_20, ema_50, ema_125, ema_200,
                    macd, dma, rsi,
                    bollinger_upper, bollinger_middle, bollinger_lower, obv,
                    pe_ratio, forward_pe, price_to_book,
                    volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                    market_cap, market_cap_proxy,
                    sector_id, subsector_id,
                    sector_weight, subsector_weight, vix_close, future_return_1d,
                    cpi_inflation, core_cpi_inflation, pce_inflation, core_pce_inflation,
                    breakeven_inflation_rate, realized_inflation, us_10y_bond_rate,
                    retail_sales, consumer_confidence_index, nfp, unemployment_rate,
                    effective_federal_funds_rate
                ) VALUES %s
                ON CONFLICT (symbol, date) DO NOTHING
            """, insert_rows)
            conn.commit()
            print(f"✅ Inserted {len(insert_rows)} rows for {symbol}")

            # --- Post-insert verification ---
            df_max_date = df["date"].max()
            cur.execute("SELECT MAX(date) FROM stock_market_table WHERE symbol = %s", (symbol,))
            db_max_date = cur.fetchone()[0]

            if db_max_date != df_max_date:
                print(f"⚠️ Verification mismatch for {symbol}: DataFrame max date = {df_max_date}, DB max date = {db_max_date}")
            else:
                print(f"🔍 Verification passed for {symbol}: Latest date in DB = {db_max_date}")

        except Exception as e:
            print(f"❌ Failed batch insert {symbol}: {e}")
            conn.rollback()

    cur.close()
    conn.close()

def main():
    if test_database_connection():
        today_str = datetime.today().strftime("%Y-%m-%d")

        macro_df = fetch_macro_data(end_date=today_str)
        vix_df = fetch_vix_data()

        for sector, subsectors in SECTOR_STOCKS.items():
            for subsector, symbols in subsectors.items():
                for symbol in symbols:
                    print(f"📈 Updating {symbol} ({sector} - {subsector})...")
                    try:
                        df, market_data = fetch_stock_data_incremental(symbol)
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