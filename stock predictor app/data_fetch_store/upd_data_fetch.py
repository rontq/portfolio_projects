import time
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
from db_params import DB_CONFIG, test_database_connection, api_key
from stock_list import SECTOR_STOCKS, MACRO_CODES
from fredapi import Fred
from datetime import datetime, timedelta

from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator
from ta.volatility import BollingerBands

SECTOR_IDS = {sector: idx for idx, sector in enumerate(SECTOR_STOCKS.keys(), 1)}

all_subsectors = []
for sector, subsectors in SECTOR_STOCKS.items():
    all_subsectors.extend(subsectors.keys())
SUBSECTOR_IDS = {subsector: idx for idx, subsector in enumerate(sorted(set(all_subsectors)), 1)}

ALL_SYMBOLS = sorted({symbol for sector in SECTOR_STOCKS.values() for subsector in sector.values() for symbol in subsector})
SYMBOL_IDS = {symbol: idx for idx, symbol in enumerate(ALL_SYMBOLS, 1)}

def get_latest_global_date():
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()
    try:
        cur.execute("SELECT MAX(date) FROM stock_market_table;")
        result = cur.fetchone()
        return result[0] if result and result[0] else None
    finally:
        cur.close()
        conn.close()

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

def fetch_macro_data(start_date):
    fred = Fred(api_key=api_key)
    end_date = datetime.today().date().isoformat()
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
        ffill_cols = [col for col in macro_df.columns if col != "date" and col != "breakeven_inflation_rate"]
        macro_df.sort_values("date", inplace=True)
        macro_df[ffill_cols] = macro_df[ffill_cols].ffill()
        return macro_df
    else:
        return pd.DataFrame()

def fetch_stock_data(symbol, start_date):
    buffer_days = 300  # For SMA/EMA continuity
    history_start = start_date - timedelta(days=buffer_days)

    for _ in range(3):
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=history_start)
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
            print(f"⏳ Retry for {symbol}: {e}")
            time.sleep(2)
    else:
        print(f"❌ Failed all retries for {symbol}")
        return None, None

    try:
        close = df["close"]
        volume = df["volume"]

        for w in [5, 20, 50, 125, 200]:
            df[f"sma_{w}"] = SMAIndicator(close, window=w).sma_indicator()
            df[f"ema_{w}"] = EMAIndicator(close, window=w).ema_indicator()

        df["macd"] = MACD(close).macd_diff()
        df["dma"] = close - df["sma_50"]
        df["rsi"] = RSIIndicator(close).rsi()

        bb = BollingerBands(close)
        df["bollinger_upper"] = bb.bollinger_hband()
        df["bollinger_middle"] = bb.bollinger_mavg()
        df["bollinger_lower"] = bb.bollinger_lband()
        df["obv"] = OnBalanceVolumeIndicator(close, volume).on_balance_volume()
        df["sma_200_weekly"] = close.rolling(window=1000).mean()

        df["adj_close"] = close
        df["market_cap_proxy"] = df["close"] * df["volume"]
        df["date"] = pd.to_datetime(df["date"])
        df["day_of_week"] = df["date"].dt.dayofweek + 1
        df["week_of_year"] = df["date"].dt.isocalendar().week

        for w in [5, 10, 20, 40]:
            df[f"volatility_{w}d"] = df["close"].rolling(w).std()

        df["date"] = df["date"].dt.date
        df = df[df["date"] >= start_date]  # filter only new rows to insert
        return df, market_data

    except Exception as e:
        print(f"⚠️ Indicator calc failed for {symbol}: {e}")
        return None, None
    
def insert_data(symbol, sector, subsector, df, market_data):
    symbol_id = SYMBOL_IDS[symbol]
    sector_id = SECTOR_IDS[sector]
    subsector_id = SUBSECTOR_IDS[subsector]

    insert_rows = []
    for _, row in df.iterrows():
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
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
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
            print(f"✅ Inserted {symbol} ({len(insert_rows)} rows)")
        except Exception as e:
            print(f"❌ Insert failed for {symbol}: {e}")
            conn.rollback()
        finally:
            cur.close()
            conn.close()
    else:
        print(f"⚠️ No rows to insert for {symbol}")

def main():
    if not test_database_connection():
        print("❌ DB Connection failed.")
        return

    latest_date = get_latest_global_date()
    if not latest_date:
        print("❌ No existing data found in DB.")
        return

    start_date = latest_date + timedelta(days=1)
    while start_date.weekday() >= 5:
        start_date += timedelta(days=1)
    
    

    macro_df = fetch_macro_data(start_date)
    vix_df = fetch_vix_data(start_date)

    for sector, subsectors in SECTOR_STOCKS.items():
        for subsector, symbols in subsectors.items():
            for symbol in symbols:
                print(f"📈 Updating {symbol} ({sector} - {subsector})...")
                try:
                    df, market_data = fetch_stock_data(symbol, start_date)
                    if df is not None and not df.empty:
                        df = df.merge(vix_df, on="date", how="left")
                        if not macro_df.empty:
                            df = df.merge(macro_df, on="date", how="left")
                        insert_data(symbol, sector, subsector, df, market_data)
                except Exception as e:
                    print(f"⚠️ Failed to update {symbol}: {e}")

if __name__ == "__main__":
    main()
