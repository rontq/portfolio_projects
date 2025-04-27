import time
import yfinance as yf
import pandas as pd
import psycopg2
from psycopg2 import OperationalError, sql
from db_params import DB_CONFIG, test_database_connection, create_table
from stock_list import SECTOR_STOCKS

from ta.trend import SMAIndicator, EMAIndicator, MACD
from ta.momentum import RSIIndicator
from ta.volume import OnBalanceVolumeIndicator
from ta.volatility import BollingerBands

# For masking purposes
SECTOR_IDS = {name: idx for idx, name in enumerate(SECTOR_STOCKS.keys(), 1)}
SUBSECTOR_IDS = {
    subsector: idx
    for sector in SECTOR_STOCKS
    for idx, subsector in enumerate(SECTOR_STOCKS[sector].keys(), 1)
}

# Symbol ID mapping
ALL_SYMBOLS = sorted({symbol for sector in SECTOR_STOCKS.values() for subsector in sector.values() for symbol in subsector})
SYMBOL_IDS = {symbol: idx for idx, symbol in enumerate(ALL_SYMBOLS, 1)}

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
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    sector_id = SECTOR_IDS.get(sector, None)
    subsector_id = SUBSECTOR_IDS.get(subsector, None)
    symbol_id = SYMBOL_IDS.get(symbol)

    df = df.merge(vix_df, on="date", how="left")

    for _, row in df.iterrows():
        if pd.isna(row["date"]):
            continue
        try:
            cur.execute(
                sql.SQL("""
                    INSERT INTO stock_market_table (
                        symbol, sector, subsector, date, day_of_week, week_of_year, is_adr, symbol_id,
                        open, high, low, close, volume, adj_close,
                        sma_5, sma_20, sma_50, sma_125, sma_200, sma_200_weekly,
                        ema_5, ema_20, ema_50, ema_125, ema_200,
                        macd, dma, rsi,
                        bollinger_upper, bollinger_middle, bollinger_lower, obv,
                        pe_ratio, forward_pe, price_to_book,
                        volatility_5d, volatility_10d, volatility_20d, volatility_40d,
                        market_cap, market_cap_proxy,
                        sector_id, subsector_id,
                        sector_weight, subsector_weight, vix_close
                    ) VALUES (
                        %s, %s, %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s, %s,
                        %s, %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s, %s,
                        %s, %s, %s, %s,
                        %s, %s,
                        %s, %s,
                        %s, %s, %s
                    )
                    ON CONFLICT (symbol, date) DO NOTHING
                """),
                (
                    symbol, sector, subsector, row["date"], row["day_of_week"], row["week_of_year"], market_data.get("is_adr", False), symbol_id,
                    row["open"], row["high"], row["low"], row["close"], row["volume"], row["adj_close"],
                    row.get("sma_5"), row.get("sma_20"), row.get("sma_50"), row.get("sma_125"), row.get("sma_200"), row.get("sma_200_weekly"),
                    row.get("ema_5"), row.get("ema_20"), row.get("ema_50"), row.get("ema_125"), row.get("ema_200"),
                    row.get("macd"), row.get("dma"), row.get("rsi"),
                    row.get("bollinger_upper"), row.get("bollinger_middle"), row.get("bollinger_lower"), row.get("obv"),
                    market_data["pe_ratio"], market_data["forward_pe"], market_data["price_to_book"],
                    row.get("volatility_5d"), row.get("volatility_10d"), row.get("volatility_20d"), row.get("volatility_40d"),
                    market_data["market_cap"], row.get("market_cap_proxy"),
                    sector_id, subsector_id,
                    row.get("sector_weight"), row.get("subsector_weight"), row.get("vix_close")
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

def main():
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


if __name__ == "__main__":
    main()
