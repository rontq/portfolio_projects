# fetch_entity_data.py

import psycopg2
import pandas as pd
from datetime import datetime
from db_params import DB_CONFIG

EXCLUDE_MACRO_COLUMNS = {
    "cpi_inflation",
    "core_cpi_inflation",
    "pce_inflation",
    "core_pce_inflation",
    "breakeven_inflation_rate",
    "realized_inflation",
    "us_10y_bond_rate",
    "retail_sales",
    "consumer_confidence_index",
    "nfp",
    "unemployment_rate",
    "effective_federal_funds_rate"
}

CORE_COLUMNS = [
    "open", "high", "low", "close", "volume", "adj_close"
]

EXPANDED_COLUMNS = CORE_COLUMNS + [
    "sma_5", "sma_20", "sma_50", "sma_125", "sma_200", "sma_200_weekly",
    "ema_5", "ema_20", "ema_50", "ema_125", "ema_200",
    "macd", "dma", "rsi"
]


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def get_closest_date_for_entity(entity_type, entity_name, target_date):
    table = "stock_market_table" if entity_type == "company" else "sector_index_table"
    field = {
        "company": "symbol",
        "subsector": "subsector",
        "sector": "sector"
    }[entity_type]

    with get_db_connection() as conn, conn.cursor() as cur:
        cur.execute(f"""
            SELECT MAX(date)
            FROM {table}
            WHERE {field} = %s AND date <= %s
        """, (entity_name, target_date))
        result = cur.fetchone()
        return result[0] if result else None


def fetch_entity_data(entity_name, date_str=None, mode="MAX"):
    date_obj = None
    if date_str and date_str.upper() not in {"CORE", "EXPANDED", "MAX"}:
        try:
            date_obj = datetime.strptime(date_str, "%m-%d-%Y").date()
        except ValueError:
            print(f"❌ Invalid date format: {date_str}. Please use MM-DD-YYYY.")
            return pd.DataFrame()

    if date_str and date_str.upper() in {"CORE", "EXPANDED", "MAX"}:
        mode = date_str.upper()
        date_obj = None

    entity_type = "company"
    if len(entity_name) > 5:
        if any(c in entity_name.lower() for c in ["sector"]):
            entity_type = "sector"
        elif any(c in entity_name.lower() for c in ["semi", "bank", "tech", "sub"]):
            entity_type = "subsector"

    table = "stock_market_table" if entity_type == "company" else "sector_index_table"
    column = {
        "company": "symbol",
        "subsector": "subsector",
        "sector": "sector"
    }[entity_type]

    if not date_obj:
        with get_db_connection() as conn:
            df = pd.read_sql(f"""
                SELECT * FROM {table}
                WHERE {column} = %s
                ORDER BY date DESC LIMIT 1
            """, conn, params=(entity_name,))
    else:
        closest = get_closest_date_for_entity(entity_type, entity_name, date_obj)
        if not closest:
            print(f"⚠️ No historical data available for {entity_name} at or before {date_obj}.")
            return pd.DataFrame()

        with get_db_connection() as conn:
            df = pd.read_sql(f"""
                SELECT * FROM {table}
                WHERE {column} = %s AND date = %s
            """, conn, params=(entity_name, closest))

    if df.empty:
        return pd.DataFrame()

    df = df.drop(columns=[col for col in df.columns if col in EXCLUDE_MACRO_COLUMNS], errors='ignore')

    if mode == "CORE":
        df = df[[col for col in CORE_COLUMNS if col in df.columns]]
    elif mode == "EXPANDED":
        df = df[[col for col in EXPANDED_COLUMNS if col in df.columns]]

    return df


if __name__ == "__main__":
    import sys

    if len(sys.argv) < 2:
        print("⚠️ Usage: python fetch_entity_data.py <entity_name> [MM-DD-YYYY] [CORE|EXPANDED|MAX]")
        sys.exit(1)

    entity_name = sys.argv[1]
    date_str = sys.argv[2] if len(sys.argv) > 2 else None
    mode = sys.argv[3].upper() if len(sys.argv) > 3 else "MAX"

    df = fetch_entity_data(entity_name, date_str, mode)
    if not df.empty:
        print(df.T)
    else:
        print("⚠️ No data found.")
