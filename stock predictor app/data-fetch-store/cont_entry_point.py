import psycopg2
import pandas as pd
from db_params import DB_CONFIG, ALLOWED_COLUMNS
from datetime import datetime

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def find_closest_valid_date(level, name, target_date):
    """Finds closest prior valid trading date if target_date missing."""
    conn = get_db_connection()
    cur = conn.cursor()

    table = "stock_market_table" if level == "company" else "sector_index_table"
    condition = "symbol = %s" if level == "company" else ("subsector = %s" if level == "subsector" else "sector = %s")

    query = f"""
        SELECT MAX(date)
        FROM {table}
        WHERE {condition} AND date <= %s
    """
    cur.execute(query, (name, target_date))
    result = cur.fetchone()

    conn.close()

    if result and result[0]:
        return result[0]
    else:
        return None

def fetch_data(level: str, name: str, columns: list, start_date: str, end_date: str):
    """Fetches data dynamically at company/subsector/sector level."""
    if level not in ALLOWED_COLUMNS:
        raise ValueError(f"Invalid level '{level}'. Must be one of {list(ALLOWED_COLUMNS.keys())}.")

    invalid_columns = [col for col in columns if col not in ALLOWED_COLUMNS[level]]
    if invalid_columns:
        raise ValueError(f"Invalid columns requested: {invalid_columns}")

    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()

    real_start = find_closest_valid_date(level, name, start_date_obj)
    real_end = find_closest_valid_date(level, name, end_date_obj)

    if not real_start:
        raise ValueError(f"No valid trading date found before {start_date} for {level} '{name}'.")
    if not real_end:
        raise ValueError(f"No valid trading date found before {end_date} for {level} '{name}'.")

    if real_start != start_date_obj:
        print(f"⚠️ Requested start date {start_date} not available. Using closest prior: {real_start}")

    if real_end != end_date_obj:
        print(f"⚠️ Requested end date {end_date} not available. Using closest prior: {real_end}")

    conn = get_db_connection()

    table = "stock_market_table" if level == "company" else "sector_index_table"
    condition = "symbol = %s" if level == "company" else ("subsector = %s" if level == "subsector" else "sector = %s")

    columns_sql = ", ".join(columns)

    query = f"""
        SELECT {columns_sql}
        FROM {table}
        WHERE {condition} AND date BETWEEN %s AND %s
        ORDER BY date
    """

    try:
        df = pd.read_sql(query, conn, params=[name, real_start, real_end])
        return df

    finally:
        conn.close()
