# entrypoint.py

import psycopg2
import pandas as pd
from db_params import DB_CONFIG, ALLOWED_COLUMNS
from datetime import datetime

import upd_company_weight
import upd_data_fetch
import upd_index_sector_calc
import upd_index_subsector_calc


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_data(level: str, name: str, columns: list, start_date: str, end_date: str = None):
    if level not in ALLOWED_COLUMNS:
        raise ValueError(f"Invalid level '{level}'. Must be one of {list(ALLOWED_COLUMNS.keys())}.")

    invalid_columns = [col for col in columns if col not in ALLOWED_COLUMNS[level]]
    if invalid_columns:
        raise ValueError(f"Invalid columns requested: {invalid_columns}")

    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date() if end_date else None

    table = "stock_market_table" if level == "company" else "sector_index_table"
    condition = "symbol = %s" if level == "company" else ("subsector = %s" if level == "subsector" else "sector = %s")

    columns_sql = ", ".join(columns)
    conn = get_db_connection()

    try:
        if end_date_obj:
            query = f"""
                SELECT {columns_sql}
                FROM {table}
                WHERE {condition} AND date BETWEEN %s AND %s
                ORDER BY date
            """
            params = [name, start_date_obj, end_date_obj]
        else:
            query = f"""
                SELECT {columns_sql}
                FROM {table}
                WHERE {condition} AND date = %s
                ORDER BY date
            """
            params = [name, start_date_obj]

        df = pd.read_sql(query, conn, params=params)

        if df.empty:
            print("⚠️ No info detected: Trades not available during Weekends or Federal Vacations.")
            return pd.DataFrame()
        else:
            return df

    finally:
        conn.close()

def update_database():
    upd_data_fetch.main()

def update_sector_index():
    upd_index_sector_calc.main()

def update_subsector_index():
    upd_index_subsector_calc.main()

def update_company_weight():
    upd_company_weight.main()


def main():
    print("⚙️ Running full update pipeline...")
    update_database()
    update_sector_index()
    update_subsector_index()
    update_company_weight()
    print("✅ All update modules completed.")

if __name__ == "__main__":
    main()
