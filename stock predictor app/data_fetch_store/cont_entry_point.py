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

def fetch_data(level: str, name: str, columns: list, start_date: str, end_date: str = None):
    """Fetch data at company / subsector / sector level with smart date handling."""
    if level not in ALLOWED_COLUMNS:
        raise ValueError(f"Invalid level '{level}'. Must be one of {list(ALLOWED_COLUMNS.keys())}.")

    invalid_columns = [col for col in columns if col not in ALLOWED_COLUMNS[level]]
    if invalid_columns:
        raise ValueError(f"Invalid columns requested: {invalid_columns}")

    # Parse dates safely
    start_date_obj = datetime.strptime(start_date, "%Y-%m-%d").date()

    if end_date:
        end_date_obj = datetime.strptime(end_date, "%Y-%m-%d").date()
    else:
        end_date_obj = None

    table = "stock_market_table" if level == "company" else "sector_index_table"
    condition = "symbol = %s" if level == "company" else ("subsector = %s" if level == "subsector" else "sector = %s")

    columns_sql = ", ".join(columns)

    conn = get_db_connection()

    try:
        if end_date_obj:
            # Fetch a RANGE
            query = f"""
                SELECT {columns_sql}
                FROM {table}
                WHERE {condition} AND date BETWEEN %s AND %s
                ORDER BY date
            """
            params = [name, start_date_obj, end_date_obj]
        else:
            # Fetch SINGLE date only
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

def update_database(force_update: bool = False, start_date: datetime.date = None):
    upd_data_fetch.main(force_update=force_update, start_date=start_date)

def update_sector_index(force_update: bool = False, start_date: datetime.date = None):
    upd_index_sector_calc.calculate_sector(force_update=force_update, start_date=start_date)

def update_subsector_index(force_update: bool = False, start_date: datetime.date = None):
    upd_index_subsector_calc.calculate_subsector(force_update=force_update, start_date=start_date)

def update_company_weight(force_update: bool = False, start_date: datetime.date = None):
    upd_company_weight.calculate_company_weight(force_update=force_update, start_date=start_date)


def cli_entry():
    import sys

    if len(sys.argv) < 2:
        print("⚠️ Usage: python entry.py <command> [parameters]")
        print("Available commands: fetch_data, update_database, update_sector_index, update_subsector_index, update_subsector_vol_sma, update_company_weight")
        return

    command = sys.argv[1]

    if command == "fetch_data":
        if len(sys.argv) < 6:
            print("⚠️ Usage: python entry.py fetch_data <level> <name> <start_date> <columns_comma_separated> [end_date_optional]")
            return

        level = sys.argv[2]
        name = sys.argv[3]
        start_date = sys.argv[4]
        columns = sys.argv[5].split(",")
        end_date = sys.argv[6] if len(sys.argv) > 6 else None

        df = fetch_data(level, name, columns, start_date, end_date)
        print(df)
    elif command == "update_database":
        update_database()
    elif command == "update_sector_index":
        update_sector_index()
    elif command == "update_subsector_index":
        update_subsector_index()
    elif command == "update_company_weight":
        update_company_weight()
    else:
        print(f"❌ Unknown command '{command}'")


if __name__ == "__main__":
    cli_entry()