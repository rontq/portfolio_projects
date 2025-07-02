# entrypoint.py

import psycopg2
from db_params import DB_CONFIG

import upd_company_weight
import upd_data_fetch
import upd_index_sector_calc
import upd_index_subsector_calc
import db_extract


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def update_database():
    upd_data_fetch.main()

def update_sector_index():
    upd_index_sector_calc.main()

def update_subsector_index():
    upd_index_subsector_calc.main()

def update_company_weight():
    upd_company_weight.main()

def fetch_data():
    db_extract.fetch_entity_data()

def main():
    print("⚙️ Running full update pipeline...")
    update_database()
    update_sector_index()
    update_subsector_index()
    update_company_weight()
    print("✅ All update modules completed.")

if __name__ == "__main__":
    main()
