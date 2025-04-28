import psycopg2
from psycopg2 import OperationalError
import os
from dotenv import load_dotenv

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../credentials/.env'))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

def test_database_connection():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        conn.close()
        print("Successfully connected to PostgreSQL database.")
        return True
    except OperationalError as e:
        print("Could not connect to the database:", e)
        return False

def create_table():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        cur = conn.cursor()
        with open("schema/company.schema.sql", "r") as f:
            cur.execute(f.read())
        conn.commit()
        cur.close()
        conn.close()
        print("Table created or already exists.")
    except Exception as e:
        print("Error creating table:", e)