import psycopg2
import os
import pandas as pd
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from data_fetch_store.db_params import DB_CONFIG

def fetch_sector_index_data(sector_index_name):
    conn = psycopg2.connect(**DB_CONFIG)
    query = """
        SELECT date, index_value
        FROM sector_index_table
        WHERE sector_index = %s
        ORDER BY date
    """
    df = pd.read_sql(query, conn, params=(sector_index_name,))
    conn.close()
    return df

def plot_sector_index(df, sector_index_name):
    plt.figure(figsize=(12, 6))
    plt.plot(df['date'], df['index_val'], label=sector_index_name)
    plt.xlabel('Date')
    plt.ylabel('Index Value')
    plt.title(f'{sector_index_name} Over Time')
    plt.legend()
    plt.grid(True)
    plt.tight_layout()
    plt.show()

if __name__ == "__main__":
    sector = "Financials"  # You can change this
    df = fetch_sector_index_data(sector)
    if not df.empty:
        plot_sector_index(df, sector)
    else:
        print(f"No data found for {sector}")
