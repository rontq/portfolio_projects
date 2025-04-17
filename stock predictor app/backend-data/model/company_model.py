import os
import json
import psycopg2
import pandas as pd
import xgboost as xgb
import numpy as np
import gc  # For clearing memory
from sklearn.model_selection import train_test_split
from dotenv import load_dotenv
import joblib
from pathlib import Path



# === CONFIG ===
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../credentials/.env'))

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME"),
    "user": os.getenv("DB_USER"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST"),
    "port": os.getenv("DB_PORT"),
}

# List of company tickers
COMPANY_TICKERS = [
    "NVDA", "AMD", "INTC", "QCOM", "TXN", "AVGO", "MU", "ADI", "KLAC", "MRVL",  # Semiconductors
    "MSFT", "NOW", "ORCL", "PANW", "FTNT", "ADBE", "CRM", "SNOW", "WDAY", "DDOG",  # System Software
    "ACN", "IBM", "CDW", "EPAM", "CTSH", "INFY", "DXC", "GLOB", "GEN", "NTCT",  # IT Services & Consulting
    "AAPL", "HPQ", "DELL", "LOGI", "ANET", "ZBRA", "HPE", "NTAP", "STX", "WDC",  # Hardware & Peripherals
    
    "JPM", "BAC", "WFC", "C", "USB", "PNC", "TFC", "FITB", "KEY", "RF",  # Banks
    "GS", "MS", "BLK", "SCHW", "AMP", "TROW", "IVZ", "BX", "KKR", "APO",  # Investment Management
    "AIG", "CB", "MET", "PRU", "TRV", "ALL", "PGR", "HIG", "LNC", "CINF",  # Insurance
    "CME", "ICE", "NDAQ", "MKTX", "COIN", "INTU", "FIS", "GPN", "PYPL", "DFS",  # Exchanges & Financial Services
    
    "PFE", "MRK", "LLY", "BMY", "ABBV", "AMGN", "GILD", "VRTX", "ZTS", "REGN",  # Pharmaceuticals
    "MDT", "SYK", "BSX", "ISRG", "ZBH", "EW", "STE", "BAX", "TFX", "PKI",  # Healthcare Equipment
    "UNH", "CI", "HUM", "CNC", "MCK", "CAH", "HCA", "ELV", "MOH", "HCA",  # Healthcare Services
    "BIIB", "ILMN", "INCY", "NVAX", "EXEL", "CRSP", "BLUE", "ALNY", "GILD",  # Biotech & Research
    
    "AMZN", "HD", "LOW", "TGT", "BBY", "ROST", "TJX", "DG", "FIVE", "WSM",  # Retail
    "TSLA", "F", "GM", "HOG", "LCID", "RIVN", "NIO", "XPEV", "STLA", "TM",  # Automotive
    "MCD", "SBUX", "YUM", "CMG", "DPZ", "QSR", "WEN", "SHAK", "DNUT", "CAKE",  # Restaurants
    "BKNG", "MAR", "RCL", "LVS", "CCL", "H", "NCLH", "EXPE", "HLT", "TRIP",  # Travel & Leisure
    
    "BA", "LMT", "GD", "NOC", "RTX", "HII", "SPR", "TDG", "COL", "HEI",  # Aerospace & Defense
    "CAT", "DE", "PCAR", "SAND", "HON", "ITT", "CMI", "AOS", "MAN", "MTW",  # Machinery
    "UPS", "FDX", "CSX", "NSC", "WAB", "UNP", "LSTR", "ODFL", "JBHT", "UBER",  # Transportation
    "FLR", "KBR", "HIT", "TTEK", "STRL", "MTZ", "MTRX", "ACM", "PWR",  # Construction & Engineering
    
    "KO", "PEP", "MDLZ", "K", "GIS", "CPB", "KHC", "HSY", "TSN", "CAG",  # Food & Beverage
    "WMT", "COST", "KR", "TGT", "ACI", "SFM", "BJ", "WBA", "CVS", "CASY",  # Retail & Distribution
    "PG", "CL", "KMB", "CHD", "ECL", "NWL", "ENR", "SPB", "UL", "REYN",  # Household Products
    "MO", "PM", "STZ", "BUD", "TAP", "DEO", "DEO", "SAM", "HEINY", "CCEP",  # Tobacco & Alcohol
    
    "GOOGL", "META", "NFLX", "ZM", "TWLO", "DDOG", "DOCN", "ABNB", "DUOL", "YELP",  # Internet Services
    "DIS", "PARA", "FOXA", "WBD", "ROKU", "LYV", "IMAX", "SIRI", "SPOT", "CURI",  # Media & Entertainment
    "VZ", "T", "TMUS", "CHTR", "LUMN", "USM", "SHEN", "ATEX", "WOW",  # Telecom
    "EA", "TTWO", "RBLX", "HUYA", "BILI", "PLTK", "U", "SKLZ", "NTES",  # Gaming & Interactive Media
    
    "NEE", "DUK", "SO", "D", "EXC", "AEP", "ED", "XEL", "FE", "EIX",  # Electric Utilities
    "SRE", "NI", "UGI", "OKE", "ATO", "SWX", "NWN", "SR", "WMB", "CNP",  # Gas Utilities
    "RUN", "ENPH", "SEDG", "FSLR", "CWEN", "ORA", "TPIC",  # Renewables
    "AWK", "WTRG", "SJW", "YORW", "MSEX", "AWR", "CWCO", "ARTNA", "SBS"  # Water Utilities
]


MODEL_DIR = "trained_company_models"
SAVE_DIR = Path(MODEL_DIR)
SAVE_DIR.mkdir(exist_ok=True)

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def fetch_company_data(conn, symbol):
    query = """
    SELECT * FROM stock_market_table
    WHERE symbol = %s
    ORDER BY symbol, date;
    """
    return pd.read_sql(query, conn, params=(symbol,))

def preprocess(df):
    df = df.copy()

    ohlcv_cols = ['open', 'close', 'high', 'low', 'volume']
    df = df.dropna(subset=ohlcv_cols)

    df = df.drop(columns=['date', 'symbol', 'sector', 'subsector'], errors='ignore')

    # Convert pe_ratio safely to float
    if 'pe_ratio' in df.columns:
        df['pe_ratio'] = pd.to_numeric(df['pe_ratio'], errors='coerce')  # Turn anything non-numeric into NaN
        if df['pe_ratio'].isna().all():
            df.drop(columns=['pe_ratio'], inplace=True)

    # Drop any other non-numeric columns silently
    df = df.select_dtypes(include=[np.number])

    # Prepare the target variable (next day's close)
    target = df['close'].shift(-1)
    df = df.iloc[:-1]
    target = target.iloc[:-1]

    if df.empty or target.empty:
        raise ValueError("No valid data left after preprocessing.")

    return df, target



def train_model(X, y):
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.1, shuffle=False)

    y_train = y_train.dropna()
    y_test = y_test.dropna()

    model = xgb.XGBRegressor(objective="reg:squarederror", n_estimators=100, enable_categorical=True)
    model.fit(X_train, y_train)

    preds = model.predict(X_test)
    min_len = min(len(y_test), len(preds))
    y_test = y_test.iloc[:min_len]
    preds = preds[:min_len]

    mask = ~np.isnan(y_test) & ~np.isnan(preds)
    y_test = y_test[mask]
    preds = preds[mask]

    return model, y_test, preds

def save_results(sector, model, feature_names):
    save_path = SAVE_DIR / f"{sector}_model.joblib"
    
    # Combine model and metadata into a single dictionary
    data_to_save = {
        "sector": sector,
        "model": model,
        "features": feature_names
    }

    # Save the entire bundle using joblib
    joblib.dump(data_to_save, save_path)
    print(f"📦 Model and metadata saved to {save_path}")


def main():
    conn = get_db_connection()
    trained_companies = []

    # Always train through all companies
    for symbol in COMPANY_TICKERS:
        print(f"\n--- Training model for company: {symbol} ---")
        gc.collect()  # Clear memory cache between training
        print("Clearing cache")
        df = fetch_company_data(conn, symbol)
        if df.empty:
            print(f"⚠️ No data found for {symbol}, skipping.")
            continue

        X, y = preprocess(df)
        if X.empty or y.isnull().all():
            print(f"⚠️ Not enough data after preprocessing for {symbol}, skipping.")
            continue

        model, y_test, preds  = train_model(X, y)
        print(f"✅ Finished training {symbol}")

        save_results(symbol, model, X.columns.tolist())
        trained_companies.append(symbol)

    conn.close()
    print(f"\n✅ Trained companies: {trained_companies}")
    print("\n🎉 All company models trained and saved.")

if __name__ == "__main__":
    main()
