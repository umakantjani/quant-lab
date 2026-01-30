import pandas as pd
import requests
from io import StringIO
from sqlalchemy import create_engine, text
from logic.db_config import get_engine

# --- CONFIGURATION ---
# DB_URI = "postgresql://quant:password@localhost:5432/stock_master"
# engine = create_engine(DB_URI)
engine = get_engine()

HEADERS = {
    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.114 Safari/537.36"
}

def clean_columns(df):
    """Standardizes column names to avoid key errors."""
    df.columns = [c.lower().strip() for c in df.columns]
    return df

def fetch_html_table(url, match_str):
    """Fetches HTML and finds the table containing a specific string."""
    try:
        response = requests.get(url, headers=HEADERS)
        response.raise_for_status()
        tables = pd.read_html(StringIO(response.text))
        
        for t in tables:
            # Convert columns to string to avoid errors
            cols = [str(c).lower() for c in t.columns]
            if match_str.lower() in cols:
                return t
        return pd.DataFrame()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return pd.DataFrame()

def get_sp500():
    print("   > Fetching S&P 500...")
    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    # Look for table with 'Symbol'
    df = fetch_html_table(url, 'Symbol')
    
    if not df.empty:
        df = clean_columns(df)
        # Rename known columns to our standard
        rename_map = {'symbol': 'symbol', 'security': 'company_name', 'gics sector': 'sector'}
        df = df.rename(columns=rename_map)
        
        # Select only what we need, filling missing cols if structure changed
        if 'sector' not in df.columns: df['sector'] = 'Unknown'
        
        df = df[['symbol', 'company_name', 'sector']]
        df['source_index'] = 'SP500'
        return df
    return pd.DataFrame()

def get_nasdaq100():
    print("   > Fetching Nasdaq 100...")
    url = "https://en.wikipedia.org/wiki/Nasdaq-100"
    # Look for table with 'Ticker'
    df = fetch_html_table(url, 'Ticker')
    
    if not df.empty:
        df = clean_columns(df)
        rename_map = {'ticker': 'symbol', 'company': 'company_name', 'gics sector': 'sector', 'sector': 'sector', 'industry': 'sector'}
        df = df.rename(columns=rename_map)
        
        if 'sector' not in df.columns: df['sector'] = 'Technology' # Fallback
        
        df = df[['symbol', 'company_name', 'sector']]
        df['source_index'] = 'NASDAQ100'
        return df
    return pd.DataFrame()

def get_dow():
    print("   > Fetching Dow Jones...")
    url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
    # Look for table with 'Symbol'
    df = fetch_html_table(url, 'Symbol')
    
    if not df.empty:
        df = clean_columns(df)
        rename_map = {'symbol': 'symbol', 'company': 'company_name', 'sector': 'sector', 'industry': 'sector'}
        df = df.rename(columns=rename_map)
        
        # Dow table often lacks a 'Sector' column in the main view on Wikipedia now
        if 'sector' not in df.columns: df['sector'] = 'Large Cap' 
        
        # Ensure we have the right columns
        cols_to_keep = ['symbol', 'company_name', 'sector']
        # If any are missing, fill them
        for c in cols_to_keep:
            if c not in df.columns: df[c] = 'N/A'
            
        df = df[cols_to_keep]
        df['source_index'] = 'DOW'
        return df
    return pd.DataFrame()

def update_master_ticker_list():
    print("📥 INGESTING MARKET INDICES...")
    
    df1 = get_sp500()
    print(f"     - S&P 500: {len(df1)} found")
    
    df2 = get_nasdaq100()
    print(f"     - Nasdaq 100: {len(df2)} found")
    
    df3 = get_dow()
    print(f"     - Dow Jones: {len(df3)} found")
    
    # Concatenate all
    master_df = pd.concat([df1, df2, df3])
    
    if master_df.empty:
        print("❌ CRITICAL: No tickers found. Check internet connection or Wikipedia format.")
        return

    # CLEANUP: Replace dots with dashes (BRK.B -> BRK-B) for Yahoo
    master_df['symbol'] = master_df['symbol'].astype(str).str.replace('.', '-', regex=False)
    
    # DEDUPLICATE: Keep first occurrence
    master_df = master_df.drop_duplicates(subset=['symbol'])
    
    print(f"   > TOTAL UNIQUE TICKERS: {len(master_df)}")
    
    # SAVE TO DB
    try:
        master_df.to_sql('tickers', engine, if_exists='replace', index=False)
        # Re-add primary key
        with engine.connect() as conn:
            conn.execute(text("ALTER TABLE tickers ADD PRIMARY KEY (symbol);"))
        print("✅ MASTER TICKER LIST UPDATED in DB.")
    except Exception as e:
        print(f"❌ DB ERROR: {e}")

if __name__ == "__main__":
    update_master_ticker_list()