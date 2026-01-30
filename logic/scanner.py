import pandas as pd
from sqlalchemy import create_engine, text
from logic.db_config import get_engine

engine = get_engine()

def run_scanner():
    print("🔍 SCANNING DATABASE FOR CANDIDATES...")
    
    # 1. Get Tickers
    tickers = pd.read_sql("SELECT symbol FROM tickers", engine)['symbol'].tolist()
    
    candidates = []
    
    # 2. We can do a lot of this via SQL for speed, but let's stick to Python for logic clarity
    # Fetch latest price for all stocks
    query = """
    SELECT ticker, close, date 
    FROM prices 
    WHERE date = (SELECT MAX(date) FROM prices)
    """
    try:
        df_latest = pd.read_sql(query, engine)
    except:
        print("Error reading prices. Run data_pipeline.py first.")
        return

    print(f"   > Analyzing {len(df_latest)} stocks...")

    # Simple Scanner Logic: Price > $10 (Example)
    # You can add more complex logic here (e.g. SMA 200)
    for _, row in df_latest.iterrows():
        if row['close'] > 10:
            candidates.append(row)
            
    df_scan = pd.DataFrame(candidates)
    
    # 3. Save to Postgres (Instead of CSV)
    table_name = "alpha_candidates"
    df_scan.to_sql(table_name, engine, if_exists='replace', index=False)
    
    print(f"✅ FOUND {len(df_scan)} CANDIDATES.")
    print(f"   Saved to DB table: '{table_name}'")

if __name__ == "__main__":
    run_scanner()