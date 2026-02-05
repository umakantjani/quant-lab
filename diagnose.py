import pandas as pd
from sqlalchemy import create_engine, text
from logic.db_config import get_engine

# --- CONFIGURATION ---
engine = get_engine()

def diagnose():
    print("🩺 STARTING DATABASE DIAGNOSTICS...\n")
    
    with engine.connect() as conn:
        # 1. CHECK TABLES EXIST
        print("--- 1. TABLE CHECK ---")
        inspector = pd.read_sql("SELECT table_name FROM information_schema.tables WHERE table_schema='public'", conn)
        tables = inspector['table_name'].tolist()
        print(f"Tables found: {tables}")
        
        required = ['tickers', 'prices', 'fundamentals', 'quant_rankings']
        missing = [t for t in required if t not in tables]
        if missing:
            print(f"❌ CRITICAL ERROR: Missing tables: {missing}")
            print("   -> Run 'setup_db.py' immediately.")
            return
        else:
            print("✅ All required tables exist.")

        # 2. CHECK ROW COUNTS
        print("\n--- 2. ROW COUNTS ---")
        
        # Tickers
        n_tickers = pd.read_sql("SELECT count(*) FROM tickers", conn).iloc[0,0]
        print(f"Tickers (Master List): {n_tickers}")
        
        # Prices
        n_prices = pd.read_sql("SELECT count(*) FROM prices", conn).iloc[0,0]
        n_price_tickers = pd.read_sql("SELECT count(DISTINCT ticker) FROM prices", conn).iloc[0,0]
        print(f"Price Rows: {n_prices} (across {n_price_tickers} tickers)")
        
        # Fundamentals
        n_fund = pd.read_sql("SELECT count(*) FROM fundamentals", conn).iloc[0,0]
        print(f"Fundamental Rows:      {n_fund}")
        
        # Rankings
        n_rank = pd.read_sql("SELECT count(*) FROM quant_rankings", conn).iloc[0,0]
        print(f"Model Rankings:        {n_rank}")
        
        # 3. ANALYSIS
        print("\n--- 3. DIAGNOSIS ---")
        if n_tickers == 0:
            print("❌ ERROR: No tickers defined.")
            print("   -> SOLUTION: Run 'ingest_tickers.py'")
        elif n_prices == 0:
            print("❌ ERROR: No price data.")
            print("   -> SOLUTION: Run 'data_pipeline.py'")
        elif n_fund == 0:
            print("❌ ERROR: No fundamental data (P/E, Margins).")
            print("   -> REASON: Yahoo Finance likely blocked the scraper or the script crashed.")
            print("   -> SOLUTION: Run 'data_pipeline.py' again. If it fails, we need a better fundamental source.")
        elif n_rank == 0:
            print("⚠️ WARNING: Data exists, but Model produced 0 results.")
            print("   -> REASON: The Inner Join failed. Tickers in 'prices' might not match 'fundamentals'.")
            print("   -> SOLUTION: Run 'model_engine.py' and check the logs.")
        else:
            print("✅ SYSTEM LOOKS HEALTHY. The App should work.")

if __name__ == "__main__":
    diagnose()