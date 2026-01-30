import pandas as pd
from sqlalchemy import create_engine, text
from logic.db_config import get_engine

# --- CONFIGURATION ---
# DB_URI = "postgresql://quant:password@localhost:5432/stock_master"
# engine = create_engine(DB_URI)
engine = get_engine()

def reset_database():
    print("🧹 STARTING HOUSE CLEANING...")
    
    with engine.connect() as conn:
        conn.execution_options(isolation_level="AUTOCOMMIT")
        
        # 1. DROP OLD TABLES
        print("   - Dropping messy tables...")
        conn.execute(text("DROP TABLE IF EXISTS prices;"))
        conn.execute(text("DROP TABLE IF EXISTS fundamentals;"))
        conn.execute(text("DROP TABLE IF EXISTS tickers;"))
        conn.execute(text("DROP TABLE IF EXISTS quant_rankings;")) # Also clear model results
        
        # 2. CREATE TICKERS TABLE (The Master List)
        print("   - Creating 'tickers' table...")
        conn.execute(text("""
            CREATE TABLE tickers (
                symbol TEXT PRIMARY KEY,
                company_name TEXT,
                sector TEXT,
                source_index TEXT
            );
        """))
        
        # 3. CREATE PRICES TABLE (With Unique Constraint on Ticker + Date)
        print("   - Creating 'prices' table (Strict Mode)...")
        conn.execute(text("""
            CREATE TABLE prices (
                date TIMESTAMP,
                ticker TEXT,
                open REAL,
                high REAL,
                low REAL,
                close REAL,
                volume REAL,
                PRIMARY KEY (ticker, date)
            );
        """))
        
        # 4. CREATE FUNDAMENTALS TABLE (With Unique Constraint on Ticker)
        print("   - Creating 'fundamentals' table...")
        conn.execute(text("""
            CREATE TABLE fundamentals (
                ticker TEXT PRIMARY KEY,
                pe_ratio REAL,
                forward_pe REAL,
                peg_ratio REAL,
                market_cap REAL,
                book_value REAL,
                dividend_yield REAL,
                profit_margin REAL,
                beta REAL,
                price_to_book REAL,
                free_cash_flow REAL
            );
        """))
        
        # 5. CREATE QUANT RANKINGS TABLE (For Model Output)
        print("   - Creating 'quant_rankings' table...")
        conn.execute(text("""
            CREATE TABLE quant_rankings (
                ticker TEXT PRIMARY KEY,
                "TOTAL_SCORE" REAL,
                current_price REAL,
                pe_ratio REAL,
                profit_margin REAL,
                momentum_pct REAL
            );
        """))
        
    print("✨ HOUSE CLEANING COMPLETE. Database is spotless.")

if __name__ == "__main__":
    reset_database()