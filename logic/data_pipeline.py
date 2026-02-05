import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import time
import sys
# import requests  <-- REMOVED: No longer needed
from db_config import get_engine

# --- CONFIGURATION ---
engine = get_engine()

def get_master_tickers():
    try:
        df = pd.read_sql("SELECT symbol FROM tickers", engine)
        return df['symbol'].tolist()
    except:
        return []

def fetch_and_save(ticker):
    # REMOVED session=session. yfinance handles stealth automatically now.
    stock = yf.Ticker(ticker)
    
    # 1. PRICES (Standard History)
    try:
        df_p = stock.history(period="2y")
        
        if not df_p.empty:
            df_p = df_p.reset_index()
            df_p.columns = [c.lower() for c in df_p.columns]
            df_p['ticker'] = ticker
            
            # Timezone Fix
            col_date = 'date' if 'date' in df_p.columns else 'datetime'
            if col_date in df_p.columns:
                 df_p['date'] = pd.to_datetime(df_p[col_date]).dt.tz_localize(None)

            df_p = df_p[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
            
            # Clean Insert
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM prices WHERE ticker = '{ticker}'"))
                conn.commit()
            
            df_p.to_sql('prices', engine, if_exists='append', index=False)
    except Exception as e:
        pass 

    # 2. FUNDAMENTALS (The Problem Area)
    try:
        # Strategy A: Try the standard .info
        info = stock.info
        
        # Strategy B: Use fast_info (Reliable fallback for price/mkt cap)
        fast = stock.fast_info
        
        # Safe Extraction Helper
        def get_val(key, fallback=None):
            return info.get(key, fallback)

        # -- FALLBACK CALCULATIONS --
        # If P/E is missing, try to calculate it: Price / EPS
        pe = get_val('trailingPE')
        if pe is None:
            try:
                price = fast.last_price
                eps = info.get('trailingEps')
                if price and eps: pe = price / eps
            except: pe = None

        market_cap = get_val('marketCap')
        if market_cap is None:
            try: market_cap = fast.market_cap
            except: market_cap = None

        data = {
            'ticker': ticker,
            'pe_ratio': pe,
            'forward_pe': get_val('forwardPE'),
            'peg_ratio': get_val('pegRatio'),
            'market_cap': market_cap,
            'book_value': get_val('bookValue'),
            'dividend_yield': get_val('dividendYield'),
            'profit_margin': get_val('profitMargins'),
            'beta': get_val('beta'),
            'price_to_book': get_val('priceToBook'),
            'free_cash_flow': get_val('freeCashflow')
        }
        
        # Validation: If we missed both Market Cap AND P/E, we probably got blocked or data is empty
        if data['market_cap'] is None and data['pe_ratio'] is None:
            return False 

        df_f = pd.DataFrame([data])
        
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM fundamentals WHERE ticker = '{ticker}'"))
            conn.commit()
            
        df_f.to_sql('fundamentals', engine, if_exists='append', index=False)
        return True
        
    except Exception as e:
        return False

if __name__ == "__main__":
    tickers = get_master_tickers()
    total = len(tickers)
    
    if not tickers:
        print("ERROR: No tickers found. Run ingest_tickers.py first!")
    else:
        print(f"🚀 STARTING PIPELINE FOR {total} TICKERS...")
        start_time = time.time()
        
        success_count = 0
        
        for i, t in enumerate(tickers):
            # No need for manual sleep if using built-in stealth, 
            # but a tiny pause helps prevent generic rate limits.
            time.sleep(0.1) 
            
            success = fetch_and_save(t)
            if success: success_count += 1
            
            # PROGRESS TRACKER
            elapsed = time.time() - start_time
            avg_time = elapsed / (i + 1)
            remaining = (total - (i + 1)) * avg_time
            
            status = "✅" if success else "⚠️"
            # \r allows us to overwrite the line (cleaner terminal)
            print(f"[{i+1}/{total}] {status} {t} | Fund Data: {'OK' if success else 'MISSING'} | ETA: {int(remaining)}s")
            sys.stdout.flush() 
            
        print(f"\n🏁 PIPELINE FINISHED. Successfully fetched fundamentals for {success_count}/{total} stocks.")