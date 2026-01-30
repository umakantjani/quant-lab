import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import time
import sys
from logic.db_config import get_engine

# --- CONFIGURATION ---
engine = get_engine()

def get_master_tickers():
    try:
        df = pd.read_sql("SELECT symbol FROM tickers", engine)
        return df['symbol'].tolist()
    except:
        return []

def fetch_and_save(ticker):
    # 1. PRICES
    try:
        stock = yf.Ticker(ticker)
        df_p = stock.history(period="2y")
        
        if not df_p.empty:
            df_p = df_p.reset_index()
            df_p.columns = [c.lower() for c in df_p.columns]
            df_p['ticker'] = ticker
            # Fix Timezone
            if 'date' in df_p.columns:
                df_p['date'] = pd.to_datetime(df_p['date']).dt.tz_localize(None)
            
            df_p = df_p[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
            
            # Clean Insert
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM prices WHERE ticker = '{ticker}'"))
                conn.commit()
            
            df_p.to_sql('prices', engine, if_exists='append', index=False)
            return True
    except Exception as e:
        pass # Fail silently on prices to keep moving

    # 2. FUNDAMENTALS
    try:
        info = stock.info
        data = {
            'ticker': ticker,
            'pe_ratio': info.get('trailingPE'),
            'forward_pe': info.get('forwardPE'),
            'peg_ratio': info.get('pegRatio'),
            'market_cap': info.get('marketCap'),
            'book_value': info.get('bookValue'),
            'dividend_yield': info.get('dividendYield'),
            'profit_margin': info.get('profitMargins'),
            'beta': info.get('beta'),
            'price_to_book': info.get('priceToBook'),
            'free_cash_flow': info.get('freeCashflow')
        }
        df_f = pd.DataFrame([data])
        
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM fundamentals WHERE ticker = '{ticker}'"))
            conn.commit()
            
        df_f.to_sql('fundamentals', engine, if_exists='append', index=False)
        return True
    except:
        return False

if __name__ == "__main__":
    tickers = get_master_tickers()
    total = len(tickers)
    
    if not tickers:
        print("ERROR: No tickers found. Run ingest_tickers.py first!")
    else:
        print(f"🚀 STARTING PIPELINE FOR {total} TICKERS...")
        start_time = time.time()
        
        for i, t in enumerate(tickers):
            success = fetch_and_save(t)
            
            # PROGRESS TRACKER
            elapsed = time.time() - start_time
            avg_time = elapsed / (i + 1)
            remaining = (total - (i + 1)) * avg_time
            
            status = "✅" if success else "⚠️"
            print(f"[{i+1}/{total}] {status} {t} | Elapsed: {int(elapsed)}s | ETA: {int(remaining)}s")
            
            # Flush stdout so Streamlit sees it immediately
            sys.stdout.flush() 
            
        print(f"🏁 PIPELINE FINISHED in {int(time.time() - start_time)} seconds.")