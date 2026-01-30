import yfinance as yf
import pandas as pd
from sqlalchemy import create_engine, text
import time
from logic.db_config import get_engine

# DB_URI = "postgresql://quant:password@localhost:5432/stock_master"
# engine = create_engine(DB_URI)
engine = get_engine()

def get_master_tickers():
    """Reads the full unique list from our database."""
    try:
        df = pd.read_sql("SELECT symbol FROM tickers", engine)
        return df['symbol'].tolist()
    except:
        return []

def fetch_and_save(ticker):
    # 1. PRICES
    try:
        stock = yf.Ticker(ticker)
        df_p = stock.history(period="2y") # Fetching 2 years is enough for analysis
        
        if not df_p.empty:
            df_p = df_p.reset_index()
            df_p.columns = [c.lower() for c in df_p.columns]
            df_p['ticker'] = ticker
            df_p['date'] = pd.to_datetime(df_p['date']).dt.tz_localize(None)
            df_p = df_p[['date', 'ticker', 'open', 'high', 'low', 'close', 'volume']]
            
            # CLEAN INSERT: Delete old rows for this ticker first to avoid dupes
            with engine.connect() as conn:
                conn.execute(text(f"DELETE FROM prices WHERE ticker = '{ticker}'"))
                conn.commit()
            
            df_p.to_sql('prices', engine, if_exists='append', index=False)
            print(f"   + {ticker}: Updated {len(df_p)} price rows.")
    except Exception as e:
        print(f"   x {ticker} Price Error: {e}")

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
        
        # CLEAN INSERT
        with engine.connect() as conn:
            conn.execute(text(f"DELETE FROM fundamentals WHERE ticker = '{ticker}'"))
            conn.commit()
            
        df_f.to_sql('fundamentals', engine, if_exists='append', index=False)
    except Exception as e:
        pass # Silent fail on fundamentals is okay

if __name__ == "__main__":
    tickers = get_master_tickers()
    
    if not tickers:
        print("ERROR: No tickers found in DB. Run ingest_tickers.py first!")
    else:
        print(f"🚀 STARTING PIPELINE FOR {len(tickers)} TICKERS...")
        # RUNNING FOR ALL STOCKS
        for t in tickers: 
            fetch_and_save(t)
            time.sleep(0.5)
        print("🏁 PIPELINE FINISHED.")