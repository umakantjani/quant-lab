import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from logic.db_config import get_engine
import time

# --- CONFIGURATION ---
engine = get_engine()

SECTOR_ETF_MAP = {
    "Technology": "XLK",
    "Financial Services": "XLF",
    "Healthcare": "XLV",
    "Consumer Cyclical": "XLY",
    "Industrials": "XLI",
    "Consumer Defensive": "XLP",
    "Energy": "XLE",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Basic Materials": "XLB",
    "Communication Services": "XLC"
}

def run_etf_mapper():
    print("🛡 MAPPING SECTOR EXPOSURE & HEDGES...")
    
    # 1. Get Buy Candidates
    try:
        df_val = pd.read_sql("SELECT ticker, intrinsic_value FROM alpha_valuation WHERE upside_pct > 0", engine)
        tickers = df_val['ticker'].tolist()
    except:
        print("No valuation data found. Run Valuation Model first.")
        return

    if not tickers:
        print("No buy candidates to analyze.")
        return

    print(f"   > Analyzing sector exposure for {len(tickers)} candidates...")
    
    # 2. Fetch Sectors (Stealthily)
    sector_data = []
    for t in tickers[:20]: # Limit to Top 20 to be fast
        try:
            # Try to get sector from yfinance
            info = yf.Ticker(t).info
            sector = info.get('sector', 'Unknown')
            sector_data.append({'ticker': t, 'sector': sector})
            time.sleep(0.1)
        except:
            sector_data.append({'ticker': t, 'sector': 'Unknown'})
            
    df_sectors = pd.DataFrame(sector_data)
    
    # 3. Calculate Exposure
    exposure = df_sectors['sector'].value_counts(normalize=True).reset_index()
    exposure.columns = ['sector', 'weight']
    
    # 4. Map to ETFs
    exposure['hedge_etf'] = exposure['sector'].map(SECTOR_ETF_MAP)
    exposure['recommendation'] = exposure.apply(
        lambda x: f"Hedge with {x['hedge_etf']}" if x['weight'] > 0.30 else "Diversified", 
        axis=1
    )
    
    # 5. Save to DB
    exposure.to_sql('etf_hedges', engine, if_exists='replace', index=False)
    
    print(f"✅ EXPOSURE MAPPED. Top Sector: {exposure.iloc[0]['sector']} ({int(exposure.iloc[0]['weight']*100)}%)")
    print(exposure.to_string(index=False))

if __name__ == "__main__":
    run_etf_mapper()