import pandas as pd
import numpy as np
from sqlalchemy import create_engine
from logic.db_config import get_engine

# --- CONFIGURATION ---
# DB_URI = "postgresql://quant:password@localhost:5432/stock_master"
# engine = create_engine(DB_URI)
engine = get_engine()

def get_data():
    """Fetches all price and fundamental data from Postgres."""
    print("Fetching data from Database...")
    
    # 1. Get Fundamentals
    query_fund = "SELECT * FROM fundamentals"
    df_fund = pd.read_sql(query_fund, engine)
    
    # 2. Get Price Trends (Calculate SMA 200 manually from history)
    query_prices = """
    SELECT ticker, date, close 
    FROM prices 
    ORDER BY date ASC
    """
    df_prices = pd.read_sql(query_prices, engine)
    
    return df_fund, df_prices

def calculate_momentum(df_prices):
    """Calculates 'Distance from 200-Day SMA' for each ticker."""
    momentum_scores = []
    
    for ticker in df_prices['ticker'].unique():
        # Filter for specific stock
        mask = df_prices['ticker'] == ticker
        df_stock = df_prices[mask].copy()
        
        if len(df_stock) > 200:
            current_price = df_stock['close'].iloc[-1]
            sma_200 = df_stock['close'].rolling(200).mean().iloc[-1]
            
            # Momentum Score: % above/below the 200 SMA
            mom_score = (current_price - sma_200) / sma_200
            
            momentum_scores.append({
                'ticker': ticker, 
                'current_price': current_price,
                'sma_200': sma_200,
                'momentum_pct': mom_score
            })
            
    return pd.DataFrame(momentum_scores)

def run_quant_model():
    # 1. Load Data
    df_fund, df_prices = get_data()
    
    # 2. Process Momentum
    df_mom = calculate_momentum(df_prices)
    
    # 3. Merge Fundamentals + Momentum
    # (Inner join ensures we have data for both)
    df_final = pd.merge(df_fund, df_mom, on='ticker', how='inner')
    
    # --- THE SCORING ENGINE ---
    # We rank stocks from 0 to 100 on each factor (Percentile Ranking)
    
    # A. Value Score (Lower P/E is better)
    # We invert the rank so Low P/E = High Score
    df_final['value_score'] = df_final['pe_ratio'].rank(ascending=False, pct=True) * 100
    
    # B. Quality Score (Higher Margins is better)
    df_final['quality_score'] = df_final['profit_margin'].rank(ascending=True, pct=True) * 100
    
    # C. Momentum Score (Higher Trend is better)
    df_final['trend_score'] = df_final['momentum_pct'].rank(ascending=True, pct=True) * 100
    
    # D. Composite Score (Weighted Average)
    # Strategy: 40% Value, 40% Trend, 20% Quality
    df_final['TOTAL_SCORE'] = (
        (0.40 * df_final['value_score']) +
        (0.40 * df_final['trend_score']) +
        (0.20 * df_final['quality_score'])
    )
    
    # Sort by Total Score
    df_final = df_final.sort_values('TOTAL_SCORE', ascending=False)
    
    # --- SAVE TO DB ---
    # We use 'replace' to ensure the table always holds the latest snapshot
    table_name = 'quant_rankings'
    try:
        df_final.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"Success: Rankings saved to table '{table_name}'")
    except Exception as e:
        print(f"Error saving to DB: {e}")

    return df_final[['ticker', 'TOTAL_SCORE', 'current_price', 'pe_ratio', 'profit_margin', 'momentum_pct']]

# --- EXECUTION ---
if __name__ == "__main__":
    print("Running Quant Model...")
    results = run_quant_model()
    print("\n=== TOP 10 RANKINGS ===")
    print(results.head(100).to_string(index=False))