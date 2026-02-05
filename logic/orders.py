import pandas as pd
from sqlalchemy import create_engine
from logic.db_config import get_engine
import os

# --- CONFIGURATION ---
engine = get_engine()
CAPITAL = 100000        # Virtual Portfolio Size ($100k)
MAX_POSITIONS = 10      # Top 10 High Conviction Ideas
TARGET_ALLOC = CAPITAL / MAX_POSITIONS # $10k per stock

def generate_orders():
    print("🤖 GENERATING ALGORITHMIC ORDERS...")
    
    # 1. Read Deep Value Opportunities
    try:
        query = """
        SELECT * FROM alpha_valuation 
        WHERE upside_pct > 0.15 
        ORDER BY upside_pct DESC 
        LIMIT 20
        """
        df_val = pd.read_sql(query, engine)
    except Exception as e:
        print(f"Error: {e}")
        return

    if df_val.empty:
        print("No stocks meet the criteria (Upside > 15%).")
        return

    print(f"   > Filtering Top {MAX_POSITIONS} from {len(df_val)} value candidates...")
    
    # 2. Create Orders
    orders = []
    df_val = df_val.head(MAX_POSITIONS)
    
    for _, row in df_val.iterrows():
        ticker = row['ticker']
        price = row['current_price']
        
        if price > 0:
            shares = int(TARGET_ALLOC / price)
            
            if shares > 0:
                orders.append({
                    'Ticker': ticker,
                    'Action': 'BUY',
                    'Order_Type': 'LMT',
                    'Limit_Price': price, # Buy at current market price
                    'Shares': shares,
                    'Est_Value': shares * price,
                    'Upside': f"{row['upside_pct']}%",
                    'Reason': "Deep Value"
                })

    # 3. Save to DB (For UI Display)
    if orders:
        df_orders = pd.DataFrame(orders)
        df_orders.to_sql('alpha_orders', engine, if_exists='replace', index=False)
        
        # Also save CSV for Blotter
        df_orders.to_csv("data/final_buy_orders.csv", index=False)
        
        print(f"✅ GENERATED {len(df_orders)} ORDERS.")
        print(df_orders[['Ticker', 'Shares', 'Limit_Price', 'Upside']].to_string(index=False))
    else:
        print("No valid orders generated.")

if __name__ == "__main__":
    generate_orders()