import pandas as pd
import os
import math
import yfinance as yf

# --- PATH CONFIG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Inputs
ETF_FILE = os.path.join(DATA_DIR, "my_etf_strategy.csv")
STOCK_FILE = os.path.join(DATA_DIR, "my_deep_value_portfolio.csv")

# Output
OUTPUT_FILE = os.path.join(DATA_DIR, "final_buy_orders.csv")

# --- STRATEGY CONFIG ---
TOTAL_CAPITAL = 10000  # Your starting amount
CORE_ALLOCATION = 0.60  # 60% into ETFs
SATELLITE_ALLOCATION = 0.40  # 40% into Individual Stocks

if __name__ == "__main__":
    print(f"GENERATING ORDER TICKET FOR ${TOTAL_CAPITAL:,} PORTFOLIO")
    print("=" * 60)

    orders = []

    # ==============================================================================
    # PART 1: THE CORE (ETFs)
    # ==============================================================================
    capital_core = TOTAL_CAPITAL * CORE_ALLOCATION

    if os.path.exists(ETF_FILE):
        df_etf = pd.read_csv(ETF_FILE)

        # Filter for BUY or UPTREND only
        # We check if 'Action' column exists, otherwise fallback to simple logic
        if 'Action' in df_etf.columns:
            df_etf = df_etf[df_etf['Action'] == 'BUY']

        if not df_etf.empty:
            # We split the core capital across top 3 valid ETFs
            top_etfs = df_etf.head(3)
            allocation_per_etf = capital_core / len(top_etfs)

            print(f"Allocating ${capital_core:,.0f} to {len(top_etfs)} Core ETFs...")

            for index, row in top_etfs.iterrows():
                ticker = row['ETF'] if 'ETF' in row else row['ETF_Ticker']  # Handle column naming diffs

                # Fetch current price
                price = 100  # Default fallback
                try:
                    price = yf.Ticker(ticker).fast_info['last_price']
                except:
                    pass

                shares = math.floor(allocation_per_etf / price)
                cost = shares * price

                orders.append({
                    "Type": "CORE (ETF)",
                    "Ticker": ticker,
                    "Reason": f"Sector Trend ({row['Weight'] if 'Weight' in row else 'High'})",
                    "Shares": shares,
                    "Price": round(price, 2),
                    "Total_Cost": round(cost, 2)
                })
        else:
            print("No ETFs in Buy Zone. Holding Core capital in Cash.")
    else:
        print(f"ETF file not found at {ETF_FILE}. Run etf.py first.")

    # ==============================================================================
    # PART 2: THE SATELLITE (Deep Value Stocks)
    # ==============================================================================
    capital_sat = TOTAL_CAPITAL * SATELLITE_ALLOCATION

    if os.path.exists(STOCK_FILE):
        df_stocks = pd.read_csv(STOCK_FILE)

        # Filter: Must be Actionable AND Safe (Credit Rating not D)
        # We prioritize 'STRONG_VALUE' first, then 'BUY_CANDIDATE' or just 'BUY'
        # Adapting to your various naming conventions in previous scripts
        valid_actions = ['STRONG_VALUE', 'BUY_CANDIDATE', 'BUY']
        bad_ratings = ['D', 'CCC', 'CC']

        candidates = df_stocks[
            (df_stocks['Action'].isin(valid_actions)) &
            (~df_stocks['Rating'].isin(bad_ratings))
            ].copy()

        if not candidates.empty:
            # Pick top 5 based on Upside
            # Ensure Upside is numeric for sorting
            if candidates['Upside %'].dtype == 'O':
                candidates['Upside_Float'] = candidates['Upside %'].str.rstrip('%').astype(float)
            else:
                candidates['Upside_Float'] = candidates['Upside %']

            top_stocks = candidates.sort_values('Upside_Float', ascending=False).head(5)

            allocation_per_stock = capital_sat / len(top_stocks)

            print(f"Allocating ${capital_sat:,.0f} to {len(top_stocks)} Value Stocks...")

            for index, row in top_stocks.iterrows():
                price = row['Current Price']
                shares = math.floor(allocation_per_stock / price)
                cost = shares * price

                orders.append({
                    "Type": "SATELLITE (Stock)",
                    "Ticker": row['Ticker'],
                    "Reason": f"Value Gap (+{row['Upside %']})",
                    "Shares": shares,
                    "Price": round(price, 2),
                    "Total_Cost": round(cost, 2)
                })
        else:
            print("No stocks met the strict BUY criteria. Holding Satellite capital in Cash.")
    else:
        print(f"Valuation file not found at {STOCK_FILE}. Run valuation.py first.")

    # ==============================================================================
    # PART 3: SAVE THE TICKET
    # ==============================================================================
    if orders:
        df_orders = pd.DataFrame(orders)
        print("\n" + "=" * 60)
        print("FINAL BUY ORDER LIST")
        print("=" * 60)
        print(df_orders[['Type', 'Ticker', 'Shares', 'Price', 'Total_Cost']])

        df_orders.to_csv(OUTPUT_FILE, index=False)
        print(f"\nSaved to {OUTPUT_FILE}")
    else:
        print("No trades generated.")