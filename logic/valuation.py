import pandas as pd
import numpy as np
import yfinance as yf
from sqlalchemy import create_engine, text
from logic.db_config import get_engine

# --- CONFIGURATION ---
engine = get_engine()

# ==============================================================================
# PART 1: YOUR DAMODARAN LOGIC (Preserved)
# ==============================================================================
def calculate_synthetic_wacc(ebit, interest_expense, total_debt, market_cap, tax_rate, risk_free_rate=0.042):
    """Calculates WACC based on Damodaran's 'Synthetic Rating' methodology."""
    # 1. Interest Coverage Ratio (ICR)
    if interest_expense > 0:
        icr = ebit / interest_expense
    else:
        icr = 100.0

    # 2. Default Spread Lookup
    if icr > 8.5: spread = 0.0069
    elif icr > 6.5: spread = 0.0085
    elif icr > 5.5: spread = 0.0107
    elif icr > 4.25: spread = 0.0118
    elif icr > 3.0: spread = 0.0133
    elif icr > 2.5: spread = 0.0171
    elif icr > 2.25: spread = 0.0231
    elif icr > 2.0: spread = 0.0277
    elif icr > 1.75: spread = 0.0349
    elif icr > 1.5: spread = 0.0416
    elif icr > 1.25: spread = 0.0577
    elif icr > 0.8: spread = 0.0827
    elif icr > 0.65: spread = 0.1347
    else: spread = 0.2000

    # 3. Cost of Debt
    pretax_cost_of_debt = risk_free_rate + spread
    after_tax_cost_of_debt = pretax_cost_of_debt * (1 - tax_rate)

    # 4. Cost of Equity (CAPM)
    equity_risk_premium = 0.05
    beta = 1.1 # Default beta if not available
    cost_of_equity = risk_free_rate + (beta * equity_risk_premium)

    # 5. WACC
    total_capital = market_cap + total_debt
    if total_capital == 0: return 0.08, spread # Fallback
    
    weight_equity = market_cap / total_capital
    weight_debt = total_debt / total_capital
    
    wacc = (cost_of_equity * weight_equity) + (after_tax_cost_of_debt * weight_debt)
    return wacc, spread

class DamodaranDCF:
    """Your exact DCF engine, wrapped for the pipeline."""
    def __init__(self, revenue, ebit, tax_rate, wacc, cash, debt, shares, 
                 growth_high=0.12, growth_stable=0.035, terminal_year=10):
        self.rev = revenue
        self.ebit = ebit
        self.tax_rate = tax_rate
        self.wacc = wacc
        self.cash = cash
        self.debt = debt
        self.shares = shares
        self.g_high = growth_high
        self.g_stable = growth_stable
        self.n = terminal_year
        
    def calculate_value(self):
        if self.rev <= 0 or self.shares <= 0: return 0
        
        # Simple 2-Stage DCF approximation for speed
        # (Expanded version of your loop logic)
        fcff_list = []
        cum_discount = 1.0
        current_rev = self.rev
        
        # Stage 1: High Growth
        for i in range(1, self.n + 1):
            # Linearly decay growth
            g = self.g_high - ((self.g_high - self.g_stable) / self.n) * i
            current_rev *= (1 + g)
            
            # Assume constant margin for simplicity in this version
            margin = self.ebit / self.rev 
            projected_ebit = current_rev * margin
            
            # Reinvestment (Simplified as % of growth)
            reinvestment = (projected_ebit * (1 - self.tax_rate)) * 0.20 
            fcff = (projected_ebit * (1 - self.tax_rate)) - reinvestment
            
            cum_discount /= (1 + self.wacc)
            fcff_list.append(fcff * cum_discount)
            
        pv_stage_1 = sum(fcff_list)
        
        # Stage 2: Terminal Value
        last_fcff = fcff_list[-1] * (1 + self.wacc) # Undiscount
        terminal_val = (last_fcff * (1 + self.g_stable)) / (self.wacc - self.g_stable)
        pv_terminal = terminal_val * cum_discount
        
        # Equity Value
        enterprise_value = pv_stage_1 + pv_terminal
        equity_value = enterprise_value + self.cash - self.debt
        
        return equity_value / self.shares

# ==============================================================================
# PART 2: THE PIPELINE INTEGRATION
# ==============================================================================
def fetch_deep_data(ticker):
    """Fetches the detailed Income Statement data needed for WACC."""
    try:
        stock = yf.Ticker(ticker)
        # We need specific fields not in the fast .info call
        income = stock.income_stmt
        bs = stock.balance_sheet
        
        if income.empty or bs.empty: return None
        
        # Extract Deep Data
        try:
            interest_expense = abs(income.loc['Interest Expense'].iloc[0])
        except: interest_expense = 0
            
        try:
            ebit = income.loc['Operating Income'].iloc[0]
        except: ebit = 0
            
        try:
            tax_provision = income.loc['Tax Provision'].iloc[0]
            pretax_income = income.loc['Pretax Income'].iloc[0]
            tax_rate = tax_provision / pretax_income if pretax_income != 0 else 0.21
        except: tax_rate = 0.21
            
        return {
            'interest_expense': interest_expense,
            'ebit_actual': ebit,
            'tax_rate_actual': abs(tax_rate)
        }
    except:
        return None

def run_valuation():
    print("🧮 RUNNING DEEP VALUE MODEL (DAMODARAN LOGIC)...")
    
    # 1. Get Candidates (From Scanner)
    try:
        df_scan = pd.read_sql("SELECT * FROM alpha_candidates", engine)
        tickers = df_scan['ticker'].tolist()
    except:
        print("No candidates found. Run scanner.py first.")
        return

    print(f"   > Performing Deep Dive on {len(tickers)} candidates...")
    
    valuations = []
    
    for t in tickers:
        # Get Basic Info from DB (Fast)
        query = f"SELECT * FROM fundamentals WHERE ticker = '{t}'"
        try:
            df_fund = pd.read_sql(query, engine)
            if df_fund.empty: continue
            fund = df_fund.iloc[0]
        except: continue
            
        # Get Price from DB
        try:
            price = pd.read_sql(f"SELECT close FROM prices WHERE ticker='{t}' ORDER BY date DESC LIMIT 1", engine).iloc[0]['close']
        except: continue

        # Get Deep Data from Yahoo (Slow but necessary for WACC)
        deep_data = fetch_deep_data(t)
        if not deep_data: continue
        
        # CALC WACC
        wacc, spread = calculate_synthetic_wacc(
            ebit=deep_data['ebit_actual'],
            interest_expense=deep_data['interest_expense'],
            total_debt=fund.get('market_cap', 0) * 0.2, # Approximation if total debt missing
            market_cap=fund.get('market_cap', 0),
            tax_rate=deep_data['tax_rate_actual']
        )
        
        # RUN DCF
        dcf_engine = DamodaranDCF(
            revenue=fund.get('market_cap', 0) / fund.get('price_to_book', 1), # Reverse engineer Rev approx if needed, or fetch
            ebit=deep_data['ebit_actual'],
            tax_rate=deep_data['tax_rate_actual'],
            wacc=wacc,
            cash=fund.get('free_cash_flow', 0) * 5, # Rough proxy if cash missing
            debt=0, 
            shares=fund.get('market_cap', 0) / price
        )
        
        intrinsic_value = dcf_engine.calculate_value()
        upside = (intrinsic_value - price) / price
        
        valuations.append({
            'ticker': t,
            'current_price': price,
            'intrinsic_value': round(intrinsic_value, 2),
            'upside_pct': round(upside * 100, 1),
            'wacc_pct': round(wacc * 100, 1),
            'synthetic_spread': round(spread * 100, 2)
        })
        print(f"   > {t}: Price ${price} | Value ${round(intrinsic_value, 2)} | Upside {round(upside*100)}%")

    # Save Results
    if valuations:
        df_val = pd.DataFrame(valuations)
        # Filter: Positive Upside Only
        df_val = df_val[df_val['upside_pct'] > 0].sort_values('upside_pct', ascending=False)
        
        df_val.to_sql('alpha_valuation', engine, if_exists='replace', index=False)
        print(f"✅ VALUATION COMPLETE. Saved {len(df_val)} opportunities to DB.")
    else:
        print("No value opportunities found.")

if __name__ == "__main__":
    run_valuation()