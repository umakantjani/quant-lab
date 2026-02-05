import pandas as pd
import numpy as np
import yfinance as yf
from sqlalchemy import create_engine, text
from logic.db_config import get_engine
import math

# --- CONFIGURATION ---
engine = get_engine()

# ==============================================================================
# PART 1: DAMODARAN LOGIC (MATH ENGINE)
# ==============================================================================
def calculate_synthetic_wacc(ebit, interest_expense, total_debt, market_cap, tax_rate, risk_free_rate=0.042):
    """Calculates WACC based on Damodaran's 'Synthetic Rating' methodology."""
    # Ensure inputs are floats (handle potential None/NaN passed in)
    ebit = float(ebit) if pd.notna(ebit) else 0.0
    interest_expense = float(interest_expense) if pd.notna(interest_expense) else 0.0
    
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
    beta = 1.1 
    cost_of_equity = risk_free_rate + (beta * equity_risk_premium)

    # 5. WACC
    total_capital = market_cap + total_debt
    if total_capital == 0: return 0.08, spread 
    
    weight_equity = market_cap / total_capital
    weight_debt = total_debt / total_capital
    
    wacc = (cost_of_equity * weight_equity) + (after_tax_cost_of_debt * weight_debt)
    return wacc, spread

class DamodaranDCF:
    def __init__(self, revenue, ebit, tax_rate, wacc, cash, debt, shares, 
                 growth_high=0.12, growth_stable=0.035, terminal_year=10):
        self.rev = float(revenue) if pd.notna(revenue) else 0.0
        self.ebit = float(ebit) if pd.notna(ebit) else 0.0
        self.tax_rate = float(tax_rate) if pd.notna(tax_rate) else 0.21
        self.wacc = float(wacc) if pd.notna(wacc) else 0.10
        self.cash = float(cash) if pd.notna(cash) else 0.0
        self.debt = float(debt) if pd.notna(debt) else 0.0
        self.shares = float(shares) if pd.notna(shares) else 1.0
        self.g_high = growth_high
        self.g_stable = growth_stable
        self.n = terminal_year
        
    def calculate_value(self):
        if self.rev <= 0 or self.shares <= 0: return 0.0
        
        fcff_list = []
        cum_discount = 1.0
        current_rev = self.rev
        
        # Stage 1: High Growth
        for i in range(1, self.n + 1):
            g = self.g_high - ((self.g_high - self.g_stable) / self.n) * i
            current_rev *= (1 + g)
            
            # Safe margin calc
            margin = (self.ebit / self.rev) if self.rev else 0.10
            projected_ebit = current_rev * margin
            
            reinvestment = (projected_ebit * (1 - self.tax_rate)) * 0.20 
            fcff = (projected_ebit * (1 - self.tax_rate)) - reinvestment
            
            cum_discount /= (1 + self.wacc)
            fcff_list.append(fcff * cum_discount)
            
        pv_stage_1 = sum(fcff_list)
        
        # Stage 2: Terminal Value
        if not fcff_list: return 0.0
        
        last_fcff = fcff_list[-1] * (1 + self.wacc) 
        denom = (self.wacc - self.g_stable)
        if denom <= 0.001: denom = 0.05 # Prevent divide by zero/negative
            
        terminal_val = (last_fcff * (1 + self.g_stable)) / denom
        pv_terminal = terminal_val * cum_discount
        
        enterprise_value = pv_stage_1 + pv_terminal
        equity_value = enterprise_value + self.cash - self.debt
        
        return max(0.0, equity_value / self.shares)

# ==============================================================================
# PART 2: PIPELINE INTEGRATION (DATA FETCHING)
# ==============================================================================
def fetch_deep_data(ticker):
    try:
        stock = yf.Ticker(ticker)
        income = stock.income_stmt
        
        if income.empty: return None
        
        # Helper to safely grab first row and force float
        def get_first(df, name, fallback=0.0):
            try: 
                if name not in df.index: return fallback
                val = df.loc[name].iloc[0]
                if val is None or pd.isna(val): return fallback
                return float(val)
            except: return fallback

        interest = abs(get_first(income, 'Interest Expense', 0.0))
        ebit = get_first(income, 'Operating Income', 0.0)
        tax = get_first(income, 'Tax Provision', 0.0)
        pretax = get_first(income, 'Pretax Income', 0.0)
        
        tax_rate = tax / pretax if pretax != 0 else 0.21
            
        return {
            'interest_expense': interest,
            'ebit_actual': ebit,
            'tax_rate_actual': abs(tax_rate)
        }
    except:
        return None

def run_valuation():
    print("🧮 RUNNING DEEP VALUE MODEL (DAMODARAN LOGIC)...")
    
    try:
        df_scan = pd.read_sql("SELECT * FROM alpha_candidates", engine)
        tickers = df_scan['ticker'].tolist()
    except:
        print("No candidates found. Run scanner.py first.")
        return

    print(f"   > Performing Deep Dive on {len(tickers)} candidates...")
    
    valuations = []
    
    for t in tickers:
        # 1. FUNDAMENTALS
        query = f"SELECT * FROM fundamentals WHERE ticker = '{t}'"
        try:
            df_fund = pd.read_sql(query, engine)
            if df_fund.empty: continue
            fund = df_fund.iloc[0]
        except: continue
            
        # 2. PRICE
        try:
            price_df = pd.read_sql(f"SELECT close FROM prices WHERE ticker='{t}' ORDER BY date DESC LIMIT 1", engine)
            if price_df.empty: continue
            price = float(price_df.iloc[0]['close'])
        except: continue

        # 3. DEEP DATA (Slow)
        deep_data = fetch_deep_data(t)
        if not deep_data: continue
        
        # --- SAFE GET HELPER ---
        def safe_get(key, default=0.0):
            val = fund.get(key)
            if val is None or pd.isna(val): return float(default)
            return float(val)

        market_cap = safe_get('market_cap')
        
        # 4. WACC
        wacc, spread = calculate_synthetic_wacc(
            ebit=deep_data['ebit_actual'],
            interest_expense=deep_data['interest_expense'],
            total_debt=market_cap * 0.2, 
            market_cap=market_cap,
            tax_rate=deep_data['tax_rate_actual']
        )
        
        # 5. INPUTS
        p_to_b = safe_get('price_to_book', 1.0)
        if p_to_b <= 0: p_to_b = 1.0
        
        revenue_est = market_cap / p_to_b 
        
        dcf_engine = DamodaranDCF(
            revenue=revenue_est,
            ebit=deep_data['ebit_actual'],
            tax_rate=deep_data['tax_rate_actual'],
            wacc=wacc,
            cash=safe_get('free_cash_flow') * 5, 
            debt=0, 
            shares=market_cap / price if price else 0
        )
        
        intrinsic_value = dcf_engine.calculate_value()
        
        # --- SANITIZED OUTPUT ---
        if price > 0 and not pd.isna(intrinsic_value) and intrinsic_value > 0:
            upside = (intrinsic_value - price) / price
            
            # Rounding for Display
            val_disp = round(intrinsic_value, 2)
            upside_disp = round(upside * 100)
            
            valuations.append({
                'ticker': t,
                'current_price': price,
                'intrinsic_value': val_disp,
                'upside_pct': round(upside * 100, 1),
                'wacc_pct': round(wacc * 100, 1),
                'synthetic_spread': round(spread * 100, 2)
            })
            print(f"   > {t}: Price ${price:.2f} | Value ${val_disp} | Upside {upside_disp}%")

    # 6. SAVE
    if valuations:
        df_val = pd.DataFrame(valuations)
        # Sort by Upside
        df_val = df_val.sort_values('upside_pct', ascending=False)
        
        df_val.to_sql('alpha_valuation', engine, if_exists='replace', index=False)
        print(f"✅ VALUATION COMPLETE. Saved {len(df_val)} opportunities to DB.")
    else:
        print("No value opportunities found.")

if __name__ == "__main__":
    run_valuation()