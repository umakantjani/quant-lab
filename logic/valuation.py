import yfinance as yf
import pandas as pd
import numpy as np
import os
import warnings
from datetime import datetime

# --- CONFIGURATION (The only change: Pointing to the right folders) ---
warnings.filterwarnings("ignore")
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")

# Input: The list from your Scanner
INPUT_WATCHLIST = os.path.join(DATA_DIR, "filtered_watchlist.csv")
# Output: The Deep Value Report
OUTPUT_FILE = os.path.join(DATA_DIR, "my_deep_value_portfolio.csv")


# ==============================================================================
# PART 1: THE SYNTHETIC RATING ENGINE (Damodaran's WACC Logic)
# ==============================================================================
def calculate_synthetic_wacc(ebit, interest_expense, total_debt, market_cap, tax_rate, risk_free_rate):
    """
    Calculates WACC based on Damodaran's 'Synthetic Rating' methodology.
    """
    # 1. Calculate Interest Coverage Ratio (ICR)
    if interest_expense > 0:
        icr = ebit / interest_expense
    else:
        icr = 100.0  # High safety

    # 2. Look up Default Spread (Based on Damodaran's Large Cap Table)
    if icr > 8.5:
        spread = 0.0069; rating = "AAA"
    elif icr > 6.5:
        spread = 0.0085; rating = "AA"
    elif icr > 5.5:
        spread = 0.0107; rating = "A+"
    elif icr > 4.25:
        spread = 0.0118; rating = "A"
    elif icr > 3.0:
        spread = 0.0133; rating = "A-"
    elif icr > 2.5:
        spread = 0.0171; rating = "BBB"
    elif icr > 2.25:
        spread = 0.0231; rating = "BB+"
    elif icr > 2.0:
        spread = 0.0277; rating = "BB"
    elif icr > 1.75:
        spread = 0.0349; rating = "B+"
    elif icr > 1.5:
        spread = 0.0416; rating = "B"
    elif icr > 1.25:
        spread = 0.0577; rating = "B-"
    elif icr > 0.8:
        spread = 0.0827; rating = "CCC"
    elif icr > 0.65:
        spread = 0.1347; rating = "CC"
    else:
        spread = 0.2000; rating = "D"

    # 3. Cost of Debt
    pretax_cost_of_debt = risk_free_rate + spread
    after_tax_cost_of_debt = pretax_cost_of_debt * (1 - tax_rate)

    # 4. Cost of Equity (CAPM)
    equity_risk_premium = 0.05
    beta = 1.1  # Default beta
    cost_of_equity = risk_free_rate + (beta * equity_risk_premium)

    # 5. WACC Calculation
    total_capital = market_cap + total_debt
    if total_capital == 0: return 0.08, spread, icr, rating

    weight_equity = market_cap / total_capital
    weight_debt = total_debt / total_capital

    wacc = (cost_of_equity * weight_equity) + (after_tax_cost_of_debt * weight_debt)

    return wacc, spread, icr, rating


# ==============================================================================
# PART 2: THE VALUATION ENGINE (The Excel Logic)
# ==============================================================================
class DamodaranGinzuValuation:
    def __init__(self, company_name, current_revenue, current_ebit, tax_rate_initial,
                 tax_rate_marginal, wacc_initial, wacc_stable, growth_rate_high,
                 growth_rate_stable, sales_to_capital_ratio, target_margin, cash,
                 debt, minority_interests, shares_outstanding, non_operating_assets=0,
                 convergence_year=5, terminal_year=10):

        self.company_name = company_name
        self.rev_0 = current_revenue
        self.ebit_0 = current_ebit
        self.tax_initial = tax_rate_initial
        self.tax_marginal = tax_rate_marginal
        self.wacc_high = wacc_initial
        self.wacc_stable = wacc_stable
        self.g_high = growth_rate_high
        self.g_stable = growth_rate_stable
        self.sales_to_cap = sales_to_capital_ratio
        self.target_margin = target_margin
        self.cash = cash
        self.debt = debt
        self.minority_interests = minority_interests
        self.shares = shares_outstanding
        self.non_op_assets = non_operating_assets
        self.convergence_year = convergence_year
        self.terminal_year = terminal_year

    def run_valuation(self):
        if self.rev_0 <= 0: return None

        # Initialize projections
        years = list(range(1, self.terminal_year + 2))
        df = pd.DataFrame(index=years, columns=['Revenue', 'Growth_Rate', 'EBIT_Margin',
                                                'EBIT', 'Tax_Rate', 'EBIT_1_t', 'Reinvestment',
                                                'FCFF', 'WACC', 'Cum_Discount_Factor', 'PV_FCFF'])

        current_rev = self.rev_0

        # --- PROJECTION LOOP ---
        for year in range(1, self.terminal_year + 1):
            # 1. Growth Rate
            if year <= self.convergence_year:
                g = self.g_high
            else:
                steps = self.terminal_year - self.convergence_year
                g = self.g_high - ((self.g_high - self.g_stable) / steps) * (year - self.convergence_year)

            # 2. Revenue
            prev_rev = current_rev
            current_rev = prev_rev * (1 + g)

            # 3. Margins
            margin_step = (self.target_margin - (self.ebit_0 / self.rev_0)) / self.terminal_year
            margin = (self.ebit_0 / self.rev_0) + margin_step * year

            # 4. EBIT & Tax
            ebit = current_rev * margin
            tax_rate = self.tax_initial + ((self.tax_marginal - self.tax_initial) / self.terminal_year) * year
            ebit_after_tax = ebit * (1 - tax_rate)

            # 5. Reinvestment
            rev_change = current_rev - prev_rev
            reinvestment = rev_change / self.sales_to_cap if self.sales_to_cap else 0

            # 6. FCFF
            fcff = ebit_after_tax - reinvestment

            # 7. WACC
            if year <= 5:
                wacc = self.wacc_high
            else:
                wacc = self.wacc_high - ((self.wacc_high - self.wacc_stable) / 5) * (year - 5)

            df.loc[year] = [current_rev, g, margin, ebit, tax_rate, ebit_after_tax, reinvestment, fcff, wacc, 0, 0]

        # --- DISCOUNTING ---
        cum_discount = 1.0
        for year in range(1, self.terminal_year + 1):
            cum_discount /= (1 + df.loc[year, 'WACC'])
            df.loc[year, 'Cum_Discount_Factor'] = cum_discount
            df.loc[year, 'PV_FCFF'] = df.loc[year, 'FCFF'] * cum_discount

        # --- TERMINAL VALUE ---
        stable_reinv_rate = self.g_stable / self.wacc_stable
        terminal_ebit_1_t = df.loc[self.terminal_year, 'EBIT_1_t'] * (1 + self.g_stable)
        terminal_fcff = terminal_ebit_1_t * (1 - stable_reinv_rate)

        terminal_value = terminal_fcff / (self.wacc_stable - self.g_stable)
        pv_terminal_value = terminal_value * df.loc[self.terminal_year, 'Cum_Discount_Factor']

        operating_assets_value = df['PV_FCFF'].sum() + pv_terminal_value
        equity_value = operating_assets_value + self.cash + self.non_op_assets - self.debt - self.minority_interests
        value_per_share = equity_value / self.shares if self.shares else 0

        return {
            "Value_Per_Share": round(value_per_share, 2),
            "Equity_Value_B": round(equity_value / 1e9, 2),
            "WACC_Initial": round(self.wacc_high * 100, 2),
            "Stable_WACC": round(self.wacc_stable * 100, 2)
        }


# ==============================================================================
# PART 3: THE DATA CONNECTOR (Yahoo + Synthetic WACC Integration)
# ==============================================================================
def get_live_valuation_data(ticker):
    print(f"Fetching data for {ticker}...")
    try:
        stock = yf.Ticker(ticker)
        info = stock.info
        income = stock.income_stmt

        current_revenue = info.get('totalRevenue')
        shares = info.get('sharesOutstanding')
        price = info.get('currentPrice', info.get('previousClose'))
        cash = info.get('totalCash')
        debt = info.get('totalDebt')

        if not all([current_revenue, shares, price]):
            return None

        # EBIT & Interest
        ebit = info.get('ebit')
        if not ebit:
            try:
                ebit = income.loc['Operating Income'].iloc[0]
            except:
                ebit = current_revenue * 0.15

        try:
            interest_expense = abs(income.loc['Interest Expense'].iloc[0])
        except:
            interest_expense = debt * 0.05 if debt else 0

        # Tax Rate
        try:
            tax_rate = income.loc['Tax Provision'].iloc[0] / income.loc['Pretax Income'].iloc[0]
            tax_rate = max(0.10, min(tax_rate, 0.30))
        except:
            tax_rate = 0.21

        # Risk Free
        try:
            risk_free = yf.Ticker("^TNX").history(period="5d")['Close'].iloc[-1] / 100
        except:
            risk_free = 0.042

        market_cap = price * shares
        wacc, spread, icr, rating = calculate_synthetic_wacc(
            ebit, interest_expense, debt, market_cap, tax_rate, risk_free
        )

        return {
            "revenue": current_revenue, "ebit": ebit, "tax_rate": tax_rate,
            "wacc": wacc, "synthetic_spread": spread, "icr": icr, "rating": rating,
            "cash": cash, "debt": debt, "shares": shares, "price": price
        }
    except Exception as e:
        print(f"Error fetching {ticker}: {e}")
        return None


# ==============================================================================
# PART 4: THE TECHNICAL ANALYSIS ENGINE
# ==============================================================================
def check_technical_signals(ticker):
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="60d")
        if len(df) < 14: return None

        delta = df['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        df['RSI'] = 100 - (100 / (1 + rs))
        current_rsi = df['RSI'].iloc[-1]

        # Volume Spike
        df['Vol_Avg'] = df['Volume'].rolling(20).mean()
        vol_spike = df['Volume'].iloc[-1] > (1.5 * df['Vol_Avg'].iloc[-1])

        # Hammer
        row = df.iloc[-1]
        body = abs(row['Close'] - row['Open'])
        lower_wick = min(row['Open'], row['Close']) - row['Low']
        upper_wick = row['High'] - max(row['Open'], row['Close'])
        body = max(body, 0.01)
        is_hammer = (lower_wick > 2 * body) and (upper_wick < body)

        # Engulfing
        today = df.iloc[-1]
        yesterday = df.iloc[-2]
        is_engulfing = (yesterday['Close'] < yesterday['Open']) and \
                       (today['Close'] > today['Open']) and \
                       (today['Open'] < yesterday['Close']) and \
                       (today['Close'] > yesterday['Open'])

        signals = []
        if current_rsi < 35: signals.append("OVERSOLD")
        if is_hammer: signals.append("HAMMER")
        if is_engulfing: signals.append("ENGULFING")
        if vol_spike: signals.append("HIGH_VOL")

        status = "WAIT"
        if "OVERSOLD" in signals and ("HAMMER" in signals or "ENGULFING" in signals):
            status = "STRONG_BUY_SIGNAL"
        elif "OVERSOLD" in signals:
            status = "WATCH"

        return {"RSI": round(current_rsi, 1), "Signals": " + ".join(signals) if signals else "None", "Action": status}

    except Exception as e:
        return None


# ==============================================================================
# PART 5: MAIN EXECUTION (Updated for Folder Structure)
# ==============================================================================

if __name__ == "__main__":

    # 1. LOAD WATCHLIST (From Scanner or Default)
    if os.path.exists(INPUT_WATCHLIST):
        print(f"Loading tickers from {INPUT_WATCHLIST}...")
        df_watch = pd.read_csv(INPUT_WATCHLIST)
        my_watchlist = df_watch['Ticker'].tolist()
    else:
        print("No scanner file found. Using default watchlist.")
        my_watchlist = ["GOOGL", "AAPL", "MSFT", "AMZN", "TSLA", "NVDA"]

    assumptions = {
        "growth_high": 0.12, "growth_stable": 0.035,
        "target_margin": 0.25, "sales_to_cap": 1.5
    }

    results_list = []
    print(f"\nAnalyzing {len(my_watchlist)} stocks...")
    print(f"{'TICKER':<8} {'PRICE':<10} {'VALUE':<10} {'UPSIDE':<10} {'RATING':<8} {'ACTION'}")
    print("-" * 85)

    for ticker in my_watchlist:
        val_data = get_live_valuation_data(ticker)
        tech_data = check_technical_signals(ticker)

        if val_data and tech_data:
            engine = DamodaranGinzuValuation(
                company_name=ticker,
                current_revenue=val_data['revenue'],
                current_ebit=val_data['ebit'],
                tax_rate_initial=val_data['tax_rate'],
                tax_rate_marginal=0.25,
                wacc_initial=val_data['wacc'],
                wacc_stable=0.075,
                growth_rate_high=assumptions['growth_high'],
                growth_rate_stable=assumptions['growth_stable'],
                sales_to_capital_ratio=assumptions['sales_to_cap'],
                target_margin=assumptions['target_margin'],
                cash=val_data['cash'],
                debt=val_data['debt'],
                minority_interests=0,
                shares_outstanding=val_data['shares']
            )

            result = engine.run_valuation()

            if result:
                current_price = val_data['price']
                intrinsic_value = result['Value_Per_Share']
                upside_pct = (intrinsic_value - current_price) / current_price

                # Combine Logic: Buy if Cheap AND Safe (Not 'D' rating)
                final_action = "WAIT"
                if upside_pct > 0.15 and val_data['rating'] not in ['D', 'CC', 'CCC']:
                    final_action = "BUY"

                print(f"{ticker:<8} ${round(current_price, 2):<9} ${round(intrinsic_value, 2):<9} "
                      f"{round(upside_pct * 100, 1):>5}%    "
                      f"{val_data['rating']:<8} {final_action}")

                # Save Data for UI
                row = {
                    'Ticker': ticker,
                    'Action': final_action,
                    'Upside %': round(upside_pct * 100, 1),
                    'Current Price': current_price,
                    'Fair Value': intrinsic_value,
                    'Rating': val_data['rating'],
                    'WACC': f"{round(val_data['wacc'] * 100, 1)}%",
                    'RSI': tech_data['RSI'],
                    'Tech Signals': tech_data['Signals']
                }
                results_list.append(row)

    # Save to CSV in DATA_DIR
    if results_list:
        pd.DataFrame(results_list).to_csv(OUTPUT_FILE, index=False)
        print(f"\nSaved detailed report to {OUTPUT_FILE}")