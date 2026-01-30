import yfinance as yf
import pandas as pd
import numpy as np
import os
import requests
from io import StringIO
import warnings

warnings.filterwarnings("ignore")

DATA_DIR = "data"


def ensure_data_dir():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)


# ==============================================================================
# 1. THE LOGIC CORES (Damodaran Math)
# ==============================================================================

def calculate_synthetic_wacc(ebit, interest_expense, total_debt, market_cap, tax_rate, risk_free_rate, beta):
    """Calculates WACC based on Credit Rating logic (The Damodaran way)."""
    if interest_expense > 0:
        icr = ebit / interest_expense
    else:
        icr = 100.0  # Debt-free / Safe

    # Damodaran's Lookup Table (Large Cap)
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
    else:
        spread = 0.2000; rating = "D"

    pretax_cost_of_debt = risk_free_rate + spread
    after_tax_cost_of_debt = pretax_cost_of_debt * (1 - tax_rate)

    # Cost of Equity using CAPM
    equity_risk_premium = 0.05
    cost_of_equity = risk_free_rate + (beta * equity_risk_premium)

    total_capital = market_cap + total_debt
    if total_capital == 0: return 0.08, spread, icr, rating

    wacc = (cost_of_equity * (market_cap / total_capital)) + (after_tax_cost_of_debt * (total_debt / total_capital))
    return wacc, spread, icr, rating


class DamodaranGinzuValuation:
    """The verified Python replica of the Excel model."""

    def __init__(self, company_name, current_revenue, current_ebit, tax_rate_initial,
                 wacc_initial, wacc_stable, growth_rate_high, growth_rate_stable,
                 sales_to_capital_ratio, target_margin, cash, debt, shares_outstanding):

        self.company_name = company_name
        self.rev_0 = current_revenue
        self.ebit_0 = current_ebit
        self.tax_initial = tax_rate_initial
        self.wacc_high = wacc_initial
        self.wacc_stable = wacc_stable
        self.g_high = growth_rate_high
        self.g_stable = growth_rate_stable
        self.sales_to_cap = sales_to_capital_ratio
        self.target_margin = target_margin
        self.cash = cash
        self.debt = debt
        self.shares = shares_outstanding
        self.terminal_year = 10

    def run_valuation(self):
        if self.rev_0 <= 0 or self.shares <= 0: return None

        current_rev = self.rev_0
        pv_fcff_sum = 0
        cum_discount = 1.0

        # We need final_ebit_1_t for terminal value, defined in loop
        final_ebit_1_t = 0
        final_discount = 0

        for year in range(1, self.terminal_year + 1):
            # Growth (Linear Fade)
            if year <= 5:
                g = self.g_high
            else:
                g = self.g_high - ((self.g_high - self.g_stable) / 5) * (year - 5)

            prev_rev = current_rev
            current_rev = prev_rev * (1 + g)

            # Margin (Linear Move to Target)
            current_margin_pct = (self.ebit_0 / self.rev_0)
            margin_step = (self.target_margin - current_margin_pct) / 10
            margin = current_margin_pct + margin_step * year

            ebit = current_rev * margin

            # Tax (Fade to 25%)
            tax_rate = self.tax_initial + ((0.25 - self.tax_initial) / 10) * year
            ebit_after_tax = ebit * (1 - tax_rate)

            # Reinvestment
            reinvestment = (current_rev - prev_rev) / self.sales_to_cap
            fcff = ebit_after_tax - reinvestment

            # WACC (Fade to Stable)
            if year <= 5:
                wacc = self.wacc_high
            else:
                wacc = self.wacc_high - ((self.wacc_high - self.wacc_stable) / 5) * (year - 5)

            cum_discount /= (1 + wacc)
            pv_fcff_sum += (fcff * cum_discount)

            if year == 10:
                final_ebit_1_t = ebit_after_tax
                final_discount = cum_discount

        # Terminal Value
        stable_reinv_rate = self.g_stable / self.wacc_stable
        terminal_fcff = (final_ebit_1_t * (1 + self.g_stable)) * (1 - stable_reinv_rate)
        terminal_value = terminal_fcff / (self.wacc_stable - self.g_stable)
        pv_terminal = terminal_value * final_discount

        equity_value = (pv_fcff_sum + pv_terminal) + self.cash - self.debt
        return equity_value / self.shares


# ==============================================================================
# 2. THE PIPELINE FUNCTIONS (Called by UI)
# ==============================================================================

def run_scanner():
    """Scrapes S&P 500 and filters for Technical Setup."""
    print("Running Scanner...")
    ensure_data_dir()

    url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
    headers = {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}

    try:
        resp = requests.get(url, headers=headers)
        table = pd.read_html(StringIO(resp.text))
        tickers = [t.replace('.', '-') for t in table[0]['Symbol'].tolist()]

        results = []
        # Scanning first 50 for speed in demo. REMOVE [:50] for full scan.
        scan_list = tickers[:50]

        for t in scan_list:
            try:
                df = yf.download(t, period="1y", progress=False)
                if len(df) > 200:
                    curr = df['Close'].iloc[-1].item()
                    sma200 = df['Close'].rolling(200).mean().iloc[-1].item()
                    high = df['High'].max().item()

                    # RSI
                    delta = df['Close'].diff()
                    gain = (delta.where(delta > 0, 0)).rolling(14).mean()
                    loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                    rs = gain / loss
                    rsi = 100 - (100 / (1 + rs))
                    curr_rsi = rsi.iloc[-1].item()

                    # Criteria: Above 200 SMA (Uptrend) AND RSI < 60 (Not Overbought)
                    if curr_rsi < 60 and curr > sma200:
                        results.append({
                            "Ticker": t,
                            "Price": round(curr, 2),
                            "RSI": round(curr_rsi, 1),
                            "Discount": round((high - curr) / high * 100, 1)
                        })
            except:
                pass

        df = pd.DataFrame(results)
        df.to_csv(f"{DATA_DIR}/watchlist.csv", index=False)
        return f"Scan Complete. Found {len(df)} candidates."
    except Exception as e:
        return f"Error: {e}"


def run_valuation():
    """Runs the Full Damodaran Engine on the Watchlist."""
    ensure_data_dir()
    try:
        if not os.path.exists(f"{DATA_DIR}/watchlist.csv"):
            return "No Watchlist found. Run Scanner first."

        df_watch = pd.read_csv(f"{DATA_DIR}/watchlist.csv")
        results = []

        # Valuation Assumptions
        ASSUMPTIONS = {
            "growth_high": 0.10,  # 10% Growth for 5 years
            "growth_stable": 0.035,  # 3.5% Terminal
            "sales_to_cap": 1.5,  # Efficiency
            "target_margin": 0.25  # Long term margin
        }

        for ticker in df_watch['Ticker']:
            try:
                stock = yf.Ticker(ticker)
                info = stock.info
                income = stock.income_stmt

                # --- 1. GET DATA ---
                rev = info.get('totalRevenue')
                shares = info.get('sharesOutstanding')
                price = info.get('currentPrice', info.get('previousClose'))
                cash = info.get('totalCash', 0)
                debt = info.get('totalDebt', 0)
                beta = info.get('beta', 1.1)

                if not all([rev, shares, price]): continue

                # EBIT (Operating Income)
                ebit = info.get('ebit')
                if not ebit:
                    try:
                        ebit = income.loc['Operating Income'].iloc[0]
                    except:
                        ebit = rev * 0.15

                    # Interest Expense (Critical for Synthetic Rating)
                try:
                    interest = abs(income.loc['Interest Expense'].iloc[0])
                except:
                    interest = debt * 0.05

                # Tax Rate
                try:
                    tax_rate = income.loc['Tax Provision'].iloc[0] / income.loc['Pretax Income'].iloc[0]
                except:
                    tax_rate = 0.21
                tax_rate = max(0.10, min(tax_rate, 0.30))

                # Risk Free Rate
                try:
                    rf = yf.Ticker("^TNX").history(period="5d")['Close'].iloc[-1] / 100
                except:
                    rf = 0.042

                # --- 2. CALCULATE WACC (Synthetic) ---
                market_cap = price * shares
                wacc, spread, icr, rating = calculate_synthetic_wacc(
                    ebit, interest, debt, market_cap, tax_rate, rf, beta
                )

                # --- 3. RUN GINZU MODEL ---
                engine = DamodaranGinzuValuation(
                    company_name=ticker,
                    current_revenue=rev,
                    current_ebit=ebit,
                    tax_rate_initial=tax_rate,
                    wacc_initial=wacc,
                    wacc_stable=0.075,  # Converge to 7.5%
                    growth_rate_high=ASSUMPTIONS['growth_high'],
                    growth_rate_stable=ASSUMPTIONS['growth_stable'],
                    sales_to_capital_ratio=ASSUMPTIONS['sales_to_cap'],
                    target_margin=ASSUMPTIONS['target_margin'],
                    cash=cash,
                    debt=debt,
                    shares_outstanding=shares
                )

                intrinsic_val = engine.run_valuation()

                if intrinsic_val:
                    upside = (intrinsic_val - price) / price

                    results.append({
                        "Ticker": ticker,
                        "Action": "BUY" if upside > 0.15 and rating not in ['D', 'CCC'] else "WAIT",
                        "Upside %": round(upside * 100, 1),
                        "Current Price": price,
                        "Fair Value": round(intrinsic_val, 2),
                        "Rating": rating,
                        "WACC": f"{round(wacc * 100, 1)}%"
                    })
            except Exception as e:
                print(f"Skipping {ticker}: {e}")
                pass

        df = pd.DataFrame(results)
        df.to_csv(f"{DATA_DIR}/valuation.csv", index=False)
        return f"Valuation Complete. Analyzed {len(df)} stocks."

    except Exception as e:
        return f"Critical Error: {e}"


def generate_orders():
    """Generates Allocation based on Valuation results."""
    ensure_data_dir()
    try:
        df = pd.read_csv(f"{DATA_DIR}/valuation.csv")
        # Filter: Action == BUY
        buys = df[df['Action'] == 'BUY'].copy()

        # Satellite Capital Allocation ($4,000 for stocks)
        total_satellite_cap = 4000

        if not buys.empty:
            # Sort by Upside
            buys = buys.sort_values("Upside %", ascending=False).head(5)
            per_stock = total_satellite_cap / len(buys)

            buys['Shares'] = (per_stock / buys['Current Price']).astype(int)
            buys['Cost'] = buys['Shares'] * buys['Current Price']

            output = buys[['Ticker', 'Shares', 'Current Price', 'Cost', 'Upside %', 'Rating']]
            output.to_csv(f"{DATA_DIR}/orders.csv", index=False)
            return "Orders Generated successfully."
        else:
            return "No stocks met the BUY criteria."

    except:
        return "Valuation data missing"