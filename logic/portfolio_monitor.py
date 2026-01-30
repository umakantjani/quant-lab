import yfinance as yf
import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings("ignore")

# ==============================================================================
# CONFIGURATION
# ==============================================================================
PORTFOLIO_FILE = 'final_buy_orders.csv'
TRAILING_STOP_ATR_MULTIPLIER = 2.0  # Sell if price drops 2x average volatility
HARD_STOP_PERCENT = -0.08  # Sell immediately if down 8% (Catastrophe protection)


# ==============================================================================
# LOGIC
# ==============================================================================
def get_exit_signals(ticker, entry_price):
    try:
        # Fetch history to calculate volatility (ATR)
        df = yf.Ticker(ticker).history(period="3mo")
        if len(df) < 20: return None

        current_price = df['Close'].iloc[-1]

        # 1. Calculate ATR (Average True Range) - The Volatility Measure
        high_low = df['High'] - df['Low']
        high_close = np.abs(df['High'] - df['Close'].shift())
        low_close = np.abs(df['Low'] - df['Close'].shift())
        ranges = pd.concat([high_low, high_close, low_close], axis=1)
        true_range = np.max(ranges, axis=1)
        atr = true_range.rolling(14).mean().iloc[-1]

        # 2. Calculate Trailing Stop Price
        # The "High Watermark" is the highest price since we bought (simplified here to recent high)
        recent_high = df['High'].iloc[-20:].max()

        # If we just bought, the high watermark might be our entry.
        # We want to protect profits, so we base the stop off the HIGHEST price seen.
        stop_price = recent_high - (atr * TRAILING_STOP_ATR_MULTIPLIER)

        # 3. Check Status
        pnl_pct = (current_price - entry_price) / entry_price

        action = "HOLD"
        reason = "Trend is healthy"

        # Condition A: Hard Stop (Disaster Check)
        if pnl_pct < HARD_STOP_PERCENT:
            action = "SELL NOW"
            reason = f"Hard Stop Hit ({round(pnl_pct * 100, 1)}%)"

        # Condition B: Trailing Stop (Protect Profits)
        elif current_price < stop_price:
            action = "SELL NOW"
            reason = f"Trailing Stop Hit (Below {round(stop_price, 2)})"

        return {
            "Current_Price": round(current_price, 2),
            "Stop_Price": round(stop_price, 2),
            "PnL": f"{round(pnl_pct * 100, 1)}%",
            "ATR": round(atr, 2),
            "Action": action,
            "Reason": reason
        }
    except Exception as e:
        return None


# ==============================================================================
# MAIN EXECUTION
# ==============================================================================
if __name__ == "__main__":

    if not os.path.exists(PORTFOLIO_FILE):
        print(f"Portfolio file '{PORTFOLIO_FILE}' not found.")
        print("Please run 'generate_order_ticket.py' first, or create a CSV with columns: Ticker, Price, Shares.")
        exit()

    print("MONITORING PORTFOLIO HEALTH...")
    print("=" * 75)

    df_portfolio = pd.read_csv(PORTFOLIO_FILE)

    report = []

    for index, row in df_portfolio.iterrows():
        ticker = row['Ticker']
        entry_price = row['Price']  # This is your "Cost Basis"

        print(f"Checking {ticker}...", end="\r")

        data = get_exit_signals(ticker, entry_price)

        if data:
            entry = {
                "Ticker": ticker,
                "Shares": row['Shares'],
                "Entry_Cost": entry_price,
                "Current_Price": data['Current_Price'],
                "Market_Value": round(data['Current_Price'] * row['Shares'], 2),
                "PnL_%": data['PnL'],
                "Stop_Loss_Level": data['Stop_Price'],
                "Action": data['Action'],
                "Comment": data['Reason']
            }
            report.append(entry)

    print("\n" + "=" * 75)
    if report:
        df_report = pd.DataFrame(report)

        # Display the "Alerts" first (Rows where Action is SELL)
        alerts = df_report[df_report['Action'] == "SELL NOW"]
        if not alerts.empty:
            print("!!! ACTION REQUIRED !!!")
            print(alerts[['Ticker', 'PnL_%', 'Stop_Loss_Level', 'Comment']])
            print("-" * 75)

        print("FULL PORTFOLIO STATUS")
        # Clean display
        cols = ['Ticker', 'Entry_Cost', 'Current_Price', 'PnL_%', 'Stop_Loss_Level', 'Action']
        print(df_report[cols])

        # Total Stats
        total_invested = df_portfolio['Total_Cost'].sum()
        current_value = df_report['Market_Value'].sum()
        total_pnl = current_value - total_invested

        print("=" * 75)
        print(f"TOTAL INVESTED:  ${total_invested:,.2f}")
        print(f"CURRENT VALUE:   ${current_value:,.2f}")
        print(f"TOTAL P&L:       ${total_pnl:,.2f}")
    else:
        print("Error fetching portfolio data.")