import yfinance as yf
import pandas as pd
import numpy as np
import os
import warnings

warnings.filterwarnings("ignore")

# --- PATH CONFIG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_FILE = os.path.join(DATA_DIR, "filtered_watchlist.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "technical_analysis_report.csv")


def check_patterns(df):
    today = df.iloc[-1]
    yesterday = df.iloc[-2]
    signals = []

    # Hammer
    body = abs(today['Close'] - today['Open'])
    lower_wick = min(today['Open'], today['Close']) - today['Low']
    upper_wick = today['High'] - max(today['Open'], today['Close'])
    if (lower_wick > 2 * body) and (upper_wick < body): signals.append("HAMMER")

    # Engulfing
    if (yesterday['Close'] < yesterday['Open']) and \
            (today['Close'] > today['Open']) and \
            (today['Open'] < yesterday['Close']) and \
            (today['Close'] > yesterday['Open']):
        signals.append("ENGULFING")

    return signals


if __name__ == "__main__":
    if os.path.exists(INPUT_FILE):
        print(f"Loading tickers from {INPUT_FILE}...")
        df_watch = pd.read_csv(INPUT_FILE)
        tickers = df_watch['Ticker'].tolist()
    else:
        print("Using default list.")
        tickers = ["GOOGL", "AAPL", "MSFT", "AMZN", "TSLA"]

    results = []
    print(f"Running Technical Analysis on {len(tickers)} tickers...")

    for ticker in tickers:
        try:
            stock = yf.Ticker(ticker)
            df = stock.history(period="1y")

            if len(df) > 200:
                # Indicators
                df['SMA_200'] = df['Close'].rolling(200).mean()

                # RSI
                delta = df['Close'].diff()
                gain = (delta.where(delta > 0, 0)).rolling(14).mean()
                loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
                rs = gain / loss
                df['RSI'] = 100 - (100 / (1 + rs))

                curr = df.iloc[-1]
                patterns = check_patterns(df)

                # Status
                status = "WAIT"
                if curr['RSI'] < 35: status = "OVERSOLD"
                if "HAMMER" in patterns or "ENGULFING" in patterns: status = "PATTERN_TRIGGER"

                results.append({
                    "Ticker": ticker,
                    "Price": round(curr['Close'], 2),
                    "Status": status,
                    "RSI": round(curr['RSI'], 1),
                    "Patterns": " + ".join(patterns) if patterns else "None",
                    "Trend": "UPTREND" if curr['Close'] > curr['SMA_200'] else "DOWNTREND"
                })
        except:
            pass

    if results:
        df_out = pd.DataFrame(results)
        df_out.to_csv(OUTPUT_FILE, index=False)
        print(f"\nReport generated: {OUTPUT_FILE}")
    else:
        print("No results found.")