import yfinance as yf
import pandas as pd
import requests
from io import StringIO
import os

# --- PATH CONFIG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
OUTPUT_FILE = os.path.join(DATA_DIR, "filtered_watchlist.csv")


# ==============================================================================
# 1. LIST FETCHING FUNCTIONS
# ==============================================================================

def get_headers():
    return {"User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)"}


def fetch_sp500():
    """Fetches the S&P 500 Tickers"""
    try:
        url = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
        resp = requests.get(url, headers=get_headers())
        table = pd.read_html(StringIO(resp.text))
        # The S&P 500 table is usually the first one [0]
        df = table[0]
        return df['Symbol'].tolist()
    except Exception as e:
        print(f"Error fetching S&P 500: {e}")
        return []


def fetch_nasdaq100():
    """Fetches the Nasdaq 100 Tickers"""
    try:
        url = "https://en.wikipedia.org/wiki/Nasdaq-100"
        resp = requests.get(url, headers=get_headers())
        tables = pd.read_html(StringIO(resp.text))

        # We need to find the table with 'Ticker' or 'Symbol'
        for t in tables:
            if 'Ticker' in t.columns:
                return t['Ticker'].tolist()
            elif 'Symbol' in t.columns:
                return t['Symbol'].tolist()
        return []
    except Exception as e:
        print(f"Error fetching Nasdaq 100: {e}")
        return []


def fetch_dow_jones():
    """Fetches the Dow Jones Industrial Average (DJIA) Tickers"""
    try:
        url = "https://en.wikipedia.org/wiki/Dow_Jones_Industrial_Average"
        resp = requests.get(url, headers=get_headers())
        tables = pd.read_html(StringIO(resp.text))

        # Dow table is usually Table 1, looking for 'Symbol'
        for t in tables:
            if 'Symbol' in t.columns:
                return t['Symbol'].tolist()
        return []
    except Exception as e:
        print(f"Error fetching Dow Jones: {e}")
        return []


def get_combined_market_list():
    """Aggregates all lists and removes duplicates."""
    print("Fetching indices...")
    sp500 = fetch_sp500()
    nasdaq = fetch_nasdaq100()
    dow = fetch_dow_jones()

    print(f"  > S&P 500 found: {len(sp500)}")
    print(f"  > Nasdaq 100 found: {len(nasdaq)}")
    print(f"  > Dow Jones found: {len(dow)}")

    # Combine and Deduplicate
    combined = list(set(sp500 + nasdaq + dow))

    # Clean Tickers (Replace dots with dashes, e.g., BRK.B -> BRK-B)
    cleaned = [t.replace('.', '-') for t in combined]

    print(f"  > TOTAL UNIQUE TICKERS: {len(cleaned)}")
    return cleaned


# ==============================================================================
# 2. ANALYSIS LOGIC
# ==============================================================================

def analyze_ticker(ticker):
    try:
        # Download 1 year of data
        df = yf.download(ticker, period="1y", progress=False)

        # Handling Yahoo's sometimes empty returns
        if len(df) < 200: return None

        # Handle MultiIndex columns if Yahoo returns them
        try:
            close_prices = df['Close']
            # If it's a dataframe (multi-column), squeeze it
            if isinstance(close_prices, pd.DataFrame):
                close_prices = close_prices.iloc[:, 0]

            curr = close_prices.iloc[-1]
            sma200 = close_prices.rolling(200).mean().iloc[-1]

            high_prices = df['High']
            if isinstance(high_prices, pd.DataFrame):
                high_prices = high_prices.iloc[:, 0]
            high = high_prices.max()

        except Exception as e:
            # print(f"Data formatting error for {ticker}: {e}")
            return None

        # --- CALCULATIONS ---
        # 1. RSI (Relative Strength Index)
        delta = close_prices.diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))
        curr_rsi = rsi.iloc[-1]

        # 2. Discount from High
        discount_pct = (high - curr) / high

        # --- THE FILTER ---
        # Rule 1: Not Overbought (RSI < 60) - We want value/dips
        # Rule 2: Long Term Uptrend (Price > 200 SMA) - We want healthy trends
        # Rule 3: Pullback (At least 5% off highs) - We want a discount

        if (curr_rsi < 60) and (curr > sma200) and (discount_pct > 0.05):
            return {
                "Ticker": ticker,
                "Price": round(curr, 2),
                "RSI": round(curr_rsi, 1),
                "Discount": f"{round(discount_pct * 100, 1)}%"
            }
        return None

    except Exception:
        return None


# ==============================================================================
# 3. MAIN EXECUTION
# ==============================================================================

if __name__ == "__main__":
    if not os.path.exists(DATA_DIR): os.makedirs(DATA_DIR)

    # 1. Get the Expanded List
    tickers = get_combined_market_list()
    results = []

    print("-" * 50)
    print(f"Scanning {len(tickers)} stocks for 'Uptrend Pullbacks'...")

    # --- SCANNING LOOP ---
    # NOTE: Depending on your internet speed, scanning 600+ stocks can take 2-3 mins.
    # To test quickly, you can uncomment the next line:
    # tickers = tickers[:20]

    for i, t in enumerate(tickers):
        # Simple progress bar
        if i % 10 == 0: print(f"Scanning {i}/{len(tickers)}...", end="\r")

        data = analyze_ticker(t)
        if data: results.append(data)

    # 2. Save Results
    if results:
        df = pd.DataFrame(results)
        # Sort by RSI (Most oversold first)
        df = df.sort_values(by="RSI", ascending=True)

        df.to_csv(OUTPUT_FILE, index=False)
        print(f"\n\nSUCCESS: Found {len(df)} candidates.")
        print(f"Saved to: {OUTPUT_FILE}")
        print("-" * 50)
        print(df.head(5))
    else:
        print("\nNo stocks matched the criteria.")