import yfinance as yf
import pandas as pd
import os
import warnings

warnings.filterwarnings("ignore")

# --- PATH CONFIG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
INPUT_FILE = os.path.join(DATA_DIR, "my_deep_value_portfolio.csv")
BACKUP_FILE = os.path.join(DATA_DIR, "filtered_watchlist.csv")
OUTPUT_FILE = os.path.join(DATA_DIR, "my_etf_strategy.csv")

# --- SECTOR MAP ---
SECTOR_ETF_MAP = {
    "Technology": "XLK",
    "Healthcare": "XLV",
    "Financial Services": "XLF",
    "Energy": "XLE",
    "Basic Materials": "XLB",
    "Industrials": "XLI",
    "Consumer Cyclical": "XLY",
    "Consumer Defensive": "XLP",
    "Utilities": "XLU",
    "Real Estate": "XLRE",
    "Communication Services": "XLC"
}


def get_etf_technicals(ticker):
    """Checks if the ETF itself is healthy."""
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y")

        if len(hist) < 200: return None

        curr_price = hist['Close'].iloc[-1]
        sma_200 = hist['Close'].rolling(200).mean().iloc[-1]

        # RSI
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(14).mean()
        rs = gain / loss
        rsi = 100 - (100 / (1 + rs))

        return {
            "ETF_Price": round(curr_price, 2),
            "ETF_Trend": "UPTREND" if curr_price > sma_200 else "DOWNTREND",
            "ETF_RSI": round(rsi.iloc[-1], 1)
        }
    except:
        return None


if __name__ == "__main__":
    # 1. Load Data
    if os.path.exists(INPUT_FILE):
        print(f"Reading from {INPUT_FILE}...")
        df = pd.read_csv(INPUT_FILE)
        tickers = df['Ticker'].tolist()
    elif os.path.exists(BACKUP_FILE):
        print(f"Reading from {BACKUP_FILE}...")
        df = pd.read_csv(BACKUP_FILE)
        tickers = df['Ticker'].tolist()
    else:
        print("No input file found.")
        tickers = []

    if tickers:
        print(f"Mapping {len(tickers)} stocks to sectors...")
        sector_counts = {}

        for t in tickers:
            try:
                info = yf.Ticker(t).info
                sector = info.get('sector', 'Unknown')
                if sector == "Financials": sector = "Financial Services"

                etf = SECTOR_ETF_MAP.get(sector, "VTI")

                if etf in sector_counts:
                    sector_counts[etf]['count'] += 1
                    sector_counts[etf]['stocks'].append(t)
                else:
                    sector_counts[etf] = {'count': 1, 'stocks': [t], 'sector': sector}
            except:
                pass

        # 2. Analyze ETFs
        results = []
        total = len(tickers)

        for etf, data in sector_counts.items():
            print(f"Checking {etf}...", end="\r")
            techs = get_etf_technicals(etf)
            if techs:
                weight = data['count'] / total
                action = "BUY" if techs['ETF_Trend'] == "UPTREND" and techs['ETF_RSI'] < 60 else "WAIT"

                results.append({
                    "ETF": etf,
                    "Sector": data['sector'],
                    "Weight": f"{round(weight * 100, 1)}%",
                    "Trend": techs['ETF_Trend'],
                    "Action": action,
                    "Holdings": ", ".join(data['stocks'])
                })

        # 3. Save
        if results:
            df_out = pd.DataFrame(results)
            df_out.to_csv(OUTPUT_FILE, index=False)
            print(f"\nSaved ETF Strategy to {OUTPUT_FILE}")
            print(df_out[['ETF', 'Sector', 'Action']])
        else:
            print("\nNo valid ETF data found.")