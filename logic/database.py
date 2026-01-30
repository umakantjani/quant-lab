import sqlite3
import pandas as pd
import os
from datetime import datetime

# --- PATH CONFIG ---
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
DB_FILE = os.path.join(DATA_DIR, "portfolio.db")


def init_db():
    if not os.path.exists(DATA_DIR):
        os.makedirs(DATA_DIR)

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # 1. Holdings (What you own)
    c.execute('''CREATE TABLE IF NOT EXISTS holdings 
                 (ticker TEXT PRIMARY KEY, shares REAL, avg_cost REAL)''')

    # 2. Trade History (What you did)
    c.execute('''CREATE TABLE IF NOT EXISTS history 
                 (date TEXT, ticker TEXT, action TEXT, shares REAL, price REAL)''')

    # 3. Prediction Log (What the model thought - For Backtesting)
    c.execute('''CREATE TABLE IF NOT EXISTS predictions 
                 (run_date TEXT, ticker TEXT, model_price REAL, fair_value REAL, 
                  upside_pct REAL, rsi REAL, rating TEXT)''')

    conn.commit()
    conn.close()


# --- PORTFOLIO FUNCTIONS ---
def execute_trade(ticker, shares, price, action="BUY"):
    """Updates holdings and logs transaction history."""
    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    # Update Holdings
    c.execute("SELECT shares, avg_cost FROM holdings WHERE ticker=?", (ticker,))
    curr = c.fetchone()

    if action == "BUY":
        if curr:
            new_shares = curr[0] + shares
            # Weighted Average Cost
            new_cost = ((curr[0] * curr[1]) + (shares * price)) / new_shares
            c.execute("UPDATE holdings SET shares=?, avg_cost=? WHERE ticker=?", (new_shares, new_cost, ticker))
        else:
            c.execute("INSERT INTO holdings VALUES (?, ?, ?)", (ticker, shares, price))

    elif action == "SELL":
        if curr:
            left = curr[0] - shares
            if left <= 0:
                c.execute("DELETE FROM holdings WHERE ticker=?", (ticker,))
            else:
                c.execute("UPDATE holdings SET shares=? WHERE ticker=?", (left, ticker))

    # Log to History
    c.execute("INSERT INTO history VALUES (datetime('now', 'localtime'), ?, ?, ?, ?)",
              (ticker, action, shares, price))

    conn.commit()
    conn.close()


def get_portfolio():
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql("SELECT * FROM holdings", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df


# --- LOGGING FUNCTIONS (For Backtesting) ---
def log_model_run(df_results):
    """Saves a snapshot of the valuation run for future backtesting."""
    if df_results.empty: return

    conn = sqlite3.connect(DB_FILE)
    c = conn.cursor()

    run_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # We expect the DF to have columns from valuation.py
    # Mapping CSV columns to DB columns
    for _, row in df_results.iterrows():
        try:
            ticker = row.get('Ticker')
            price = row.get('Current Price')
            fair_val = row.get('Fair Value')

            # Clean up Upside string ("15.5%") -> float (15.5)
            upside = row.get('Upside %')
            if isinstance(upside, str):
                upside = float(upside.strip('%'))

            rsi = row.get('RSI', 0)
            rating = row.get('Rating', 'N/A')

            c.execute('''INSERT INTO predictions VALUES (?, ?, ?, ?, ?, ?, ?)''',
                      (run_date, ticker, price, fair_val, upside, rsi, rating))
        except Exception as e:
            print(f"Skipping log for row: {e}")
            continue

    conn.commit()
    conn.close()


def get_prediction_logs():
    conn = sqlite3.connect(DB_FILE)
    try:
        df = pd.read_sql("SELECT * FROM predictions ORDER BY run_date DESC", conn)
    except:
        df = pd.DataFrame()
    conn.close()
    return df