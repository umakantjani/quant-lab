import google.generativeai as genai
import pandas as pd
import yfinance as yf
import os
import streamlit as st
from sqlalchemy import text, create_engine, inspect
from logic.db_config import get_engine
from datetime import datetime

# --- CONFIGURATION ---
engine = get_engine()

try:
    API_KEY = st.secrets["GEMINI_API_KEY"]
except:
    try: API_KEY = os.environ.get("GEMINI_API_KEY")
    except: API_KEY = None

# --- DATABASE INIT (Auto-Create Table) ---
def init_reports_db():
    """Creates the reports table if it doesn't exist."""
    try:
        with engine.connect() as conn:
            conn.execute(text("""
                CREATE TABLE IF NOT EXISTS research_reports (
                    id SERIAL PRIMARY KEY,
                    ticker TEXT,
                    report_date DATE,
                    version INT,
                    signal TEXT,
                    content TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """))
            conn.commit()
    except Exception as e:
        # Fallback for SQLite (if not using Postgres)
        pass

init_reports_db()

# --- CORE FUNCTIONS ---

def get_valuation(ticker):
    try:
        query = text(f"SELECT * FROM alpha_valuation WHERE ticker = '{ticker}'")
        df = pd.read_sql(query, engine)
        if not df.empty: return df.iloc[0].to_dict()
        return None
    except: return None

def save_report(ticker, content, signal="NEUTRAL"):
    """Saves the report with auto-versioning."""
    today = datetime.now().date()
    
    with engine.connect() as conn:
        # Check for existing version today
        query = text(f"SELECT MAX(version) FROM research_reports WHERE ticker = '{ticker}' AND report_date = '{today}'")
        result = conn.execute(query).scalar()
        
        new_version = 1 if result is None else result + 1
        
        # Parse Signal if generic
        if "STRONG BUY" in content: signal = "STRONG BUY"
        elif "SELL" in content: signal = "SELL"
        elif "WAIT" in content: signal = "WAIT"
        
        # Insert
        insert_sql = text("""
            INSERT INTO research_reports (ticker, report_date, version, signal, content)
            VALUES (:t, :d, :v, :s, :c)
        """)
        conn.execute(insert_sql, {"t": ticker, "d": today, "v": new_version, "s": signal, "c": content})
        conn.commit()
        
    return new_version

def get_report_history(ticker=None):
    """Fetches historical reports."""
    sql = "SELECT ticker, report_date, version, signal, created_at, content FROM research_reports"
    if ticker:
        sql += f" WHERE ticker = '{ticker}'"
    sql += " ORDER BY report_date DESC, version DESC"
    
    return pd.read_sql(sql, engine)

def generate_ai_report(ticker):
    # 1. CHECK FOR API KEY
    if not API_KEY:
        return "❌ **MISSING API KEY.** Please add `GEMINI_API_KEY` to your `.streamlit/secrets.toml` file."

    # 2. GATHER RAW DATA
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y")
        if len(hist) < 200: return f"⚠️ Not enough data for {ticker}."
        
        price = hist['Close'].iloc[-1]
        sma_200 = hist['Close'].rolling(200).mean().iloc[-1]
        sma_50 = hist['Close'].rolling(50).mean().iloc[-1]
        
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = (100 - (100 / (1 + rs))).iloc[-1]
        
        val_data = get_valuation(ticker)
        if val_data:
            val_context = f"- **Intrinsic Value (DCF):** ${val_data.get('intrinsic_value', 0)} (Upside: {val_data.get('upside_pct', 0)}%)"
        else:
            val_context = "- **Intrinsic Value:** Data Unavailable."

        report_date = datetime.now().strftime("%B %d, %Y")

        # 3. PROMPT
        prompt = f"""
        You are the CIO of a Quantitative Hedge Fund. Write a Detailed Investment Memo for **{ticker}**.
        
        ### RAW DATA
        - Date: {report_date}
        - Price: ${price:.2f}
        - Trend: 200 SMA is ${sma_200:.2f} (Price is {"ABOVE" if price > sma_200 else "BELOW"})
        - RSI: {rsi:.1f}
        - Value: {val_context}
        
        ### STRUCTURE
        # Detailed Investment Report: {ticker}
        **Date:** {report_date} | **Price:** ${price:.2f}

        ## 1. Executive Summary
        * **Regime:** (Bull/Bear/Correction)
        * **Signal:** (BUY/SELL/WAIT)
        * **Target Entry:** (Price Level)

        ## 2. Technical Trinity
        * **Trend:** (Analysis of 200 SMA)
        * **Zone:** (Analysis of RSI)
        * **Trigger:** (Candlestick/Price Action)

        ## 3. Valuation
        * **Verdict:** (Cheap/Expensive based on DCF)

        ## 4. Trade Plan
        * **Strategy:** (e.g. Trend Follow, Dip Buy)
        * **Stop Loss:** (Level)
        """

        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('models/gemini-2.5-flash') 
        
        try:
            response = model.generate_content(prompt)
            return response.text
        except:
            model = genai.GenerativeModel('models/gemini-flash-latest')
            response = model.generate_content(prompt)
            return response.text

    except Exception as e:
        return f"❌ System Error: {str(e)}"