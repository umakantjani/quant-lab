import google.generativeai as genai
import pandas as pd
import yfinance as yf
import os
import streamlit as st
from sqlalchemy import text
from logic.db_config import get_engine

# --- CONFIGURATION ---
engine = get_engine()

try:
    API_KEY = st.secrets["GEMINI_API_KEY"]
except:
    try: API_KEY = os.environ.get("GEMINI_API_KEY")
    except: API_KEY = None

def get_valuation(ticker):
    try:
        query = text(f"SELECT * FROM alpha_valuation WHERE ticker = '{ticker}'")
        df = pd.read_sql(query, engine)
        if not df.empty: return df.iloc[0].to_dict()
        return None
    except: return None

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
        
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = (100 - (100 / (1 + rs))).iloc[-1]
        
        val_data = get_valuation(ticker)
        val_text = "No DCF Valuation available."
        if val_data:
            val_text = f"Intrinsic Value: ${val_data.get('intrinsic_value', 0)} (Upside: {val_data.get('upside_pct', 0)}%)"

        # 3. PROMPT
        prompt = f"""
        You are a Senior Technical Analyst. Analyze {ticker}.
        
        DATA:
        - Price: ${price:.2f}
        - 200 SMA: ${sma_200:.2f} (Trend is {"BULLISH" if price > sma_200 else "BEARISH"})
        - RSI: {rsi:.1f}
        - Value: {val_text}
        
        Task: Write a short, punchy Executive Summary (Buy/Sell/Wait) and 3 bullet points on the Trinity (Trend, Zone, Trigger).
        """

        # 4. CALL API (Safety Mode)
        genai.configure(api_key=API_KEY)
        
        # We try the most basic stable model ID first
        try:
            model = genai.GenerativeModel('gemini-1.0-pro')
            response = model.generate_content(prompt)
            return response.text
        except Exception as e1:
            # Fallback to listing models to debug
            try:
                available = [m.name for m in genai.list_models()]
                return f"❌ Connection Error. Available models: {available}. Error: {e1}"
            except Exception as e2:
                return f"❌ Critical API Error: {e1}"

    except Exception as e:
        return f"❌ System Error: {str(e)}"