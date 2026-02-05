import google.generativeai as genai
import pandas as pd
import yfinance as yf
import os
import streamlit as st
from sqlalchemy import text
from logic.db_config import get_engine

# --- CONFIGURATION ---
engine = get_engine()

# Try to get API Key from Streamlit Secrets or Environment Variable
try:
    API_KEY = st.secrets["GEMINI_API_KEY"]
except:
    try:
        API_KEY = os.environ.get("GEMINI_API_KEY")
    except:
        API_KEY = None

def get_valuation(ticker):
    """Fetches valuation data from your DB."""
    try:
        query = text(f"SELECT * FROM alpha_valuation WHERE ticker = '{ticker}'")
        df = pd.read_sql(query, engine)
        if not df.empty:
            return df.iloc[0].to_dict()
        return None
    except:
        return None

def generate_ai_report(ticker):
    """Orchestrates the data gathering and AI generation."""
    
    # 1. CHECK FOR API KEY
    if not API_KEY:
        return "❌ **MISSING API KEY.** Please add `GEMINI_API_KEY` to your `.streamlit/secrets.toml` file."

    # 2. GATHER RAW DATA (The "Context")
    try:
        stock = yf.Ticker(ticker)
        hist = stock.history(period="1y")
        if len(hist) < 200: return f"⚠️ Not enough data for {ticker}."
        
        # Calculate Technicals
        price = hist['Close'].iloc[-1]
        sma_200 = hist['Close'].rolling(200).mean().iloc[-1]
        
        # RSI 14
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = (100 - (100 / (1 + rs))).iloc[-1]
        
        # Valuation
        val_data = get_valuation(ticker)
        val_text = "No DCF Valuation available."
        if val_data:
            val_text = f"""
            - Intrinsic Value: ${val_data.get('intrinsic_value', 0)}
            - Upside Potential: {val_data.get('upside_pct', 0)}%
            - WACC: {val_data.get('wacc_pct', 0)}%
            """

        # 3. CONSTRUCT THE PROMPT
        prompt = f"""
        You are a Senior Technical Analyst at a top Hedge Fund. You use the "Trinity" methodology (Trend, Zone, Trigger).
        
        Analyze the following stock data for **{ticker}** and write a professional "Executive Summary" report.
        
        ### RAW DATA:
        - **Current Price:** ${price:.2f}
        - **200-Day SMA (The Trend):** ${sma_200:.2f} (Price is {"ABOVE" if price > sma_200 else "BELOW"} the trend)
        - **RSI (The Zone):** {rsi:.1f} ({"OVERSOLD (<35)" if rsi < 35 else "OVERBOUGHT (>70)" if rsi > 70 else "NEUTRAL"})
        - **Valuation Data:** {val_text}
        
        ### INSTRUCTIONS:
        1. **Tone:** Professional, direct, "Wall Street" style. No fluff.
        2. **Structure:**
           - **Executive Summary:** Give a clear signal (STRONG BUY, WAIT, or SELL).
           - **The Trinity Analysis:** Bullet points analyzing the Trend (Tide), Zone (RSI), and Trigger.
           - **Valuation Check:** Is it fundamentally cheap?
           - **The Trade Plan:** Give specific price levels to watch (Support/Resistance).
        3. **Formatting:** Use Markdown, bolding, and emojis like ✅, ❌, ⚠️.
        
        Write the report now.
        """

        # 4. CALL GEMINI API (ROBUST MODE)
        genai.configure(api_key=API_KEY)
        
        # List of models to try in order of preference
        # This handles region differences and version deprecation automatically
        models_to_try = ['gemini-1.5-flash', 'gemini-pro', 'gemini-1.5-pro-latest']
        
        last_error = ""
        
        for model_name in models_to_try:
            try:
                # print(f"Trying model: {model_name}...") # Debug log
                model = genai.GenerativeModel(model_name)
                response = model.generate_content(prompt)
                return response.text
            except Exception as e:
                last_error = str(e)
                continue # Try the next model
        
        # If we get here, all models failed
        return f"❌ AI Connection Failed. Tried {models_to_try}. Last error: {last_error}"

    except Exception as e:
        return f"❌ System Error: {str(e)}"