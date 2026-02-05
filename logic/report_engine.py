import google.generativeai as genai
import pandas as pd
import yfinance as yf
import os
import streamlit as st
from sqlalchemy import text
from logic.db_config import get_engine
from datetime import datetime

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
        
        # Current Data
        price = hist['Close'].iloc[-1]
        sma_200 = hist['Close'].rolling(200).mean().iloc[-1]
        sma_50 = hist['Close'].rolling(50).mean().iloc[-1]
        
        # RSI 14 Calculation
        delta = hist['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
        rs = gain / loss
        rsi = (100 - (100 / (1 + rs))).iloc[-1]
        
        # Valuation Data
        val_data = get_valuation(ticker)
        if val_data:
            fair_value = val_data.get('intrinsic_value', 0)
            upside = val_data.get('upside_pct', 0)
            wacc = val_data.get('wacc_pct', 0)
            val_context = f"""
            - **Intrinsic Value (DCF):** ${fair_value:.2f}
            - **Current Upside to Fair Value:** {upside:.1f}%
            - **WACC (Risk Level):** {wacc:.1f}%
            - **Verdict:** {'UNDERVALUED' if upside > 15 else 'OVERVALUED' if upside < -15 else 'FAIRLY VALUED'}
            """
        else:
            val_context = "- **Intrinsic Value:** Data Unavailable (Run Valuation Pipeline). Assume 'Fairly Valued' for now."

        # Date for the report
        report_date = datetime.now().strftime("%B %d, %Y")

        # 3. THE "DEEP DIVE" PROMPT
        prompt = f"""
        You are the Chief Investment Officer (CIO) of a Quantitative Hedge Fund. 
        Write a **Detailed Investment Memo** for **{ticker}** based on the data below.
        
        ### 📊 RAW DATA
        - **Date:** {report_date}
        - **Price:** ${price:.2f}
        - **200-Day SMA (Trend):** ${sma_200:.2f} (Price is {"ABOVE" if price > sma_200 else "BELOW"} Trend)
        - **50-Day SMA:** ${sma_50:.2f}
        - **RSI (14):** {rsi:.1f} ({"EXTREME OVERSOLD" if rsi < 30 else "OVERSOLD" if rsi < 40 else "OVERBOUGHT" if rsi > 70 else "NEUTRAL"})
        - **Valuation:** {val_context}
        
        ### 📝 REPORT STRUCTURE (Strictly follow this Markdown format)

        # Detailed Technical & Valuation Report: {ticker}
        **Date:** {report_date} | **Current Price:** ${price:.2f}

        ## 1. Executive Summary: The Regime Check
        * **The Narrative:** Describe if the stock is in a Bull Market, Bear Market, or "Falling Knife" scenario based on the Price vs 200 SMA.
        * **Short-Term Signal:** (BUY / SELL / WAIT).
        * **Long-Term Signal:** (ACCUMULATE / DISTRIBUTE).
        * **The "Kill Zone" (Buy Target):** Estimate a safe entry price based on the 200 SMA or psychological round numbers below current price.

        ## 2. Technical Analysis: The Sniper's Trinity
        ### A. The Tide (Trend) - {"✅ PASS" if price > sma_200 else "❌ FAIL"}
        * **Indicator:** 200-Day SMA.
        * **Status:** Price is trading {"ABOVE" if price > sma_200 else "BELOW"} the long-term trend line (${sma_200:.2f}).
        * **Interpretation:** Explain what this means for institutional flows (Accumulation vs Distribution). Mention the 50-Day SMA trend.

        ### B. The Zone (Momentum) - {"✅ PASS (Oversold)" if rsi < 40 else "⚠️ NEUTRAL" if rsi < 70 else "❌ FAIL (Overbought)"}
        * **Indicator:** RSI (14-Day).
        * **Status:** Current Level is {rsi:.1f}.
        * **Interpretation:** Is this a panic selling opportunity (RSI < 30) or a "don't chase" moment?

        ### C. The Trigger (Price Action)
        * **Indicator:** Candlestick & Support.
        * **Status:** [Analyze recent price action - infer from price level].
        * **Support Levels:** Identify the nearest round number support or the 200 SMA level.

        ## 3. Valuation Analysis: The Safety Margin
        * **Damodaran Intrinsic Value:** Use the provided valuation data.
        * **Verdict:** Is the stock cheap? (Compare Price to Intrinsic Value).
        * **The Logic:** Briefly explain *why* it might be cheap/expensive (e.g., market overreaction vs fundamental deterioration).

        ## 4. The Trade Plan
        * **Strategy:** Give a named strategy (e.g., "The Trap & Snap", "Trend Follow", "Wait & See").
        * **Entry Trigger:** Specific price to watch for.
        * **Stop Loss (Mental):** A level where the trade fails.
        * **Conclusion:** One final sentence on whether to act today or wait.
        """

        # 4. CALL API (Using the Robust 2.5 Flash Model)
        genai.configure(api_key=API_KEY)
        model = genai.GenerativeModel('models/gemini-2.5-flash')
        
        try:
            response = model.generate_content(prompt)
            return response.text
        except Exception as e:
            # Fallback to standard flash if 2.5 is busy/region-locked
            model = genai.GenerativeModel('models/gemini-flash-latest')
            response = model.generate_content(prompt)
            return response.text

    except Exception as e:
        return f"❌ System Error: {str(e)}"