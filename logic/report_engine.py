import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine, text
from logic.db_config import get_engine
from datetime import datetime

# --- CONFIGURATION ---
engine = get_engine()

def check_trinity(df):
    """Calculates the Technical Trinity status."""
    # 1. Trend (200 SMA)
    sma_200 = df['Close'].rolling(200).mean().iloc[-1]
    price = df['Close'].iloc[-1]
    trend = "BULL" if price > sma_200 else "BEAR"
    
    # 2. Zone (RSI 14)
    delta = df['Close'].diff()
    gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
    rs = gain / loss
    df['RSI'] = 100 - (100 / (1 + rs))
    rsi = df['RSI'].iloc[-1]
    
    zone = "OVERSOLD" if rsi < 35 else ("OVERBOUGHT" if rsi > 70 else "NEUTRAL")
    
    # 3. Trigger (Hammer/Engulfing)
    row = df.iloc[-1]
    body = abs(row['Close'] - row['Open'])
    lower = min(row['Open'], row['Close']) - row['Low']
    upper = row['High'] - max(row['Open'], row['Close'])
    
    is_hammer = (lower > 2 * body) and (upper < body)
    prev = df.iloc[-2]
    is_engulfing = (row['Close'] > row['Open']) and (row['Open'] < prev['Close']) and (row['Close'] > prev['Open'])
    
    trigger = "HAMMER" if is_hammer else ("ENGULFING" if is_engulfing else "NONE")
    
    return {
        "price": price,
        "sma_200": sma_200,
        "trend": trend,
        "rsi": rsi,
        "zone": zone,
        "trigger": trigger
    }

def get_valuation(ticker):
    """Fetches valuation data from your DB if it exists."""
    try:
        query = text(f"SELECT * FROM alpha_valuation WHERE ticker = '{ticker}'")
        df = pd.read_sql(query, engine)
        if not df.empty:
            return df.iloc[0]
        return None
    except:
        return None

def generate_ai_report(ticker):
    """Generates the 'Gemini-Style' Executive Report."""
    try:
        stock = yf.Ticker(ticker)
        df = stock.history(period="1y")
        
        if len(df) < 200:
            return f"⚠️ Not enough data to analyze {ticker}."
            
        # 1. Run The Numbers
        tech = check_trinity(df)
        val = get_valuation(ticker)
        
        # 2. Determine The Narrative
        date_str = datetime.now().strftime("%B %d, %Y")
        
        # Signal Decoder
        if tech['trend'] == "BULL" and tech['zone'] == "OVERSOLD" and tech['trigger'] != "NONE":
            summary = "🟢 **STRONG BUY** (The Perfect Setup)"
            bias = "Bullish"
        elif tech['trend'] == "BEAR" and tech['zone'] == "OVERSOLD":
            summary = "🟡 **WATCHLIST / RISKY BUY** (Falling Knife Candidate)"
            bias = "Bearish (Correction)"
        elif tech['trend'] == "BEAR":
            summary = "🔴 **AVOID / SELL** (Broken Trend)"
            bias = "Bearish"
        else:
            summary = "⚪ **HOLD / WAIT** (No Edge)"
            bias = "Neutral"

        # 3. Build the Markdown Report
        report = f"""
### 🕵️‍♂️ **GEMINI ANALYST REPORT: {ticker}**
**Date:** {date_str}  
**Price:** ${tech['price']:.2f}

---

#### **1. EXECUTIVE SUMMARY**
**Signal:** {summary}  
The stock is currently in a **{bias}** regime. 

"""
        # Add Trinity Section
        trend_icon = "✅" if tech['trend'] == "BULL" else "❌"
        zone_icon = "✅" if tech['zone'] == "OVERSOLD" else "⚠️"
        trig_icon = "✅" if tech['trigger'] != "NONE" else "❌"
        
        report += f"""
#### **2. TECHNICAL TRINITY (The Rules)**
* {trend_icon} **The Tide (Trend):** {tech['trend']}
    * *Price (${tech['price']:.2f}) vs 200 SMA (${tech['sma_200']:.2f})*
    * *Verdict:* {"Institutions are buying." if tech['trend'] == "BULL" else "Institutions are selling (Distributing)."}
    
* {zone_icon} **The Zone (Momentum):** {tech['zone']} (RSI: {tech['rsi']:.1f})
    * *Verdict:* {"Price is unfairly cheap (Panic Selling)." if tech['zone'] == "OVERSOLD" else "Price is in 'No Man's Land'."}

* {trig_icon} **The Trigger (Action):** {tech['trigger']}
    * *Verdict:* {"Buyers have stepped in (Reversal Pattern)." if tech['trigger'] != "NONE" else "No sign of a reversal yet."}

---
"""
        # Add Valuation Section
        if val is not None:
            upside = val['upside_pct']
            report += f"""
#### **3. FUNDAMENTAL VALUATION**
* **Intrinsic Value:** ${val['intrinsic_value']:.2f}
* **Current Price:** ${val['current_price']:.2f}
* **Upside Potential:** **{upside:.1f}%**
* **WACC (Risk):** {val['wacc_pct']:.1f}%

**Analyst Note:** The stock is trading at a {upside:.1f}% discount to its fair value based on cash flows.
"""
        else:
            report += """
#### **3. FUNDAMENTAL VALUATION**
* *No Deep Value data found in database.*
* *Action: Run the 'Valuation Pipeline' in Mode 1 to calculate Intrinsic Value.*
"""

        return report

    except Exception as e:
        return f"❌ Error generating report: {str(e)}"