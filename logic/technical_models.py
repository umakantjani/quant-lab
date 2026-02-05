import pandas as pd
import pandas_ta as ta
import yfinance as yf
from sqlalchemy import create_engine
from logic.db_config import get_engine
import time
# --- CONFIGURATION ---
# DB_URI = "postgresql://quant:password@localhost:5432/stock_master"
# engine = create_engine(DB_URI)
engine = get_engine()

def run_technical_sniper():
    print("🎯 RUNNING TECHNICAL SNIPER (THE TRINITY)...")
    
    # 1. Get Tickers (Process Top 50 Liquid Stocks for speed, or all)
    try:
        tickers = pd.read_sql("SELECT symbol FROM tickers LIMIT 100", engine)['symbol'].tolist()
    except:
        # Fallback if DB is empty
        tickers = ["AAPL", "NVDA", "TSLA", "AMD", "AMZN", "GOOGL", "MSFT", "EPAM"]
    
    results = []
    print(f"   > Scanning {len(tickers)} stocks for setups...")
    
    for t in tickers:
        try:
            stock = yf.Ticker(t)
            # We need 1 year of data to calculate the 200-Day SMA
            df = stock.history(period="1y")
            
            if len(df) < 200: continue
            
            # --- 1. THE TREND (200 SMA) ---
            sma_200 = df['Close'].rolling(200).mean().iloc[-1]
            price = df['Close'].iloc[-1]
            
            # Trend Check: PASS if Price > SMA 200
            trend_pass = price > sma_200
            trend_dist = (price - sma_200) / sma_200
            
            # --- 2. THE ZONE (RSI) ---
            # Manual RSI 14 Calculation to avoid extra dependencies
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            rsi = df['RSI'].iloc[-1]
            
            # Zone Check: PASS if RSI < 30 (Oversold) or RSI > 70 (Overbought - for shorts)
            # For Buying, we want cheap.
            zone_pass = rsi < 35 # slightly loose filter for "Watch" list
            
            # --- 3. THE TRIGGER (CANDLES) ---
            # Hammer Detection
            row = df.iloc[-1]
            body = abs(row['Close'] - row['Open'])
            lower_wick = min(row['Open'], row['Close']) - row['Low']
            upper_wick = row['High'] - max(row['Open'], row['Close'])
            
            # Hammer Rule: Lower wick is 2x the body, Upper wick is tiny
            is_hammer = (lower_wick > 2 * body) and (upper_wick < body)
            
            # Engulfing Detection
            prev = df.iloc[-2]
            is_engulfing = (row['Close'] > row['Open']) and \
                           (row['Open'] < prev['Close']) and \
                           (row['Close'] > prev['Open']) and \
                           (prev['Close'] < prev['Open'])
                           
            trigger_type = "Hammer" if is_hammer else ("Engulfing" if is_engulfing else "None")
            trigger_pass = is_hammer or is_engulfing

            # --- FINAL SCORING ---
            signal = "WAIT"
            if zone_pass and trigger_pass:
                if trend_pass: signal = "STRONG BUY" # The Trinity!
                else: signal = "RISKY BUY" # Counter-Trend (like EPAM)
            elif zone_pass:
                signal = "WATCH" # Cheap, but no trigger yet
            
            # Only save interesting stocks
            if signal != "WAIT":
                results.append({
                    "ticker": t,
                    "price": price,
                    "trend_200_sma": round(sma_200, 2),
                    "trend_pass": trend_pass,
                    "rsi": round(rsi, 1),
                    "zone_pass": zone_pass,
                    "trigger_type": trigger_type,
                    "trigger_pass": trigger_pass,
                    "Signal": signal
                })
                print(f"   > {t}: {signal} | RSI: {int(rsi)} | Trigger: {trigger_type}")
                
        except Exception as e:
            continue

    # Save to DB
    if results:
        df_res = pd.DataFrame(results)
        df_res.to_sql('technical_signals', engine, if_exists='replace', index=False)
        print(f"✅ SCAN COMPLETE. Found {len(df_res)} signals.")
    else:
        print("No signals found.")

if __name__ == "__main__":
    run_technical_sniper()