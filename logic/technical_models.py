import pandas as pd
import yfinance as yf
from sqlalchemy import create_engine
from logic.db_config import get_engine
import time

# --- CONFIGURATION ---
engine = get_engine()

def run_technical_sniper():
    print("🎯 RUNNING TECHNICAL SNIPER (FULL MARKET CHECKLIST)...")
    
    # 1. Get Tickers (Increase limit to see more stocks)
    try:
        # We fetch 100 tickers. Change LIMIT 500 if you want a wider net.
        tickers = pd.read_sql("SELECT symbol FROM tickers LIMIT 100", engine)['symbol'].tolist()
    except:
        tickers = ["AAPL", "NVDA", "TSLA", "AMD", "AMZN", "GOOGL", "MSFT", "EPAM", "SPY", "QQQ", "IWM"]
    
    results = []
    print(f"   > Scanning {len(tickers)} stocks...")
    
    for t in tickers:
        try:
            stock = yf.Ticker(t)
            df = stock.history(period="1y")
            
            if len(df) < 200: continue
            
            # --- 1. THE TREND (40 Pts) ---
            sma_200 = df['Close'].rolling(200).mean().iloc[-1]
            price = df['Close'].iloc[-1]
            trend_pass = price > sma_200
            
            # --- 2. THE ZONE (30 Pts) ---
            delta = df['Close'].diff()
            gain = (delta.where(delta > 0, 0)).rolling(window=14).mean()
            loss = (-delta.where(delta < 0, 0)).rolling(window=14).mean()
            rs = gain / loss
            df['RSI'] = 100 - (100 / (1 + rs))
            rsi = df['RSI'].iloc[-1]
            zone_pass = rsi < 35
            
            # --- 3. THE TRIGGER (30 Pts) ---
            row = df.iloc[-1]
            body = abs(row['Close'] - row['Open'])
            lower = min(row['Open'], row['Close']) - row['Low']
            upper = row['High'] - max(row['Open'], row['Close'])
            
            is_hammer = (lower > 2 * body) and (upper < body)
            
            prev = df.iloc[-2]
            is_engulfing = (row['Close'] > row['Open']) and \
                           (row['Open'] < prev['Close']) and \
                           (row['Close'] > prev['Open']) and \
                           (prev['Close'] < prev['Open'])
                           
            trigger_type = "Hammer" if is_hammer else ("Engulfing" if is_engulfing else "None")
            trigger_pass = is_hammer or is_engulfing

            # --- SCORING ---
            score = 0
            if trend_pass: score += 40
            if zone_pass: score += 30
            if trigger_pass: score += 30
            if rsi < 25: score += 10 # Bonus
            score = min(100, score)

            # --- SIGNAL ---
            signal = "WAIT"
            if score >= 90: signal = "STRONG BUY"
            elif score >= 60 and trigger_pass: signal = "RISKY BUY" 
            elif zone_pass: signal = "WATCH"
            
            # --- SAVE EVERYTHING (The Fix) ---
            # We removed the 'if signal != "WAIT"' filter
            results.append({
                "ticker": t,
                "price": price,
                "trend_200_sma": round(sma_200, 2),
                "trend_pass": trend_pass,
                "rsi": round(rsi, 1),
                "zone_pass": zone_pass,
                "trigger_type": trigger_type,
                "trigger_pass": trigger_pass,
                "Score": score,
                "Signal": signal
            })
            print(f"   > {t}: {signal} (Score: {score})")
                
        except Exception as e:
            continue

    if results:
        df_res = pd.DataFrame(results)
        df_res.to_sql('technical_signals', engine, if_exists='replace', index=False)
        print(f"✅ CHECKLIST UPDATED. Saved {len(df_res)} stocks.")
    else:
        print("No data found.")

if __name__ == "__main__":
    run_technical_sniper()