import pandas as pd
import numpy as np
from sqlalchemy import create_engine, text
from logic.db_config import get_engine

# --- CONFIGURATION ---
# DB_URI = "postgresql://quant:password@localhost:5432/stock_master"
# engine = create_engine(DB_URI)
engine = get_engine()

def get_price_history():
    """Fetches full price history for all stocks."""
    print("   > Fetching price history from DB...")
    query = "SELECT * FROM prices ORDER BY ticker, date ASC"
    return pd.read_sql(query, engine)

def calculate_rsi(series, period=14):
    """Relative Strength Index (RSI)"""
    delta = series.diff(1)
    gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
    loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
    rs = gain / loss
    return 100 - (100 / (1 + rs))

def calculate_macd(df, fast=12, slow=26, signal=9):
    """Moving Average Convergence Divergence"""
    ema_fast = df['close'].ewm(span=fast, adjust=False).mean()
    ema_slow = df['close'].ewm(span=slow, adjust=False).mean()
    macd_line = ema_fast - ema_slow
    signal_line = macd_line.ewm(span=signal, adjust=False).mean()
    histogram = macd_line - signal_line
    return macd_line, signal_line, histogram

def calculate_bollinger(df, window=20, num_std=2):
    """Bollinger Bands"""
    sma = df['close'].rolling(window=window).mean()
    std = df['close'].rolling(window=window).std()
    upper = sma + (std * num_std)
    lower = sma - (std * num_std)
    return upper, lower

def calculate_fibonacci(df, lookback=252):
    """Auto-detects Year High/Low and 61.8% Golden Pocket"""
    # Look at last 1 year (approx 252 trading days)
    recent_data = df.tail(lookback)
    if len(recent_data) < 50: return None, None, None # Not enough data
    
    swing_high = recent_data['high'].max()
    swing_low = recent_data['low'].min()
    
    # Fibonacci Levels
    diff = swing_high - swing_low
    fib_618 = swing_high - (diff * 0.618) # The "Golden Pocket" Retracement
    fib_500 = swing_high - (diff * 0.5)
    
    return swing_high, swing_low, fib_618

def analyze_technicals():
    df = get_price_history()
    signals = []
    
    # Process each ticker individually
    tickers = df['ticker'].unique()
    print(f"   > Analyzing {len(tickers)} stocks...")
    
    for t in tickers:
        # Slice data for this ticker
        # (We use .copy() to avoid SettingWithCopy warnings)
        d = df[df['ticker'] == t].copy()
        
        if len(d) < 50: continue # Skip new stocks
        
        # --- 1. CALCULATE INDICATORS ---
        d['RSI'] = calculate_rsi(d['close'])
        d['MACD'], d['Signal_Line'], d['Hist'] = calculate_macd(d)
        d['BB_Upper'], d['BB_Lower'] = calculate_bollinger(d)
        
        # Get latest row
        curr = d.iloc[-1]
        prev = d.iloc[-2]
        
        # Fibonacci Check
        high_1y, low_1y, fib_golden = calculate_fibonacci(d)
        
        # --- 2. GENERATE SIGNALS ---
        signal_score = 0
        reasons = []
        
        # A. RSI Strategy (Oversold Bounce)
        if curr['RSI'] < 30:
            signal_score += 2
            reasons.append("OVERSOLD (RSI < 30)")
        elif curr['RSI'] > 70:
            signal_score -= 2
            reasons.append("OVERBOUGHT (RSI > 70)")
            
        # B. MACD Strategy (Bullish Crossover)
        # If Histogram crossed from Negative to Positive today
        if prev['Hist'] < 0 and curr['Hist'] > 0:
            signal_score += 3
            reasons.append("MACD BULL CROSS")
            
        # C. Bollinger Band Squeeze (Price touching lower band)
        if curr['close'] <= curr['BB_Lower']:
            signal_score += 2
            reasons.append("BOLLINGER BUY ZONE")
            
        # D. The "Boris" Fib Strategy
        # If price is near the 61.8% retracement level (+/- 2%)
        if fib_golden:
            dist_to_fib = abs(curr['close'] - fib_golden) / curr['close']
            if dist_to_fib < 0.02: # Within 2%
                signal_score += 3
                reasons.append("FIBONACCI GOLDEN POCKET")

        # --- 3. FINAL VERDICT ---
        verdict = "WAIT"
        if signal_score >= 4: verdict = "STRONG BUY"
        elif signal_score >= 2: verdict = "BUY"
        elif signal_score <= -2: verdict = "SELL"
        
        signals.append({
            'ticker': t,
            'date': curr['date'],
            'close': curr['close'],
            'RSI': round(curr['RSI'], 1),
            'MACD_Hist': round(curr['Hist'], 3),
            'Fib_Level': round(fib_golden, 2) if fib_golden else 0,
            'Signal': verdict,
            'Score': signal_score,
            'Reasons': ", ".join(reasons)
        })
        
    return pd.DataFrame(signals)

if __name__ == "__main__":
    print("🚀 RUNNING TECHNICAL SNIPER MODEL...")
    df_results = analyze_technicals()
    
    # Filter for active signals
    df_active = df_results[df_results['Signal'] != 'WAIT'].sort_values('Score', ascending=False)
    
    print(f"   > Found {len(df_active)} active signals.")
    
    # Save to DB
    print("   > Saving to 'technical_signals' table...")
    df_results.to_sql('technical_signals', engine, if_exists='replace', index=False)
    
    # Set Primary Key for speed
    with engine.connect() as conn:
        try:
            conn.execute(text("ALTER TABLE technical_signals ADD PRIMARY KEY (ticker);"))
        except: pass # PK might already exist
        
    print("\n=== TOP BUY SIGNALS ===")
    print(df_active.head(10)[['ticker', 'close', 'Signal', 'RSI', 'Reasons']].to_string(index=False))