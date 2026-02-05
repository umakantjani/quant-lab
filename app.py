import streamlit as st
import pandas as pd
import os
import subprocess
import sys
import plotly.graph_objects as go
from sqlalchemy import text
from logic.db_config import get_engine

# --- CONFIGURATION ---
st.set_page_config(page_title="QuantValue Terminal (Gold)", layout="wide", initial_sidebar_state="expanded")
LOGIC_DIR = "logic"

# --- JONY IVE AESTHETIC (CSS) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;600&family=JetBrains+Mono:wght@400;500&family=Crimson+Pro:wght@400;600&display=swap');
    
    html, body, [class*="css"] { font-family: 'Inter', sans-serif; color: #E0E0E0; }
    h1, h2, h3 { font-family: 'Inter', sans-serif; font-weight: 600; letter-spacing: -0.5px; }
    p, div, li { font-family: 'Crimson Pro', serif; font-size: 1.05rem; color: #B0B0B0; }
    
    .stDataFrame, .stTable, div[data-testid="stMetricValue"] { 
        font-family: 'JetBrains Mono', monospace !important; font-size: 0.95rem; 
    }
    
    div.stButton > button {
        border-radius: 20px; border: 1px solid #404040; background-color: transparent;
        color: #E0E0E0; font-family: 'Inter', sans-serif; font-weight: 600; transition: all 0.2s ease;
    }
    div.stButton > button:hover {
        border-color: #00FF00; color: #00FF00; background-color: rgba(0, 255, 0, 0.05);
    }
    
    div[data-testid="stExpander"] {
        border: 1px solid #303030; border-radius: 6px; background-color: #0e1117;
    }
</style>
""", unsafe_allow_html=True)

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_db_connection():
    try: return get_engine()
    except Exception as e: return None

engine = get_db_connection()

# --- HELPER: DOCUMENTATION ENGINE ---
def render_guide(topic):
    """Renders the context-aware help section."""
    if topic == "valuation":
        with st.expander("📘 GUIDE: How to read the Valuation Report"):
            st.markdown("""
            **The Philosophy:** This module calculates the *Intrinsic Value* of a business using a **Discounted Cash Flow (DCF)** model, specifically adapted from Aswath Damodaran's methodology.
            
            * **Intrinsic Value:** The 'True' worth of the stock based on its ability to generate cash in the future, discounted back to today.
            * **Upside %:** The gap between Price and Value. `(Value - Price) / Price`. Look for > 15%.
            * **WACC (Risk):** The "Hurdle Rate." A higher WACC means the company is riskier (high debt or volatility). We use a *Synthetic* WACC based on their interest coverage ratio.
            * **Action:** If `Upside > 15%` and the company is not in distress (Rating > B), it is a **Buy Candidate**.
            """)
            
    elif topic == "technicals":
        with st.expander("📘 GUIDE: The Sniper's Trinity (Rules of Engagement)"):
            st.markdown("""
            **The Philosophy:** We do not guess bottoms. We wait for three specific conditions to align. If any answer is "No", there is no trade.
            
            ### **1. The Tide (The Trend)**
            * **Question:** Is the tide coming in or going out?
            * **The Rule:** Check the **200-Day SMA**.
            * **Pass:** Price > 200 SMA. (Secular Bull Market).
            * **Fail:** Price < 200 SMA. (Bear Market). *Warning: Buying here is catching a falling knife.*
            
            ### **2. The Zone (The Setup)**
            * **Question:** Is the price unfairly cheap right now?
            * **The Rule:** Check **RSI (14)** and **Bollinger Bands (20, 2)**.
            * **Pass:** RSI < 30 (Oversold) OR Price touches Lower Bollinger Band.
            * **Fail:** RSI is neutral (40-60). No statistical edge.
            
            ### **3. The Trigger (The Green Light)**
            * **Question:** Did the buyers just show up?
            * **The Rule:** Look for a **Reversal Candle** on the Daily chart.
            * **Pass:**
                * **Hammer:** Small body, long lower wick (Buyers rejected the lows).
                * **Engulfing:** Big Green candle eats the previous Red candle.
            
            ---
            **The Signal Decoder:**
            * 🟢 **STRONG BUY:** All 3 Passed (Trend + Zone + Trigger).
            * 🟡 **RISKY BUY:** Trend Failed (Below 200 SMA), but Zone & Trigger Passed. (Counter-trend trade).
            * 🟡 **WATCH:** Zone Passed (Cheap), but no Trigger yet.
            """)

    elif topic == "etf":
        with st.expander("📘 GUIDE: Portfolio Risk & Hedging"):
            st.markdown("""
            **The Philosophy:** Concentration builds wealth, but diversification stays wealthy. This module analyzes your specific "Buy List" to find hidden sector risks.
            
            * **Sector Weight:** If your picks are >30% concentrated in one sector (e.g., Tech), you are exposed to a sector crash.
            * **The Hedge:** The system maps your exposure to the correct inverse ETF or liquid Sector ETF (e.g., `XLK` for Tech).
            * **Action:** If Tech exposure is 60%, consider buying Puts on `XLK` to protect the downside while keeping your stock upside.
            """)

    elif topic == "orders":
        with st.expander("📘 GUIDE: Execution & Sizing"):
            st.markdown("""
            **The Philosophy:** Discipline over emotion. This engine allocates capital mathematically to prevent "betting the farm" on one stock.
            
            * **Position Sizing:** The system allocates equal weight (e.g., $10k) to the Top 10 highest-conviction ideas.
            * **Limit Price:** We strictly use **Limit Orders** at the current price to avoid slippage.
            * **Action:** Download the CSV. It is formatted for direct upload to interactive brokers or for your manual blotter.
            """)
# --- HELPER: SCRIPT RUNNER ---
def run_logic_script(script_name):
    script_path = os.path.join(LOGIC_DIR, script_name)
    if not os.path.exists(script_path):
        st.error(f"Script not found: {script_path}")
        return
        
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    
    st.write(f"🚀 **Initializing {script_name}...**")
    terminal_placeholder = st.empty()
    logs = []
    
    process = subprocess.Popen(
        [sys.executable, script_path], stdout=subprocess.PIPE, stderr=subprocess.STDOUT, 
        text=True, env=env, bufsize=1
    )
    
    while True:
        line = process.stdout.readline()
        if not line and process.poll() is not None: break
        if line:
            logs.append(line)
            terminal_placeholder.code("".join(logs[-15:]), language="bash")
            
    if process.returncode == 0: st.success(f"✅ {script_name} Complete.")
    else: st.error(f"❌ {script_name} Failed.")

def load_data(query):
    if engine is None: return pd.DataFrame()
    try: return pd.read_sql(query, engine)
    except: return pd.DataFrame()

# --- SIDEBAR ---
st.sidebar.title("QUANT LAB [GOLD]")
st.sidebar.markdown("---")
mode = st.sidebar.radio("WORKFLOW", ["1. DAILY ACTION (Alpha)", "2. RESEARCH LAB (Deep Dive)", "3. SYSTEM ADMIN (Cloud)"])

# ==============================================================================
# MODE 1: DAILY ALPHA
# ==============================================================================
if mode == "1. DAILY ACTION (Alpha)":
    st.title("⚡ DAILY ALPHA GENERATOR")
    st.markdown("Generate today's buy list based on **Deep Value (Damodaran)** and **Price Momentum**.")
    
    c1, c2, c3, c4, c5 = st.columns(5)
    with c1: 
        st.subheader("1. SCAN")
        if st.button("RUN SCANNER"): run_logic_script("scanner.py")
    with c2: 
        st.subheader("2. VALUE")
        if st.button("RUN VALUATION"): run_logic_script("valuation.py")
    with c3: 
        st.subheader("3. CHARTS")
        if st.button("RUN TECHNICALS"): run_logic_script("technical_models.py")
    with c4: 
        st.subheader("4. HEDGE")
        if st.button("RUN ETF MAPPER"): run_logic_script("etf_mapper.py")
    with c5: 
        st.subheader("5. ALLOCATE")
        if st.button("GENERATE ORDERS"): run_logic_script("orders.py")

    st.markdown("---")
    
    tab_val, tab_tech, tab_etf, tab_ord = st.tabs(["💎 VALUATION REPORT", "📈 TECHNICAL SIGNALS", "🛡 ETF STRATEGY", "🛒 BUY ORDERS"])
    
    with tab_val:
        render_guide("valuation") # <--- INFO ADDED
        df = load_data("SELECT * FROM alpha_valuation ORDER BY upside_pct DESC")
        if not df.empty:
            st.metric("Undervalued Opportunities", len(df))
            st.dataframe(df.style.format({"current_price": "${:.2f}", "intrinsic_value": "${:.2f}", "upside_pct": "{:.1f}%", "wacc_pct": "{:.1f}%"}).background_gradient(subset=['upside_pct'], cmap="Greens"), use_container_width=True, height=500)
        else: st.info("No Valuation Data. Run Steps 1 & 2.")

    with tab_tech:
        render_guide("technicals")
        
        # Load the detailed data
        df_tech = load_data("SELECT * FROM technical_signals ORDER BY \"rsi\" ASC")
        
        if not df_tech.empty:
            st.markdown("### 🎯 Sniper Checklist (The Trinity)")
            
            # Formatting for the UI
            def format_trend(pass_fail):
                return "✅ BULL (>200)" if pass_fail else "❌ BEAR (<200)"
                
            def format_zone(rsi_val):
                return f"✅ CHEAP ({rsi_val})" if rsi_val < 35 else f"⚠️ HIGH ({rsi_val})"
                
            def format_trigger(trig_type):
                return f"✅ {trig_type}" if trig_type != "None" else "❌ Waiting"

            # Create a Display View (Checklist)
            df_display = pd.DataFrame()
            df_display['Ticker'] = df_tech['ticker']
            df_display['Price'] = df_tech['price'].apply(lambda x: f"${x:.2f}")
            
            # 1. The Tide
            df_display['🌊 Trend (200 SMA)'] = df_tech['trend_pass'].apply(format_trend)
            
            # 2. The Zone
            df_display['📉 Zone (RSI)'] = df_tech['rsi'].apply(format_zone)
            
            # 3. The Trigger
            df_display['🕯️ Trigger'] = df_tech['trigger_type'].apply(format_trigger)
            
            # 4. Final Call
            df_display['SIGNAL'] = df_tech['Signal']
            
            # Render Styled Table
            st.dataframe(
                df_display.style.map(
                    lambda x: 'color: #00FF00; font-weight: bold' if 'STRONG BUY' in str(x) else 
                             ('color: #FFA500' if 'RISKY' in str(x) else 
                             ('color: #FFFF00' if 'WATCH' in str(x) else '')), 
                    subset=['SIGNAL']
                ),
                use_container_width=True,
                height=600
            )
        else:
            st.info("No active signals found. The market is quiet.")

    with tab_etf:
        render_guide("etf") # <--- INFO ADDED
        df_etf = load_data("SELECT * FROM etf_hedges ORDER BY weight DESC")
        if not df_etf.empty:
            c1, c2 = st.columns([2, 1])
            with c1: st.dataframe(df_etf.style.format({"weight": "{:.1%}"}).background_gradient(subset=['weight'], cmap="Reds"), use_container_width=True)
            with c2: 
                fig = go.Figure(data=[go.Pie(labels=df_etf['sector'], values=df_etf['weight'], hole=.4)])
                fig.update_layout(showlegend=False, margin=dict(t=0,b=0,l=0,r=0), height=250)
                st.plotly_chart(fig, use_container_width=True)
        else: st.info("No ETF Data. Run 'RUN ETF MAPPER'.")

    with tab_ord:
        render_guide("orders") # <--- INFO ADDED
        df_ord = load_data("SELECT * FROM alpha_orders")
        if not df_ord.empty:
            st.success(f"Generated {len(df_ord)} Buy Orders.")
            st.dataframe(df_ord.style.format({"Limit_Price": "${:.2f}", "Est_Value": "${:,.2f}"}), use_container_width=True)
            csv = df_ord.to_csv(index=False).encode('utf-8')
            st.download_button("⬇️ Download Order File (CSV)", csv, "basket_orders.csv", "text/csv")
        else: st.info("No Orders Generated. Run 'GENERATE ORDERS'.")

# ==============================================================================
# MODE 2: RESEARCH LAB
# ==============================================================================
elif mode == "2. RESEARCH LAB (Deep Dive)":
    st.title("🔬 QUANTITATIVE RESEARCH LAB")
    
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### 🧠 FUNDAMENTAL MODELS")
        if st.button("RUN MULTI-FACTOR SCORING"): run_logic_script("model_engine.py")
    with c2:
        st.markdown("### 📈 TECHNICAL MODELS")
        if st.button("RUN TECHNICAL SNIPER"): run_logic_script("technical_models.py")

    st.markdown("---")

    t1, t2, t3 = st.tabs(["🏆 FACTOR RANKINGS", "🎯 SNIPER SIGNALS", "🔎 STOCK INSPECTOR"])
    
    with t1:
        df_rank = load_data("SELECT * FROM quant_rankings ORDER BY \"TOTAL_SCORE\" DESC")
        if not df_rank.empty: st.dataframe(df_rank.style.background_gradient(subset=['TOTAL_SCORE'], cmap="RdYlGn"), use_container_width=True, height=600)
        else: st.warning("No Rankings. Run Multi-Factor Scoring.")

    with t2:
        render_guide("technicals")
        
        # TOGGLE: Show specific signals vs. everything
        filter_mode = st.radio("Show:", ["Active Signals Only (Buy/Watch)", "Full Checklist (All Stocks)"], horizontal=True)
        
        # Load Data
        df_tech = load_data("SELECT * FROM technical_signals ORDER BY \"Score\" DESC")
        
        if not df_tech.empty:
            # Apply Filter
            if filter_mode == "Active Signals Only (Buy/Watch)":
                df_display_source = df_tech[df_tech['Signal'] != 'WAIT']
            else:
                df_display_source = df_tech

            st.markdown(f"### 🎯 Sniper Checklist ({len(df_display_source)} Tickers)")
            
            # --- BUILD TABLE ---
            df_display = pd.DataFrame()
            df_display['Ticker'] = df_display_source['ticker']
            df_display['Price'] = df_display_source['price'].apply(lambda x: f"${x:.2f}")
            df_display['Score'] = df_display_source['Score']
            df_display['Signal'] = df_display_source['Signal']
            
            # 1. THE TREND
            df_display['🌊 Trend'] = df_display_source.apply(
                lambda x: f"✅ BULL (${x['trend_200_sma']:.2f})" if x['trend_pass'] 
                else f"❌ BEAR (${x['trend_200_sma']:.2f})", axis=1
            )
            
            # 2. THE ZONE
            df_display['📉 Zone'] = df_display_source.apply(
                lambda x: f"✅ LOW ({x['rsi']})" if x['rsi'] < 35 
                else f"⚠️ ({x['rsi']})", axis=1
            )
            
            # 3. THE TRIGGER
            df_display['🕯️ Trigger'] = df_display_source['trigger_type'].apply(
                lambda x: f"✅ {x}" if x != "None" else "❌ Wait"
            )
            
            # --- RENDER ---
            st.dataframe(
                df_display.style.background_gradient(subset=['Score'], cmap="RdYlGn", vmin=0, vmax=100)
                .map(lambda x: 'color: #00FF00; font-weight: bold' if 'STRONG' in str(x) else 
                             ('color: #FFA500' if 'RISKY' in str(x) else 
                             ('color: #FFFF00' if 'WATCH' in str(x) else '')), subset=['Signal']),
                use_container_width=True,
                height=600
            )
        else:
            st.info("No data. Run 'RUN TECHNICAL SNIPER'.")
    with t3:
        # Import the new AI Engine locally to avoid top-level crashes if file is missing
        try:
            from logic.report_engine import generate_ai_report
        except ImportError:
            st.error("Report Engine not found. Please create 'logic/report_engine.py'.")

        st.markdown("### 🧬 MOLECULAR INSPECTOR (Gemini AI)")
        
        # 1. SELECTION CONTROLS
        c_sel1, c_sel2 = st.columns([1, 3])
        with c_sel1:
            # Load tickers but allow custom entry
            db_tickers = load_data("SELECT DISTINCT ticker FROM prices ORDER BY ticker")['ticker'].tolist() if not load_data("SELECT DISTINCT ticker FROM prices").empty else ["AAPL", "MSFT", "TSLA"]
            ticker_input = st.selectbox("Select Ticker", db_tickers)
        
        with c_sel2:
            st.write("") # Spacer
            st.write("") 
            # THE "MAGIC" BUTTON
            if st.button(f"✨ ASK GEMINI: ANALYZE {ticker_input}"):
                with st.spinner(f"Connecting to Neural Core... Analyzing {ticker_input}..."):
                    report = generate_ai_report(ticker_input)
                    
                    # 2. RENDER REPORT
                    st.markdown("---")
                    st.markdown(report)
                    
                    # 3. SHOW CHART (Context)
                    df_p = load_data(f"SELECT date, close FROM prices WHERE ticker = '{ticker_input}' ORDER BY date")
                    if not df_p.empty:
                        fig = go.Figure()
                        fig.add_trace(go.Scatter(x=df_p['date'], y=df_p['close'], mode='lines', name='Price', line=dict(color='#00FF00', width=2)))
                        # Add simple annotations or style
                        fig.update_layout(
                            title=f"{ticker_input} Price Action",
                            template="plotly_dark", 
                            height=300, 
                            margin=dict(l=0, r=0, t=30, b=0),
                            plot_bgcolor='rgba(0,0,0,0)',
                            paper_bgcolor='rgba(0,0,0,0)'
                        )
                        st.plotly_chart(fig, use_container_width=True)
        

# ==============================================================================
# MODE 3: SYSTEM ADMIN
# ==============================================================================
elif mode == "3. SYSTEM ADMIN (Cloud)":
    st.title("🛠 SYSTEM ADMINISTRATION")
    st.warning("These actions affect the Live Database.")
    
    with st.expander("INIT: DATABASE SETUP (Run Once)", expanded=True):
        c1, c2 = st.columns(2)
        with c1: 
            if st.button("1. RESET DATABASE TABLES"): run_logic_script("setup_db.py")
        with c2: 
            if st.button("2. INGEST TICKERS"): run_logic_script("ingest_tickers.py")
            
    with st.expander("DATA PIPELINE (Daily Update)", expanded=True):
        if st.button("RUN FULL DATA PIPELINE"): run_logic_script("data_pipeline.py")
            
    with st.expander("DIAGNOSTICS"):
        if st.button("RUN SYSTEM HEALTH CHECK"): run_logic_script("diagnose.py")