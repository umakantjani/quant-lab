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

# --- CUSTOM STYLING ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto+Mono:wght@400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Roboto Mono', monospace; }
    .stMetric { background-color: #0e1117; padding: 10px; border-radius: 5px; border: 1px solid #303030; }
    .stDataFrame { border: 1px solid #303030; border-radius: 5px; }
    div.stButton > button:first-child { border: 1px solid #00FF00; color: #00FF00; background-color: transparent; }
    div.stButton > button:first-child:hover { background-color: #00FF00; color: black; }
</style>
""", unsafe_allow_html=True)

# --- DATABASE CONNECTION ---
@st.cache_resource
def get_db_connection():
    try:
        return get_engine()
    except Exception as e:
        st.error(f"❌ DATABASE CONNECTION FAILED: {e}")
        return None

engine = get_db_connection()

# --- HELPER FUNCTIONS ---
def run_logic_script(script_name):
    script_path = os.path.join(LOGIC_DIR, script_name)
    if not os.path.exists(script_path):
        st.error(f"Script not found: {script_path}")
        return
        
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    
    with st.status(f"🚀 Executing {script_name}...", expanded=True) as status:
        st.write("Initializing process...")
        result = subprocess.run(
            [sys.executable, script_path], 
            capture_output=True, 
            text=True,
            env=env
        )
        if result.returncode == 0:
            status.update(label=f"✅ Finished: {script_name}", state="complete", expanded=False)
            with st.expander("View Logs"):
                st.code(result.stdout)
        else:
            status.update(label="❌ Execution Failed", state="error")
            st.error(result.stderr)

def load_data(query):
    if engine is None: return pd.DataFrame()
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        return pd.DataFrame()

# --- SIDEBAR ---
st.sidebar.title("QUANT LAB [GOLD]")
st.sidebar.markdown("---")
mode = st.sidebar.radio("WORKFLOW", [
    "1. DAILY ACTION (Alpha)", 
    "2. RESEARCH LAB (Deep Dive)", 
    "3. SYSTEM ADMIN (Cloud)"
])

# ==============================================================================
# MODE 1: DAILY ALPHA (Restored & Upgraded)
# ==============================================================================
if mode == "1. DAILY ACTION (Alpha)":
    st.title("⚡ DAILY ALPHA GENERATOR")
    st.markdown("Generate today's buy list based on **Deep Value (Damodaran)** and **Price Momentum**.")
    
    # 5-STEP PIPELINE CONTROLS
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
        if st.button("RUN ETF MAPPER"): st.info("ETF Module coming soon.")
    with c5:
        st.subheader("5. ALLOCATE")
        if st.button("GENERATE ORDERS"): run_logic_script("orders.py")

    st.markdown("---")
    
    # OUTPUT VIEWER (Restored 4 Tabs)
    tab_val, tab_tech, tab_etf, tab_ord = st.tabs(["💎 VALUATION REPORT", "📈 TECHNICAL SIGNALS", "🛡 ETF STRATEGY", "🛒 BUY ORDERS"])
    
    with tab_val:
        df = load_data("SELECT * FROM alpha_valuation ORDER BY upside_pct DESC")
        if not df.empty:
            st.metric("Undervalued Opportunities", len(df))
            st.dataframe(
                df.style.format({
                    "current_price": "${:.2f}", 
                    "intrinsic_value": "${:.2f}", 
                    "upside_pct": "{:.1f}%",
                    "wacc_pct": "{:.1f}%"
                }).background_gradient(subset=['upside_pct'], cmap="Greens"),
                use_container_width=True, height=500
            )
        else:
            st.info("No Valuation Data. Run Steps 1 & 2.")

    with tab_tech:
        # We link this to the same table as Mode 2 for consistency
        df_tech = load_data("SELECT * FROM technical_signals WHERE \"Signal\" != 'WAIT' ORDER BY \"Score\" DESC")
        if not df_tech.empty:
             st.dataframe(
                df_tech.style.applymap(lambda x: 'color: #00FF00' if 'BUY' in str(x) else ('color: #FF0000' if 'SELL' in str(x) else ''), subset=['Signal']),
                use_container_width=True
            )
        else:
            st.info("No Technical Signals. Run Step 3.")

    with tab_etf:
        st.info("ETF Hedging Module is under construction.")

    with tab_ord:
        # Placeholder for order table 
        st.info("Order Generation logic pending final DB migration.")

# ==============================================================================
# MODE 2: RESEARCH LAB (Deep Dive)
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
        if not df_rank.empty:
            st.dataframe(df_rank.style.background_gradient(subset=['TOTAL_SCORE'], cmap="RdYlGn"), use_container_width=True, height=600)
        else:
            st.warning("No Rankings. Run Multi-Factor Scoring.")

    with t2:
        show_all = st.checkbox("Show Neutral Signals", value=False)
        query = "SELECT * FROM technical_signals" if show_all else "SELECT * FROM technical_signals WHERE \"Signal\" != 'WAIT'"
        df_tech = load_data(query + " ORDER BY \"Score\" DESC")
        if not df_tech.empty:
            st.dataframe(
                df_tech.style.applymap(lambda x: 'color: #00FF00' if 'BUY' in str(x) else ('color: #FF0000' if 'SELL' in str(x) else ''), subset=['Signal']),
                use_container_width=True, height=600
            )
        else:
            st.info("No active signals.")

    with t3:
        tickers = load_data("SELECT DISTINCT ticker FROM prices ORDER BY ticker")
        if not tickers.empty:
            sel = st.selectbox("Select Ticker", tickers['ticker'])
            if sel:
                df_p = load_data(f"SELECT date, close FROM prices WHERE ticker = '{sel}' ORDER BY date")
                df_f = load_data(f"SELECT * FROM fundamentals WHERE ticker = '{sel}'")
                
                # --- SAFE METRICS RENDERER ---
                if not df_f.empty:
                    d = df_f.iloc[0]
                    
                    # Helper to convert None -> 0.0 safely
                    def safe_num(val):
                        if val is None or pd.isna(val): return 0.0
                        return float(val)

                    pe = safe_num(d.get('pe_ratio'))
                    margin = safe_num(d.get('profit_margin'))
                    peg = safe_num(d.get('peg_ratio'))
                    mkt_cap = safe_num(d.get('market_cap'))

                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("P/E Ratio", f"{pe:.2f}")
                    m2.metric("Profit Margin", f"{margin * 100:.1f}%")
                    m3.metric("PEG Ratio", f"{peg:.2f}")
                    m4.metric("Market Cap", f"${mkt_cap / 1e9:.1f}B")
                
                if not df_p.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df_p['date'], y=df_p['close'], mode='lines', name='Close', line=dict(color='#00FF00')))
                    fig.update_layout(template="plotly_dark", height=400, margin=dict(l=0, r=0, t=30, b=0))
                    st.plotly_chart(fig, use_container_width=True)

# ==============================================================================
# MODE 3: SYSTEM ADMIN (Cloud)
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