import streamlit as st
import pandas as pd
import os
import subprocess
import sys
import plotly.graph_objects as go
from sqlalchemy import text
from logic.db_config import get_engine

# --- CONFIGURATION ---
st.set_page_config(page_title="QuantValue Terminal (Beta)", layout="wide", initial_sidebar_state="expanded")
LOGIC_DIR = "logic"

# --- CUSTOM STYLING (The "Beta" Polish) ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto+Mono:wght@400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Roboto Mono', monospace; }
    .stMetric { background-color: #0e1117; padding: 10px; border-radius: 5px; border: 1px solid #303030; }
    .stDataFrame { border: 1px solid #303030; border-radius: 5px; }
    /* Highlight the Run Buttons */
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
        
    # Inject Root Path for Imports
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
        # Graceful failure - returns empty DF instead of crashing app
        return pd.DataFrame()

# --- SIDEBAR ---
st.sidebar.title("QUANT LAB [BETA]")
st.sidebar.markdown("---")
mode = st.sidebar.radio("WORKFLOW", [
    "1. DAILY ACTION (Alpha)", 
    "2. RESEARCH LAB (Deep Dive)", 
    "3. SYSTEM ADMIN (Cloud)"
])

# ==============================================================================
# MODE 1: DAILY ALPHA (The Scanner)
# ==============================================================================
if mode == "1. DAILY ACTION (Alpha)":
    st.title("⚡ DAILY ALPHA GENERATOR")
    st.markdown("Generate today's buy list based on **Deep Value (Damodaran)** and **Price Momentum**.")
    
    col1, col2, col3 = st.columns(3)
    with col1:
        if st.button("1. RUN MARKET SCANNER"):
            run_logic_script("scanner.py")
    with col2:
        if st.button("2. RUN VALUATION MODEL"):
            run_logic_script("valuation.py")
    with col3:
        if st.button("3. GENERATE BUY ORDERS"):
            run_logic_script("orders.py")
            
    st.markdown("---")
    
    # RESULT TABS
    tab_val, tab_ord = st.tabs(["💎 DEEP VALUE OPPORTUNITIES", "🛒 FINAL BUY ORDERS"])
    
    with tab_val:
        df = load_data("SELECT * FROM alpha_valuation ORDER BY upside_pct DESC")
        if not df.empty:
            st.metric("Undervalued Stocks Found", len(df))
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
            st.info("Waiting for Valuation Model... (Run Step 1 & 2)")

    with tab_ord:
        # Placeholder for order table if you implement order logic to DB
        st.info("Order Generation logic pending final DB migration.")

# ==============================================================================
# MODE 2: RESEARCH LAB (The Models)
# ==============================================================================
elif mode == "2. RESEARCH LAB (Deep Dive)":
    st.title("🔬 QUANTITATIVE RESEARCH LAB")
    
    # Control Panel
    c1, c2 = st.columns(2)
    with c1:
        st.markdown("### 🧠 FUNDAMENTAL MODELS")
        if st.button("RUN MULTI-FACTOR SCORING"):
            run_logic_script("model_engine.py")
            
    with c2:
        st.markdown("### 📈 TECHNICAL MODELS")
        if st.button("RUN TECHNICAL SNIPER"):
            run_logic_script("technical_models.py")

    st.markdown("---")

    # Output
    t1, t2, t3 = st.tabs(["🏆 FACTOR RANKINGS", "🎯 SNIPER SIGNALS", "🔎 STOCK INSPECTOR"])
    
    with t1:
        df_rank = load_data("SELECT * FROM quant_rankings ORDER BY \"TOTAL_SCORE\" DESC")
        if not df_rank.empty:
            st.dataframe(
                df_rank.style.background_gradient(subset=['TOTAL_SCORE'], cmap="RdYlGn"), 
                use_container_width=True, height=600
            )
        else:
            st.warning("No Rankings Found. Click 'Run Multi-Factor Scoring'.")

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
            st.info("No active signals. Market is quiet.")

    with t3:
        tickers = load_data("SELECT DISTINCT ticker FROM prices ORDER BY ticker")
        if not tickers.empty:
            sel = st.selectbox("Select Ticker", tickers['ticker'])
            if sel:
                # Layout
                p_query = f"SELECT date, close FROM prices WHERE ticker = '{sel}' ORDER BY date"
                f_query = f"SELECT * FROM fundamentals WHERE ticker = '{sel}'"
                
                df_p = load_data(p_query)
                df_f = load_data(f_query)
                
                if not df_f.empty:
                    d = df_f.iloc[0]
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("P/E Ratio", f"{d.get('pe_ratio',0):.2f}")
                    m2.metric("Profit Margin", f"{d.get('profit_margin',0)*100:.1f}%")
                    m3.metric("PEG Ratio", f"{d.get('peg_ratio',0):.2f}")
                    m4.metric("Market Cap", f"${d.get('market_cap',0)/1e9:.1f}B")
                
                if not df_p.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df_p['date'], y=df_p['close'], mode='lines', name='Close', line=dict(color='#00FF00')))
                    fig.update_layout(template="plotly_dark", height=400, margin=dict(l=0, r=0, t=30, b=0))
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.error("No Data. Go to System Admin -> Ingest Data.")

# ==============================================================================
# MODE 3: SYSTEM ADMIN (Cloud Setup)
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
        st.write("Fetches latest Prices & Fundamentals from Yahoo.")
        if st.button("RUN FULL DATA PIPELINE"):
            run_logic_script("data_pipeline.py")
            
    with st.expander("DIAGNOSTICS"):
        if st.button("RUN SYSTEM HEALTH CHECK"):
            run_logic_script("diagnose.py")