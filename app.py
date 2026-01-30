import streamlit as st
import pandas as pd
import os
import subprocess
import sys
import yfinance as yf
from sqlalchemy import create_engine
import plotly.graph_objects as go
from logic import database  # Your local SQLite logic (Portfolio)
from logic.db_config import get_engine


# --- CONFIGURATION ---
DATA_DIR = "data"
LOGIC_DIR = "logic"

# 1. POSTGRES CONNECTION (For Quant Lab Data)
# DB_URI = "postgresql://quant:password@localhost:5432/stock_master"
engine = get_engine()
# db_engine = create_engine(DB_URI)

st.set_page_config(page_title="QuantValue Terminal", layout="wide")

# --- CUSTOM CSS ---
st.markdown("""
<style>
    @import url('https://fonts.googleapis.com/css2?family=Roboto+Mono:wght@400;700&display=swap');
    html, body, [class*="css"] { font-family: 'Roboto Mono', monospace; }
    .stDataFrame, .stTable { font-family: 'Roboto Mono', monospace !important; }
    div[data-testid="stMetricValue"] { font-family: 'Roboto Mono', monospace !important; }
</style>
""", unsafe_allow_html=True)

# Initialize SQLite (For Trade History)
database.init_db()

# --- HELPER FUNCTIONS ---
def run_logic_script(script_name):
    script_path = os.path.join(LOGIC_DIR, script_name)
    if not os.path.exists(script_path):
        st.error(f"Script not found: {script_path}")
        return False
        
    # --- THE FIX: INJECT ROOT PATH ---
    # We add the current directory (project root) to PYTHONPATH
    # This allows scripts in 'logic/' to say "from logic.db_config import..."
    env = os.environ.copy()
    env["PYTHONPATH"] = os.getcwd()
    
    with st.spinner(f"Executing {script_name}..."):
        result = subprocess.run(
            [sys.executable, script_path], 
            capture_output=True, 
            text=True,
            env=env  # <--- PASS THE ENVIRONMENT WITH PYTHONPATH
        )
        
        if result.returncode == 0:
            st.success(f"SUCCESS: {script_name}")
            with st.expander("View Logs"):
                st.code(result.stdout)
            return True
        else:
            st.error(f"FAILED: {script_name}")
            st.error(result.stderr) # Show the actual error
            return False

def load_csv(filename):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path): return pd.read_csv(path)
    return None

def load_from_postgres(query):
    try:
        return pd.read_sql(query, engine)
    except Exception as e:
        return pd.DataFrame()

# --- SIDEBAR NAVIGATION ---
st.sidebar.title("[ SYSTEM CONTROL ]")
mode = st.sidebar.radio("MODE", [
    "1. ALPHA PIPELINE (Daily)", 
    "2. QUANT LAB (Research)", 
    "3. EXECUTION (Blotter)", 
    "4. PORTFOLIO (Live)"
])

# ==============================================================================
# MODE 1: ALPHA PIPELINE (The Original 5-Step Factory)
# ==============================================================================
if mode == "1. ALPHA PIPELINE (Daily)":
    st.title("ALPHA GENERATION PIPELINE")
    st.markdown("---")
    
    # 5 Columns for the 5 Steps
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.subheader("1. SCAN")
        if st.button("RUN SCANNER"):
            run_logic_script("scanner.py")
            
    with col2:
        st.subheader("2. VALUE")
        if st.button("RUN VALUATION"):
            run_logic_script("valuation.py")
            
    with col3:
        st.subheader("3. CHARTS")
        if st.button("RUN TECHNICALS"):
            run_logic_script("technicals.py")

    with col4:
        st.subheader("4. HEDGE")
        if st.button("RUN ETF MAPPER"):
            run_logic_script("etf.py")

    with col5:
        st.subheader("5. ALLOCATE")
        if st.button("GENERATE ORDERS"):
            run_logic_script("orders.py")

    st.markdown("---")
    
    # OUTPUT VIEWER
    tab1, tab2, tab3, tab4 = st.tabs(["VALUATION REPORT", "TECHNICALS", "ETF STRATEGY", "BUY ORDERS"])
    
    with tab1:
        # READ FROM DB NOW
        df = load_from_postgres("SELECT * FROM alpha_valuation")
        if not df.empty: 
            st.dataframe(df, use_container_width=True)
        else: 
            st.warning("No Data. Run Step 1 & 2.")
        
    with tab2:
        # You can migrate technicals.py later, for now we leave as placeholder
        st.info("Technical Analysis pipeline pending DB migration.")
        
    with tab3:
        st.info("ETF Strategy pipeline pending DB migration.")

    with tab4:
        # Orders would read from 'alpha_orders' table eventually
        st.info("Order generation pending DB migration.")

# ==============================================================================
# MODE 2: QUANT LAB (The New Database Engine)
# ==============================================================================
elif mode == "2. QUANT LAB (Research)":
    st.title("QUANTITATIVE RESEARCH LAB")
    st.info("Powered by Postgres Database (stock_master)")
    
    # --- NEW: SYSTEM INITIALIZATION (For Cloud Setup) ---
    with st.expander("🛠 SYSTEM SETUP (RUN ONCE FOR NEW DB)", expanded=False):
        st.warning("Only run these if your cloud database is empty!")
        c_setup1, c_setup2 = st.columns(2)
        with c_setup1:
            if st.button("1. RESET DATABASE TABLES"):
                run_logic_script("setup_db.py")
        with c_setup2:
            if st.button("2. INGEST MARKET TICKERS"):
                run_logic_script("ingest_tickers.py")

    # CONTROL PANEL
    c1, c2 = st.columns(2)
    with c1:
        st.subheader("A. DATA INGESTION")
        if st.button("FETCH YAHOO DATA TO DB"):
            run_logic_script("data_pipeline.py")
            
    with c2:
        st.subheader("B. MODEL SCORING")
        if st.button("RUN MULTI-FACTOR MODEL"):
            run_logic_script("model_engine.py")
        if st.button("RUN TECHNICAL SNIPER"):
            run_logic_script("technical_models.py")
    
    st.markdown("---")
    
    # THREE TABS
    tab1, tab2, tab3 = st.tabs(["RANKINGS (FUNDAMENTAL)", "SIGNALS (TECHNICAL)", "INSPECTOR (CHARTS)"])
    
    # ... (Keep the rest of the tab logic exactly as it was) ...
    
    # --- TAB 1: FUNDAMENTAL RANKINGS ---
    with tab1:
        st.subheader("MULTI-FACTOR LEADERBOARD")
        
        # CONTROLS for List Size
        rc1, rc2 = st.columns([1, 4])
        with rc1:
            rank_limit = st.number_input("Top N Stocks", min_value=1, value=50, step=10)
        with rc2:
            st.write("") # Spacer
            show_all_ranks = st.checkbox("Show Entire Market (All Tickers)", value=False)

        # Load Data
        df_rank = load_from_postgres("SELECT * FROM quant_rankings ORDER BY \"TOTAL_SCORE\" DESC")
        
        if not df_rank.empty:
            # Apply Filter
            if show_all_ranks:
                df_display = df_rank
            else:
                df_display = df_rank.head(rank_limit)
            
            st.caption(f"Displaying top {len(df_display)} of {len(df_rank)} stocks evaluated.")
            
            st.dataframe(
                df_display.style.background_gradient(subset=['TOTAL_SCORE'], cmap="RdYlGn"),
                use_container_width=True,
                height=600
            )
        else:
            st.info("No rankings found in DB. Run Multi-Factor Model.")

    # --- TAB 2: TECHNICAL SIGNALS (The New Sniper) ---
    with tab2:
        st.subheader("TECHNICAL SNIPER SIGNALS")
        
        # TOGGLE: Show Everything vs. Just Actionable
        show_all = st.checkbox("Show 'WAIT' Signals (All Stocks)", value=False)
        
        if show_all:
            query = "SELECT * FROM technical_signals ORDER BY \"Score\" DESC"
        else:
            query = "SELECT * FROM technical_signals WHERE \"Signal\" != 'WAIT' ORDER BY \"Score\" DESC"
            
        df_tech = load_from_postgres(query)
        
        if not df_tech.empty:
            st.caption(f"Found {len(df_tech)} stocks matching criteria.")
            st.dataframe(
                df_tech.style.applymap(
                    lambda x: 'color: green; font-weight: bold' if x == 'STRONG BUY' else 
                             ('color: lightgreen' if x == 'BUY' else 
                             ('color: red' if x == 'SELL' else '')), 
                    subset=['Signal']
                ),
                use_container_width=True,
                height=600
            )
        else:
            if not show_all:
                st.info("No active 'BUY' or 'SELL' signals found. The market is quiet. Check 'Show WAIT Signals' to verify data.")
            else:
                st.info("No data found. Run 'Technical Sniper'.")

    # --- TAB 3: STOCK INSPECTOR (The Deep Dive) ---
    with tab3:
        st.subheader("DEEP DIVE VISUALIZER")
        
        df_tickers = load_from_postgres("SELECT DISTINCT ticker FROM prices ORDER BY ticker")
        if not df_tickers.empty:
            c_sel1, c_sel2 = st.columns([1, 3])
            with c_sel1:
                selected_ticker = st.selectbox("Select Ticker", df_tickers['ticker'])
            
            if selected_ticker:
                # Fetch Data
                query_price = f"SELECT date, close, volume FROM prices WHERE ticker = '{selected_ticker}' ORDER BY date"
                df_price = load_from_postgres(query_price)
                
                query_fund = f"SELECT * FROM fundamentals WHERE ticker = '{selected_ticker}'"
                df_fund = load_from_postgres(query_fund)
                
                # Display Fundamentals
                if not df_fund.empty:
                    f = df_fund.iloc[0]
                    m1, m2, m3, m4 = st.columns(4)
                    m1.metric("P/E Ratio", round(f.get('pe_ratio', 0) or 0, 2))
                    m2.metric("Profit Margin", f"{round((f.get('profit_margin', 0) or 0)*100, 1)}%")
                    m3.metric("PEG Ratio", f.get('peg_ratio', 'N/A'))
                    m4.metric("Market Cap", f"${(f.get('market_cap', 0) or 0)/1e9:,.1f}B")
                
                # Display Chart
                if not df_price.empty:
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df_price['date'], y=df_price['close'], mode='lines', name='Close'))
                    fig.update_layout(title=f"{selected_ticker} Price History", height=500)
                    st.plotly_chart(fig, use_container_width=True)
        else:
            st.warning("Database empty. Run Data Ingestion.")

# ==============================================================================
# MODE 3: EXECUTION (The Trader)
# ==============================================================================
elif mode == "3. EXECUTION (Blotter)":
    st.title("ORDER EXECUTION")
    
    df_orders = load_csv("final_buy_orders.csv")
    if df_orders is not None:
        df_orders['EXECUTE'] = False
        edited = st.data_editor(
            df_orders, 
            column_config={
                "EXECUTE": st.column_config.CheckboxColumn(default=False),
                "Price": st.column_config.NumberColumn(format="$%.2f"),
                "Total_Cost": st.column_config.NumberColumn(format="$%.2f")
            }, 
            use_container_width=True
        )
        
        if st.button("CONFIRM TRADES"):
            count = 0
            for idx, row in edited.iterrows():
                if row['EXECUTE']:
                    database.execute_trade(row['Ticker'], row['Shares'], row['Price'], "BUY")
                    count += 1
            if count > 0: st.success(f"Booked {count} trades.")
    else:
        st.info("No orders generated.")

# ==============================================================================
# MODE 4: PORTFOLIO (The Dashboard)
# ==============================================================================
elif mode == "4. PORTFOLIO (Live)":
    st.title("LIVE PORTFOLIO")
    
    if st.button("REFRESH LIVE PRICES"):
        pass 
        
    df = database.get_portfolio()
    if not df.empty:
        tickers = df['ticker'].tolist()
        try:
            live = yf.download(tickers, period="1d", progress=False)['Close'].iloc[-1]
        except: live = None
        
        def get_price(t):
            if isinstance(live, float): return live 
            if live is not None and t in live: return live[t]
            return 0
            
        df['Current Price'] = df['ticker'].apply(get_price)
        df['Current Price'] = df.apply(lambda x: x['avg_cost'] if x['Current Price'] == 0 else x['Current Price'], axis=1)
        
        df['Market Value'] = df['shares'] * df['Current Price']
        df['P&L'] = df['Market Value'] - (df['shares'] * df['avg_cost'])
        
        col1, col2 = st.columns(2)
        col1.metric("TOTAL EQUITY", f"${df['Market Value'].sum():,.2f}")
        col2.metric("TOTAL P&L", f"${df['P&L'].sum():,.2f}")
        
        st.dataframe(df.style.format({"avg_cost": "${:.2f}", "Current Price": "${:.2f}", "P&L": "${:.2f}", "Market Value": "${:.2f}"}), use_container_width=True)
        
        st.markdown("---")
        st.subheader("MANUAL TRADE")
        with st.form("manual"):
            c1, c2, c3 = st.columns(3)
            with c1: t = st.text_input("Ticker").upper()
            with c2: s = st.number_input("Shares", min_value=1.0)
            with c3: p = st.number_input("Price", min_value=0.0)
            if st.form_submit_button("ADD"):
                if p==0: 
                    try: p = yf.Ticker(t).fast_info['last_price']
                    except: pass
                database.execute_trade(t, s, p, "BUY")
                st.rerun()
    else:
        st.info("Portfolio Empty.")