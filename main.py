
import streamlit as st
import pandas as pd
from datetime import datetime
import os
import sys

# Ensure local imports work
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from supabase_db import db_service

st.set_page_config(page_title="RayBot Manual Trade Manager", layout="wide")

st.title("RayBot Manual Execution Dashboard")

# --- Authentication Check (Simple)
# In real prod, this should be behind proper auth
password = st.sidebar.text_input("Admin Password", type="password")
if password != "admin123":
    st.error("Please enter admin password to manage trades.")
    st.stop()

# --- Section 1: Pending Signals
st.header("1. Pending Signals to Execute")

pending_signals = db_service.fetch_pending_signals()
if pending_signals:
    df_signals = pd.DataFrame(pending_signals)
    # Display key columns
    st.dataframe(df_signals[["id", "symbol", "direction", "confidence", "price_at_signal", "created_at"]])
    
    # Execution Form
    st.subheader("Execute Signal")
    col1, col2, col3 = st.columns(3)
    
    with col1:
        signal_id_to_exec = st.selectbox("Select Signal ID", df_signals["id"].tolist())
    
    # Get selected signal details for default values
    selected_sig = next((s for s in pending_signals if s["id"] == signal_id_to_exec), None)
    default_price = selected_sig["price_at_signal"] if selected_sig else 0.0
    
    with col2:
        entry_price = st.number_input("Fill Price", value=float(default_price), format="%.4f")
    
    with col3:
        if st.button("Confirm Entry"):
            with st.spinner("Creating trade..."):
                res = db_service.create_trade_from_signal(
                    signal_id_to_exec, 
                    entry_price, 
                    datetime.utcnow().isoformat()
                )
                if res:
                    st.success(f"Trade opened! ID: {res['id']}")
                    st.rerun()
                else:
                    st.error("Failed to create trade.")

else:
    st.info("No pending signals.")

st.markdown("---")

# --- Section 2: Open Trades Management
st.header("2. Manage Open Trades")

open_trades = db_service.fetch_open_trades()
if open_trades:
    df_trades = pd.DataFrame(open_trades)
    st.dataframe(df_trades[["id", "symbol", "direction", "entry_price", "entry_time"]])
    
    st.subheader("Close Trade")
    colA, colB, colC = st.columns(3)
    
    with colA:
        trade_id_to_close = st.selectbox("Select Trade ID to Close", df_trades["id"].tolist())
        
    with colB:
        # Default close price suggestions could be live price if we fetched it, 
        # but for now manual input
        close_price = st.number_input("Exit Price", min_value=0.0, format="%.4f")
        exit_reason = st.selectbox("Exit Reason", ["MANUAL", "TP", "SL", "TIME_EXIT"])
        
    with colC:
        if st.button("Close Trade"):
            with st.spinner("Closing trade..."):
                res = db_service.close_trade(
                    trade_id_to_close,
                    close_price,
                    datetime.utcnow().isoformat(),
                    exit_reason
                )
                if res:
                    st.success(f"Trade closed! PnL: {res.get('pnl_usd')}")
                    st.rerun()
                else:
                    st.error("Failed to close trade.")
else:
    st.info("No open trades.")

# --- Section 3: Recent Performance (Optional)
st.markdown("---")
st.header("3. Recent Performance")
# You could add fetch_recent_trades logic here later
