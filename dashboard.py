import time
from datetime import datetime
import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine, text

st.set_page_config(page_title="Real-Time Card Transactions", layout="wide")
st.title("💳 Real-Time Card Transactions Dashboard")

DATABASE_URL = "postgresql://kafka_user:kafka_password@localhost:5434/kafka_db"

@st.cache_resource
def get_engine(url: str):
    return create_engine(url, pool_pre_ping=True)

engine = get_engine(DATABASE_URL)

def load_data(status_filter: str | None = None, limit: int = 500) -> pd.DataFrame:
    base_query = "SELECT * FROM transactions"
    params = {}
    if status_filter and status_filter != "All":
        base_query += " WHERE status = :status"
        params["status"] = status_filter
    base_query += " ORDER BY timestamp DESC LIMIT :limit"
    params["limit"] = int(limit)

    try:
        df = pd.read_sql_query(text(base_query), con=engine.connect(), params=params)
        return df
    except Exception as e:
        st.error(f"Error loading data from database: {e}")
        return pd.DataFrame()

# Sidebar controls
status_options = ["All", "Approved", "Declined", "Reversed"]
selected_status = st.sidebar.selectbox("Filter by Status", status_options)
update_interval = st.sidebar.slider("Update Interval (seconds)", min_value=2, max_value=20, value=5)
limit_records = st.sidebar.number_input(
    "Number of records to load",
    min_value=50, max_value=5000, value=500, step=50
)
show_only_anomalies = st.sidebar.checkbox("Show only anomalous transactions", value=False)

if st.sidebar.button("Refresh now"):
    st.experimental_rerun()

placeholder = st.empty()

while True:
    df_tx = load_data(selected_status, limit=int(limit_records))

    with placeholder.container():
        if df_tx.empty:
            st.warning("No transactions found. Waiting for data...")
            time.sleep(update_interval)
            continue

        if "timestamp" in df_tx.columns:
            df_tx["timestamp"] = pd.to_datetime(df_tx["timestamp"])

        # optional anomaly filter
        if show_only_anomalies and "is_anomalous" in df_tx.columns:
            df_tx = df_tx[df_tx["is_anomalous"] == True]

        # KPIs
        total_tx = len(df_tx)
        total_spend = df_tx["amount"].sum()
        avg_tx = total_spend / total_tx if total_tx > 0 else 0.0

        approved = len(df_tx[df_tx["status"] == "Approved"])
        declined = len(df_tx[df_tx["status"] == "Declined"])
        approval_rate = (approved / total_tx * 100) if total_tx > 0 else 0.0

        anomalies = 0
        if "is_anomalous" in df_tx.columns:
            anomalies = df_tx["is_anomalous"].sum()
        anomaly_rate = (anomalies / total_tx * 100) if total_tx > 0 else 0.0

        st.subheader(
            f"Displaying {total_tx} transactions "
            f"(Status filter: {selected_status}, "
            f"Anomalies-only: {show_only_anomalies})"
        )

        k1, k2, k3, k4, k5 = st.columns(5)
        k1.metric("Total Transactions", total_tx)
        k2.metric("Total Spend", f"${total_spend:,.2f}")
        k3.metric("Average Transaction", f"${avg_tx:,.2f}")
        k4.metric("Approval Rate", f"{approval_rate:,.2f}%")
        k5.metric("Anomalous Tx (%)", f"{anomaly_rate:,.2f}%")

        st.markdown("### Recent Transactions (Top 20)")
        st.dataframe(df_tx.head(20), use_container_width=True)

        # Charts
        if "category" in df_tx.columns:
            grouped_cat = (
                df_tx.groupby("category")["amount"]
                .sum()
                .reset_index()
                .sort_values("amount", ascending=False)
            )
            fig_cat = px.bar(
                grouped_cat, x="category", y="amount",
                title="Spend by Category"
            )
        else:
            fig_cat = None

        if "city" in df_tx.columns:
            grouped_city = (
                df_tx.groupby("city")["amount"]
                .sum()
                .reset_index()
                .sort_values("amount", ascending=False)
            )
            fig_city = px.bar(
                grouped_city, x="city", y="amount",
                title="Spend by City"
            )
        else:
            fig_city = None

        col1, col2 = st.columns(2)
        if fig_cat is not None:
            with col1:
                st.plotly_chart(fig_cat, use_container_width=True)
        if fig_city is not None:
            with col2:
           	    st.plotly_chart(fig_city, use_container_width=True)

        st.markdown("---")
        st.caption(
            f"Last updated: {datetime.now().isoformat()} • "
            f"Auto-refresh: {update_interval}s"
        )

    time.sleep(update_interval)