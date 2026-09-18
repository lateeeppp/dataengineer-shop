"""
Olist E-Commerce Lakehouse BI Dashboard
Built with Streamlit and DuckDB (Serving Layer for Gold Data Marts)
"""

import os
from pathlib import Path

import duckdb
import pandas as pd
import streamlit as st

# Setup Path & Page Configuration
PROJECT_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_LOCAL_GOLD = PROJECT_ROOT / "data" / "gold"

st.set_page_config(
    page_title="Olist Retail Lakehouse BI",
    page_icon="🛍️",
    layout="wide",
    initial_sidebar_state="expanded",
)

# Custom Styling
st.markdown(
    """
    <style>
    .metric-card {
        background-color: #f8f9fa;
        border-radius: 8px;
        padding: 16px;
        border-left: 4px solid #1E88E5;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
    }
    .stMetric label {
        font-size: 0.9rem;
        font-weight: 600;
        color: #555;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


@st.cache_resource
def get_duckdb_connection():
    con = duckdb.connect()
    # Install & load httpfs/aws jika query langsung ke S3
    try:
        con.execute("INSTALL httpfs; LOAD httpfs;")
    except Exception:
        pass
    return con


def load_data(gold_path: str, con: duckdb.DuckDBPyConnection):
    """Load 4 Gold Data Marts into Pandas DataFrames using DuckDB"""
    gold_p = Path(gold_path)

    # 1. Daily Sales Summary
    daily_sales_p = gold_p / "daily_sales_summary"
    if daily_sales_p.exists():
        df_sales = con.execute(
            f"SELECT * FROM read_parquet('{daily_sales_p}/*.parquet') ORDER BY purchase_date"
        ).df()
        if "purchase_date" in df_sales.columns:
            df_sales["purchase_date"] = pd.to_datetime(df_sales["purchase_date"])
    else:
        df_sales = pd.DataFrame()

    # 2. Seller Performance Daily
    seller_p = gold_p / "seller_performance_daily"
    if seller_p.exists():
        df_seller = con.execute(
            f"SELECT * FROM read_parquet('{seller_p}/*.parquet') ORDER BY purchase_date, total_revenue DESC"
        ).df()
        if "purchase_date" in df_seller.columns:
            df_seller["purchase_date"] = pd.to_datetime(df_seller["purchase_date"])
    else:
        df_seller = pd.DataFrame()

    # 3. Category Performance
    cat_p = gold_p / "category_performance"
    if cat_p.exists():
        df_category = con.execute(
            f"SELECT * FROM read_parquet('{cat_p}/*.parquet') ORDER BY total_revenue DESC"
        ).df()
    else:
        df_category = pd.DataFrame()

    # 4. Daily Review Summary
    review_p = gold_p / "daily_review_summary"
    if review_p.exists():
        df_review = con.execute(
            f"SELECT * FROM read_parquet('{review_p}/*.parquet') ORDER BY purchase_date"
        ).df()
        if "purchase_date" in df_review.columns:
            df_review["purchase_date"] = pd.to_datetime(df_review["purchase_date"])
    else:
        df_review = pd.DataFrame()

    return df_sales, df_seller, df_category, df_review


# ==========================================
# Sidebar: Controls & Metadata
# ==========================================
st.sidebar.title("⚙️ Dashboard Settings")
st.sidebar.caption("Retail Data Lakehouse - Serving Layer")

gold_storage_mode = st.sidebar.radio(
    "Data Source Mode",
    ["Local Parquet (data/gold)", "Custom Path"],
    index=0,
)

if gold_storage_mode == "Local Parquet (data/gold)":
    gold_dir = str(DEFAULT_LOCAL_GOLD)
else:
    gold_dir = st.sidebar.text_input("Gold Path", str(DEFAULT_LOCAL_GOLD))

st.sidebar.markdown("---")
st.sidebar.subheader("📌 Architecture Highlights")
st.sidebar.markdown(
    """
- **Medallion**: S3/Local Raw $\\rightarrow$ PySpark Silver (Star Schema) $\\rightarrow$ Gold Marts
- **Modeling**: Kimball Star Schema (SCD2 Customer, SCD1 Product & Seller)
- **Serving Engine**: DuckDB Columnar Query Engine
- **Idempotency**: Date-scoped dynamic partition overwrite
"""
)

# Connect & Load Data
con = get_duckdb_connection()
df_sales, df_seller, df_category, df_review = load_data(gold_dir, con)

# ==========================================
# Main Header & High-Level KPIs
# ==========================================
st.title("🛍️ Olist E-Commerce Executive Dashboard")
st.markdown(
    "Analytics & BI Serving Layer powered by **PySpark**, **Parquet**, and **DuckDB**."
)

if df_sales.empty:
    st.warning(
        f"⚠️ Data Gold tidak ditemukan di path: `{gold_dir}`. Pastikan job `build_gold_aggregates.py` telah dijalankan."
    )
    st.stop()

# Date range filtering if applicable
min_date = df_sales["purchase_date"].min().date()
max_date = df_sales["purchase_date"].max().date()

col_filter1, col_filter2 = st.columns([2, 2])
with col_filter1:
    selected_dates = st.date_input(
        "Filter Rentang Waktu (Purchase Date):",
        value=(min_date, max_date),
        min_value=min_date,
        max_value=max_date,
    )

if isinstance(selected_dates, tuple) and len(selected_dates) == 2:
    start_d, end_d = pd.to_datetime(selected_dates[0]), pd.to_datetime(selected_dates[1])
    filtered_sales = df_sales[
        (df_sales["purchase_date"] >= start_d) & (df_sales["purchase_date"] <= end_d)
    ]
    filtered_seller = df_seller[
        (df_seller["purchase_date"] >= start_d) & (df_seller["purchase_date"] <= end_d)
    ] if not df_seller.empty else df_seller
    filtered_review = df_review[
        (df_review["purchase_date"] >= start_d) & (df_review["purchase_date"] <= end_d)
    ] if not df_review.empty else df_review
else:
    filtered_sales = df_sales
    filtered_seller = df_seller
    filtered_review = df_review

# Calculate Top KPIs
total_gross_rev = filtered_sales["gross_revenue"].sum()
total_orders = filtered_sales["order_count"].sum()
overall_aov = total_gross_rev / total_orders if total_orders > 0 else 0
total_product_rev = filtered_sales["product_revenue"].sum()
total_shipping_rev = filtered_sales["shipping_revenue"].sum()

if not filtered_review.empty and filtered_review["review_count"].sum() > 0:
    weighted_review_score = (
        filtered_review["avg_review_score"] * filtered_review["review_count"]
    ).sum() / filtered_review["review_count"].sum()
else:
    weighted_review_score = 0.0

st.markdown("### 📊 Key Performance Indicators")
kpi1, kpi2, kpi3, kpi4, kpi5 = st.columns(5)
kpi1.metric("Total Gross Revenue", f"R$ {total_gross_rev:,.2f}")
kpi2.metric("Total Orders", f"{total_orders:,}")
kpi3.metric("Avg Order Value (AOV)", f"R$ {overall_aov:,.2f}")
kpi4.metric("Product Revenue", f"R$ {total_product_rev:,.2f}")
kpi5.metric("Avg Review Score", f"⭐ {weighted_review_score:.2f} / 5.0")

st.markdown("---")

# ==========================================
# 4 Analytical Tabs
# ==========================================
tab_sales, tab_category, tab_seller, tab_reviews = st.tabs(
    [
        "📈 Sales & Revenue Trends",
        "📦 Product Category Analytics",
        "🏪 Seller & Regional Insights",
        "⭐ Customer Satisfaction (Reviews)",
    ]
)

# ----------------------------------------------------
# TAB 1: Sales & Revenue Trends
# ----------------------------------------------------
with tab_sales:
    st.subheader("Daily Sales & Revenue Dynamics")
    
    col_s1, col_s2 = st.columns([3, 2])
    with col_s1:
        st.markdown("**Gross vs Product vs Shipping Revenue Trend**")
        chart_sales = filtered_sales.set_index("purchase_date")[
            ["gross_revenue", "product_revenue", "shipping_revenue"]
        ]
        st.line_chart(chart_sales, use_container_width=True)

    with col_s2:
        st.markdown("**Daily Order Volume vs Average Order Value**")
        chart_aov = filtered_sales.set_index("purchase_date")[["order_count", "avg_order_value"]]
        st.line_chart(chart_aov, use_container_width=True)

    with st.expander("🔍 Lihat Data Tabel Penjualan Harian"):
        st.dataframe(filtered_sales, use_container_width=True)

# ----------------------------------------------------
# TAB 2: Product Category Analytics
# ----------------------------------------------------
with tab_category:
    st.subheader("Product Category Performance")
    if not df_category.empty:
        col_c1, col_c2 = st.columns([3, 2])
        
        with col_c1:
            st.markdown("**Top 15 Categories by Total Revenue**")
            top_cat = df_category.head(15).set_index("category_name")["total_revenue"]
            st.bar_chart(top_cat, use_container_width=True)

        with col_c2:
            st.markdown("**Top 15 Categories by Order Volume**")
            top_cat_orders = df_category.sort_values("order_count", ascending=False).head(15).set_index("category_name")["order_count"]
            st.bar_chart(top_cat_orders, use_container_width=True)

        st.markdown("**Detail Metrik Kategori Lengkap**")
        st.dataframe(
            df_category.style.format(
                {
                    "total_revenue": "R$ {:,.2f}",
                    "avg_order_value": "R$ {:,.2f}",
                    "order_count": "{:,}",
                }
            ),
            use_container_width=True,
        )
    else:
        st.info("Data Category Performance belum tersedia.")

# ----------------------------------------------------
# TAB 3: Seller & Regional Insights
# ----------------------------------------------------
with tab_seller:
    st.subheader("Seller & Regional Geographic Performance")
    if not filtered_seller.empty:
        col_sel1, col_sel2 = st.columns([2, 2])
        
        with col_sel1:
            st.markdown("**Total Revenue by State (Top 10)**")
            state_rev = (
                filtered_seller.groupby("state")["total_revenue"]
                .sum()
                .sort_values(ascending=False)
                .head(10)
            )
            st.bar_chart(state_rev, use_container_width=True)

        with col_sel2:
            st.markdown("**Top 10 Sellers by Revenue**")
            seller_totals = (
                filtered_seller.groupby("seller_id")
                .agg({"total_revenue": "sum", "order_count": "sum", "state": "first", "city": "first"})
                .sort_values("total_revenue", ascending=False)
                .head(10)
                .reset_index()
            )
            st.dataframe(
                seller_totals.style.format(
                    {"total_revenue": "R$ {:,.2f}", "order_count": "{:,}"}
                ),
                use_container_width=True,
            )
    else:
        st.info("Data Seller Performance belum tersedia.")

# ----------------------------------------------------
# TAB 4: Customer Satisfaction (Reviews)
# ----------------------------------------------------
with tab_reviews:
    st.subheader("Customer Review Score & Satisfaction Trends")
    if not filtered_review.empty:
        col_r1, col_r2 = st.columns([3, 2])
        
        with col_r1:
            st.markdown("**Daily Average Review Score Trend (1 - 5 ⭐)**")
            chart_review_score = filtered_review.set_index("purchase_date")["avg_review_score"]
            st.line_chart(chart_review_score, use_container_width=True)

        with col_r2:
            st.markdown("**Daily Review Count Volume**")
            chart_review_count = filtered_review.set_index("purchase_date")["review_count"]
            st.bar_chart(chart_review_count, use_container_width=True)

        st.markdown("**Tabel Rangkuman Ulasan Pelanggan**")
        st.dataframe(filtered_review, use_container_width=True)
    else:
        st.info("Data Review Summary belum tersedia.")

