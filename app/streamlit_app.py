import duckdb
import pandas as pd
import streamlit as st
from scripts.config import DUCKDB_PATH

st.set_page_config(page_title="Stages Analytics", layout="wide")

st.title("Stages (2023-2025) - Dashboard")

if not DUCKDB_PATH.exists():
    st.warning("DuckDB not found. Run scripts/03_load_duckdb.py and scripts/04_build_marts.py first.")
    st.stop()

con = duckdb.connect(str(DUCKDB_PATH), read_only=True)

def list_tables():
    try:
        return [row[0] for row in con.execute("SHOW TABLES").fetchall()]
    except Exception:
        return []

def safe_count(table_name):
    try:
        return con.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    except Exception:
        return None

def render_debug():
    tables = list_tables()
    st.write(f"DuckDB path: `{DUCKDB_PATH}`")
    st.write("Tables:", tables if tables else "None")
    for table in ["stages_clean", "stages_analytics", "mart_top_companies", "mart_geo", "mart_trends"]:
        if table in tables:
            st.write(f"{table} rows:", safe_count(table))
    if "stages_analytics" in tables:
        try:
            non_null_years = con.execute(
                "SELECT COUNT(*) FROM stages_analytics WHERE annee IS NOT NULL"
            ).fetchone()[0]
            st.write("stages_analytics annee non-null:", non_null_years)
        except Exception as exc:
            st.write("stages_analytics year check error:", str(exc))

if st.button("Refresh data"):
    st.cache_data.clear()
    st.rerun()

@st.cache_data
def load_years():
    return con.execute("SELECT DISTINCT annee FROM mart_top_companies ORDER BY annee").df()

@st.cache_data
def load_top_companies(year):
    return con.execute(
        "SELECT entreprise, nb_stages FROM mart_top_companies WHERE annee = ? ORDER BY nb_stages DESC LIMIT 20",
        [year],
    ).df()

@st.cache_data
def load_geo(year):
    return con.execute(
        "SELECT pays, nb_stages FROM mart_geo WHERE annee = ? ORDER BY nb_stages DESC",
        [year],
    ).df()

@st.cache_data
def load_trends():
    return con.execute(
        "SELECT annee, nb_stages FROM mart_trends ORDER BY annee"
    ).df()

try:
    years_df = load_years()
except Exception as exc:
    st.error("Failed to read marts from DuckDB.")
    with st.expander("Diagnostics"):
        st.write(str(exc))
        render_debug()
    st.stop()

if years_df.empty:
    st.info("No marts found yet. Run scripts/04_build_marts.py.")
    with st.expander("Diagnostics"):
        render_debug()
    st.stop()

selected_year = st.selectbox("Annee", years_df["annee"].tolist())

col1, col2 = st.columns(2)
with col1:
    st.subheader("Top entreprises")
    top_df = load_top_companies(selected_year)
    st.bar_chart(top_df, x="entreprise", y="nb_stages")

with col2:
    st.subheader("Repartition par pays")
    geo_df = load_geo(selected_year)
    st.bar_chart(geo_df, x="pays", y="nb_stages")

st.subheader("Tendance 2023 → 2025")
trends_df = load_trends()
st.line_chart(trends_df, x="annee", y="nb_stages")

st.caption("Source: stages CSV (nettoye/anonymise)")
