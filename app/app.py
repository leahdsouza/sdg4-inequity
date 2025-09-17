from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st

st.set_page_config(page_title="SDG4 Inequity Map", layout="wide")

DATA = Path("data/public/inequity_index.parquet")
if not DATA.exists():
    st.error("inequity_index.parquet not found. Run pipelines/build_index.py first.")
    st.stop()

df = pd.read_parquet(DATA).copy()
# Clean: drop rows w/ missing iso3 or index
df = df.dropna(subset=["country_iso3", "inequity_index"])
df["year"] = pd.to_numeric(df["year"], errors="coerce")
df = df.dropna(subset=["year"]).astype({"year": int})
years = sorted(df["year"].unique())
default_year = max([y for y in years if 2015 <= y <= 2024] or years)

st.title("Mapping Global Education Inequity (SDG 4)")

col_a, col_b = st.columns([2, 1], gap="large")
with col_b:
    year = st.selectbox("Select year", options=years, index=years.index(default_year))
    st.markdown(
        """
        **About the index**
        - 0 to 1 (higher = **better** equity)
        - Combines: learning, early childhood, participation, gender parity, infrastructure, teachers
        - Requires ≥2 indicators per country-year
        """
    )

# Filter by year
d = df[df["year"] == year].copy()

# Choropleth (ISO3-based)
fig = px.choropleth(
    d,
    locations="country_iso3",
    color="inequity_index",
    hover_name="country_name",
    color_continuous_scale="Viridis",
    range_color=(0, 1),
    title=f"Inequity Index by Country — {year}",
)
fig.update_geos(
    showcountries=True,
    showcoastlines=False,
    showframe=False,
    projection_type="natural earth",
)
fig.update_layout(margin=dict(l=0, r=0, t=60, b=0))

with col_a:
    st.plotly_chart(fig, use_container_width=True)

# Top / bottom tables
d_sorted = d.sort_values("inequity_index", ascending=False)
left, right = st.columns(2)
with left:
    st.subheader("Top 10 (more equitable)")
    st.dataframe(
        d_sorted.head(10)[["country_name", "inequity_index"]].reset_index(drop=True)
    )
with right:
    st.subheader("Bottom 10 (less equitable)")
    st.dataframe(
        d_sorted.tail(10)[["country_name", "inequity_index"]].reset_index(drop=True)
    )

st.caption(
    "Prototype index. Data coverage varies by country and year; see docs/coverage_by_country_year.csv."
)
