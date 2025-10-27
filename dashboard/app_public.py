# dashboard/app_public.py
import pandas as pd
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Polymarket (CSV demo)", layout="wide")
st.title("📊 Polymarket (CSV Demo)")

@st.cache_data
def load_csv(path: str):
    p = Path(path)
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
    except Exception:
        df = None
    return df

feat = load_csv("features_pre_res.csv")
bets = load_csv("bets_level_data.csv")

tab1, tab2, tab3 = st.tabs(["Overview", "Features CSV", "Bets-level CSV"])

with tab1:
    st.subheader("What you’re seeing")
    st.markdown("""
This public demo reads **CSV files committed to the repo**:
- **features_pre_res.csv** — one row per market, leakage-aware features from the last snapshot *before* resolution.
- **bets_level_data.csv** — price/volume ticks (per token), a proxy for trade flow.

> The full scraper & database run privately; we only publish CSV artifacts here.
""")
    cols = st.columns(3)
    cols[0].metric("Features rows", 0 if feat is None else len(feat))
    cols[1].metric("Bets rows", 0 if bets is None else len(bets))
    cols[2].metric("Repo mode", "CSV only")

with tab2:
    st.subheader("Features (per market)")
    if feat is None or feat.empty:
        st.warning("features_pre_res.csv not found or empty in the repo.")
    else:
        kc1, kc2 = st.columns([2,1])
        q = kc1.text_input("Search question contains", "", key="feat_query")
        n = kc2.slider("Show top N rows", 50, 5000, min(500, len(feat)), key="feat_top_n")
        dfv = feat
        if q:
            dfv = dfv[dfv["question"].astype(str).str.contains(q, case=False, na=False)]
        st.dataframe(dfv.head(n), use_container_width=True)
        st.download_button(
            "Download features_pre_res.csv",
            data=feat.to_csv(index=False).encode("utf-8"),
            file_name="features_pre_res.csv",
            mime="text/csv",
            key="feat_download",
        )

with tab3:
    st.subheader("Bets-level ticks (per token)")
    if bets is None or bets.empty:
        st.warning("bets_level_data.csv not found or empty in the repo.")
    else:
        c1, c2, c3 = st.columns([2,1,1])
        q2   = c1.text_input("Search question contains", "", key="bets_query")
        mkt  = c2.text_input("Market id equals", "", key="bets_market_id")
        top  = c3.slider("Show top N rows", 50, 5000, min(1000, len(bets)), key="bets_top_n")
        dfb = bets
        if q2:
            dfb = dfb[dfb["question"].astype(str).str.contains(q2, case=False, na=False)]
        if mkt:
            dfb = dfb[dfb["market_id"].astype(str) == mkt]
        st.dataframe(dfb.head(top), use_container_width=True)
        st.download_button(
            "Download bets_level_data.csv",
            data=bets.to_csv(index=False).encode("utf-8"),
            file_name="bets_level_data.csv",
            mime="text/csv",
            key="bets_download",
        )
