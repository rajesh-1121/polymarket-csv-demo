import pandas as pd
import streamlit as st
from pathlib import Path

st.set_page_config(page_title="Polymarket CSV Explorer", layout="wide")
st.title("📊 Polymarket CSV Explorer")

@st.cache_data
def load_csv(path: str):
    p = Path(path)
    if not p.exists():
        return None
    try:
        df = pd.read_csv(p)
    except Exception:
        # try parquet or compressed later if you want; for now just fail to None
        df = None
    return df

def download_btn(label: str, df: pd.DataFrame, file_name: str, key: str):
    st.download_button(
        label,
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=file_name,
        mime="text/csv",
        key=key,
    )

# ----- Load all possible CSVs (present-or-not)
feat = load_csv("features_pre_res.csv")
bets = load_csv("bets_level_data.csv")
mkts = load_csv("markets.csv")
toks = load_csv("tokens.csv")
phist = load_csv("price_history.csv")
micro = load_csv("microstructure.csv")
holders = load_csv("holders_snapshot.csv")
ingest = load_csv("ingest_log.csv")

tabs = st.tabs([
    "Overview",
    "Features",
    "Bets (ticks)",
    "Markets",
    "Tokens",
    "Price history",
    "Microstructure",
    "Holders",
    "Ingest log",
])

# ===================== OVERVIEW =====================
with tabs[0]:
    cols = st.columns(4)
    cols[0].metric("Features rows", 0 if feat is None else len(feat))
    cols[1].metric("Bets rows", 0 if bets is None else len(bets))
    cols[2].metric("Markets rows", 0 if mkts is None else len(mkts))
    cols[3].metric("Tokens rows", 0 if toks is None else len(toks))

    cols2 = st.columns(4)
    cols2[0].metric("Price history rows", 0 if phist is None else len(phist))
    cols2[1].metric("Micro rows", 0 if micro is None else len(micro))
    cols2[2].metric("Holders rows", 0 if holders is None else len(holders))
    cols2[3].metric("Ingest rows", 0 if ingest is None else len(ingest))

    st.info(
        "This public app reads **CSV files committed to the repo**. "
        "If a tab shows ‘not found’, commit the corresponding CSV to the repo root."
    )

# ===================== FEATURES =====================
with tabs[1]:
    st.subheader("Features (per market)")
    if feat is None or feat.empty:
        st.warning("features_pre_res.csv not found or empty.")
    else:
        c1, c2 = st.columns([2,1])
        q = c1.text_input("Search question contains", "", key="feat_q")
        n = c2.slider("Top N rows", 50, 5000, min(500, len(feat)), key="feat_n")
        dfv = feat
        if "question" in dfv.columns and q:
            dfv = dfv[dfv["question"].astype(str).str.contains(q, case=False, na=False)]
        st.dataframe(dfv.head(n), use_container_width=True)
        download_btn("Download features_pre_res.csv", feat, "features_pre_res.csv", "dl_feat")

# ===================== BETS =====================
with tabs[2]:
    st.subheader("Bets-level ticks (per token)")
    if bets is None or bets.empty:
        st.warning("bets_level_data.csv not found or empty.")
    else:
        c1, c2, c3 = st.columns([2,1,1])
        q2 = c1.text_input("Search question contains", "", key="bets_q")
        mkt = c2.text_input("Market id equals", "", key="bets_mkt")
        n2 = c3.slider("Top N rows", 50, 5000, min(1000, len(bets)), key="bets_n")
        dfb = bets
        if "question" in dfb.columns and q2:
            dfb = dfb[dfb["question"].astype(str).str.contains(q2, case=False, na=False)]
        if "market_id" in dfb.columns and mkt:
            dfb = dfb[dfb["market_id"].astype(str) == mkt]
        st.dataframe(dfb.head(n2), use_container_width=True)
        download_btn("Download bets_level_data.csv", bets, "bets_level_data.csv", "dl_bets")

# ===================== MARKETS =====================
with tabs[3]:
    st.subheader("Markets")
    if mkts is None or mkts.empty:
        st.warning("markets.csv not found or empty.")
    else:
        c1, c2 = st.columns([2,1])
        q3 = c1.text_input("Search question/slug contains", "", key="mkts_q")
        n3 = c2.slider("Top N rows", 50, 5000, min(1000, len(mkts)), key="mkts_n")
        dfm = mkts
        for col in ["question", "slug", "clob_slug"]:
            if col in dfm.columns and q3:
                dfm = dfm[dfm[col].astype(str).str.contains(q3, case=False, na=False)]
        st.dataframe(dfm.head(n3), use_container_width=True)
        download_btn("Download markets.csv", mkts, "markets.csv", "dl_mkts")

# ===================== TOKENS =====================
with tabs[4]:
    st.subheader("Tokens")
    if toks is None or toks.empty:
        st.warning("tokens.csv not found or empty.")
    else:
        c1, c2 = st.columns([2,1])
        mkt2 = c1.text_input("Filter by market_id", "", key="toks_mkt")
        n4 = c2.slider("Top N rows", 50, 5000, min(1000, len(toks)), key="toks_n")
        dft = toks
        if "market_id" in dft.columns and mkt2:
            dft = dft[dft["market_id"].astype(str) == mkt2]
        st.dataframe(dft.head(n4), use_container_width=True)
        download_btn("Download tokens.csv", toks, "tokens.csv", "dl_toks")

# ===================== PRICE HISTORY =====================
with tabs[5]:
    st.subheader("Price history (ticks)")
    if phist is None or phist.empty:
        st.warning("price_history.csv not found or empty.")
    else:
        c1, c2, c3 = st.columns([2,1,1])
        tid = c1.text_input("Filter by token_id", "", key="ph_tid")
        n5 = c2.slider("Top N rows", 50, 10000, min(2000, len(phist)), key="ph_n")
        sort_asc = c3.toggle("Sort by ts ascending", value=True, key="ph_sort")
        dfp = phist
        if "token_id" in dfp.columns and tid:
            dfp = dfp[dfp["token_id"].astype(str) == tid]
        if "ts" in dfp.columns:
            dfp = dfp.sort_values("ts", ascending=sort_asc)
        st.dataframe(dfp.head(n5), use_container_width=True)
        download_btn("Download price_history.csv", phist, "price_history.csv", "dl_ph")

# ===================== MICROSTRUCTURE =====================
with tabs[6]:
    st.subheader("Microstructure (top-of-book snapshots)")
    if micro is None or micro.empty:
        st.warning("microstructure.csv not found or empty.")
    else:
        c1, c2 = st.columns([2,1])
        tid2 = c1.text_input("Filter by token_id", "", key="micro_tid")
        n6 = c2.slider("Top N rows", 50, 10000, min(2000, len(micro)), key="micro_n")
        dfmi = micro
        if "token_id" in dfmi.columns and tid2:
            dfmi = dfmi[dfmi["token_id"].astype(str) == tid2]
        st.dataframe(dfmi.head(n6), use_container_width=True)
        download_btn("Download microstructure.csv", micro, "microstructure.csv", "dl_micro")

# ===================== HOLDERS =====================
with tabs[7]:
    st.subheader("Holders (snapshot)")
    if holders is None or holders.empty:
        st.warning("holders_snapshot.csv not found or empty.")
    else:
        c1, c2 = st.columns([2,1])
        mkt3 = c1.text_input("Filter by market_id", "", key="holders_mkt")
        n7 = c2.slider("Top N rows", 50, 5000, min(1000, len(holders)), key="holders_n")
        dfh = holders
        if "market_id" in dfh.columns and mkt3:
            dfh = dfh[dfh["market_id"].astype(str) == mkt3]
        st.dataframe(dfh.head(n7), use_container_width=True)
        download_btn("Download holders_snapshot.csv", holders, "holders_snapshot.csv", "dl_holders")

# ===================== INGEST LOG =====================
with tabs[8]:
    st.subheader("Ingest log")
    if ingest is None or ingest.empty:
        st.warning("ingest_log.csv not found or empty.")
    else:
        c1, c2 = st.columns([2,1])
        ep = c1.text_input("Filter by endpoint contains", "", key="ing_ep")
        n8 = c2.slider("Top N rows", 50, 5000, min(1000, len(ingest)), key="ing_n")
        dfi = ingest
        if "endpoint" in dfi.columns and ep:
            dfi = dfi[dfi["endpoint"].astype(str).str.contains(ep, case=False, na=False)]
        st.dataframe(dfi.head(n8), use_container_width=True)
        download_btn("Download ingest_log.csv", ingest, "ingest_log.csv", "dl_ingest")
