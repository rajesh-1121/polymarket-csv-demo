# dashboard/app_monitor.py
import os
from datetime import datetime, timezone
from typing import Dict, Any

import io
from pathlib import Path

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# --- bootstrap
load_dotenv(".env")
DB_URL = os.environ.get("DATABASE_URL")
if not DB_URL:
    raise RuntimeError("DATABASE_URL not set. Run: `python -m streamlit run dashboard/app_monitor.py`")

st.set_page_config(page_title="Polymarket Pipeline Monitor", layout="wide")
st.title("🔎 Polymarket Pipeline Monitor")

# ---------- engine + query helpers (pooled + timeouts)
@st.cache_resource
def get_engine():
    return create_engine(
        DB_URL,
        pool_pre_ping=True,
        pool_size=8,
        max_overflow=4,
        pool_recycle=1800,
        connect_args={"options": "-c statement_timeout=5000"}  # 5s server-side timeout
    )

@st.cache_data(ttl=30)
def q(sql: str, params: Dict[str, Any] | None = None, timeout_ms: int = 5000) -> pd.DataFrame:
    eng = get_engine()
    with eng.begin() as conn:
        conn.execute(text(f"SET LOCAL statement_timeout = {timeout_ms}"))
        return pd.read_sql(text(sql), conn, params=params or {})

def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode("utf-8")

# ---------- KPI helpers
def metric_row():
    col1, col2, col3, col4, col5 = st.columns(5)
    kpis = q("""
    SELECT
      (SELECT count(*) FROM markets)                                        AS markets,
      (SELECT count(*) FROM tokens)                                         AS tokens_rows,
      (SELECT count(DISTINCT market_id) FROM tokens)                        AS markets_with_tokens,
      (SELECT count(*) FROM price_history)                                  AS price_points,
      (SELECT count(*) FROM ingest_log)                                     AS ingest_events
    """)
    col1.metric("Markets", int(kpis.at[0, "markets"]))
    col2.metric("Tokens rows", int(kpis.at[0, "tokens_rows"]))
    col3.metric("Markets w/ tokens", int(kpis.at[0, "markets_with_tokens"]))
    col4.metric("Price points", int(kpis.at[0, "price_points"]))
    col5.metric("Ingest events", int(kpis.at[0, "ingest_events"]))

def fmt_ts(x):
    if x is None:
        return "—"
    # ensure tz-aware UTC for consistent display
    if isinstance(x, pd.Timestamp):
        dt = x.to_pydatetime()
    else:
        dt = x
    if getattr(dt, "tzinfo", None) is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.strftime("%Y-%m-%d %H:%M:%S UTC")

@st.cache_data(ttl=60)
def get_time_bounds() -> pd.DataFrame:
    def one(sql: str, tmo: int = 15000):
        try:
            df = q(sql, timeout_ms=tmo)
            return df.iloc[0, 0]
        except Exception:
            return None

    markets_first = one("SELECT MIN(inserted_at) FROM markets", 15000)
    markets_last  = one("SELECT MAX(inserted_at) FROM markets", 15000)

    price_first   = one("SELECT MIN(ts) FROM price_history", 15000)
    price_last    = one("SELECT MAX(ts) FROM price_history", 15000)

    micro_first   = one("SELECT MIN(ts) FROM microstructure", 15000)
    micro_last    = one("SELECT MAX(ts) FROM microstructure", 15000)

    holders_first = one("SELECT MIN(ts) FROM holders_snapshot", 15000)
    holders_last  = one("SELECT MAX(ts) FROM holders_snapshot", 15000)

    ingest_first  = one("SELECT MIN(ts) FROM ingest_log", 15000)
    ingest_last   = one("SELECT MAX(ts) FROM ingest_log", 15000)

    rows = [
        {"Dataset":"Markets (inserted_at)", "Oldest": fmt_ts(markets_first), "Newest": fmt_ts(markets_last)},
        {"Dataset":"Price history",          "Oldest": fmt_ts(price_first),   "Newest": fmt_ts(price_last)},
        {"Dataset":"Microstructure",         "Oldest": fmt_ts(micro_first),   "Newest": fmt_ts(micro_last)},
        {"Dataset":"Holders snapshot",       "Oldest": fmt_ts(holders_first), "Newest": fmt_ts(holders_last)},
        {"Dataset":"Ingest log",             "Oldest": fmt_ts(ingest_first),  "Newest": fmt_ts(ingest_last)},
    ]
    return pd.DataFrame(rows)


# ---------- TABS
tab_overview, tab_markets, tab_tokens, tab_prices, tab_micro, tab_holders, tab_logs, tab_exports = st.tabs(
    ["Overview", "Markets", "Tokens", "Prices", "Micro", "Holders", "Logs", "Exports"]
)

# ===================== OVERVIEW =====================
with tab_overview:
    # Refresh controls
    rc1, rc2 = st.columns([1,3])
    with rc1:
        if st.button("↻ Refresh data (reload UI cache)"):
            st.cache_data.clear()
            st.rerun()
    with rc2:
        st.caption(f"Last UI refresh: {datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S UTC')}")

    metric_row()

    st.subheader("Data freshness (oldest ↔ newest)")
    freshness_df = get_time_bounds()
    st.dataframe(freshness_df, width='stretch')

    st.subheader("Coverage")
    cov1, cov2, cov3 = st.columns(3)

    tok_cov = q("""
    SELECT
      COUNT(*) FILTER (WHERE t.token_id_yes IS NOT NULL) AS markets_token_yes,
      COUNT(*) FILTER (WHERE t.token_id_no  IS NOT NULL) AS markets_token_no,
      COUNT(*)                                           AS total_markets
    FROM markets m
    LEFT JOIN tokens t ON t.market_id = m.market_id
    """)
    with cov1:
        st.caption("Token coverage")
        st.dataframe(tok_cov, width='stretch')

    cut_cov = q("""
    SELECT
      COUNT(*) FILTER (WHERE resolution_time_uma IS NOT NULL) AS markets_with_cutoff,
      COUNT(*) AS total_markets
    FROM markets
    """)
    with cov2:
        st.caption("Cutoff coverage")
        st.dataframe(cut_cov, width='stretch')

    # Price history coverage (fast)
    hp_cov = q("""
    WITH markets_with_prices AS (
    SELECT DISTINCT t.market_id
    FROM tokens t
    JOIN price_history ph
        ON ph.token_id = t.token_id_yes
        OR ph.token_id = t.token_id_no
    )
    SELECT
    (SELECT COUNT(*) FROM markets_with_prices) AS markets_with_prices,
    (SELECT COUNT(*) FROM markets)            AS total_markets
    """, timeout_ms=15000)
    with cov3:
        st.caption("Price history coverage")
        st.dataframe(hp_cov, width='stretch')


    st.subheader("Leakage guard (should be empty)")
    leak = q("""
    SELECT ph.token_id, ph.ts, m.resolution_time_uma, m.market_id
    FROM price_history ph
    JOIN tokens t ON t.token_id_yes = ph.token_id OR t.token_id_no = ph.token_id
    JOIN markets m ON m.market_id = t.market_id
    WHERE m.resolution_time_uma IS NOT NULL
      AND ph.ts >= m.resolution_time_uma
    ORDER BY ph.ts DESC
    LIMIT 200
    """)
    if leak.empty:
        st.success("No rows at/after cutoff ✅")
    else:
        st.error("Found post-cutoff rows — fix endTs/WHERE ts < cutoff")
        st.dataframe(leak, width='stretch')

    st.subheader("Gap finders")
    with st.expander("Markets missing tokens (join by slug)"):
        no_tok = q("""
        SELECT m.market_id, COALESCE(m.clob_slug,m.slug) AS slug, m.question, m.end_time, m.resolution_time_uma
        FROM markets m
        LEFT JOIN tokens t ON t.market_id = m.market_id
        WHERE t.market_id IS NULL
        ORDER BY m.inserted_at DESC
        LIMIT 200
        """)
        st.dataframe(no_tok, width='stretch')

    with st.expander("Markets with tokens but **no** price_history"):
        tok_no_price = q("""
        SELECT m.market_id, COALESCE(m.clob_slug,m.slug) AS slug, t.token_id_yes, t.token_id_no, m.resolution_time_uma
        FROM markets m
        JOIN tokens t ON t.market_id = m.market_id
        WHERE NOT EXISTS (
          SELECT 1 FROM price_history ph
          WHERE ph.token_id = t.token_id_yes OR ph.token_id = t.token_id_no
        )
        ORDER BY m.inserted_at DESC
        LIMIT 200
        """)
        st.dataframe(tok_no_price, width='stretch')

# ===================== MARKETS =====================
with tab_markets:
    st.subheader("Markets (filterable)")
    fl1, fl2, fl3, fl4 = st.columns([2,1,1,1])
    search = fl1.text_input("Search (question or slug contains)", "")
    has_tokens = fl2.selectbox("Has tokens?", ["All", "Yes", "No"])
    has_prices = fl3.selectbox("Has price history?", ["All", "Yes", "No"])
    has_cutoff = fl4.selectbox("Has UMA cutoff?", ["All", "Yes", "No"])

    where = ["1=1"]
    params: Dict[str, Any] = {}
    if search:
        where.append("(LOWER(m.question) LIKE :s OR LOWER(COALESCE(m.clob_slug,m.slug)) LIKE :s)")
        params["s"] = f"%{search.lower()}%"
    if has_tokens != "All":
        where.append(("EXISTS" if has_tokens == "Yes" else "NOT EXISTS") + """ (
            SELECT 1 FROM tokens tt WHERE tt.market_id = m.market_id
        )""")
    if has_prices != "All":
        where.append(("EXISTS" if has_prices == "Yes" else "NOT EXISTS") + """ (
            SELECT 1 FROM price_history ph
            JOIN tokens tt ON tt.token_id_yes = ph.token_id OR tt.token_id_no = ph.token_id
            WHERE tt.market_id = m.market_id
        )""")
    if has_cutoff != "All":
        where.append("m.resolution_time_uma IS NOT NULL" if has_cutoff == "Yes" else "m.resolution_time_uma IS NULL")

    sql = f"""
    SELECT m.market_id, COALESCE(m.clob_slug,m.slug) AS slug, m.question, m.end_time, m.resolution_time_uma
    FROM markets m
    WHERE {' AND '.join(where)}
    ORDER BY m.inserted_at DESC
    LIMIT 1000
    """
    dfm = q(sql, params, timeout_ms=4000)
    st.dataframe(dfm, width='stretch')

# ===================== TOKENS =====================
with tab_tokens:
    st.subheader("Tokens")
    df_tokens = q("""
    SELECT t.market_id, t.token_id_yes, t.token_id_no, COALESCE(m.clob_slug,m.slug) AS slug, m.question
    FROM tokens t
    JOIN markets m ON m.market_id = t.market_id
    ORDER BY m.inserted_at DESC
    LIMIT 2000
    """, timeout_ms=4000)
    st.dataframe(df_tokens, width='stretch')

    st.caption("Pick a market to inspect tokens & linked data")
    if not df_tokens.empty:
        mid = st.selectbox("Market", options=df_tokens["market_id"])
        tokens = df_tokens[df_tokens["market_id"] == mid].iloc[0]
        st.write(f"**Slug:** `{tokens['slug']}`\n\n**Question:** {tokens['question']}")
        st.write(f"**YES token:** `{tokens['token_id_yes']}`  |  **NO token:** `{tokens['token_id_no']}`")

# ===================== PRICES =====================
with tab_prices:
    st.subheader("Price history explorer (by token)")
    token_id = st.text_input("Enter a token_id (YES or NO)", "")
    limit = st.slider("Rows to fetch", 50, 5000, 500, step=50)
    if token_id:
        dfp = q("""
        SELECT token_id, ts, price_cents, volume
        FROM price_history
        WHERE token_id = :tid
        ORDER BY ts ASC
        LIMIT :lim
        """, {"tid": token_id, "lim": int(limit)}, timeout_ms=6000)
        if dfp.empty:
            st.warning("No price history for that token yet.")
        else:
            st.dataframe(dfp, width='stretch')
            # matplotlib line chart (no seaborn, single plot, default colors)
            import matplotlib.pyplot as plt
            fig, ax = plt.subplots()
            ax.plot(dfp["ts"], dfp["price_cents"])
            ax.set_title("price_cents over time")
            ax.set_xlabel("timestamp")
            ax.set_ylabel("price (cents)")
            st.pyplot(fig)

# ===================== MICRO =====================
with tab_micro:
    st.subheader("Microstructure snapshots (by token)")
    token_id_m = st.text_input("Enter a token_id to view microstructure", key="micro_token")
    if token_id_m:
        dfmicro = q("""
        SELECT token_id, ts, best_bid, best_ask, mid, spread, depth_k
        FROM microstructure
        WHERE token_id = :tid
        ORDER BY ts DESC
        LIMIT 500
        """, {"tid": token_id_m}, timeout_ms=6000)
        if dfmicro.empty:
            st.info("No microstructure snapshots for that token yet.")
        else:
            st.dataframe(dfmicro, width='stretch')

# ===================== HOLDERS =====================
with tab_holders:
    st.subheader("Holders snapshots")
    dfh = q("""
    SELECT market_id, ts, jsonb_array_length(top_holders) AS n_holders
    FROM holders_snapshot
    ORDER BY ts DESC
    LIMIT 500
    """, timeout_ms=4000)
    st.dataframe(dfh, width='stretch')

# ===================== LOGS =====================
with tab_logs:
    st.subheader("Recent ingest events")
    if st.toggle("Show logs (last 500)", value=False):
        logs = q("""
        SELECT ts, endpoint, status, market_id, LEFT(url, 100) AS url_short, params
        FROM ingest_log
        ORDER BY ts DESC
        LIMIT 500
        """, timeout_ms=4000)
        st.dataframe(logs, width='stretch')
    else:
        st.info("Toggle on to load logs.")

# ===================== EXPORTS =====================
with tab_exports:
    st.subheader("CSV Exports (always fresh, overwrites existing files)")

    # ---- Build & download features_pre_res.csv
    st.markdown("### Features CSV (leakage-aware, per market)")
    colf1, colf2 = st.columns([1,3])
    with colf1:
        build_features = st.button("Build features_pre_res.csv")
    features_outfile = "features_pre_res.csv"

    if build_features:
        from features.build import main as build_features_main
        build_features_main(limit_markets=2000, outfile=features_outfile)
        st.success(f"Built {features_outfile}")

    if Path(features_outfile).exists():
        df_feat = pd.read_csv(features_outfile)
        st.caption(f"{features_outfile} — {len(df_feat):,} rows")
        st.dataframe(df_feat.head(20), width='stretch')
        st.download_button(
            "Download features_pre_res.csv",
            data=df_to_csv_bytes(df_feat),
            file_name="features_pre_res.csv",
            mime="text/csv",
        )
    else:
        st.info("features_pre_res.csv not found yet. Click the button above to build it.")

    st.divider()

    # ---- Build & download bets_level_data.csv
    st.markdown("### Bets-level CSV (price/volume ticks per token)")
    colb1, colb2 = st.columns([1,3])
    with colb1:
        build_bets = st.button("Build bets_level_data.csv")

    bets_outfile = "bets_level_data.csv"
    BETS_SQL = """
    SELECT
      m.market_id,
      m.question,
      ph.token_id,
      ph.ts AS timestamp,
      (ph.price_cents / 100.0) AS price,
      ph.volume
    FROM price_history ph
    JOIN tokens t
      ON t.token_id_yes = ph.token_id OR t.token_id_no = ph.token_id
    JOIN markets m
      ON m.market_id = t.market_id
    ORDER BY m.market_id, ph.ts
    """

    if build_bets:
        df_bets = q(BETS_SQL, timeout_ms=15000)  # allow a bit more time
        cols = ["market_id", "question", "token_id", "timestamp", "price", "volume"]
        df_bets = df_bets.reindex(columns=cols)
        df_bets.to_csv(bets_outfile, index=False)
        st.success(f"Built {bets_outfile}")

    if Path(bets_outfile).exists():
        df_bets_prev = pd.read_csv(bets_outfile)
        st.caption(f"{bets_outfile} — {len(df_bets_prev):,} rows")
        st.dataframe(df_bets_prev.head(20), width='stretch')
        st.download_button(
            "Download bets_level_data.csv",
            data=df_to_csv_bytes(df_bets_prev),
            file_name="bets_level_data.csv",
            mime="text/csv",
        )
    else:
        st.info("bets_level_data.csv not found yet. Click the button above to build it.")
