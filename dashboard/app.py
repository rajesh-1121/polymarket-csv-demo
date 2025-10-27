# dashboard/app.py
import streamlit as st
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import os

load_dotenv(".env")
engine = create_engine(os.environ["DATABASE_URL"])

st.set_page_config(page_title="Polymarket Dashboard", layout="wide")

st.title("🧠 Polymarket Data Viewer")

# markets table
st.subheader("Markets")
df = pd.read_sql("SELECT market_id, question, slug, end_time FROM markets ORDER BY end_time DESC LIMIT 50", engine)
st.dataframe(df, use_container_width=True)

# token table
st.subheader("Tokens")
df_tokens = pd.read_sql("SELECT * FROM tokens LIMIT 50", engine)
st.dataframe(df_tokens, use_container_width=True)

# inspect raw json
market_choice = st.selectbox("Inspect raw market JSON", df["market_id"] if not df.empty else [])
if market_choice:
    raw_json = pd.read_sql("SELECT raw_gamma FROM markets WHERE market_id = %s", engine, params=(market_choice,))
    if not raw_json.empty:
        st.json(raw_json["raw_gamma"].iloc[0])

# --- add below your existing code in dashboard/app.py

st.header("Market detail & price history")

# pick a market that has tokens/history
df_choice = pd.read_sql("""
    SELECT m.market_id, m.slug, m.question,
           t.token_id_yes, t.token_id_no,
           m.resolution_time_uma
    FROM markets m
    JOIN tokens t ON t.market_id = m.market_id
    ORDER BY m.inserted_at DESC
    LIMIT 200
""", engine)

if df_choice.empty:
    st.info("No markets with tokens yet. Run the token mapper & prices ingestion.")
else:
    mk = st.selectbox("Choose a market", df_choice["market_id"])
    row = df_choice[df_choice["market_id"]==mk].iloc[0]
    st.write(f"**{row['question']}**")
    st.write(f"slug: `{row['slug']}`  |  cutoff: `{row['resolution_time_uma']}`")
    tid = st.radio("Outcome to plot", ["YES","NO"])
    tok = row["token_id_yes"] if tid=="YES" else row["token_id_no"]

    # pull from DB
    q = """
      SELECT ts, price_cents
      FROM price_history
      WHERE token_id = %(tid)s
      ORDER BY ts ASC
    """
    dfp = pd.read_sql(q, engine, params={"tid": tok})

    if dfp.empty:
        st.warning("No price history for this token yet.")
    else:
        # matplotlib line chart (no seaborn, one chart, no custom colors)
        import matplotlib.pyplot as plt
        fig, ax = plt.subplots()
        ax.plot(dfp["ts"], dfp["price_cents"])
        ax.set_title(f"{tid} price_cents over time")
        ax.set_xlabel("timestamp")
        ax.set_ylabel("price (cents)")
        st.pyplot(fig)
