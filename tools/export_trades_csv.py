# tools/export_trades_csv.py
import os
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
DB_URL = os.environ["DATABASE_URL"]
engine = create_engine(DB_URL, future=True)

OUTFILE = "bets_level_data.csv"

SQL = """
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

def main(outfile: str = OUTFILE):
  with engine.begin() as conn:
    df = pd.read_sql(text(SQL), conn)
  # ensure exact column order
  cols = ["market_id", "question", "token_id", "timestamp", "price", "volume"]
  df = df.reindex(columns=cols)
  df.to_csv(outfile, index=False)
  print(f"✅ wrote {len(df):,} rows → {outfile}")

if __name__ == "__main__":
  main()
