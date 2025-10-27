# ingest/clob_snapshot.py
import json
from sqlalchemy import text
from ingest.common import ENGINE, http_client, log_ingest

BASE = "https://clob.polymarket.com/markets"

def fetch_all():
    out = []
    params = {}
    with http_client() as client:
        while True:
            r = client.get(BASE, params=params)
            r.raise_for_status()
            p = r.json()
            arr = p.get("markets") or p.get("data") or []
            out.extend(arr)
            nc = p.get("next_cursor")
            if not nc or nc == "LTE=":
                break
            params = {"next_cursor": nc}
    return out

def main():
    rows = fetch_all()
    inserted = 0
    with ENGINE.begin() as conn:
        for m in rows:
            slug = m.get("slug") or m.get("market_slug")
            cond = m.get("condition_id") or m.get("conditionId")
            toks = m.get("tokens") or []
            if not slug:
                continue
            conn.execute(text("""
                INSERT INTO clob_markets (slug, condition_id, tokens, raw)
                VALUES (:slug, :cond, CAST(:tokens AS JSONB), CAST(:raw AS JSONB))
                ON CONFLICT (slug) DO UPDATE SET
                  condition_id = COALESCE(EXCLUDED.condition_id, clob_markets.condition_id),
                  tokens = EXCLUDED.tokens,
                  raw = EXCLUDED.raw,
                  inserted_at = now()
            """), {
                "slug": slug,
                "cond": cond,
                "tokens": json.dumps(toks),
                "raw": json.dumps(m),
            })
            inserted += 1
            log_ingest(conn, market_id=None, endpoint="clob/markets", url=BASE,
                       params={}, status=200, payload=m)
    print(f"✅ clob_markets upserted: {inserted}")

if __name__ == "__main__":
    main()
