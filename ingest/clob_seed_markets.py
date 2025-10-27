# ingest/clob_seed_markets.py
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

def main(limit=None):
    rows = fetch_all()
    if limit:
        rows = rows[:limit]
    up = 0
    with ENGINE.begin() as conn:
        for m in rows:
            slug = m.get("slug") or m.get("market_slug")
            cond = m.get("condition_id") or m.get("conditionId")
            q    = m.get("question") or m.get("title")
            # choose a stable market_id; prefer CLOB's id if present, else slug
            mid  = m.get("id") or m.get("market_id") or slug
            if not mid:
                continue

            conn.execute(text("""
    INSERT INTO markets (
        market_id, event_id, question, slug, clob_slug, clob_condition_id,
        end_time, resolution_time_uma, resolution_source_url,
        resolved_outcome, raw_gamma
    )
    VALUES (
        :mid, NULL, :q, :slug, :cslug, :cond,
        NULL, NULL, NULL,
        NULL, CAST(:raw AS JSONB)
    )
    ON CONFLICT (market_id) DO UPDATE SET
        question          = COALESCE(EXCLUDED.question, markets.question),
        slug              = COALESCE(EXCLUDED.slug, markets.slug),
        clob_slug         = COALESCE(EXCLUDED.clob_slug, markets.clob_slug),
        clob_condition_id = COALESCE(EXCLUDED.clob_condition_id, markets.clob_condition_id),
        raw_gamma         = EXCLUDED.raw_gamma
"""), {
    "mid": mid,
    "q": q,
    "slug": slug,          # <-- use param here, not "COALESCE(:slug, slug)"
    "cslug": slug,
    "cond": cond,
    "raw": json.dumps(m)
})

            log_ingest(conn, market_id=mid, endpoint="clob/markets",
                       url=BASE, params={}, status=200, payload=m)
            up += 1
    print(f"✅ markets upserted/updated from CLOB: {up}")

if __name__ == "__main__":
    main()
