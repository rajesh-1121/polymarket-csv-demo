import os, hashlib, json, time
from datetime import datetime, timezone
from urllib.parse import urlencode

import httpx
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()
engine = create_engine(os.environ["DATABASE_URL"], future=True)

# tiny helpers
def log_ingest(conn, market_id, endpoint, url, params, status, payload):
    sha = hashlib.sha256(json.dumps(payload, separators=(',',':')).encode()).hexdigest()
    conn.execute(
        text("""INSERT INTO ingest_log (market_id, endpoint, url, params, status, sha256)
                VALUES (:m,:e,:u,:p,:s,:h)"""),
        {"m": market_id, "e": endpoint, "u": url, "p": json.dumps(params), "s": status, "h": sha}
    )

def now_utc():
    return datetime.now(timezone.utc)

# v0: insert a dummy event/market so you can see data flow working
with engine.begin() as conn:
    # seed one fake event & market (for plumbing test)
    conn.execute(text("""
        INSERT INTO events(event_id, title, category, tags, created_at, updated_at)
        VALUES ('evt_demo', 'DEMO EVENT', 'demo', ARRAY['demo'], now(), now())
        ON CONFLICT (event_id) DO NOTHING
    """))
    conn.execute(text("""
    INSERT INTO markets (
        market_id, event_id, question, slug, series_id, end_time,
        resolution_time_uma, resolution_source_url, resolved_outcome, raw_gamma
    )
    VALUES (
        :mid, :eid, :q, :slug, :sid, :endt,
        NULL, NULL, NULL, CAST(:raw AS JSONB)
    )
    ON CONFLICT (market_id) DO UPDATE SET
        event_id   = EXCLUDED.event_id,
        question   = EXCLUDED.question,
        slug       = EXCLUDED.slug,
        series_id  = EXCLUDED.series_id,
        end_time   = COALESCE(EXCLUDED.end_time, markets.end_time),
        raw_gamma  = EXCLUDED.raw_gamma
"""), {
    "mid": market_id,
    "eid": event_id,
    "q": question,
    "slug": slug,
    "sid": series_id,
    "endt": end_time,
    "raw": json.dumps(m)
})


    # pretend we fetched a price point
    conn.execute(text("""
        INSERT INTO price_history(token_id, ts, price_cents, volume, raw)
        VALUES ('token_yes_demo', now(), 55, 100, '{"src":"demo"}'::jsonb)
        ON CONFLICT DO NOTHING
    """))
    # log the ingest
    log_ingest(conn, "mkt_demo", "demo_endpoint", "https://example.com", {"q": "demo"}, 200, {"ok": True})

print("Plumbing test: wrote demo rows. Check pgAdmin tables (events, markets, price_history, ingest_log).")
