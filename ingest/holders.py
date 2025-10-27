# ingest/holders.py
import json
from datetime import datetime, timezone
from typing import Optional
from sqlalchemy import text
from ingest.common import ENGINE, http_client, log_ingest

DATA_BASE = "https://data-api.polymarket.com"

def extract_condition_id(raw_gamma: dict | None) -> Optional[str]:
    if not raw_gamma: return None
    cond = raw_gamma.get("condition") or {}
    cid = cond.get("id") or raw_gamma.get("condition_id") or raw_gamma.get("conditionId")
    return cid

def fetch_holders(condition_id: str):
    url = f"{DATA_BASE}/holders"
    params = {"market": condition_id}
    with http_client() as client:
        r = client.get(url, params=params)
        r.raise_for_status()
        return url, params, r.json(), r.status_code

def main(limit_markets=200, top_n=25):
    written = 0
    with ENGINE.begin() as conn:
        rows = conn.execute(text("""
            SELECT m.market_id, m.raw_gamma, m.resolution_time_uma
            FROM markets m
            ORDER BY m.inserted_at DESC
            LIMIT :lim
        """), {"lim": limit_markets}).fetchall()

        for mid, raw, cutoff in rows:
            raw_gamma = raw if isinstance(raw, dict) else (json.loads(raw) if raw else None)
            cond = extract_condition_id(raw_gamma)
            if not cond:
                continue
            try:
                url, params, payload, status = fetch_holders(cond)
            except Exception:
                continue

            # Choose snapshot time:
            ts = datetime.now(timezone.utc)
            if cutoff and isinstance(cutoff, datetime) and ts >= cutoff:
                # we can't time-travel; store at cutoff-1sec for v0 bookkeeping
                ts = cutoff.astimezone(timezone.utc)
                ts = ts.replace(microsecond=0)

            # keep only top N for storage compactness
            top = (payload.get("holders") or payload)[:top_n] if isinstance(payload, dict) else payload
            conn.execute(text("""
                INSERT INTO holders_snapshot (market_id, ts, top_holders)
                VALUES (:m, :ts, CAST(:raw AS JSONB))
                ON CONFLICT (market_id, ts) DO NOTHING
            """), {"m": mid, "ts": ts, "raw": json.dumps(top)})

            log_ingest(conn, market_id=mid, endpoint="data/holders", url=url, params=params, status=status, payload=payload)
            written += 1

    print(f"✅ holders snapshots written: {written}")

if __name__ == "__main__":
    main()
