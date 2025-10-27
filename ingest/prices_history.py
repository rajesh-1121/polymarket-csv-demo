# ingest/prices_history.py
import json
from datetime import timezone
from typing import Optional
from sqlalchemy import text
from ingest.common import ENGINE, http_client, log_ingest
from datetime import datetime

CLOB_BASE = "https://clob.polymarket.com"

# ingest/prices_history.py (replace get_prices_history)
# --- replace your get_prices_history() and ingest_for_token() with this ---

# --- in ingest/prices_history.py ---

def get_prices_history(token_id: str, end_ts: Optional[int] = None, start_ts: Optional[int] = None):
    url = f"{CLOB_BASE}/prices-history"
    params = {"market": token_id}
    if start_ts is not None: params["startTs"] = int(start_ts)
    if end_ts is not None:   params["endTs"]   = int(end_ts)

    # IMPORTANT: default to full history if no bounds are provided
    if "startTs" not in params and "endTs" not in params:
        params["interval"] = "max"

    with http_client() as client:
        r = client.get(url, params=params)
        status = r.status_code
        try:
            r.raise_for_status()
            payload = r.json()
        except Exception:
            try: payload = r.json()
            except Exception: payload = {"error": f"HTTP {status}"}
    return url, params, payload, status




def ingest_for_token(conn, token_id: str, cutoff: Optional[datetime]):
    """
    Pull history for a single token. If cutoff provided, use endTs=cutoff-1 (strict).
    If API returns 400/404 or empty history, log and skip gracefully.
    """
    end_ts = None
    if cutoff:
        end_ts = int(cutoff.timestamp()) - 1  # strictly before cutoff

    url, params, payload, status = get_prices_history(token_id, end_ts=end_ts)

    hist = payload.get("history") or payload.get("data") or []
    # log before any early return
    log_ingest(conn, market_id=None, endpoint="clob/prices-history", url=url, params=params, status=status, payload=payload)

    if status >= 400:
        # bad request or not found — skip token, but don't crash the whole batch
        return 0

    inserted = 0
    for row in hist:
        # expected: {"t": 1690000000, "p": 55, "v": 123} (p is cents or dollars*100)
        try:
            ts = datetime.fromtimestamp(int(row["t"]), tz=timezone.utc)
            price = row.get("p")
            vol = row.get("v")
        except Exception:
            continue

        conn.execute(text("""
            INSERT INTO price_history(token_id, ts, price_cents, volume, raw)
            VALUES (:tid, :ts, :p, :v, CAST(:raw AS JSONB))
            ON CONFLICT (token_id, ts) DO NOTHING
        """), {"tid": token_id, "ts": ts, "p": price, "v": vol, "raw": json.dumps(row)})
        inserted += 1

    return inserted


def main(limit_markets=50):
    total = 0
    with ENGINE.begin() as conn:
        # join tokens + markets to get cutoff
        rows = conn.execute(text("""
            SELECT t.token_id_yes, t.token_id_no, m.market_id, m.resolution_time_uma
            FROM tokens t
            JOIN markets m ON m.market_id = t.market_id
            ORDER BY m.inserted_at DESC
            LIMIT :lim
        """), {"lim": limit_markets}).fetchall()

        for yes, no, mid, cutoff in rows:
            if not yes:
                continue
            total += ingest_for_token(conn, yes, cutoff)
            if no:
                total += ingest_for_token(conn, no, cutoff)

    print(f"✅ inserted ~{total} price_history rows (pre-cutoff where available)")

if __name__ == "__main__":
    main()
