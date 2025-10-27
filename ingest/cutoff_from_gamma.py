# ingest/cutoff_from_gamma.py
import json
from datetime import datetime, timezone
from typing import Any, Dict, Optional
from sqlalchemy import text
from ingest.common import ENGINE

def to_utc(dt_str: Optional[str]) -> Optional[datetime]:
    if not dt_str:
        return None
    try:
        return datetime.fromisoformat(dt_str.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None

def extract_cutoff(raw: Dict[str, Any]) -> Optional[datetime]:
    """
    Best-effort cutoff from raw Gamma JSON. Tries several keys:
      - resolution.assertion_time / resolution_time / resolved_at
      - end_date_iso / closed_time / closedAt
    Returns a UTC datetime or None.
    """
    # resolution-like fields
    res = raw.get("resolution") or {}
    for k in ("assertion_time", "assertedAt", "resolution_time", "resolved_at", "resolvedAt"):
        d = to_utc(res.get(k) or raw.get(k))
        if d: return d

    # fallback to market closed time if resolution is not present
    for k in ("end_date_iso", "endDate", "end_time", "endTime", "closed_time", "closedAt"):
        d = to_utc(raw.get(k))
        if d: return d

    # last resort: if condition exists with closeTime
    cond = raw.get("condition") or {}
    for k in ("closeTime", "closedAt", "endTime"):
        d = to_utc(cond.get(k))
        if d: return d

    return None

def main(batch=1000):
    updated = 0
    with ENGINE.begin() as conn:
        rows = conn.execute(text("""
            SELECT market_id, raw_gamma
            FROM markets
            WHERE resolution_time_uma IS NULL
            ORDER BY inserted_at DESC
            LIMIT :n
        """), {"n": batch}).fetchall()

        for market_id, raw in rows:
            if not raw:
                continue
            data = raw if isinstance(raw, dict) else json.loads(raw)
            cutoff = extract_cutoff(data)
            if not cutoff:
                continue
            conn.execute(text("""
                UPDATE markets
                SET resolution_time_uma = :cutoff
                WHERE market_id = :mid AND (resolution_time_uma IS NULL OR resolution_time_uma > :cutoff)
            """), {"cutoff": cutoff, "mid": market_id})
            updated += 1

    print(f"✅ set resolution_time_uma for {updated} markets (best-effort from Gamma)")

if __name__ == "__main__":
    main()
