# ingest/gamma_markets.py
from typing import Any, Dict, List, Tuple
import json

from sqlalchemy import text

from ingest.common import ENGINE, http_client, to_utc, log_ingest

GAMMA_BASE = "https://gamma-api.polymarket.com"

def fetch_markets(limit=50, offset=0) -> Tuple[str, dict, dict, int]:
    url = f"{GAMMA_BASE}/markets"
    params = {"limit": limit, "offset": offset}
    with http_client() as client:
        r = client.get(url, params=params)
        status = r.status_code
        payload = r.json()
    return url, params, payload, status

def upsert_event(conn, event_id: str, title: str | None, category: str | None, tags: List[str] | None, created_at, updated_at):
    conn.execute(text("""
        INSERT INTO events (event_id, title, category, tags, created_at, updated_at)
        VALUES (:id, :title, :cat, :tags, :ca, :ua)
        ON CONFLICT (event_id) DO UPDATE
            SET title = COALESCE(:title, events.title),
                category = COALESCE(:cat, events.category),
                tags = COALESCE(:tags, events.tags),
                updated_at = COALESCE(:ua, events.updated_at)
    """), {"id": event_id, "title": title, "cat": category, "tags": tags or [], "ca": created_at, "ua": updated_at})

# --- return the market_id so the caller can pass it to upsert_tokens
def upsert_market(conn, m: Dict[str, Any]) -> str | None:
    market_id = m.get("id") or m.get("market_id") or m.get("slug")
    question  = m.get("question") or m.get("title")
    slug      = m.get("slug")
    series_id = m.get("series_id") or (m.get("series") or {}).get("id")
    category  = m.get("category")
    tags      = m.get("tags") or []
    end_iso   = m.get("end_date_iso") or m.get("endDate") or m.get("end_time") or m.get("endTime")
    end_time  = to_utc(end_iso)
    event_id  = m.get("event_id") or m.get("eventId") or (f"ev_{market_id}" if market_id else None)

    upsert_event(conn, event_id=event_id, title=question, category=category, tags=tags,
                 created_at=None, updated_at=None)

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
        "mid": market_id, "eid": event_id, "q": question, "slug": slug,
        "sid": series_id, "endt": end_time, "raw": json.dumps(m)
    })
    return market_id



def extract_token_ids(m: dict) -> tuple[str | None, str | None]:
    """
    Try multiple layouts to find YES/NO token ids.
    """
    yes = no = None

    # A) tokens as array: [{"token_id": "...", "outcome":"Yes"}, {"token_id":"...","outcome":"No"}]
    for path in ("tokens", "outcomeTokens"):
        arr = m.get(path)
        if isinstance(arr, list):
            for itm in arr:
                if not isinstance(itm, dict):
                    continue
                tok = itm.get("token_id") or itm.get("tokenId") or itm.get("id")
                out = (itm.get("outcome") or itm.get("name") or "").lower()
                if tok and "yes" in out:
                    yes = yes or tok
                elif tok and "no" in out:
                    no = no or tok

    # B) nested under condition.tokens.{yes,no}
    cond = m.get("condition") or {}
    toks = cond.get("tokens") or {}
    yes = yes or toks.get("yes") or (toks.get("YES") if isinstance(toks, dict) else None)
    no  = no  or toks.get("no")  or (toks.get("NO")  if isinstance(toks, dict) else None)

    # C) flat keys
    yes = yes or m.get("outcomeTokenYes") or (m.get("tokens") or {}).get("yes")
    no  = no  or m.get("outcomeTokenNo")  or (m.get("tokens") or {}).get("no")

    # D) last resort: single outcomeTokens dict {"yes":"...","no":"..."}
    if not (yes and no):
        ot = m.get("outcomeTokens")
        if isinstance(ot, dict):
            yes = yes or ot.get("yes")
            no  = no  or ot.get("no")

    return yes, no

def upsert_tokens(conn, market_id: str, m: Dict[str, Any]):
    yes, no = extract_token_ids(m)
    if not yes:
        return  # skip for now; we’ll fill later once we see schema shape
    conn.execute(text("""
        INSERT INTO tokens (token_id_yes, token_id_no, market_id)
        VALUES (:y, :n, :mid)
        ON CONFLICT (token_id_yes) DO UPDATE SET
            token_id_no = COALESCE(EXCLUDED.token_id_no, tokens.token_id_no),
            market_id = EXCLUDED.market_id
    """), {"y": yes, "n": no, "mid": market_id})

def main(limit=50, max_pages=2):
    total_inserted = 0
    with ENGINE.begin() as conn:
        for page in range(max_pages):
            offset = page * limit
            url, params, payload, status = fetch_markets(limit=limit, offset=offset)
            log_ingest(conn, market_id=None, endpoint="gamma/markets", url=url, params=params, status=status, payload=payload)

            markets = payload.get("data") if isinstance(payload, dict) else payload
            if markets is None:
                markets = payload.get("markets", [])
            if not isinstance(markets, list):
                print("Unexpected payload shape.")
                break

            for m in markets:
                mid = upsert_market(conn, m)
                if mid:   # <-- ✅ write tokens immediately if present in the same payload
                    upsert_tokens(conn, mid, m)
                total_inserted += 1

            if len(markets) < limit:
                break
    print(f"Done. Upserted ~{total_inserted} market rows (plus tokens where available).")

if __name__ == "__main__":
    main()
