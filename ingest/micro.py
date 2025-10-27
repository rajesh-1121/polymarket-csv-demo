# ingest/micro.py
from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import Optional, Tuple, Dict, Any, List

from sqlalchemy import text

from ingest.common import ENGINE, http_client, log_ingest

CLOB_BASE = "https://clob.polymarket.com"

# -------------- helpers

def _to_float(x) -> Optional[float]:
    """Convert int/float/str/Decimal-like to float safely."""
    if x is None:
        return None
    try:
        return float(x)
    except Exception:
        try:
            return float(str(x).strip())
        except Exception:
            return None


def _as_prob(v: Optional[float]) -> Optional[float]:
    """
    Normalize a price that may be in cents (>1.0) or prob (0..1).
    If v is None: returns None.
    """
    if v is None:
        return None
    # Heuristic: if > 1.0, treat as cents and convert to probability.
    return v / 100.0 if v > 1.0 else v


def fetch_orderbook(token_id: str, depth: int = 1) -> Tuple[str, Dict[str, Any], Dict[str, Any], int]:
    """
    Try several endpoint/param combos:
      /book?token_id=, /orderbook?token_id=, /book?market=, /orderbook?market=
    Returns first payload that has bids/asks arrays (even if empty).
    """
    candidates: List[Tuple[str, Dict[str, Any]]] = []
    for path in ("book", "orderbook"):
        for key in ("token_id", "tokenId", "market"):
            candidates.append((f"{CLOB_BASE}/{path}", {key: token_id, "depth": depth}))

    with http_client() as client:
        for url, params in candidates:
            try:
                r = client.get(url, params=params)
                status = r.status_code
                # If non-2xx, keep trying others
                if status >= 400:
                    continue
                try:
                    p = r.json()
                except Exception:
                    p = {}
                bids = p.get("bids") or p.get("bestBids") or []
                asks = p.get("asks") or p.get("bestAsks") or []
                # Accept lists (even empty); caller decides if usable
                if isinstance(bids, list) or isinstance(asks, list):
                    return url, params, {"bids": bids, "asks": asks}, status
            except Exception:
                continue

    # Fallback: empty book (status 200 for a graceful skip downstream)
    return f"{CLOB_BASE}/book", {"token_id": token_id, "depth": depth}, {"bids": [], "asks": []}, 200


def best_levels(ob_payload: Dict[str, Any]) -> Tuple[Optional[float], Optional[float], Optional[float]]:
    """
    Compute best bid/ask (as probabilities 0..1) and a simple depth_k from level-1 qty.
    Handles strings/ints/floats and missing fields.
    """
    bids = ob_payload.get("bids") or []
    asks = ob_payload.get("asks") or []

    def _price(row: Any) -> Optional[float]:
        v = None
        if isinstance(row, dict):
            v = row.get("p")
            if v is None:
                v = row.get("price")
        v = _to_float(v)
        return _as_prob(v)

    def _qty(row: Any) -> float:
        q = None
        if isinstance(row, dict):
            q = row.get("q")
            if q is None:
                q = row.get("quantity")
        q = _to_float(q)
        return q if q is not None else 0.0

    bid = _price(bids[0]) if bids else None
    ask = _price(asks[0]) if asks else None

    depth_k = 0.0
    if bids:
        depth_k += _qty(bids[0])
    if asks:
        depth_k += _qty(asks[0])

    return bid, ask, depth_k


def insert_snapshot(
    conn,
    token_id: str,
    ts: datetime,
    bid: Optional[float],
    ask: Optional[float],
    depth_k: Optional[float],
    raw: dict,
) -> None:
    """
    Insert a single microstructure snapshot row. Computes mid/spread if both sides exist.
    """
    mid = None
    spread = None
    if bid is not None and ask is not None:
        mid = 0.5 * (bid + ask)
        spread = ask - bid
    elif bid is not None or ask is not None:
        # if only one side, treat mid as that side (spread stays None)
        mid = bid if bid is not None else ask

    conn.execute(
        text(
            """
        INSERT INTO microstructure(token_id, ts, best_bid, best_ask, mid, spread, depth_k, raw)
        VALUES (:tid, :ts, :bb, :ba, :mid, :spr, :dk, CAST(:raw AS JSONB))
        ON CONFLICT (token_id, ts) DO NOTHING
        """
        ),
        {"tid": token_id, "ts": ts, "bb": bid, "ba": ask, "mid": mid, "spr": spread, "dk": depth_k, "raw": json.dumps(raw)},
    )


def _effective_snapshot_time(cutoff: Optional[datetime]) -> datetime:
    """
    If cutoff exists and is in the past, use cutoff - 1s (strictly before cutoff).
    Otherwise use now().
    """
    now = datetime.now(timezone.utc).replace(microsecond=0)
    if not cutoff:
        return now

    if cutoff.tzinfo is None:
        cutoff = cutoff.replace(tzinfo=timezone.utc)
    cutoff = cutoff.astimezone(timezone.utc).replace(microsecond=0)

    # if cutoff is in the future, just use now
    if cutoff > now:
        return now

    # strictly before cutoff
    return cutoff - timedelta(seconds=1)


# -------------- main

def main(limit_markets: int = 200, depth: int = 1) -> None:
    """
    For each token (YES/NO) in the most recent markets, fetch a 1-level order book and write ONE snapshot.
    Timestamp is set to:
      - cutoff - 1s  if resolution_time_uma exists and is in the past (keeps leakage rules), else
      - now()
    """
    written = 0
    attempted = 0

    with ENGINE.begin() as conn:
        rows = conn.execute(
            text(
                """
            SELECT t.token_id_yes, t.token_id_no, m.market_id, m.resolution_time_uma
            FROM tokens t
            JOIN markets m ON m.market_id = t.market_id
            ORDER BY m.inserted_at DESC
            LIMIT :lim
            """
            ),
            {"lim": limit_markets},
        ).fetchall()

        for token_yes, token_no, market_id, cutoff in rows:
            for tid in filter(None, (token_yes, token_no)):
                attempted += 1
                try:
                    url, params, payload, status = fetch_orderbook(tid, depth=depth)
                    bid, ask, depth_k = best_levels(payload)

                    # If we have literally no sides and no depth, skip (book is empty)
                    if bid is None and ask is None and (depth_k is None or depth_k == 0.0):
                        log_ingest(conn, market_id=market_id, endpoint="clob/book", url=url, params=params, status=status, payload=payload)
                        continue

                    snap_ts = _effective_snapshot_time(cutoff)
                    insert_snapshot(conn, tid, snap_ts, bid, ask, depth_k, payload)

                    log_ingest(conn, market_id=market_id, endpoint="clob/book", url=url, params=params, status=status, payload=payload)
                    written += 1
                except Exception as e:
                    # Log lightweight error entry in ingest_log and continue
                    try:
                        log_ingest(
                            conn,
                            market_id=market_id,
                            endpoint="clob/book",
                            url=url if "url" in locals() else f"{CLOB_BASE}/book",
                            params=params if "params" in locals() else {"token_id": tid, "depth": depth},
                            status=599,
                            payload={"error": str(e)},
                        )
                    except Exception:
                        pass
                    continue

    print(f"✅ microstructure snapshots written: {written} (attempted: {attempted})")


if __name__ == "__main__":
    # You can tweak defaults here for quick runs, e.g., depth=1, limit_markets=500
    main()
