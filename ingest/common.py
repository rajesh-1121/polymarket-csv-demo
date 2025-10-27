# ingest/common.py
import os
import json
import hashlib
from datetime import datetime, timezone
from typing import Any, Dict

import httpx
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv(".env")
ENGINE = create_engine(os.environ["DATABASE_URL"], future=True)

def http_client():
    # simple client with retries/backoff for 429/5xx
    transport = httpx.HTTPTransport(retries=3)
    return httpx.Client(timeout=30, transport=transport)

def to_utc(dt_str: str | None):
    if not dt_str:
        return None
    # handle common ISO forms; fall back safely
    try:
        # fromisoformat handles "YYYY-MM-DDTHH:MM:SS[.fff][+/-HH:MM]"
        dt = datetime.fromisoformat(dt_str.replace("Z", "+00:00"))
        return dt.astimezone(timezone.utc)
    except Exception:
        return None

def log_ingest(conn, market_id: str | None, endpoint: str, url: str, params: Dict[str, Any], status: int, payload: Any):
    sha = hashlib.sha256(json.dumps(payload, separators=(",", ":")).encode()).hexdigest()
    conn.execute(
        text("""INSERT INTO ingest_log (market_id, endpoint, url, params, status, sha256)
                VALUES (:m,:e,:u,:p,:s,:h)"""),
        {"m": market_id, "e": endpoint, "u": url, "p": json.dumps(params), "s": status, "h": sha},
    )
