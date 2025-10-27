# ingest/token_mapper.py
from sqlalchemy import text
from ingest.common import ENGINE

def pick_yes_no(tokens_array):
    yes = no = None
    if not isinstance(tokens_array, list): return None, None
    for t in tokens_array:
        if not isinstance(t, dict): continue
        tok = t.get("token_id") or t.get("tokenId") or t.get("id")
        outcome = (t.get("outcome") or t.get("name") or "").strip().lower()
        if tok and "yes" in outcome and not yes:
            yes = tok
        if tok and "no" in outcome and not no:
            no = tok
    return yes, no

def main(batch=100000):
    mapped = missing = 0
    with ENGINE.begin() as conn:
        rows = conn.execute(text("""
            SELECT m.market_id, COALESCE(m.clob_slug, m.slug) AS slug_key, c.tokens
            FROM markets m
            JOIN clob_markets c ON c.slug = COALESCE(m.clob_slug, m.slug)
            ORDER BY m.inserted_at DESC
            LIMIT :n
        """), {"n": batch}).fetchall()

        for market_id, slug_key, tokens_json in rows:
            yes, no = pick_yes_no(tokens_json)
            if not yes:
                missing += 1
                continue
            conn.execute(text("""
                INSERT INTO tokens (token_id_yes, token_id_no, market_id)
                VALUES (:y, :n, :m)
                ON CONFLICT (token_id_yes) DO UPDATE SET
                  token_id_no = COALESCE(EXCLUDED.token_id_no, tokens.token_id_no),
                  market_id   = EXCLUDED.market_id
            """), {"y": yes, "n": no, "m": market_id})
            mapped += 1
    print(f"✅ tokens mapped: {mapped} | ⚠️ missing: {missing}")

if __name__ == "__main__":
    main()
