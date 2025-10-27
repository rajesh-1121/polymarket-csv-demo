# tools/probe_prices.py
# Usage: python tools/probe_prices.py <token_id>
import sys, json, httpx

BASES = [
    "https://clob.polymarket.com/prices-history",
    "https://clob.polymarket.com/prices/history",
    "https://clob.polymarket.com/price-history",
]
PARAM_KEYS = ["token_id", "tokenId", "market", "token"]
QUERIES = [
    lambda tid: { "interval": "max" },
    lambda tid: { "startTs": 0, "endTs": 4102444800 },  # until 2100
]

def main():
    if len(sys.argv) < 2:
        print("pass a token_id")
        return
    tid = sys.argv[1]
    headers = {"User-Agent": "polymarket-pipeline/0.1"}

    with httpx.Client(timeout=30, headers=headers) as client:
        for base in BASES:
            for key in PARAM_KEYS:
                for qfn in QUERIES:
                    params = qfn(tid)
                    params[key] = tid
                    try:
                        r = client.get(base, params=params)
                        print(f"TRY {base} params={params} -> {r.status_code}")
                        js = r.json()
                        hist = js.get("history") or js.get("data") or js.get("candles") or []
                        print("   items:", len(hist), "keys:", list(js.keys())[:5])
                        if hist:
                            # print first row shape so we know field names
                            print("   sample:", hist[0])
                            return
                    except Exception as e:
                        print("   err:", e)

if __name__ == "__main__":
    main()
