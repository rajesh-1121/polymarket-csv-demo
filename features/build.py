# features/build.py
from __future__ import annotations

import argparse
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional, Tuple

import pandas as pd
from sqlalchemy import text

from ingest.common import ENGINE


@dataclass
class BuildConfig:
    limit_markets: int = 2000
    outfile: str = "features_pre_res.csv"
    # If a market has no resolution_time_uma, choose a cutoff:
    #   "now" -> use current UTC time,
    #   "last" -> use last ts from available price_history for that token,
    #   "skip" -> skip markets with no cutoff
    cutoff_fallback: str = "last"
    # require at least this many price points strictly before cutoff
    min_points: int = 3


def _ensure_tz_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _choose_cutoff(default_cutoff: Optional[datetime], df_prices: pd.DataFrame, cfg: BuildConfig) -> Optional[datetime]:
    """
    Decide an effective cutoff based on config and what data exists.
    - If default_cutoff is provided, use that.
    - Else respect cfg.cutoff_fallback.
    """
    if default_cutoff is not None:
        return _ensure_tz_utc(default_cutoff)

    if cfg.cutoff_fallback == "skip":
        return None
    if cfg.cutoff_fallback == "now":
        return datetime.now(timezone.utc)
    # "last"
    if not df_prices.empty:
        # take the latest price timestamp as the cutoff (still leakage-prone, but good for demo)
        return _ensure_tz_utc(df_prices["ts"].max().to_pydatetime())

    # if we got here and have no data, there's nothing meaningful to build
    return None


def compute_windows(df: pd.DataFrame, cutoff: datetime) -> dict:
    """
    df: rows with [ts, price_cents, volume], sorted ASC, ts tz-aware
    Returns dict of vol/volat/momentum.
    """
    out: dict = {}
    df = df.sort_values("ts")

    # convert cents -> 0..1
    df = df.copy()
    df["p"] = df["price_cents"] / 100.0
    df["ret"] = df["p"].pct_change()

    def window(seconds: int) -> pd.DataFrame:
        start = cutoff - timedelta(seconds=seconds)
        return df[(df["ts"] < cutoff) & (df["ts"] >= start)]

    for name, seconds in [("1h", 3600), ("24h", 86400), ("7d", 7 * 86400)]:
        win = window(seconds)
        out[f"vol_{name}"] = float(win.get("volume", pd.Series(dtype=float)).fillna(0).sum())
        # use population std (ddof=0) and default to 0.0 if not enough points
        out[f"volat_{'1d' if name=='24h' else name}"] = float(win["ret"].std(ddof=0)) if len(win) >= 2 else 0.0

    for name, seconds in [("1h", 3600), ("24h", 86400)]:
        win = window(seconds)
        if len(win) >= 2:
            out[f"momentum_{name}"] = float(win["p"].iloc[-1] - win["p"].iloc[0])
        else:
            out[f"momentum_{name}"] = 0.0

    return out


def last_micro(conn, token_id: str, cutoff: datetime) -> Tuple[Optional[float], Optional[float], Optional[float], Optional[float], Optional[float]]:
    row = conn.execute(
        text(
            """
        SELECT best_bid, best_ask, mid, spread, depth_k
        FROM microstructure
        WHERE token_id = :t AND ts < :cut
        ORDER BY ts DESC
        LIMIT 1
        """
        ),
        {"t": token_id, "cut": cutoff},
    ).fetchone()
    if not row:
        return None, None, None, None, None
    return row


def _load_prices(conn, token_id: str) -> pd.DataFrame:
    return pd.read_sql(
        text(
            """
        SELECT ts, price_cents, COALESCE(volume,0) AS volume
        FROM price_history
        WHERE token_id = :t
        ORDER BY ts ASC
        """
        ),
        conn,
        params={"t": token_id},
    )


def _filter_before_cutoff(df: pd.DataFrame, cutoff: datetime) -> pd.DataFrame:
    if df.empty:
        return df
    # ensure tz-aware
    if df["ts"].dtype == "datetime64[ns]":
        df = df.copy()
        df["ts"] = df["ts"].dt.tz_localize("UTC")
    return df[df["ts"] < cutoff]


def build_one(conn, market_id: str, question: str, yes_id: Optional[str], no_id: Optional[str],
              default_cutoff: Optional[datetime], cfg: BuildConfig):
    # try YES then NO
    df_yes = _load_prices(conn, yes_id) if yes_id else pd.DataFrame(columns=["ts","price_cents","volume"])
    df_no  = _load_prices(conn, no_id)  if no_id  else pd.DataFrame(columns=["ts","price_cents","volume"])

    # pick the richer token (or the one that exists)
    if len(df_yes) >= len(df_no):
        base_df, token_used = df_yes, yes_id
    else:
        base_df, token_used = df_no, no_id

    # if we truly have no prices, try to synthesize a minimal row from microstructure
    if base_df.empty:
        # choose a cutoff: prefer explicit, else NOW()
        cutoff = _choose_cutoff(default_cutoff, base_df, cfg) or datetime.now(timezone.utc)
        bb, ba, mid, spr, depth = (None, None, None, None, None)
        if token_used:
            bb, ba, mid, spr, depth = last_micro(conn, token_used, cutoff)
        if mid is None:
            # still nothing? skip — we have no signal at all
            return None
        return {
            "market_id": market_id,
            "question": question,
            "cutoff_ts": cutoff,
            "token_used": token_used,
            "last_mid": float(mid),
            "last_bid": bb,
            "last_ask": ba,
            "spread": spr,
            "depth_k": depth,
            "ob_imbalance": None,
            "vol_1h": 0.0, "vol_24h": 0.0, "vol_7d": 0.0,
            "volat_1d": 0.0, "volat_7d": 0.0,
            "momentum_1h": 0.0, "momentum_24h": 0.0,
        }

    # normal path: we have prices
    cutoff = _choose_cutoff(default_cutoff, base_df, cfg)
    if cutoff is None:
        return None
    df_cut = _filter_before_cutoff(base_df, cutoff)
    if len(df_cut) < cfg.min_points:
        # if we at least have micro, allow micro-only fallback
        bb, ba, mid, spr, depth = (None, None, None, None, None)
        if token_used:
            bb, ba, mid, spr, depth = last_micro(conn, token_used, cutoff)
        if mid is None:
            return None
        return {
            "market_id": market_id,
            "question": question,
            "cutoff_ts": cutoff,
            "token_used": token_used,
            "last_mid": float(mid),
            "last_bid": bb,
            "last_ask": ba,
            "spread": spr,
            "depth_k": depth,
            "ob_imbalance": None,
            "vol_1h": 0.0, "vol_24h": 0.0, "vol_7d": 0.0,
            "volat_1d": 0.0, "volat_7d": 0.0,
            "momentum_1h": 0.0, "momentum_24h": 0.0,
        }

    last_row = df_cut.iloc[-1]
    last_mid_price = float(last_row["price_cents"] / 100.0)

    bb, ba, mid, spr, depth = last_micro(conn, token_used, cutoff)
    if mid is None:
        mid, spr = last_mid_price, None
        bb = ba = depth = None

    win = compute_windows(df_cut, cutoff)

    return {
        "market_id": market_id,
        "question": question,
        "cutoff_ts": cutoff,
        "token_used": token_used,
        "last_mid": mid,
        "last_bid": bb,
        "last_ask": ba,
        "spread": spr,
        "depth_k": depth,
        "ob_imbalance": None,
        "vol_1h": win["vol_1h"],
        "vol_24h": win["vol_24h"],
        "vol_7d": win["vol_7d"],
        "volat_1d": win["volat_1d"],
        "volat_7d": win["volat_7d"],
        "momentum_1h": win["momentum_1h"],
        "momentum_24h": win["momentum_24h"],
    }



def main(cfg: Optional[BuildConfig] = None):
    if cfg is None:
        # CLI
        parser = argparse.ArgumentParser(description="Build leakage-aware features (with practical fallbacks).")
        parser.add_argument("--limit", type=int, default=2000, help="Max markets to process")
        parser.add_argument("--outfile", type=str, default="features_pre_res.csv", help="CSV output path")
        parser.add_argument("--cutoff-fallback", choices=["last", "now", "skip"], default="last",
                            help="If resolution_time_uma is NULL: use last data point time, 'now', or skip market")
        parser.add_argument("--min-points", type=int, default=3, help="Minimum price points before cutoff")
        args = parser.parse_args()
        cfg = BuildConfig(limit_markets=args.limit, outfile=args.outfile, cutoff_fallback=args.cutoff_fallback, min_points=args.min_points)

    rows_out: list[dict] = []

    with ENGINE.begin() as conn:
        # pull markets that have tokens joined; we won’t require resolution_time_uma anymore
        mk = conn.execute(
            text(
                """
            SELECT m.market_id, m.question, m.resolution_time_uma, m.resolved_outcome,
                   t.token_id_yes, t.token_id_no
            FROM markets m
            JOIN tokens t ON t.market_id = m.market_id
            ORDER BY COALESCE(m.resolution_time_uma, m.inserted_at) DESC
            LIMIT :lim
            """
            ),
            {"lim": cfg.limit_markets},
        ).fetchall()

        for market_id, question, cutoff, label, yes_id, no_id in mk:
            feat = build_one(conn, market_id, question, yes_id, no_id, cutoff, cfg)
            if not feat:
                continue
            feat["label_outcome"] = label
            rows_out.append(feat)

        # upsert into DB table (create the table beforehand as you already did)
        for r in rows_out:
            conn.execute(
                text(
                    """
                INSERT INTO features_pre_res(
                    market_id, cutoff_ts, last_mid, last_bid, last_ask, spread,
                    depth_k, ob_imbalance, vol_1h, vol_24h, vol_7d,
                    volat_1d, volat_7d, momentum_1h, momentum_24h, label_outcome
                )
                VALUES (:market_id, :cutoff_ts, :last_mid, :last_bid, :last_ask, :spread,
                        :depth_k, :ob_imbalance, :vol_1h, :vol_24h, :vol_7d,
                        :volat_1d, :volat_7d, :momentum_1h, :momentum_24h, :label_outcome)
                ON CONFLICT (market_id) DO UPDATE SET
                    cutoff_ts = EXCLUDED.cutoff_ts,
                    last_mid = EXCLUDED.last_mid,
                    last_bid = EXCLUDED.last_bid,
                    last_ask = EXCLUDED.last_ask,
                    spread = EXCLUDED.spread,
                    depth_k = EXCLUDED.depth_k,
                    ob_imbalance = EXCLUDED.ob_imbalance,
                    vol_1h = EXCLUDED.vol_1h,
                    vol_24h = EXCLUDED.vol_24h,
                    vol_7d = EXCLUDED.vol_7d,
                    volat_1d = EXCLUDED.volat_1d,
                    volat_7d = EXCLUDED.volat_7d,
                    momentum_1h = EXCLUDED.momentum_1h,
                    momentum_24h = EXCLUDED.momentum_24h,
                    label_outcome = EXCLUDED.label_outcome
                """
                ),
                r,
            )

    # dump CSV for the prof
    pd.DataFrame(rows_out).to_csv(cfg.outfile, index=False)
    print(f"✅ wrote {len(rows_out)} rows → {cfg.outfile}")


if __name__ == "__main__":
    main()
