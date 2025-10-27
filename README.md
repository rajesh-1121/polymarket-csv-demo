# Polymarket CSV Demo

This public Streamlit app (CSV-only) shows:
- `features_pre_res.csv` — one row per market with leakage-aware microstructure features
- `bets_level_data.csv` — price/volume ticks per token (proxy for trade flow)

The CSVs are generated offline by the private ingestors and committed to the repo.
App entrypoint: `dashboard/app_public.py`
