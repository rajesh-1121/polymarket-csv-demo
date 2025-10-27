.PHONY: ingest prices micro features holders monitor

ingest:
	python -m ingest.clob_seed_markets
	python -m ingest.clob_snapshot
	python -m ingest.token_mapper
	python -m ingest.cutoff_from_gamma
	python -m ingest.prices_history
	python -m ingest.micro
	python -m ingest.holders
	python -m features.build --cutoff-fallback last --min-points 1 --limit 2000 --outfile features_pre_res.csv
