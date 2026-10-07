.PHONY: setup data ingest transform reconcile report pipeline drift-demo api test all clean

PY ?= python3
DBT = dbt --no-use-colors
DBT_ARGS = --project-dir transform --profiles-dir transform
DATA_FILE = data/landing/focus_sample_100000.csv.gz

setup:            ## install dependencies
	$(PY) -m pip install -r requirements.txt

data:             ## download the FOCUS sample billing dataset
	$(PY) scripts/download_data.py --size full

ingest:           ## contract gate + idempotent landing to partitioned Parquet
	$(PY) ingestion/load_raw.py $(DATA_FILE)

transform:        ## dbt seeds, models and data tests
	$(DBT) build $(DBT_ARGS)

reconcile:        ## three-way reconciliation with audit trail
	$(PY) reconciliation/reconcile.py

report:           ## Finance-facing HTML report
	$(PY) report/build_report.py

pipeline: ingest transform reconcile report

drift-demo:       ## reproduce a schema-change incident and watch the gate block it
	$(PY) scripts/simulate_schema_drift.py $(DATA_FILE)

api:              ## serve the chargeback REST API on :8000
	uvicorn api.main:app --reload

test:
	$(PY) -m pytest -q

all: data pipeline test

clean:
	rm -rf data/lake/raw_focus data/lake/_manifest.jsonl data/quarantine/* data/warehouse/*.duckdb* transform/target report/finops_report.html
