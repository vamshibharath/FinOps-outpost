.PHONY: setup data ingest telemetry transform full-refresh reconcile benchmark report pipeline \
        drift-demo restatement-demo api scala-test scala-run dagster test all clean

PY ?= python3
DBT = dbt --no-use-colors
DBT_ARGS = --project-dir transform --profiles-dir transform
DATA_FILE = data/landing/focus_sample_100000.csv.gz
SCALA_DIR = ingestion/spark-scala

setup:            ## install Python dependencies
	$(PY) -m pip install -r requirements.txt

data:             ## download the FOCUS sample billing dataset
	$(PY) scripts/download_data.py --size full

ingest:           ## contract gate + dedupe + restatement-safe landing to partitioned Parquet
	$(PY) ingestion/load_raw.py $(DATA_FILE)

telemetry:        ## pull service telemetry from the (mock) observability REST API
	$(PY) ingestion/telemetry/fetch_telemetry.py --start 2024-09-01 --end 2024-09-30

transform:        ## dbt seeds, models and data tests (fact table is incremental by provider/month)
	$(DBT) build $(DBT_ARGS)

full-refresh:     ## rebuild every month from scratch
	$(DBT) build $(DBT_ARGS) --full-refresh

reconcile:        ## three-way reconciliation, overall and per provider/month, with audit trail
	$(PY) reconciliation/reconcile.py

benchmark:        ## measure storage + scan savings: gzip CSV vs partitioned Parquet
	$(PY) scripts/benchmark_storage.py $(DATA_FILE)

report:           ## Finance-facing HTML report
	$(PY) report/build_report.py

pipeline: ingest telemetry transform reconcile benchmark report

drift-demo:       ## replay a schema-change incident; the gate must block it
	$(PY) scripts/simulate_schema_drift.py $(DATA_FILE)

restatement-demo: ## AWS re-sends September with corrections, late lines and duplicates
	$(PY) scripts/simulate_restatement.py $(DATA_FILE)
	$(DBT) build $(DBT_ARGS)
	$(PY) reconciliation/reconcile.py
	$(PY) report/build_report.py

api:              ## serve the chargeback REST API on :8000
	uvicorn api.main:app --reload

scala-test:       ## compile the Spark/Scala ingestion job and run its ScalaTest suite
	cd $(SCALA_DIR) && mvn -B -q test

scala-run:        ## run the Scala Spark job locally on the sample file
	./scripts/run_scala_ingest.sh $(DATA_FILE)

dagster:          ## open the Dagster UI with the same pipeline as software-defined assets
	dagster dev -f orchestration/dagster/definitions.py

test:
	$(PY) -m pytest -q

all: data pipeline test

clean:
	rm -rf data/lake/raw_focus data/lake/telemetry data/lake/raw_telemetry data/lake/_manifest.jsonl \
	       data/lake/scala_focus data/quarantine/* data/warehouse/*.duckdb* data/warehouse/*.json \
	       data/landing/focus_aws_2024-09_restated.csv transform/target report/finops_report.html
