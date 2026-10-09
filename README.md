# FinOps Cost Attribution Platform

## Client Submission

This submission demonstrates a controlled process for turning multi-cloud billing exports into trusted chargeback, cost-governance, and commitment-planning outputs.

The platform is designed around five practical controls:

1. Validate incoming billing files before they enter the data lake.
2. Handle duplicates, late-arriving lines, and provider restatements safely.
3. Allocate shared cost using documented ownership and usage rules.
4. Reconcile source, warehouse, and allocated totals before publication.
5. Make the results available through a finance report and read-only API.

## Demonstration Result

The included demonstration processed 100,000 billing line items across AWS, Microsoft Azure, Oracle Cloud, and a small Google Cloud sample.

| Measure | Demonstration result |
|---|---:|
| Billing line items | 100,000 |
| Billed cost | $1,426.922368 |
| Effective cost | $914.599955 |
| Reconciliation | Passed overall and by provider/month |
| Unallocated effective cost | $0.00 |
| Automated tests | 26 passed |
| Storage scan reduction | 99.3% with partition and column pruning |

The amounts are intentionally scaled sample values. The controls, processing flow, and allocation behaviour are the focus of this submission.

## Run the Demonstration

Requirements: Python 3.11 or newer. Java 17 and Maven are only required for the optional Spark demonstration.

```bash
make setup
make all
```

The command runs the complete local demonstration:

```text
download billing sample
    -> validate and ingest billing files
    -> load demonstration usage telemetry
    -> build dbt warehouse models
    -> reconcile financial totals
    -> benchmark the storage layout
    -> build the client report
    -> run automated tests
```

Open the finished report at [report/finops_report.html](report/finops_report.html).

Start the read-only API with:

```bash
make api
```

Then open:

- http://127.0.0.1:8000/health
- http://127.0.0.1:8000/v1/chargeback?billing_period=2024-09
- http://127.0.0.1:8000/v1/tag-coverage
- http://127.0.0.1:8000/v1/commitments

## Data Provenance

The billing input is the public FOCUS 1.0 sample published by the FinOps Foundation:

https://github.com/FinOps-Open-Cost-and-Usage-Spec/FOCUS-Sample-Data

The download command saves the full sample to `data/landing/focus_sample_100000.csv.gz`. It is anonymized and does not contain data from a client cloud account.

The billing data is public sample data. The usage telemetry used for the local demonstration is deterministic demonstration data because no public dataset pairs these billing rows with matching service metrics. A production deployment would connect the same telemetry client to the client observability endpoint.

## Client Walkthrough

See [WALKTHROUGH.md](WALKTHROUGH.md) for the end-to-end demonstration script, expected results, control explanations, and optional scenarios.

## Demonstration Scenarios

```bash
make drift-demo        # reject a file with a renamed field and invalid cost values
make restatement-demo  # replace one provider/month and verify the totals still reconcile
make scala-test        # optional Spark/Scala ingestion tests
make scala-run         # optional Spark/Scala cross-check against the Python loader
make dagster           # optional orchestration UI on port 3000
```

## Delivery Scope

| Area | Local demonstration | Production direction |
|---|---|---|
| Ingestion | Python, DuckDB, Parquet | Spark/Glue and cloud object storage |
| Warehouse | DuckDB with dbt | Snowflake or equivalent warehouse |
| Reference data | dbt seeds | Governed reference tables |
| Orchestration | Make, Airflow, or Dagster definitions | Scheduled production orchestration |
| Consumer outputs | HTML report and REST API | Finance portal, BI, and downstream services |
| Infrastructure | Terraform validation assets | Cloud deployment after environment review |

## Limitations and Assumptions

- The billing sample is anonymized, scaled, and row-sampled.
- Demonstration telemetry is deterministic and local; production telemetry requires an endpoint adapter and credentials supplied through the runtime environment.
- The restatement scenario is generated from the public sample to demonstrate the control; it is not a provider invoice correction.
- Terraform and the production warehouse profile are included for deployment planning but are not deployed by this local submission.
