# Client Demonstration Walkthrough

This guide is a short, end-to-end demonstration of the FinOps Cost Attribution Platform. It is written for a review session: run each command, show the result, and explain the control it demonstrates.

## 1. Prepare the demonstration

```bash
python3 -m venv .venv
source .venv/bin/activate
make setup
```

Use Python 3.11 or newer. On this workstation, Python 3.13 is available and is used for the completed demonstration.

## 2. Establish data provenance

```bash
make data
```

The command downloads `focus_sample_100000.csv.gz` from the FinOps Foundation's public FOCUS 1.0 sample repository and saves it under `data/landing/`.

The file contains 100,000 anonymized billing line items from AWS, Microsoft Azure, Oracle Cloud, and a small Google Cloud sample. It is not a client account export.

For this local demonstration, service usage is supplied by deterministic demonstration telemetry. This is kept separate from the billing source so the allocation rules can be exercised without client credentials.

## 3. Run the complete flow

```bash
make pipeline
```

The pipeline performs these steps:

1. **Ingest:** validate the billing contract, remove duplicate lines, and write partitioned Parquet data.
2. **Telemetry:** retrieve daily service usage with pagination and retry handling.
3. **Transform:** build the dbt staging, allocation, chargeback, completeness, and commitment models.
4. **Reconcile:** compare source totals, warehouse totals, and allocated totals.
5. **Benchmark:** compare compressed CSV scans with Parquet column and partition pruning.
6. **Report:** publish the finance-facing HTML report.

Expected headline results from the submitted run:

```text
100,000 billing rows loaded
25 telemetry pages and 240,750 usage rows processed
dbt: 26 passes, 1 completeness warning, 0 errors
reconciliation: passed
unallocated effective cost: $0.00
pytest: 26 passed
```

Open [report/finops_report.html](report/finops_report.html) after the run.

## 4. Show the data-quality gate

```bash
make drift-demo
```

This creates a deliberately invalid copy of the billing file. The contract gate detects a renamed required field and invalid cost values, then moves the file to `data/quarantine/` instead of loading it.

Message to highlight during the demo: a bad file is stopped before it can change financial results.

## 5. Show restatement handling

```bash
make restatement-demo
```

This creates a provider re-delivery containing corrected values, late lines, and duplicates. The loader:

- removes duplicate line items;
- replaces only the affected provider/month partition;
- keeps other provider/month partitions unchanged; and
- reruns the affected warehouse models.

Reconciliation runs again after the replacement. This demonstrates why a revised provider file does not double-count the month.

## 6. Explain shared-cost allocation

The platform first uses governed tag aliases to identify the owning business unit. Untagged costs are then allocated according to the category rule:

| Cost category | Allocation basis |
|---|---|
| Compute | CPU core-hours |
| Networking | Request volume |
| Storage and databases | Share of tagged category spend |
| Credits, tax, and adjustments | Share of total provider spend |

Every allocated row records its allocation method. If the preferred usage driver is unavailable, the model falls back to the documented spend rule and ultimately to `UNALLOCATED`.

## 7. Show the API output

Start the API:

```bash
make api
```

Open these endpoints:

```text
http://127.0.0.1:8000/health
http://127.0.0.1:8000/v1/chargeback?billing_period=2024-09
http://127.0.0.1:8000/v1/tag-coverage
http://127.0.0.1:8000/v1/commitments
```

The API is read-only and reads from the local DuckDB warehouse. The health response confirms whether the latest reconciliation passed.

## 8. Optional engineering demonstrations

```bash
make scala-test
make scala-run
make dagster
```

The Scala commands require Java 17 and Maven. The Dagster command opens the orchestration view on port 3000.

## 9. What to discuss in the review

- The public billing source is clearly identified and separated from demonstration telemetry.
- Contract failures are quarantined before they affect the warehouse.
- Restatements replace partitions rather than appending duplicate months.
- Allocation methods are visible and traceable.
- Reconciliation prevents publication when totals do not tie out.
- The HTML report and API provide practical consumer outputs.

## 10. Production handoff

The local demonstration uses DuckDB and files on disk. The included production direction uses cloud object storage, Spark/Glue ingestion, a managed warehouse, scheduled orchestration, and client-provided observability credentials. Deployment requires an environment review, credential configuration, and validation against the client's billing and tagging policies.
