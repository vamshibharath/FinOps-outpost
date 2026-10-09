# Walkthrough (about 7 minutes)

Have open: terminal at the repo root, `report/finops_report.html`, and an editor.
Run `make all` and `make restatement-demo` before the call so everything is warm.

The stakeholder screening asked about: allocation drivers, duplicate and late-arriving
records, how completeness was validated, how savings were measured, small files and schema
evolution, REST API ingestion, Scala and Dagster. Each step below answers one of those.

## 1. Frame it (30 sec)
"I built this around the problems in your job description: multi-cloud attribution, numbers
Finance can trust, and commitment modeling. It runs on the FinOps Foundation's FOCUS sample:
anonymized real billing from AWS, Azure, Oracle and a little GCP."

## 2. Contract gate (45 sec) — `make drift-demo`
"A renamed field and costs arriving as text like `$0.03`. Everything is read as text first, so
the gate counts the bad values instead of turning them into NULLs. The file is quarantined and
the lake is untouched."

## 3. Late-arriving and duplicate billing (75 sec) — `make restatement-demo`, report "Batch history"
"AWS re-delivers the whole month several times, then revises it after the invoice. Here AWS
re-sends September with corrected costs, 25 late lines and 10 duplicates. The loader drops the
duplicates, replaces only the AWS September partition, and dbt rebuilds only that month: the
fact table is incremental by provider and month with delete-and-insert. Azure and Oracle aren't
touched, and reconciliation still ties out for every month."

## 4. Allocation drivers (75 sec) — `shared_cost_rules.csv`, report "Shared-cost drivers"
"Tagged spend goes direct. Untagged compute is split by each team's CPU core-hours from a
telemetry API, networking by request volume, other categories by spend share, and credits by
total spend. If telemetry is missing for a month it falls back automatically, and every row
records which rule produced it."

**Say plainly:** "The telemetry API is a local mock with synthetic numbers. No public dataset pairs
bills with metrics. The client is real: pagination, retries on 429s and 5xx, fail-fast on 4xx,
raw pages kept for replay." Show the `[telemetry] retries=3` line.

## 5. Completeness and trust (45 sec) — report "Data completeness", "Reconciliation"
"Two questions Finance asks: is the month complete, and does it add up? Azure has 19 of 30
days, so it's flagged as a warning. Reconciliation runs overall and per provider and month."

## 6. Measuring savings (45 sec) — `make benchmark`, report "Measured storage and scan savings"
"The same Finance query on the raw gzip CSV versus partitioned Parquet. Bytes scanned, which is
what Athena bills, are read from Parquet metadata, not estimated, and the script checks all
three give the same answer first. 99.3% less scanned. The sample is small, so the ratio is
the result."

## 7. Scala and Dagster (45 sec) — `ingestion/spark-scala/`, `orchestration/dagster/definitions.py`
"The ingestion step also exists as a Spark job in Scala: same contract, dedupe with a window
function, dynamic partition overwrite so a restated month replaces only itself, and
`maxRecordsPerFile` against small files. It has ScalaTest tests, and on the sample it produces
exactly the same rows and totals as the Python loader. The pipeline also runs as Dagster
assets; reconciliation is a blocking check, so the report can't publish if it fails."

## 8. Close (15 sec)
"How do you handle shared and untagged costs today?"

---

## Be accurate about these

- **Scala:** say what's true. If your production Scala experience is limited, say so, and
  point to this job as what you've built and tested. Don't claim more than you can explain
  line by line; they said the team uses Scala heavily and will probe it.
- **Telemetry:** simulated. **Restatement:** generated from the real data. **Terraform:** validated,
  not deployed. **Scala job:** tested locally in Spark local mode, not on a cluster.
- Make one change yourself before the call (a new rule, a new test) so you can talk through
  editing it, not just running it.

## Likely follow-ups

**Why replace the month instead of merging line items?** The provider's file is the source of
truth for the whole month, and line IDs aren't guaranteed stable between deliveries. Replacing
is simpler and can't leave stale rows behind.

**What if a restatement arrives after Finance closed the month?** Reprocess and compare to the
published snapshot; book a material difference as an adjustment in the current month.

**Why median of daily on-demand at the 10th percentile for commitments?** Commit to the floor so
the commitment is always fully used; break-even utilization is 1 minus the discount.

**How do you avoid small files?** One task per partition and a max-records-per-file cap on
write; in production a compaction job for streaming or frequently appended paths.

**Schema evolution?** Additive columns are allowed (warning); renames and type changes block.
Read as text, validate, then cast — never let the reader infer types.

**Airflow vs Dagster?** Same pipeline in both here. Airflow orders tasks; Dagster models the
tables themselves, so lineage and data checks are built in.
