"""The same pipeline as the Airflow DAG, expressed as Dagster software-defined assets.

Airflow thinks in tasks ("run step A, then B"). Dagster thinks in assets ("this table
should exist and be fresh"), so lineage, data checks and what-depends-on-what come for free.
Here the reconciliation check is *blocking*: if totals don't tie out, the report asset
downstream is not materialized.

    raw_billing ──> service_telemetry ─┐
         └─────────────────────────────┴─> dbt_warehouse ─> reconciliation ─[check: ties out]─>
                                                                storage_benchmark ─> finops_report

Run the UI:   dagster dev -f orchestration/dagster/definitions.py
In production the dbt step would use dagster-dbt to expose every dbt model as its own asset.
"""

import contextlib
import os
import subprocess
import sys
from datetime import date
from pathlib import Path

import dagster as dg
import duckdb

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
LANDING_FILE = os.environ.get("FINOPS_LANDING_FILE", "data/landing/focus_sample_100000.csv.gz")
WAREHOUSE = "data/warehouse/finops.duckdb"


@contextlib.contextmanager
def project_dir():
    """Pipeline code uses project-relative paths (same as `make`), so run from the repo root."""
    prev = os.getcwd()
    if str(ROOT) not in sys.path:  # step subprocesses re-import this module
        sys.path.insert(0, str(ROOT))
    os.chdir(ROOT)
    try:
        yield
    finally:
        os.chdir(prev)


@dg.asset(group_name="ingestion", kinds={"python", "parquet"},
          description="FOCUS billing export, contract-checked and landed as partitioned Parquet.")
def raw_billing(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with project_dir():
        from ingestion.load_raw import active_partitions, land
        try:
            out = land(LANDING_FILE)
        except SystemExit as exc:  # the gate exits non-zero on a contract violation
            raise dg.Failure(f"Billing batch failed its data contract (exit {exc.code}); see data/quarantine/")
        active = active_partitions()
    return dg.MaterializeResult(metadata={
        "batch_id": out["batch_id"],
        "status": out["status"],
        "active_partitions": len(active),
        "rows": dg.MetadataValue.int(sum(p["rows"] for p in active.values())),
    })


# The local mock API derives its services from the billing lake, so locally telemetry runs
# after billing lands. Against a real observability API this dependency would be dropped.
@dg.asset(deps=[raw_billing], group_name="ingestion", kinds={"python", "api"},
          description="Daily CPU core-hours and requests per service from the observability REST API.")
def service_telemetry() -> dg.MaterializeResult:
    with project_dir():
        from ingestion.telemetry.fetch_telemetry import run
        out = run(date(2024, 9, 1), date(2024, 9, 30))
    return dg.MaterializeResult(metadata={"pages": out["pages"], "rows": out["rows"], "retries": out["retries"]})


@dg.asset(deps=[raw_billing, service_telemetry], group_name="transform", kinds={"dbt", "duckdb"},
          description="dbt build: staging, tag resolution, allocation, marts and data tests.")
def dbt_warehouse(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    with project_dir():
        proc = subprocess.run(
            ["dbt", "--no-use-colors", "build", "--project-dir", "transform", "--profiles-dir", "transform"],
            capture_output=True, text=True)
    context.log.info(proc.stdout[-4000:])
    if proc.returncode != 0:
        raise dg.Failure("dbt build failed", metadata={"log_tail": dg.MetadataValue.text(proc.stdout[-4000:])})
    summary = next((l for l in proc.stdout.splitlines() if "Done. PASS=" in l), "")
    return dg.MaterializeResult(metadata={"dbt_summary": summary.strip()})


@dg.asset(deps=[dbt_warehouse], group_name="quality", kinds={"python"},
          description="Source = warehouse = allocated, overall and per provider/month; writes an audit row.")
def reconciliation() -> dg.MaterializeResult:
    with project_dir():
        from reconciliation.reconcile import reconcile
        out = reconcile()
    failed = [c["check"] for c in out["checks"] if not c["passed"]]
    return dg.MaterializeResult(metadata={
        "run_id": out["run_id"],
        "checks": len(out["checks"]),
        "failed_checks": dg.MetadataValue.json(failed),
        "unallocated_effective_cost": out["unallocated_effective_cost"],
    })


@dg.asset_check(asset=reconciliation, blocking=True,
                description="Latest reconciliation run must pass every check before anything is published.")
def reconciliation_ties_out() -> dg.AssetCheckResult:
    with project_dir():
        con = duckdb.connect(WAREHOUSE, read_only=True)
        passed, n = con.execute("""
            SELECT bool_and(passed), count(*) FROM audit.reconciliation_runs
            WHERE run_id = (SELECT run_id FROM audit.reconciliation_runs ORDER BY run_at DESC LIMIT 1)""").fetchone()
        con.close()
    return dg.AssetCheckResult(passed=bool(passed), metadata={"checks_in_run": n})


@dg.asset_check(asset=dbt_warehouse, description="Warn when a billing month has days with no usage.")
def billing_months_complete() -> dg.AssetCheckResult:
    with project_dir():
        con = duckdb.connect(WAREHOUSE, read_only=True)
        rows = con.execute("""SELECT provider || '/' || billing_period, missing_days
                              FROM marts.mart_data_completeness WHERE missing_days > 0""").fetchall()
        con.close()
    return dg.AssetCheckResult(passed=not rows, severity=dg.AssetCheckSeverity.WARN,
                               metadata={"incomplete_months": dg.MetadataValue.json(dict(rows))})


@dg.asset(deps=[reconciliation], group_name="reporting", kinds={"python"},
          description="Bytes scanned and latency: gzip CSV vs partitioned Parquet.")
def storage_benchmark() -> dg.MaterializeResult:
    with project_dir():
        from scripts.benchmark_storage import run
        out = run(Path(LANDING_FILE))
    best = out["scenarios"]["parquet_column_and_partition_pruning"]
    return dg.MaterializeResult(metadata={"scan_reduction_vs_csv": best["scan_reduction_vs_csv"],
                                          "results_match": out["results_match"]})


@dg.asset(deps=[storage_benchmark], group_name="reporting", kinds={"html"},
          description="Finance-facing HTML report.")
def finops_report() -> dg.MaterializeResult:
    with project_dir():
        from report.build_report import main
        main()
    return dg.MaterializeResult(metadata={"path": str(ROOT / "report" / "finops_report.html")})


# In-process: the steps share one local DuckDB file, and the pipeline is sequential anyway.
daily_job = dg.define_asset_job("finops_daily", selection=dg.AssetSelection.all(),
                                executor_def=dg.in_process_executor)

defs = dg.Definitions(
    assets=[raw_billing, service_telemetry, dbt_warehouse, reconciliation, storage_benchmark, finops_report],
    asset_checks=[reconciliation_ties_out, billing_months_complete],
    jobs=[daily_job],
    schedules=[dg.ScheduleDefinition(job=daily_job, cron_schedule="0 9 * * *", execution_timezone="UTC")],
)
