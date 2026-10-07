"""Daily FinOps attribution pipeline.

contract_gate -> land_raw -> dbt_build (models + tests) -> reconcile -> publish
Any failure stops downstream publishing and raises a Datadog event, so Finance never
sees numbers that didn't tie out.
"""
from __future__ import annotations

import json
import os
import urllib.request
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

PROJECT = os.environ.get("FINOPS_PROJECT_DIR", "/opt/finops-attribution-platform")
LANDING_FILE = os.environ.get("FINOPS_LANDING_FILE", "data/landing/focus_sample_100000.csv.gz")


def datadog_alert(context) -> None:
    """Post a Datadog event on task failure (no-op when DD_API_KEY is unset, e.g. locally)."""
    api_key = os.environ.get("DD_API_KEY")
    ti = context["task_instance"]
    payload = {
        "title": f"FinOps pipeline failed: {ti.dag_id}.{ti.task_id}",
        "text": f"Run {context['run_id']} failed. Log: {ti.log_url}",
        "alert_type": "error",
        "tags": ["team:finops-data", f"task:{ti.task_id}", "pipeline:cost-attribution"],
    }
    if not api_key:
        print("[datadog] DD_API_KEY not set; would send:", json.dumps(payload))
        return
    req = urllib.request.Request(
        f"https://api.{os.environ.get('DD_SITE', 'datadoghq.com')}/api/v1/events",
        data=json.dumps(payload).encode(),
        headers={"Content-Type": "application/json", "DD-API-KEY": api_key},
        method="POST",
    )
    urllib.request.urlopen(req, timeout=10)


def _contract_gate() -> None:
    import sys
    sys.path.insert(0, PROJECT)
    from ingestion.validate_contract import validate_file

    result = validate_file(os.path.join(PROJECT, LANDING_FILE))
    print(json.dumps(result.to_dict(), indent=2))
    if not result.passed:
        raise ValueError(f"Contract violations: {result.errors}")


default_args = {
    "owner": "finops-data",
    "retries": 1,
    "retry_delay": timedelta(minutes=10),
    "on_failure_callback": datadog_alert,
    "sla": timedelta(hours=3),
}

with DAG(
    dag_id="finops_cost_attribution",
    description="Ingest FOCUS billing, allocate shared cost, reconcile, publish chargeback",
    start_date=datetime(2026, 1, 1),
    schedule="0 9 * * *",          # after providers' daily billing export refresh
    catchup=False,
    max_active_runs=1,
    default_args=default_args,
    tags=["finops", "billing", "chargeback"],
) as dag:
    contract_gate = PythonOperator(task_id="contract_gate", python_callable=_contract_gate)

    land_raw = BashOperator(
        task_id="land_raw",
        bash_command=f"cd {PROJECT} && python ingestion/load_raw.py {LANDING_FILE}",
    )

    dbt_build = BashOperator(
        task_id="dbt_build",
        bash_command=f"cd {PROJECT} && dbt build --project-dir transform --profiles-dir transform "
                     "--target {{ var.value.get('finops_dbt_target', 'dev') }}",
    )

    reconcile = BashOperator(
        task_id="reconcile",
        bash_command=f"cd {PROJECT} && python reconciliation/reconcile.py",
    )

    publish_report = BashOperator(
        task_id="publish_report",
        bash_command=f"cd {PROJECT} && python report/build_report.py",
    )

    contract_gate >> land_raw >> dbt_build >> reconcile >> publish_report
