"""Three-way reconciliation with an audit trail.

    source file control totals (ingestion manifest)
        == warehouse staging totals
        == allocated chargeback totals

Every run appends to audit.reconciliation_runs so anyone can answer "did last month's
numbers tie out, and when did we check?" Exits non-zero on any break so the
orchestrator fails the run and pages instead of publishing wrong numbers.
"""
from __future__ import annotations

import json
import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

WAREHOUSE = Path("data/warehouse/finops.duckdb")
MANIFEST = Path("data/lake/_manifest.jsonl")
TOLERANCE = 0.0001


def reconcile(warehouse: Path = WAREHOUSE, manifest: Path = MANIFEST, tol: float = TOLERANCE) -> dict:
    batches = [json.loads(line) for line in manifest.read_text().splitlines() if line]
    control_billed = sum(b["control_billed_cost"] for b in batches)
    control_effective = sum(b["control_effective_cost"] for b in batches)
    control_rows = sum(b["rows"] for b in batches)

    con = duckdb.connect(str(warehouse))
    stg_rows, stg_billed, stg_eff = con.execute(
        "SELECT count(*), sum(billed_cost), sum(effective_cost) FROM staging.stg_focus__charges"
    ).fetchone()
    alloc_billed, alloc_eff, unalloc = con.execute(
        """SELECT sum(allocated_billed_cost), sum(allocated_effective_cost),
                  coalesce(sum(allocated_effective_cost) FILTER (WHERE allocation_method = 'unallocated'), 0)
           FROM marts.fct_cost_allocated"""
    ).fetchone()

    checks = {
        "rows_source_vs_staging": (control_rows, stg_rows),
        "billed_source_vs_staging": (control_billed, stg_billed),
        "effective_source_vs_staging": (control_effective, stg_eff),
        "billed_staging_vs_allocated": (stg_billed, alloc_billed),
        "effective_staging_vs_allocated": (stg_eff, alloc_eff),
    }
    results = []
    for name, (expected, actual) in checks.items():
        diff = abs((expected or 0) - (actual or 0))
        results.append({"check": name, "expected": round(expected or 0, 6),
                        "actual": round(actual or 0, 6), "diff": round(diff, 8), "passed": diff <= tol})

    run = {
        "run_id": str(uuid.uuid4()),
        "run_at": datetime.now(timezone.utc).isoformat(),
        "batches": [b["batch_id"] for b in batches],
        "unallocated_effective_cost": round(unalloc, 6),
        "passed": all(r["passed"] for r in results),
        "checks": results,
    }

    con.execute("CREATE SCHEMA IF NOT EXISTS audit")
    con.execute("""CREATE TABLE IF NOT EXISTS audit.reconciliation_runs (
        run_id VARCHAR, run_at TIMESTAMPTZ, check_name VARCHAR, expected DOUBLE,
        actual DOUBLE, diff DOUBLE, passed BOOLEAN)""")
    con.executemany(
        "INSERT INTO audit.reconciliation_runs VALUES (?, ?, ?, ?, ?, ?, ?)",
        [(run["run_id"], run["run_at"], r["check"], r["expected"], r["actual"], r["diff"], r["passed"])
         for r in results],
    )
    con.close()
    return run


if __name__ == "__main__":
    out = reconcile()
    for r in out["checks"]:
        flag = "PASS" if r["passed"] else "FAIL"
        print(f"[reconcile] {flag}  {r['check']:<34} expected={r['expected']:>14} actual={r['actual']:>14}")
    print(f"[reconcile] unallocated effective cost: {out['unallocated_effective_cost']}")
    print(f"[reconcile] run {out['run_id']} -> {'PASSED' if out['passed'] else 'FAILED'}")
    sys.exit(0 if out["passed"] else 3)
