"""Three-way reconciliation with an audit trail.

    source control totals (ingestion manifest, current version of every month)
        == warehouse staging totals
        == allocated chargeback totals

Checked overall AND per provider/billing period, so a break points straight at the month
that drifted. Every run appends to audit.reconciliation_runs; any break exits non-zero so
the orchestrator stops before publishing.
"""
from __future__ import annotations

import sys
import uuid
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.load_raw import MANIFEST, active_partitions  # noqa: E402

WAREHOUSE = Path("data/warehouse/finops.duckdb")
TOLERANCE = 0.0001


def reconcile(warehouse: Path = WAREHOUSE, manifest: Path = MANIFEST, tol: float = TOLERANCE) -> dict:
    active = active_partitions(manifest)
    con = duckdb.connect(str(warehouse))
    stg = {(r[0], r[1]): r[2:] for r in con.execute(
        """SELECT provider, billing_period, count(*), sum(billed_cost), sum(effective_cost)
           FROM staging.stg_focus__charges GROUP BY ALL""").fetchall()}
    alloc = {(r[0], r[1]): r[2:] for r in con.execute(
        """SELECT provider, billing_period, sum(allocated_billed_cost), sum(allocated_effective_cost)
           FROM marts.fct_cost_allocated GROUP BY ALL""").fetchall()}
    unalloc = con.execute(
        """SELECT coalesce(sum(allocated_effective_cost), 0) FROM marts.fct_cost_allocated
           WHERE allocation_method = 'unallocated'""").fetchone()[0]

    results = []

    def check(name: str, expected: float, actual: float) -> None:
        diff = abs((expected or 0) - (actual or 0))
        results.append({"check": name, "expected": round(expected or 0, 6),
                        "actual": round(actual or 0, 6), "diff": round(diff, 8), "passed": diff <= tol})

    # Overall
    check("rows_source_vs_staging", sum(p["rows"] for p in active.values()), sum(v[0] for v in stg.values()))
    check("billed_source_vs_staging", sum(p["control_billed_cost"] for p in active.values()),
          sum(v[1] for v in stg.values()))
    check("effective_source_vs_staging", sum(p["control_effective_cost"] for p in active.values()),
          sum(v[2] for v in stg.values()))
    check("billed_staging_vs_allocated", sum(v[1] for v in stg.values()), sum(v[0] for v in alloc.values()))
    check("effective_staging_vs_allocated", sum(v[2] for v in stg.values()), sum(v[1] for v in alloc.values()))

    # Per provider / billing period: every month must tie out on its own
    for key in sorted(set(active) | set(stg) | set(alloc)):
        src_eff = active.get(key, {}).get("control_effective_cost", 0)
        stg_eff = stg.get(key, (0, 0, 0))[2]
        alloc_eff = alloc.get(key, (0, 0))[1]
        check(f"effective_source_vs_allocated[{key[0]}/{key[1]}]", src_eff, alloc_eff)
        check(f"effective_source_vs_staging[{key[0]}/{key[1]}]", src_eff, stg_eff)

    run = {
        "run_id": str(uuid.uuid4()),
        "run_at": datetime.now(timezone.utc).isoformat(),
        "active_batches": sorted({p["batch_id"] for p in active.values()}),
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
        print(f"[reconcile] {flag}  {r['check']:<52} expected={r['expected']:>14} actual={r['actual']:>14}")
    print(f"[reconcile] unallocated effective cost: {out['unallocated_effective_cost']}")
    print(f"[reconcile] run {out['run_id']} -> {'PASSED' if out['passed'] else 'FAILED'}")
    sys.exit(0 if out["passed"] else 3)
