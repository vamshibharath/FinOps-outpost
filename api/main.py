"""Read-only REST API over the chargeback marts.

Lets downstream tools (Slack bots, budget alerts, internal portals) pull allocated cost
without warehouse credentials. Run:  uvicorn api.main:app --reload
"""
from __future__ import annotations

import os
from typing import Optional

import duckdb
from fastapi import FastAPI, HTTPException, Query

WAREHOUSE = os.environ.get("FINOPS_WAREHOUSE", "data/warehouse/finops.duckdb")

app = FastAPI(title="FinOps Attribution API", version="1.0.0")


def _query(sql: str, params: list | None = None) -> list[dict]:
    con = duckdb.connect(WAREHOUSE, read_only=True)
    try:
        cur = con.execute(sql, params or [])
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row)) for row in cur.fetchall()]
    finally:
        con.close()


@app.get("/health")
def health() -> dict:
    last = _query("SELECT max(run_at) AS last_run, bool_and(passed) AS passed FROM audit.reconciliation_runs "
                  "WHERE run_id = (SELECT run_id FROM audit.reconciliation_runs ORDER BY run_at DESC LIMIT 1)")
    return {"status": "ok", "last_reconciliation": last[0] if last else None}


@app.get("/v1/chargeback")
def chargeback(
    billing_period: str = Query(..., pattern=r"^\d{4}-\d{2}$", examples=["2024-09"]),
    business_unit: Optional[str] = None,
    limit: int = Query(50, ge=1, le=1000),
) -> list[dict]:
    sql = """SELECT business_unit, round(sum(direct_cost), 4) AS direct_cost,
                    round(sum(shared_cost), 4) AS shared_cost, round(sum(total_chargeback), 4) AS total
             FROM marts.mart_chargeback_monthly WHERE billing_period = ?"""
    params: list = [billing_period]
    if business_unit:
        sql += " AND business_unit = ?"
        params.append(business_unit)
    sql += " GROUP BY business_unit ORDER BY total DESC LIMIT ?"
    params.append(limit)
    rows = _query(sql, params)
    if not rows:
        raise HTTPException(404, f"No chargeback data for {billing_period}")
    return rows


@app.get("/v1/tag-coverage")
def tag_coverage(provider: Optional[str] = None) -> list[dict]:
    sql = """SELECT provider, billing_period, round(sum(effective_cost), 4) AS effective_cost,
                    round(sum(tagged_cost) / nullif(sum(effective_cost), 0), 4) AS tag_coverage_pct
             FROM marts.mart_tag_coverage"""
    params: list = []
    if provider:
        sql += " WHERE provider = ?"
        params.append(provider)
    return _query(sql + " GROUP BY ALL ORDER BY effective_cost DESC", params)


@app.get("/v1/commitments")
def commitments() -> dict:
    return {
        "coverage": _query("SELECT * FROM marts.mart_commitment_coverage ORDER BY provider"),
        "recommendations": _query("SELECT * FROM marts.mart_commitment_recommendation ORDER BY provider"),
    }
