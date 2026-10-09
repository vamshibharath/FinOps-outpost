"""A Datadog-style observability API, simulated locally.

Why simulated: no public dataset pairs real cloud bills with the matching service metrics.
This mock serves *synthetic* daily usage per service (CPU core-hours, request count) for the
applications that actually appear in the billing tags, so the telemetry join is meaningful.
Values are deterministic (seeded by service name and date) so every run is reproducible.

It behaves like a real API on purpose:
  - API-key + application-key headers (403 without them)
  - cursor pagination
  - rate limiting: every 10th request returns 429 with Retry-After
  - one transient 503 early in each run
so the client's pagination, backoff and retry logic is exercised for real.

Run standalone:  uvicorn ingestion.telemetry.mock_observability_api:app --port 8081
"""
from __future__ import annotations

import base64
import hashlib
import json
import math
import os
from datetime import date, timedelta
from functools import lru_cache

import duckdb
from fastapi import FastAPI, Header, HTTPException, Query
from fastapi.responses import JSONResponse

LAKE_GLOB = os.environ.get("FINOPS_LAKE_GLOB", "data/lake/raw_focus/**/*.parquet")

app = FastAPI(title="Mock observability API (Datadog-style)", version="1.0.0")
_request_count = {"n": 0}


@lru_cache(maxsize=1)
def _services() -> list[tuple[str, str]]:
    """(service, cloud_provider) pairs observed in billing tags."""
    con = duckdb.connect()
    rows = con.execute(f"""
        SELECT DISTINCT trim(coalesce(json_extract_string(Tags, '$.application'),
                                      json_extract_string(Tags, '$.Project'))) AS service, provider
        FROM read_parquet('{LAKE_GLOB}', hive_partitioning = true)
        WHERE Tags IS NOT NULL AND trim(Tags) NOT IN ('', '{{}}')
          AND coalesce(json_extract_string(Tags, '$.application'), json_extract_string(Tags, '$.Project')) IS NOT NULL
        ORDER BY ALL
    """).fetchall()
    con.close()
    return [(s, p) for s, p in rows if s]


def _unit(seed: str) -> float:
    """Deterministic value in [0, 1) from a string."""
    return int(hashlib.sha256(seed.encode()).hexdigest()[:12], 16) / float(16 ** 12)


def _usage(service: str, provider: str, day: date) -> dict:
    base = 2 + 400 * _unit(f"{provider}|{service}") ** 3          # few big services, many small
    weekly = 1.0 - (0.35 if day.weekday() >= 5 else 0.0)            # quieter weekends
    noise = 0.85 + 0.3 * _unit(f"{provider}|{service}|{day}")
    cpu = round(base * weekly * noise, 3)
    rps_per_core = 50 + 450 * _unit(f"rps|{service}")
    requests = int(cpu * 3600 * rps_per_core * (0.9 + 0.2 * math.sin(day.toordinal())))
    return {"date": day.isoformat(), "service": service, "cloud_provider": provider,
            "cpu_core_hours": cpu, "requests": max(requests, 0)}


def _encode(offset: int) -> str:
    return base64.urlsafe_b64encode(json.dumps({"o": offset}).encode()).decode()


def _decode(cursor: str) -> int:
    try:
        return int(json.loads(base64.urlsafe_b64decode(cursor.encode()))["o"])
    except Exception as exc:  # noqa: BLE001
        raise HTTPException(400, "invalid cursor") from exc


@app.get("/api/v2/usage/services")
def service_usage(
    date_from: date = Query(..., alias="filter[from]"),
    date_to: date = Query(..., alias="filter[to]"),
    page_size: int = Query(5000, alias="page[size]", ge=1, le=10000),
    cursor: str | None = Query(None, alias="page[cursor]"),
    dd_api_key: str | None = Header(None, alias="DD-API-KEY"),
    dd_app_key: str | None = Header(None, alias="DD-APPLICATION-KEY"),
):
    if not dd_api_key or not dd_app_key:
        raise HTTPException(403, "Forbidden: missing API or application key")
    if date_to < date_from or (date_to - date_from).days > 92:
        raise HTTPException(400, "date range must be 0-92 days")

    _request_count["n"] += 1
    n = _request_count["n"]
    if n == 3:
        return JSONResponse({"errors": ["Service Unavailable"]}, status_code=503)
    if n % 10 == 0:
        return JSONResponse({"errors": ["Rate limit exceeded"]}, status_code=429,
                            headers={"Retry-After": "1", "X-RateLimit-Remaining": "0"})

    services = _services()
    days = [date_from + timedelta(d) for d in range((date_to - date_from).days + 1)]
    total = len(services) * len(days)
    start = _decode(cursor) if cursor else 0
    end = min(start + page_size, total)
    rows = []
    for i in range(start, end):
        svc, prov = services[i // len(days)]
        rows.append({"type": "service_usage", "attributes": _usage(svc, prov, days[i % len(days)])})
    return {"data": rows, "meta": {"page": {"total": total, "next_cursor": _encode(end) if end < total else None}}}
