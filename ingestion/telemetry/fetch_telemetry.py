"""Pull daily service telemetry from an observability REST API and land it in the lake.

Production-shaped REST ingestion:
  - cursor pagination with a loop guard (a repeated cursor aborts instead of spinning forever)
  - retries only on what is retryable: 429, 5xx and network errors; any other 4xx fails fast
  - honours Retry-After; otherwise exponential backoff with jitter, capped
  - every raw page is saved *before* parsing, so a parser bug can be fixed and replayed
    without calling the API again
  - a contract check on the parsed rows; the month is replaced as a whole (idempotent)

Without DD_API_KEY / DD_APP_KEY it runs against the local mock (in-process, no server needed).
"""
from __future__ import annotations

import argparse
import json
import logging
import os
import random
import shutil
import sys
import time
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Callable, Iterator

import duckdb
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

log = logging.getLogger("telemetry")
RAW_DIR = Path("data/lake/raw_telemetry")
OUT_DIR = Path("data/lake/telemetry")
ENDPOINT = "/api/v2/usage/services"
RETRYABLE = {429, 500, 502, 503, 504}


class TelemetryError(RuntimeError):
    pass


@dataclass
class FetchStats:
    pages: int = 0
    rows: int = 0
    retries: int = 0
    retry_reasons: list[str] = field(default_factory=list)


class TelemetryClient:
    def __init__(self, http: httpx.Client, max_attempts: int = 5, base_delay: float = 0.5,
                 max_delay: float = 8.0, sleep: Callable[[float], None] = time.sleep):
        self.http = http
        self.max_attempts = max_attempts
        self.base_delay = base_delay
        self.max_delay = max_delay
        self.sleep = sleep
        self.stats = FetchStats()

    def _delay(self, attempt: int, response: httpx.Response | None) -> float:
        if response is not None and response.headers.get("Retry-After"):
            try:
                return min(float(response.headers["Retry-After"]), self.max_delay)
            except ValueError:
                pass
        cap = min(self.max_delay, self.base_delay * 2 ** attempt)
        return cap / 2 + random.uniform(0, cap / 2)  # "equal jitter": spread out, never zero

    def get(self, path: str, params: dict) -> dict:
        for attempt in range(self.max_attempts):
            response = None
            try:
                response = self.http.get(path, params=params)
                if response.status_code < 400:
                    return response.json()
                if response.status_code not in RETRYABLE:
                    raise TelemetryError(f"HTTP {response.status_code} (not retryable): {response.text[:200]}")
                reason = f"HTTP {response.status_code}"
            except httpx.TransportError as exc:
                reason = f"{type(exc).__name__}"
            if attempt == self.max_attempts - 1:
                raise TelemetryError(f"giving up after {self.max_attempts} attempts (last: {reason})")
            delay = self._delay(attempt, response)
            self.stats.retries += 1
            self.stats.retry_reasons.append(reason)
            log.warning("retry %d/%d after %s, sleeping %.2fs", attempt + 1, self.max_attempts - 1, reason, delay)
            self.sleep(delay)
        raise AssertionError("unreachable")

    def iter_pages(self, start: date, end: date, page_size: int = 10000, max_pages: int = 10000) -> Iterator[dict]:
        params = {"filter[from]": start.isoformat(), "filter[to]": end.isoformat(), "page[size]": page_size}
        seen: set[str] = set()
        for _ in range(max_pages):
            page = self.get(ENDPOINT, params)
            self.stats.pages += 1
            self.stats.rows += len(page.get("data", []))
            yield page
            cursor = page.get("meta", {}).get("page", {}).get("next_cursor")
            if not cursor:
                return
            if cursor in seen:
                raise TelemetryError("API returned a cursor it already returned; aborting to avoid an infinite loop")
            seen.add(cursor)
            params["page[cursor]"] = cursor
        raise TelemetryError(f"exceeded max_pages={max_pages}")


def make_http_client() -> tuple[httpx.Client, str]:
    api_key, app_key = os.environ.get("DD_API_KEY"), os.environ.get("DD_APP_KEY")
    if api_key and app_key:
        site = os.environ.get("DD_SITE", "datadoghq.com")
        return httpx.Client(base_url=f"https://api.{site}",
                            headers={"DD-API-KEY": api_key, "DD-APPLICATION-KEY": app_key},
                            timeout=30), f"api.{site}"
    from fastapi.testclient import TestClient

    from ingestion.telemetry.mock_observability_api import app
    return TestClient(app, headers={"DD-API-KEY": "local-mock", "DD-APPLICATION-KEY": "local-mock"}), "local mock"


def validate_and_publish(raw_run_dir: Path, start: date, end: date) -> dict:
    """Parse the landed raw pages, enforce the contract, replace the month(s) in the lake."""
    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE t AS
        SELECT CAST(d.attributes.date AS DATE)                AS usage_date,
               trim(d.attributes.service)                     AS application,
               regexp_replace(trim(d.attributes.cloud_provider), '\\s+', '_', 'g') AS provider,
               TRY_CAST(d.attributes.cpu_core_hours AS DOUBLE) AS cpu_core_hours,
               TRY_CAST(d.attributes.requests AS BIGINT)      AS requests
        FROM (SELECT unnest(data) AS d FROM read_json('{raw_run_dir}/page_*.json', format='auto'))
    """)
    rows, bad, dupes, out_of_window = con.execute(f"""
        SELECT count(*),
               count(*) FILTER (WHERE application IS NULL OR provider IS NULL OR cpu_core_hours IS NULL
                                 OR requests IS NULL OR cpu_core_hours < 0 OR requests < 0),
               count(*) - (SELECT count(*) FROM (SELECT DISTINCT usage_date, application, provider FROM t)),
               count(*) FILTER (WHERE usage_date NOT BETWEEN DATE '{start}' AND DATE '{end}')
        FROM t
    """).fetchone()
    errors = []
    if rows == 0:
        errors.append("EMPTY: no telemetry rows returned")
    if bad:
        errors.append(f"INVALID_VALUES: {bad} rows with null/negative fields")
    if dupes:
        errors.append(f"DUPLICATES: {dupes} repeated (date, service, provider) rows")
    if out_of_window:
        errors.append(f"OUT_OF_WINDOW: {out_of_window} rows outside {start}..{end}")
    if errors:
        con.close()
        raise TelemetryError("telemetry contract failed: " + "; ".join(errors))

    con.execute("ALTER TABLE t ADD COLUMN usage_month VARCHAR")
    con.execute("UPDATE t SET usage_month = strftime(usage_date, '%Y-%m')")
    months = [r[0] for r in con.execute("SELECT DISTINCT usage_month FROM t ORDER BY 1").fetchall()]
    for m in months:  # replace whole months: re-runs and backfills are idempotent
        shutil.rmtree(OUT_DIR / f"usage_month={m}", ignore_errors=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    con.execute(f"COPY t TO '{OUT_DIR}' (FORMAT PARQUET, PARTITION_BY (usage_month), OVERWRITE_OR_IGNORE, "
                f"FILENAME_PATTERN 'telemetry_{{i}}')")
    con.close()
    return {"rows": rows, "months": months}


def run(start: date, end: date, page_size: int = 10000) -> dict:
    http, target = make_http_client()
    client = TelemetryClient(http)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    raw_run_dir = RAW_DIR / f"run={run_id}"
    raw_run_dir.mkdir(parents=True, exist_ok=True)
    print(f"[telemetry] fetching {start}..{end} from {target}")
    for i, page in enumerate(client.iter_pages(start, end, page_size), start=1):
        (raw_run_dir / f"page_{i:05d}.json").write_text(json.dumps(page))
    published = validate_and_publish(raw_run_dir, start, end)
    s = client.stats
    print(f"[telemetry] pages={s.pages} rows={s.rows:,} retries={s.retries} {s.retry_reasons}")
    print(f"[telemetry] raw pages kept at {raw_run_dir}; published months {published['months']}")
    return {"pages": s.pages, "rows": s.rows, "retries": s.retries, **published}


if __name__ == "__main__":
    logging.basicConfig(level=logging.WARNING, format="[telemetry] %(message)s")
    p = argparse.ArgumentParser()
    p.add_argument("--start", default="2024-09-01")
    p.add_argument("--end", default="2024-09-30")
    p.add_argument("--page-size", type=int, default=10000)
    a = p.parse_args()
    try:
        run(date.fromisoformat(a.start), date.fromisoformat(a.end), a.page_size)
    except TelemetryError as exc:
        print(f"[telemetry] FAILED: {exc}", file=sys.stderr)
        sys.exit(2)
