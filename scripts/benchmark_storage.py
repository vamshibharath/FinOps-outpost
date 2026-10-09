"""Measure, don't estimate: what does the storage layout actually save?

Compares the same Finance question ("AWS September cost by service") against
  A) the raw gzip CSV export, as delivered
  B) Parquet, column pruning only (no partition filter)
  C) Parquet, column + partition pruning (provider=AWS / billing_period=2024-09)

Athena bills by bytes scanned, so the cost metric is bytes the engine must read:
  - gzip CSV is not splittable or columnar: the whole object is scanned.
  - Parquet: only the needed column chunks, and with partition pruning only the matching
    partition's files. Read straight from Parquet footer metadata (exact, not estimated).
Wall-clock is the median of 5 local DuckDB runs (a proxy for latency, not Athena timing).

Output: data/warehouse/benchmark.json (shown in the report) + a printed summary.
The sample is small (MBs), so absolute dollars are tiny: the ratios are the result.
"""
from __future__ import annotations

import gzip
import json
import statistics
import sys
import time
from pathlib import Path

import duckdb

LAKE = Path("data/lake/raw_focus")
OUT = Path("data/warehouse/benchmark.json")
ATHENA_USD_PER_TB = 5.0
COLUMNS = ["ServiceName", "EffectiveCost"]


def _uncompressed_size(path: Path) -> int:
    if path.suffix != ".gz":
        return path.stat().st_size
    total = 0
    with gzip.open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            total += len(chunk)
    return total


def _median_seconds(con: duckdb.DuckDBPyConnection, sql: str, runs: int = 5) -> float:
    times = []
    for _ in range(runs):
        t0 = time.perf_counter()
        con.execute(sql).fetchall()
        times.append(time.perf_counter() - t0)
    return statistics.median(times)


def _parquet_bytes(con: duckdb.DuckDBPyConnection, glob: str, columns: list[str]) -> int:
    cols = ", ".join(f"'{c}'" for c in columns)
    return int(con.execute(
        f"SELECT coalesce(sum(total_compressed_size), 0) FROM parquet_metadata('{glob}') "
        f"WHERE path_in_schema IN ({cols})").fetchone()[0])


def run(csv_path: Path) -> dict:
    con = duckdb.connect()
    all_parquet = f"{LAKE}/**/*.parquet"
    aws_sept = f"{LAKE}/provider=AWS/billing_period=2024-09/*.parquet"

    csv_sql = f"""SELECT ServiceName, sum(EffectiveCost) FROM read_csv('{csv_path}', nullstr=['NULL'], header=true)
                  WHERE ProviderName = 'AWS' AND strftime(CAST(BillingPeriodStart AS TIMESTAMP), '%Y-%m') = '2024-09'
                  GROUP BY 1"""
    pq_nopart_sql = f"""SELECT ServiceName, sum(EffectiveCost) FROM read_parquet('{all_parquet}', hive_partitioning=false)
                        WHERE ProviderName = 'AWS' GROUP BY 1"""
    pq_sql = f"""SELECT ServiceName, sum(EffectiveCost) FROM read_parquet('{all_parquet}', hive_partitioning=true)
                 WHERE provider = 'AWS' AND billing_period = '2024-09' GROUP BY 1"""

    # Sanity: all three must return the same answer before comparing their cost.
    a = dict(con.execute(csv_sql).fetchall())
    c = dict(con.execute(pq_sql).fetchall())
    same = a.keys() == c.keys() and all(abs((a[k] or 0) - (c[k] or 0)) < 1e-6 for k in a)

    scenarios = {
        "gzip_csv_full_scan": {
            "bytes_scanned": csv_path.stat().st_size,
            "seconds": _median_seconds(con, csv_sql),
        },
        "parquet_column_pruning": {
            "bytes_scanned": _parquet_bytes(con, all_parquet, COLUMNS + ["ProviderName"]),
            "seconds": _median_seconds(con, pq_nopart_sql),
        },
        "parquet_column_and_partition_pruning": {
            "bytes_scanned": _parquet_bytes(con, aws_sept, COLUMNS),
            "seconds": _median_seconds(con, pq_sql),
        },
    }
    base = scenarios["gzip_csv_full_scan"]
    for s in scenarios.values():
        s["usd_per_1000_queries"] = round(s["bytes_scanned"] / 1e12 * ATHENA_USD_PER_TB * 1000, 6)
        s["scan_reduction_vs_csv"] = round(1 - s["bytes_scanned"] / base["bytes_scanned"], 4)
        s["speedup_vs_csv"] = round(base["seconds"] / s["seconds"], 1) if s["seconds"] else None

    lake_bytes = sum(f.stat().st_size for f in LAKE.rglob("*.parquet"))
    result = {
        "query": "AWS 2024-09 effective cost by service",
        "results_match": same,
        "storage": {
            "csv_uncompressed_bytes": _uncompressed_size(csv_path),
            "csv_gzip_bytes": csv_path.stat().st_size,
            "parquet_lake_bytes": lake_bytes,
        },
        "scenarios": scenarios,
        "athena_usd_per_tb": ATHENA_USD_PER_TB,
    }
    con.close()
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(result, indent=2))
    return result


if __name__ == "__main__":
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "data/landing/focus_sample_100000.csv.gz")
    r = run(src)
    st = r["storage"]
    print(f"[benchmark] storage: CSV {st['csv_uncompressed_bytes']/1e6:.1f} MB | gzip CSV {st['csv_gzip_bytes']/1e6:.1f} MB "
          f"| Parquet lake {st['parquet_lake_bytes']/1e6:.1f} MB")
    for name, s in r["scenarios"].items():
        print(f"[benchmark] {name:<38} scanned={s['bytes_scanned']/1e6:7.2f} MB  "
              f"reduction={s['scan_reduction_vs_csv']:>6.1%}  median={s['seconds']*1000:6.1f} ms  speedup={s['speedup_vs_csv']}x")
    print(f"[benchmark] all layouts return the same answer: {r['results_match']}")
    if not r["results_match"]:
        sys.exit(4)
