"""Reproduce a provider restating a month after it was already loaded.

What AWS actually does: the current (and sometimes previous) month's cost data is
re-delivered several times, then revised again after the invoice for refunds and credits.
Each delivery is the FULL month, so the right behaviour is "replace the month", never
"append the new rows".

This script builds a restated AWS 2024-09 file from the original data:
  - every AWS line from the original month (a full re-delivery),
  - ~1% of lines with corrected costs,
  - 25 late-arriving line items that were not in the first delivery,
  - 10 exact duplicate lines (a provider/export glitch),
then lands it. Expected: only the AWS/2024-09 partition is replaced, duplicates are dropped,
Microsoft and Oracle are untouched, and reconciliation still ties out after `dbt build`.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.load_raw import active_partitions, land  # noqa: E402

OUT = Path("data/landing/focus_aws_2024-09_restated.csv")


def build_restatement(src: Path, dest: Path) -> Path:
    con = duckdb.connect()
    con.execute(f"CREATE TABLE src AS SELECT * FROM read_csv('{src}', nullstr=['NULL'], header=true, sample_size=-1)")
    con.execute("""
        CREATE TABLE aws AS
        SELECT * FROM src
        WHERE ProviderName = 'AWS' AND strftime(CAST(BillingPeriodStart AS TIMESTAMP), '%Y-%m') = '2024-09'
    """)
    # 1% of lines get a corrected cost (e.g. a pricing fix applied after first delivery)
    con.execute("""
        UPDATE aws SET BilledCost = round(BilledCost * 1.10, 10), EffectiveCost = round(EffectiveCost * 1.10, 10)
        WHERE hash(Id) % 100 = 0
    """)
    max_id = con.execute("SELECT max(Id) FROM src").fetchone()[0]
    # Late-arriving lines: copies of real compute lines with new Ids, dated the last day of the month
    con.execute(f"""
        INSERT INTO aws
        SELECT * REPLACE ({max_id} + row_number() OVER () AS Id,
                          TIMESTAMP '2024-09-30 22:00:00' AS ChargePeriodStart,
                          TIMESTAMP '2024-09-30 23:00:00' AS ChargePeriodEnd)
        FROM (SELECT * FROM aws WHERE ServiceCategory = 'Compute' AND EffectiveCost > 0 LIMIT 25)
    """)
    # Exact duplicates, as an export glitch would produce
    con.execute("INSERT INTO aws SELECT * FROM aws WHERE ServiceCategory = 'Storage' LIMIT 10")
    con.execute(f"COPY aws TO '{dest}' (HEADER, DELIMITER ',', NULLSTR 'NULL')")
    return dest


if __name__ == "__main__":
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "data/landing/focus_sample_100000.csv.gz")
    before = active_partitions().get(("AWS", "2024-09"))
    build_restatement(src, OUT)
    print(f"[restatement-demo] wrote {OUT}")
    land(OUT)
    after = active_partitions().get(("AWS", "2024-09"))
    if before and after:
        print(f"[restatement-demo] AWS 2024-09 effective cost: {before['control_effective_cost']} "
              f"-> {after['control_effective_cost']} (rows {before['rows']:,} -> {after['rows']:,})")
    print("[restatement-demo] next: `make transform reconcile` — only AWS/2024-09 is reprocessed and totals still tie out")
