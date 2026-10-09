"""Land validated billing files into the lake as partitioned Parquet.

Local layout mirrors the S3 layout used in production:
    data/lake/raw_focus/provider=<p>/billing_period=<yyyy-mm>/batch_<hash>_<n>.parquet

Guarantees
- Contract gate first: failures go to data/quarantine/ and the process exits non-zero.
- Idempotent: every file is fingerprinted (sha256); re-loading the same file is a no-op.
- Duplicates: if a file contains the same line item twice (provider + Id), the latest
  version (latest ChargePeriodEnd) is kept and the drop count is logged.
- Late-arriving data / restatements: providers re-send the WHOLE month when they revise
  it. A new file therefore *replaces* the provider/month partitions it contains (and only
  those). The manifest records which batch is active for each partition, so
  reconciliation always compares against the current version of every month.
"""
from __future__ import annotations

import hashlib
import json
import shutil
import sys
from datetime import datetime, timezone
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.validate_contract import validate_file  # noqa: E402

LAKE = Path("data/lake/raw_focus")
QUARANTINE = Path("data/quarantine")
MANIFEST = Path("data/lake/_manifest.jsonl")


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            h.update(chunk)
    return h.hexdigest()


def read_manifest(manifest: Path | None = None) -> list[dict]:
    manifest = manifest or MANIFEST
    if not manifest.exists():
        return []
    return [json.loads(line) for line in manifest.read_text().splitlines() if line]


def active_partitions(manifest: Path | None = None) -> dict[tuple[str, str], dict]:
    """(provider, billing_period) -> control totals of the batch currently live in the lake.
    Later batches win, which is exactly how the lake partitions were overwritten."""
    active: dict[tuple[str, str], dict] = {}
    for entry in read_manifest(manifest):
        for p in entry.get("partitions", []):
            active[(p["provider"], p["billing_period"])] = {**p, "batch_id": entry["batch_id"]}
    return active


def land(path: str | Path) -> dict:
    path = Path(path)
    batch_id = _sha256(path)[:16]

    if any(e["batch_id"] == batch_id for e in read_manifest()):
        print(f"[land] batch {batch_id} already loaded, skipping (idempotent)")
        return {"batch_id": batch_id, "status": "skipped"}

    result = validate_file(path)
    for w in result.warnings:
        print(f"[contract] WARN  {w}")
    if not result.passed:
        QUARANTINE.mkdir(parents=True, exist_ok=True)
        qpath = QUARANTINE / path.name
        shutil.copy2(path, qpath)
        (QUARANTINE / f"{path.name}.report.json").write_text(json.dumps(result.to_dict(), indent=2))
        for e in result.errors:
            print(f"[contract] ERROR {e}")
        print(f"[land] BLOCKED: batch quarantined at {qpath}")
        raise SystemExit(2)

    con = duckdb.connect()
    con.execute(
        "CREATE TABLE raw AS SELECT * FROM read_csv(?, nullstr=['NULL'], header=true, sample_size=-1)",
        [str(path)],
    )
    raw_rows = con.execute("SELECT count(*) FROM raw").fetchone()[0]
    con.execute(
        """
        CREATE TABLE b AS
        SELECT *,
               strftime(CAST(BillingPeriodStart AS TIMESTAMP), '%Y-%m') AS billing_period,
               regexp_replace(trim(ProviderName), '\\s+', '_', 'g')     AS provider,
               ? AS _batch_id,
               ? AS _source_file,
               current_timestamp AS _loaded_at
        FROM raw
        QUALIFY row_number() OVER (PARTITION BY ProviderName, Id
                                   ORDER BY CAST(ChargePeriodEnd AS TIMESTAMP) DESC NULLS LAST) = 1
        """,
        [batch_id, path.name],
    )
    rows = con.execute("SELECT count(*) FROM b").fetchone()[0]
    duplicates = raw_rows - rows

    parts = con.execute(
        """SELECT provider, billing_period, count(*) AS rows,
                  round(sum(BilledCost), 6) AS billed, round(sum(EffectiveCost), 6) AS effective
           FROM b GROUP BY ALL ORDER BY ALL"""
    ).fetchall()

    previous = active_partitions()
    replaced = []
    for provider, period, *_ in parts:
        pdir = LAKE / f"provider={provider}" / f"billing_period={period}"
        if pdir.exists():
            shutil.rmtree(pdir)  # restatement: the new file is the full, current month
            prior = previous.get((provider, period))
            replaced.append(f"{provider}/{period} (was batch {prior['batch_id'] if prior else '?'})")

    LAKE.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""COPY b TO '{LAKE}' (FORMAT PARQUET, PARTITION_BY (provider, billing_period),
            OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'batch_{batch_id}_{{i}}')"""
    )
    con.close()

    entry = {
        "batch_id": batch_id,
        "source_file": path.name,
        "rows": rows,
        "duplicates_dropped": duplicates,
        "control_billed_cost": round(sum(p[3] for p in parts), 6),
        "control_effective_cost": round(sum(p[4] for p in parts), 6),
        "partitions": [
            {"provider": p[0], "billing_period": p[1], "rows": p[2],
             "control_billed_cost": p[3], "control_effective_cost": p[4]}
            for p in parts
        ],
        "replaced_partitions": replaced,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST, "a") as fh:
        fh.write(json.dumps(entry) + "\n")

    print(f"[land] loaded batch {batch_id}: {rows:,} rows, billed={entry['control_billed_cost']}, "
          f"effective={entry['control_effective_cost']}, duplicates dropped={duplicates}")
    for r in replaced:
        print(f"[land] restatement: replaced partition {r}")
    return {**entry, "status": "loaded"}


if __name__ == "__main__":
    for f in sys.argv[1:]:
        land(f)
