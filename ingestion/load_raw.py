"""Land validated billing files into the lake as partitioned Parquet (idempotent).

Local layout mirrors the S3 layout used in production:
    data/lake/raw_focus/provider=<p>/billing_period=<yyyy-mm>/batch_<hash>.parquet

- Contract gate runs first; failures move the file to data/quarantine/ and exit non-zero.
- Every batch is fingerprinted (sha256); re-running the same file is a no-op.
- A manifest row (row count + control totals) is written per batch; reconciliation later
  proves the warehouse still adds up to these control totals.
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


def _already_loaded(batch_id: str) -> bool:
    if not MANIFEST.exists():
        return False
    return any(json.loads(line)["batch_id"] == batch_id for line in MANIFEST.read_text().splitlines() if line)


def land(path: str | Path) -> dict:
    path = Path(path)
    batch_id = _sha256(path)[:16]

    if _already_loaded(batch_id):
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
        """
        CREATE TABLE b AS
        SELECT *,
               strftime(CAST(BillingPeriodStart AS TIMESTAMP), '%Y-%m') AS billing_period,
               regexp_replace(trim(ProviderName), '\\s+', '_', 'g')     AS provider,
               ? AS _batch_id,
               ? AS _source_file,
               current_timestamp AS _loaded_at
        FROM read_csv(?, nullstr=['NULL'], header=true, sample_size=-1)
        """,
        [batch_id, path.name, str(path)],
    )
    LAKE.mkdir(parents=True, exist_ok=True)
    con.execute(
        f"""COPY b TO '{LAKE}' (FORMAT PARQUET, PARTITION_BY (provider, billing_period),
            OVERWRITE_OR_IGNORE, FILENAME_PATTERN 'batch_{batch_id}_{{i}}')"""
    )
    rows, billed, effective = con.execute(
        "SELECT count(*), round(sum(BilledCost), 6), round(sum(EffectiveCost), 6) FROM b"
    ).fetchone()
    entry = {
        "batch_id": batch_id, "source_file": path.name, "rows": rows,
        "control_billed_cost": billed, "control_effective_cost": effective,
        "loaded_at": datetime.now(timezone.utc).isoformat(),
    }
    MANIFEST.parent.mkdir(parents=True, exist_ok=True)
    with open(MANIFEST, "a") as fh:
        fh.write(json.dumps(entry) + "\n")
    print(f"[land] loaded batch {batch_id}: {rows:,} rows, billed={billed}, effective={effective}")
    return {**entry, "status": "loaded"}


if __name__ == "__main__":
    for f in sys.argv[1:]:
        land(f)
