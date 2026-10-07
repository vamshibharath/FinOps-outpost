"""Reproduce the two real-world upstream incidents this pipeline is built to catch.

  1. A field rename   (EffectiveCost -> Effective_Cost)
  2. Type drift       (BilledCost arrives as currency strings like "$1,234.56")

Writes a broken copy of the landing file and runs the loader against it.
Expected outcome: the batch is BLOCKED and quarantined, nothing reaches the lake.
"""
from __future__ import annotations

import sys
from pathlib import Path

import duckdb

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ingestion.load_raw import land  # noqa: E402


def make_drifted(src: Path, dest: Path, n: int = 2000) -> Path:
    con = duckdb.connect()
    con.execute(f"""
        COPY (
          SELECT * EXCLUDE (EffectiveCost, BilledCost),
                 EffectiveCost AS Effective_Cost,
                 '$' || format('{{:,.2f}}', BilledCost) AS BilledCost
          FROM read_csv('{src}', nullstr=['NULL'], header=true, sample_size=-1)
          LIMIT {n}
        ) TO '{dest}' (HEADER, DELIMITER ',')
    """)
    return dest


if __name__ == "__main__":
    src = Path(sys.argv[1] if len(sys.argv) > 1 else "data/landing/focus_sample_100000.csv.gz")
    bad = make_drifted(src, Path("data/landing/_drift_demo_focus.csv"))
    print(f"[drift-demo] wrote {bad} with a renamed column and currency-formatted costs")
    try:
        land(bad)
    except SystemExit as e:
        print(f"[drift-demo] loader exited with code {e.code} -> pipeline halted, lake untouched")
    finally:
        bad.unlink(missing_ok=True)
