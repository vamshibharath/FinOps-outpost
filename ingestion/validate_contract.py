"""Schema-contract gate for inbound billing files.

Why this exists: the classic silent failure in cost pipelines is an upstream schema change
(a renamed field, or a cost column that starts arriving as "$1,234.56") that the loader
happily ingests as NULLs. Finance then sees a dip in spend that never happened.

This gate reads every column as raw text first, so type drift is *detected* instead of
being coerced away, and it fails the batch before anything reaches the lake.
"""
from __future__ import annotations

import difflib
import json
from dataclasses import dataclass, field
from pathlib import Path

import duckdb
import yaml

CONTRACT_PATH = Path(__file__).parent / "contracts" / "focus_v1_contract.yaml"

_TYPE_CHECK_SQL = {
    "numeric": "TRY_CAST({c} AS DOUBLE) IS NULL",
    "timestamp": "TRY_CAST({c} AS TIMESTAMP) IS NULL",
    "json": "json_valid({c}) = false",
    "string": "false",
}


@dataclass
class ContractResult:
    file: str
    rows: int = 0
    errors: list[str] = field(default_factory=list)
    warnings: list[str] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return not self.errors

    def to_dict(self) -> dict:
        return {"file": self.file, "rows": self.rows, "passed": self.passed,
                "errors": self.errors, "warnings": self.warnings}


def load_contract(path: Path = CONTRACT_PATH) -> dict:
    return yaml.safe_load(path.read_text())


def _q(col: str) -> str:
    return '"' + col.replace('"', '""') + '"'


def validate_file(path: str | Path, contract: dict | None = None) -> ContractResult:
    contract = contract or load_contract()
    path = Path(path)
    res = ContractResult(file=str(path))
    con = duckdb.connect()
    # all_varchar=true: never let the reader silently coerce bad values.
    con.execute(
        "CREATE TABLE batch AS SELECT * FROM read_csv(?, all_varchar=true, nullstr=['NULL',''], header=true)",
        [str(path)],
    )
    actual = [r[0] for r in con.execute("DESCRIBE batch").fetchall()]
    res.rows = con.execute("SELECT count(*) FROM batch").fetchone()[0]

    required: dict[str, str] = contract["required_columns"]
    missing = [c for c in required if c not in actual]
    unexpected = [c for c in actual if c not in required]

    # 1. Structural checks, with rename detection so the alert says *what* changed.
    for col in missing:
        guess = difflib.get_close_matches(col, unexpected, n=1, cutoff=0.75)
        if guess:
            res.errors.append(f"PROBABLE_RENAME: required column '{col}' missing; found '{guess[0]}'")
        else:
            res.errors.append(f"MISSING_COLUMN: required column '{col}' not present")

    if res.rows < contract["thresholds"]["min_rows"]:
        res.errors.append(f"EMPTY_BATCH: {res.rows} rows")

    # 2. Type drift: count non-null values that fail to parse as the contracted type.
    for col, typ in required.items():
        if col not in actual:
            continue
        cond = _TYPE_CHECK_SQL[typ].format(c=_q(col))
        bad, sample = con.execute(
            f"SELECT count(*), any_value({_q(col)}) FROM batch WHERE {_q(col)} IS NOT NULL AND {cond}"
        ).fetchone()
        if bad:
            res.errors.append(f"TYPE_DRIFT: {col} expected {typ}, {bad} bad values (e.g. {sample!r})")

    # 3. Null thresholds on columns finance cannot live without.
    for col, max_pct in contract["thresholds"]["max_null_pct"].items():
        if col not in actual or res.rows == 0:
            continue
        nulls = con.execute(f"SELECT count(*) FROM batch WHERE {_q(col)} IS NULL").fetchone()[0]
        if nulls / res.rows > max_pct:
            res.errors.append(f"NULL_THRESHOLD: {col} has {nulls} nulls ({nulls / res.rows:.2%})")

    # 4. Domain values: unknowns are warnings (providers add categories), not hard failures.
    for col, allowed in contract.get("allowed_values", {}).items():
        if col not in actual:
            continue
        vals = con.execute(
            f"SELECT DISTINCT lower(trim({_q(col)})) FROM batch WHERE {_q(col)} IS NOT NULL"
        ).fetchall()
        unknown = sorted(v[0] for v in vals if v[0] not in allowed)
        if unknown:
            res.warnings.append(f"UNKNOWN_VALUES: {col} -> {unknown}")

    if unexpected:
        res.warnings.append(f"ADDITIVE_COLUMNS (allowed): {unexpected}")
    con.close()
    return res


if __name__ == "__main__":
    import sys

    out = validate_file(sys.argv[1])
    print(json.dumps(out.to_dict(), indent=2))
    sys.exit(0 if out.passed else 2)
