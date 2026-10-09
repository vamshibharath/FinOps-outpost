"""Late-arriving / restated billing: a re-sent month replaces only its own partition."""
import csv
from pathlib import Path

import duckdb
import pytest

import ingestion.load_raw as lr
from ingestion.validate_contract import load_contract

COLS = list(load_contract()["required_columns"])


def _row(i: int, provider: str, billed: float, charge_end: str = "2024-09-02 01:00:00") -> dict:
    r = {c: "x" for c in COLS}
    r.update({"Id": str(i), "ProviderName": provider, "BilledCost": str(billed), "EffectiveCost": str(billed),
              "ListCost": "1", "ContractedCost": "1", "ConsumedQuantity": "1", "BillingCurrency": "USD",
              "ChargeCategory": "Usage", "BillingPeriodStart": "2024-09-01 00:00:00",
              "BillingPeriodEnd": "2024-10-01 00:00:00", "ChargePeriodStart": "2024-09-02 00:00:00",
              "ChargePeriodEnd": charge_end, "Tags": "{}"})
    return r


def _write(path: Path, rows: list[dict]) -> Path:
    with open(path, "w", newline="") as fh:
        w = csv.DictWriter(fh, fieldnames=list(rows[0]))
        w.writeheader()
        w.writerows(rows)
    return path


@pytest.fixture
def lake(tmp_path, monkeypatch):
    monkeypatch.setattr(lr, "LAKE", tmp_path / "lake")
    monkeypatch.setattr(lr, "MANIFEST", tmp_path / "manifest.jsonl")
    monkeypatch.setattr(lr, "QUARANTINE", tmp_path / "quarantine")
    return tmp_path


def _totals(lake_dir: Path) -> dict:
    c = duckdb.connect()
    return {(p, n): (rows, round(cost, 6)) for p, n, rows, cost in c.execute(
        f"SELECT provider, billing_period, count(*), sum(BilledCost) FROM read_parquet('{lake_dir}/**/*.parquet', "
        "hive_partitioning=true) GROUP BY ALL").fetchall()}


def test_restated_month_replaces_only_its_partition(lake):
    first = _write(lake / "v1.csv", [_row(1, "AWS", 10.0), _row(2, "AWS", 5.0), _row(3, "Microsoft", 7.0)])
    lr.land(first)
    assert _totals(lake / "lake") == {("AWS", "2024-09"): (2, 15.0), ("Microsoft", "2024-09"): (1, 7.0)}

    # AWS re-sends September: one corrected line, one late-arriving line, one duplicate.
    restated = _write(lake / "v2.csv", [_row(1, "AWS", 12.0), _row(2, "AWS", 5.0), _row(4, "AWS", 1.0),
                                       _row(4, "AWS", 1.0, charge_end="2024-09-01 01:00:00")])
    out = lr.land(restated)

    assert out["duplicates_dropped"] == 1
    assert out["replaced_partitions"] and out["replaced_partitions"][0].startswith("AWS/2024-09")
    assert _totals(lake / "lake") == {("AWS", "2024-09"): (3, 18.0), ("Microsoft", "2024-09"): (1, 7.0)}

    active = lr.active_partitions()
    assert active[("AWS", "2024-09")]["control_billed_cost"] == 18.0
    assert active[("Microsoft", "2024-09")]["control_billed_cost"] == 7.0


def test_reloading_the_same_file_is_a_no_op(lake):
    f = _write(lake / "v1.csv", [_row(1, "AWS", 10.0)])
    lr.land(f)
    assert lr.land(f)["status"] == "skipped"
    assert len(lr.read_manifest()) == 1
