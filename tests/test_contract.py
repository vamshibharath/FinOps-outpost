"""Unit tests for the schema-contract gate (no warehouse needed)."""
from pathlib import Path

import pytest

from ingestion.validate_contract import load_contract, validate_file

CONTRACT = load_contract()
COLS = list(CONTRACT["required_columns"])


def _row(overrides: dict | None = None) -> dict:
    vals = {c: "x" for c in COLS}
    vals.update({
        "BilledCost": "1.5", "EffectiveCost": "1.2", "ListCost": "2.0", "ContractedCost": "1.4",
        "ConsumedQuantity": "1", "BillingCurrency": "USD", "ChargeCategory": "Usage",
        "BillingPeriodStart": "2024-09-01 00:00:00", "BillingPeriodEnd": "2024-10-01 00:00:00",
        "ChargePeriodStart": "2024-09-02 00:00:00", "ChargePeriodEnd": "2024-09-02 01:00:00",
        "Tags": '{"business_unit": "payments"}', "ProviderName": "AWS",
    })
    vals.update(overrides or {})
    return vals


def _write(tmp_path: Path, rows: list[dict], rename: dict | None = None) -> Path:
    import csv

    rename = rename or {}
    header = [rename.get(c, c) for c in rows[0]]
    path = tmp_path / "batch.csv"
    with open(path, "w", newline="") as fh:
        w = csv.writer(fh)
        w.writerow(header)
        for r in rows:
            w.writerow(r.values())
    return path


def test_valid_batch_passes(tmp_path):
    res = validate_file(_write(tmp_path, [_row(), _row()]))
    assert res.passed, res.errors
    assert res.rows == 2


def test_renamed_column_is_flagged_as_probable_rename(tmp_path):
    res = validate_file(_write(tmp_path, [_row()], rename={"EffectiveCost": "Effective_Cost"}))
    assert not res.passed
    assert any("PROBABLE_RENAME" in e and "Effective_Cost" in e for e in res.errors)


def test_currency_formatted_cost_is_type_drift(tmp_path):
    res = validate_file(_write(tmp_path, [_row({"BilledCost": "$1,234.56"})]))
    assert not res.passed
    assert any(e.startswith("TYPE_DRIFT: BilledCost") for e in res.errors)


def test_null_effective_cost_breaches_threshold(tmp_path):
    res = validate_file(_write(tmp_path, [_row({"EffectiveCost": ""})]))
    assert any(e.startswith("NULL_THRESHOLD: EffectiveCost") for e in res.errors)


def test_new_charge_category_is_warning_not_failure(tmp_path):
    res = validate_file(_write(tmp_path, [_row({"ChargeCategory": "Refund"})]))
    assert res.passed
    assert any("UNKNOWN_VALUES" in w for w in res.warnings)


@pytest.mark.parametrize("bad_json", ["{not json", "business_unit=payments"])
def test_invalid_tags_json_is_rejected(tmp_path, bad_json):
    res = validate_file(_write(tmp_path, [_row({"Tags": bad_json})]))
    assert any(e.startswith("TYPE_DRIFT: Tags") for e in res.errors)
