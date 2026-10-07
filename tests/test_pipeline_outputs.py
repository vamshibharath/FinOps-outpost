"""Integration tests against the built warehouse (run after `make pipeline`)."""
from pathlib import Path

import duckdb
import pytest

WAREHOUSE = Path("data/warehouse/finops.duckdb")
pytestmark = pytest.mark.skipif(not WAREHOUSE.exists(), reason="run `make pipeline` first")


@pytest.fixture(scope="module")
def con():
    c = duckdb.connect(str(WAREHOUSE), read_only=True)
    yield c
    c.close()


def test_every_dollar_is_allocated_exactly_once(con):
    src = con.execute("SELECT round(sum(effective_cost), 6) FROM staging.stg_focus__charges").fetchone()[0]
    alloc = con.execute("SELECT round(sum(allocated_effective_cost), 6) FROM marts.fct_cost_allocated").fetchone()[0]
    assert src == pytest.approx(alloc, abs=1e-4)


def test_tag_aliases_unify_legacy_keys(con):
    # 'org' / ' org' / 'CostCenter' tagged lines must resolve to a business_unit.
    unresolved = con.execute("""
        SELECT count(*) FROM intermediate.int_charges_attributed
        WHERE business_unit IS NULL
          AND tags_json IS NOT NULL
          AND (json_extract_string(tags_json, '$.org') IS NOT NULL
               OR json_extract_string(tags_json, '$.CostCenter') IS NOT NULL)
    """).fetchone()[0]
    assert unresolved == 0


def test_categories_are_case_normalized(con):
    cats = {r[0] for r in con.execute("SELECT DISTINCT charge_category FROM staging.stg_focus__charges").fetchall()}
    assert cats <= {"Usage", "Purchase", "Tax", "Credit", "Adjustment"}


def test_latest_reconciliation_passed(con):
    ok = con.execute("""SELECT bool_and(passed) FROM audit.reconciliation_runs
                        WHERE run_id = (SELECT run_id FROM audit.reconciliation_runs ORDER BY run_at DESC LIMIT 1)""").fetchone()[0]
    assert ok is True


def test_commitment_recommendation_never_exceeds_on_demand(con):
    rows = con.execute("SELECT share_of_on_demand_covered FROM marts.mart_commitment_recommendation").fetchall()
    assert all(0 <= r[0] <= 1 for r in rows)
