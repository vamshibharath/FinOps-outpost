"""The Dagster definitions load and wire the pipeline in the right order."""
import pytest

dg = pytest.importorskip("dagster")


def test_definitions_load_with_expected_lineage():
    import importlib.util
    from pathlib import Path

    spec = importlib.util.spec_from_file_location("finops_defs", Path("orchestration/dagster/definitions.py"))
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)

    graph = mod.defs.resolve_asset_graph()
    keys = {k.to_user_string() for k in graph.get_all_asset_keys()}
    assert keys == {"raw_billing", "service_telemetry", "dbt_warehouse", "reconciliation",
                    "storage_benchmark", "finops_report"}
    parents = {p.to_user_string() for p in graph.get(dg.AssetKey("dbt_warehouse")).parent_keys}
    assert parents == {"raw_billing", "service_telemetry"}
    checks = {c.name for c in graph.asset_check_keys}
    assert {"reconciliation_ties_out", "billing_months_complete"} <= checks
