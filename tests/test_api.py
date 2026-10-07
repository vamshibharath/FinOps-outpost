from pathlib import Path

import pytest
from fastapi.testclient import TestClient

pytestmark = pytest.mark.skipif(not Path("data/warehouse/finops.duckdb").exists(),
                                reason="run `make pipeline` first")


@pytest.fixture(scope="module")
def client():
    from api.main import app
    return TestClient(app)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_chargeback_returns_ranked_business_units(client):
    r = client.get("/v1/chargeback", params={"billing_period": "2024-09", "limit": 5})
    assert r.status_code == 200
    totals = [row["total"] for row in r.json()]
    assert totals == sorted(totals, reverse=True)


def test_chargeback_rejects_bad_period(client):
    assert client.get("/v1/chargeback", params={"billing_period": "Sept"}).status_code == 422


def test_unknown_period_is_404(client):
    assert client.get("/v1/chargeback", params={"billing_period": "1999-01"}).status_code == 404
