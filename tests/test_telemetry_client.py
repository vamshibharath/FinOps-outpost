"""REST ingestion behaviour: pagination, retries, fail-fast, loop guard. No network, no sleeping."""
from datetime import date

import httpx
import pytest

from ingestion.telemetry.fetch_telemetry import TelemetryClient, TelemetryError


def make_client(handler, **kw) -> TelemetryClient:
    sleeps: list[float] = []
    client = TelemetryClient(httpx.Client(transport=httpx.MockTransport(handler), base_url="https://api.test"),
                             sleep=sleeps.append, **kw)
    client.sleeps = sleeps  # type: ignore[attr-defined]
    return client


def paged(pages: int, fail: dict[int, httpx.Response] | None = None):
    """Serve `pages` pages; `fail` maps a request number to a canned error response."""
    calls = {"n": 0}

    def handler(request: httpx.Request) -> httpx.Response:
        calls["n"] += 1
        if fail and calls["n"] in fail:
            return fail[calls["n"]]
        page = int(request.url.params.get("page[cursor]", "0"))
        nxt = str(page + 1) if page + 1 < pages else None
        return httpx.Response(200, json={"data": [{"attributes": {"page": page}}] * 3,
                                         "meta": {"page": {"next_cursor": nxt}}})
    return handler, calls


def test_reads_every_page():
    handler, _ = paged(4)
    c = make_client(handler)
    pages = list(c.iter_pages(date(2024, 9, 1), date(2024, 9, 30)))
    assert len(pages) == 4 and c.stats.rows == 12 and c.stats.retries == 0


def test_retries_rate_limits_and_server_errors_then_succeeds():
    handler, calls = paged(2, fail={1: httpx.Response(429, headers={"Retry-After": "2"}),
                                    2: httpx.Response(503)})
    c = make_client(handler)
    pages = list(c.iter_pages(date(2024, 9, 1), date(2024, 9, 30)))
    assert len(pages) == 2
    assert c.stats.retries == 2
    assert c.sleeps[0] == 2.0           # honoured Retry-After
    assert 0 < c.sleeps[1] <= 1.0       # backoff with jitter for the 503 (attempt 1: cap 1s)


def test_client_errors_fail_fast_without_retry():
    handler, calls = paged(2, fail={1: httpx.Response(403, json={"errors": ["Forbidden"]})})
    c = make_client(handler)
    with pytest.raises(TelemetryError, match="403"):
        list(c.iter_pages(date(2024, 9, 1), date(2024, 9, 30)))
    assert calls["n"] == 1 and c.stats.retries == 0


def test_gives_up_after_max_attempts():
    c = make_client(lambda r: httpx.Response(500), max_attempts=3)
    with pytest.raises(TelemetryError, match="giving up after 3 attempts"):
        list(c.iter_pages(date(2024, 9, 1), date(2024, 9, 30)))
    assert c.stats.retries == 2


def test_network_errors_are_retried():
    calls = {"n": 0}

    def handler(request):
        calls["n"] += 1
        if calls["n"] == 1:
            raise httpx.ConnectError("connection reset")
        return httpx.Response(200, json={"data": [], "meta": {"page": {"next_cursor": None}}})

    c = make_client(handler)
    assert len(list(c.iter_pages(date(2024, 9, 1), date(2024, 9, 1)))) == 1
    assert c.stats.retry_reasons == ["ConnectError"]


def test_repeated_cursor_aborts_instead_of_looping_forever():
    c = make_client(lambda r: httpx.Response(200, json={"data": [], "meta": {"page": {"next_cursor": "same"}}}))
    with pytest.raises(TelemetryError, match="already returned"):
        list(c.iter_pages(date(2024, 9, 1), date(2024, 9, 30)))


def test_mock_api_rejects_missing_keys():
    from fastapi.testclient import TestClient

    from ingestion.telemetry.mock_observability_api import app
    r = TestClient(app).get("/api/v2/usage/services", params={"filter[from]": "2024-09-01", "filter[to]": "2024-09-02"})
    assert r.status_code == 403
