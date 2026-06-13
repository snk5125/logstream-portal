import pytest
import requests

from app.catalog.uc_client import CatalogUnavailable, UCClient


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payloads=None, error=None):
        self.calls, self._payloads, self._error = [], list(payloads or []), error

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        if self._error:
            raise self._error
        return FakeResponse(self._payloads.pop(0))


def test_list_schemas_hits_uc_endpoint_with_bearer_token():
    session = FakeSession(payloads=[{"schemas": [{"name": "acct_a__orders_api"}]}])
    client = UCClient("https://dbc-1.cloud.databricks.com/", "tok123", session=session)
    schemas = client.list_schemas("logging_demo")
    assert schemas == [{"name": "acct_a__orders_api"}]
    call = session.calls[0]
    assert call["url"] == "https://dbc-1.cloud.databricks.com/api/2.1/unity-catalog/schemas"
    assert call["params"] == {"catalog_name": "logging_demo"}
    assert call["headers"]["Authorization"] == "Bearer tok123"


def test_list_tables_passes_schema_and_returns_empty_when_missing_key():
    session = FakeSession(payloads=[{}])
    client = UCClient("https://h", "t", session=session)
    assert client.list_tables("logging_demo", "acct_a__orders_api") == []
    assert session.calls[0]["params"] == {
        "catalog_name": "logging_demo",
        "schema_name": "acct_a__orders_api",
    }


def test_network_error_raises_catalog_unavailable():
    session = FakeSession(error=requests.ConnectionError("no route"))
    client = UCClient("https://h", "t", session=session)
    with pytest.raises(CatalogUnavailable):
        client.list_schemas("logging_demo")


def test_http_error_raises_catalog_unavailable():
    class ErrSession(FakeSession):
        def __init__(self):
            super().__init__()

        def get(self, url, params=None, headers=None, timeout=None):
            return FakeResponse({}, status=403)

    client = UCClient("https://h", "t", session=ErrSession())
    with pytest.raises(CatalogUnavailable):
        client.list_schemas("logging_demo")
