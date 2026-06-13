"""Thin read-only client for the Unity Catalog REST API."""
import requests


class CatalogUnavailable(RuntimeError):
    """Databricks could not be reached or refused the request."""


class UCClient:
    def __init__(self, host: str, token: str, session=None):
        self._base = host.rstrip("/")
        self._token = token
        self._session = session or requests.Session()

    def _get(self, path: str, params: dict) -> dict:
        try:
            resp = self._session.get(
                self._base + path,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:  # Also catches requests.exceptions.JSONDecodeError (a RequestException subclass)
            raise CatalogUnavailable(str(exc)) from exc

    def list_schemas(self, catalog: str) -> list[dict]:
        # NOTE: no pagination; demo inventory stays far below one page
        payload = self._get("/api/2.1/unity-catalog/schemas", {"catalog_name": catalog})
        return payload.get("schemas", [])

    def list_tables(self, catalog: str, schema: str) -> list[dict]:
        # NOTE: no pagination; demo inventory stays far below one page
        payload = self._get(
            "/api/2.1/unity-catalog/tables",
            {"catalog_name": catalog, "schema_name": schema},
        )
        return payload.get("tables", [])
