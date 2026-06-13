import json
from datetime import datetime, timezone

from app.catalog.cache import SnapshotCache
from app.catalog.uc_client import CatalogUnavailable


def find_source(tree: dict, fqn: str) -> dict | None:
    """Resolve a source FQN to the tags the fork filter needs."""
    for account in tree["accounts"]:
        for workload in account["workloads"]:
            for src in workload["sources"]:
                if src["fqn"] == fqn:
                    return {
                        "fqn": fqn,
                        "account_id": account["account_id"],
                        "workload_tag": workload["name"],
                        "source_name": src["name"],
                        "sensitivity": src["sensitivity"],
                    }
    return None


def scope_tree(tree: dict, account_scope: str | None) -> dict:
    """Return the tree filtered to one account; None scope = unscoped (admin)."""
    if account_scope is None:
        return tree
    return {
        **tree,
        "accounts": [a for a in tree["accounts"] if a["account_id"] == account_scope],
    }


def annotate(tree: dict, sub_rows: list[dict]) -> dict:
    """Return a deep copy of the tree with per-user subscription refs on each source."""
    by_fqn: dict[str, list[dict]] = {}
    for row in sub_rows:
        by_fqn.setdefault(row["source_fqn"], []).append(
            {"stream_id": row["stream_id"], "stream_name": row["stream_name"], "status": row["status"]}
        )
    out = json.loads(json.dumps(tree))
    for account in out["accounts"]:
        for workload in account["workloads"]:
            for src in workload["sources"]:
                src["subscriptions"] = by_fqn.get(src["fqn"], [])
    return out


class CatalogService:
    def __init__(self, uc_client, cache: SnapshotCache, catalog_name: str):
        self._uc = uc_client
        self._cache = cache
        self._catalog_name = catalog_name

    def get_tree(self) -> dict:
        try:
            accounts = self._build()
        except CatalogUnavailable:
            snap = self._cache.load()
            if snap is None:
                raise
            return {**snap, "stale": True}
        snap = {
            "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "accounts": accounts,
        }
        self._cache.save(snap)
        return {**snap, "stale": False}

    def _build(self) -> list[dict]:
        accounts: dict[str, dict] = {}
        for schema in self._uc.list_schemas(self._catalog_name):
            schema_name = schema["name"]
            if schema_name == "information_schema":
                continue
            tables = self._uc.list_tables(self._catalog_name, schema_name)
            if not tables:
                continue
            # all tables in a schema share the same account/workload properties (seeded that way)
            props = tables[0].get("properties") or {}
            account_id = props.get("account_id", "unknown")
            account = accounts.setdefault(account_id, {
                "account_id": account_id,
                "account_alias": props.get("account_alias", account_id),
                "workloads": [],
            })
            workload = {
                "name": props.get("workload", schema_name.split("__", 1)[-1]),
                "schema": schema_name,
                "environment": props.get("environment", "prod"),
                "sources": [],
            }
            for table in tables:
                tprops = table.get("properties") or {}
                workload["sources"].append({
                    "fqn": f'{self._catalog_name}.{schema_name}.{table["name"]}',
                    "name": table["name"],
                    "log_type": tprops.get("log_type", "system"),
                    "sensitivity": tprops.get("sensitivity", "standard"),
                    "est_volume_per_min": int(tprops.get("est_volume_per_min") or 0),
                    "description": table.get("comment", ""),
                    "columns": [
                        {"name": c["name"], "type": c.get("type_text", "")}
                        for c in table.get("columns", [])
                    ],
                })
            workload["sources"].sort(key=lambda s: s["name"])
            account["workloads"].append(workload)
        out = sorted(accounts.values(), key=lambda a: a["account_id"])
        for account in out:
            account["workloads"].sort(key=lambda w: w["name"])
        return out
