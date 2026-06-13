import json
from pathlib import Path

import pytest

from app.catalog.cache import SnapshotCache
from app.catalog.service import CatalogService, annotate, find_source, scope_tree
from app.catalog.uc_client import CatalogUnavailable

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "catalog_snapshot.json"


class FakeUC:
    """Returns UC-API-shaped payloads for one account/workload/source."""

    def list_schemas(self, catalog):
        return [{"name": "acct_b__orders_api"}, {"name": "information_schema"}]

    def list_tables(self, catalog, schema):
        if schema == "information_schema":
            raise AssertionError("information_schema must be skipped")
        return [
            {
                "name": "auth_log",
                "comment": "SSH/sudo/PAM auth events",
                "properties": {
                    "sensitivity": "sensitive", "log_type": "system",
                    "account_id": "522412052544", "account_alias": "prod-ecommerce",
                    "workload": "orders_api", "environment": "prod",
                    "est_volume_per_min": "300",
                },
                "columns": [{"name": "ts", "type_text": "timestamp"}],
            },
            {
                "name": "syslog",
                "comment": "Host syslog",
                "properties": {
                    "sensitivity": "standard", "log_type": "system",
                    "account_id": "522412052544", "account_alias": "prod-ecommerce",
                    "workload": "orders_api", "environment": "prod",
                    "est_volume_per_min": "900",
                },
                "columns": [{"name": "ts", "type_text": "timestamp"}],
            },
        ]


class DownUC:
    def list_schemas(self, catalog):
        raise CatalogUnavailable("databricks unreachable")

    def list_tables(self, catalog, schema):
        raise CatalogUnavailable("databricks unreachable")


def test_builds_tree_grouped_by_account_and_saves_snapshot(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json")
    tree = CatalogService(FakeUC(), cache, "logging_demo").get_tree()
    assert tree["stale"] is False
    assert tree["as_of"]
    [account] = tree["accounts"]
    assert account["account_id"] == "522412052544"
    [workload] = account["workloads"]
    assert workload["name"] == "orders_api"
    assert [s["name"] for s in workload["sources"]] == ["auth_log", "syslog"]
    src = workload["sources"][0]
    assert src["fqn"] == "logging_demo.acct_b__orders_api.auth_log"
    assert src["sensitivity"] == "sensitive"
    assert src["est_volume_per_min"] == 300
    assert (tmp_path / "snap.json").exists()


def test_falls_back_to_cached_snapshot_when_uc_down(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json")
    CatalogService(FakeUC(), cache, "logging_demo").get_tree()  # warm the cache
    tree = CatalogService(DownUC(), cache, "logging_demo").get_tree()
    assert tree["stale"] is True
    assert tree["accounts"][0]["account_id"] == "522412052544"


def test_falls_back_to_bundled_seed_when_no_local_cache(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json", seed_path=FIXTURE)
    tree = CatalogService(DownUC(), cache, "logging_demo").get_tree()
    assert tree["stale"] is True
    assert len(tree["accounts"]) == 2


def test_raises_when_uc_down_and_no_cache_at_all(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json")
    with pytest.raises(CatalogUnavailable):
        CatalogService(DownUC(), cache, "logging_demo").get_tree()


def test_find_source_returns_filter_tags():
    tree = json.loads(FIXTURE.read_text())
    found = find_source(tree, "logging_demo.acct_c__identity_svc.auth_log")
    assert found == {
        "fqn": "logging_demo.acct_c__identity_svc.auth_log",
        "account_id": "624627265315",
        "workload_tag": "identity_svc",
        "source_name": "auth_log",
        "sensitivity": "sensitive",
    }
    assert find_source(tree, "logging_demo.nope.nope") is None


def test_annotate_merges_subscriptions_without_mutating_input():
    tree = json.loads(FIXTURE.read_text())
    rows = [{
        "source_fqn": "logging_demo.acct_b__orders_api.syslog",
        "stream_id": 7, "stream_name": "team-a-logs", "status": "active",
    }]
    out = annotate(tree, rows)
    orders = out["accounts"][0]["workloads"][0]
    syslog = [s for s in orders["sources"] if s["name"] == "syslog"][0]
    assert syslog["subscriptions"] == [
        {"stream_id": 7, "stream_name": "team-a-logs", "status": "active"}
    ]
    auth = [s for s in orders["sources"] if s["name"] == "auth_log"][0]
    assert auth["subscriptions"] == []
    assert "subscriptions" not in tree["accounts"][0]["workloads"][0]["sources"][0]


def test_corrupt_local_cache_falls_through_to_seed(tmp_path):
    corrupt = tmp_path / "snap.json"
    corrupt.write_text("{not json")
    cache = SnapshotCache(corrupt, seed_path=FIXTURE)
    tree = CatalogService(DownUC(), cache, "logging_demo").get_tree()
    assert tree["stale"] is True
    assert len(tree["accounts"]) == 2


def test_scope_tree_filters_to_matching_account():
    tree = json.loads(FIXTURE.read_text())
    scoped = scope_tree(tree, "522412052544")
    assert [a["account_id"] for a in scoped["accounts"]] == ["522412052544"]
    assert "as_of" in scoped


def test_scope_tree_none_returns_everything_unchanged():
    tree = json.loads(FIXTURE.read_text())
    assert scope_tree(tree, None) == tree
