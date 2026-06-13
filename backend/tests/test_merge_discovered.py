import copy

from app.catalog.service import merge_discovered


def _uc_source(name, sensitivity="standard"):
    return {"fqn": f"logging_demo.acct_b__storefront_web.{name}", "name": name,
            "log_type": "web", "sensitivity": sensitivity, "est_volume_per_min": 600,
            "description": "uc source", "columns": [{"name": "ts", "type": "timestamp"}]}


def _tree():
    return {"as_of": "2026-06-13T00:00:00+00:00", "stale": False, "accounts": [
        {"account_id": "522412052544", "account_alias": "prod-ecommerce", "workloads": [
            {"name": "storefront_web", "schema": "acct_b__storefront_web", "environment": "prod",
             "sources": [_uc_source("syslog")]}]}]}


def _row(account="522412052544", alias="prod-ecommerce", workload="storefront_web",
         source="new_log", env="prod", vol=300):
    return {"account_id": account, "account_alias": alias, "workload": workload,
            "source_name": source, "environment": env, "est_volume_per_min": vol}


def test_uc_wins_no_duplicate_or_override():
    tree = _tree()
    # discovered row for the SAME tuple that UC already has
    out = merge_discovered(tree, [_row(source="syslog", vol=999)])
    srcs = out["accounts"][0]["workloads"][0]["sources"]
    assert [s["name"] for s in srcs] == ["syslog"]
    assert srcs[0]["sensitivity"] == "standard"          # UC value untouched
    assert srcs[0]["fqn"].startswith("logging_demo.")     # UC fqn, not cribl://


def test_new_source_into_existing_workload():
    out = merge_discovered(_tree(), [_row(source="metrics_log", vol=120)])
    srcs = out["accounts"][0]["workloads"][0]["sources"]
    names = [s["name"] for s in srcs]
    assert names == sorted(names) and "metrics_log" in names
    injected = next(s for s in srcs if s["name"] == "metrics_log")
    assert injected["fqn"] == "cribl://522412052544/storefront_web/metrics_log"
    assert injected["sensitivity"] == "sensitive"
    assert injected["origin"] == "cribl"
    assert injected["log_type"] == "unknown"
    assert injected["est_volume_per_min"] == 120
    assert injected["columns"] == []


def test_new_workload_into_existing_account():
    out = merge_discovered(_tree(), [_row(workload="orders_api", source="auth_log")])
    wls = out["accounts"][0]["workloads"]
    assert [w["name"] for w in wls] == sorted(w["name"] for w in wls)
    new = next(w for w in wls if w["name"] == "orders_api")
    assert new["schema"] == "cribl__orders_api"
    assert new["sources"][0]["fqn"] == "cribl://522412052544/orders_api/auth_log"


def test_new_account_entirely():
    out = merge_discovered(_tree(), [_row(account="624627265315", alias="prod-platform",
                                          workload="identity_svc", source="syslog")])
    ids = [a["account_id"] for a in out["accounts"]]
    assert ids == sorted(ids) and "624627265315" in ids
    acct = next(a for a in out["accounts"] if a["account_id"] == "624627265315")
    assert acct["account_alias"] == "prod-platform"
    assert acct["workloads"][0]["sources"][0]["origin"] == "cribl"


def test_pure_and_idempotent():
    tree = _tree()
    snapshot = copy.deepcopy(tree)
    rows = [_row(source="metrics_log")]
    out1 = merge_discovered(tree, rows)
    out2 = merge_discovered(out1, rows)   # re-merging is a no-op (UC-wins on the now-present tuple)
    assert tree == snapshot               # input never mutated
    assert out1 == out2
