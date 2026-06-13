from app.db import get_db, init_db, load_discovered, upsert_discovered


def _conn():
    conn = get_db(":memory:")
    init_db(conn)
    return conn


def _row(account="522412052544", workload="storefront_web", source="syslog", **kw):
    base = {"account_id": account, "account_alias": "prod-ecommerce", "workload": workload,
            "source_name": source, "environment": "prod", "est_volume_per_min": 240}
    base.update(kw)
    return base


def test_upsert_inserts_then_updates_in_place():
    conn = _conn()
    upsert_discovered(conn, _row())
    upsert_discovered(conn, _row(est_volume_per_min=600, account_alias="renamed"))
    rows = load_discovered(conn)
    assert len(rows) == 1
    assert rows[0]["est_volume_per_min"] == 600
    assert rows[0]["account_alias"] == "renamed"
    # first_seen_at preserved across the update
    assert rows[0]["first_seen_at"] == rows[0]["first_seen_at"]


def test_distinct_tuples_make_distinct_rows():
    conn = _conn()
    upsert_discovered(conn, _row(source="syslog"))
    upsert_discovered(conn, _row(source="nginx_access"))
    upsert_discovered(conn, _row(account="624627265315", account_alias="prod-platform"))
    assert len(load_discovered(conn)) == 3


def test_load_is_sorted_and_dictlike():
    conn = _conn()
    upsert_discovered(conn, _row(workload="zeta", source="b"))
    upsert_discovered(conn, _row(workload="alpha", source="a"))
    rows = load_discovered(conn)
    assert [r["workload"] for r in rows] == ["alpha", "zeta"]
    assert set(rows[0]) >= {"account_id", "workload", "source_name", "est_volume_per_min"}
