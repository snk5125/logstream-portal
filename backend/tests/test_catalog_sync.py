import threading

from app.catalog.sync import CatalogSyncService, start_poller
from app.db import get_db, init_db, load_discovered


def _conn():
    conn = get_db(":memory:")
    init_db(conn)
    return conn


class FakeDiscovery:
    def __init__(self, rows, raises=False):
        self.rows, self.raises, self.calls = rows, raises, 0

    def discover(self):
        self.calls += 1
        if self.raises:
            raise RuntimeError("cribl down")
        return self.rows


def _row(source="syslog", vol=240):
    return {"account_id": "522412052544", "account_alias": "prod-ecommerce",
            "workload": "storefront_web", "source_name": source,
            "environment": "prod", "est_volume_per_min": vol}


def test_sync_once_upserts_rows():
    conn = _conn()
    n = CatalogSyncService(conn, FakeDiscovery([_row("syslog"), _row("nginx_access")])).sync_once()
    assert n == 2
    assert {r["source_name"] for r in load_discovered(conn)} == {"syslog", "nginx_access"}


def test_sync_once_is_idempotent_upsert():
    conn = _conn()
    sync = CatalogSyncService(conn, FakeDiscovery([_row("syslog", vol=240)]))
    sync.sync_once()
    sync._discovery.rows = [_row("syslog", vol=999)]
    sync.sync_once()
    rows = load_discovered(conn)
    assert len(rows) == 1 and rows[0]["est_volume_per_min"] == 999


def test_sync_once_swallows_discovery_errors():
    conn = _conn()
    assert CatalogSyncService(conn, FakeDiscovery([], raises=True)).sync_once() == 0
    assert load_discovered(conn) == []


def test_start_poller_runs_then_stops():
    conn = _conn()
    disc = FakeDiscovery([_row("syslog")])
    ran = threading.Event()

    class Once(CatalogSyncService):
        def sync_once(self):
            n = super().sync_once()
            ran.set()
            return n

    stop = threading.Event()
    thread = start_poller(Once(conn, disc), interval=0.01, stop_event=stop)
    assert ran.wait(timeout=2.0)
    stop.set()
    thread.join(timeout=2.0)
    assert not thread.is_alive()
    assert load_discovered(conn)[0]["source_name"] == "syslog"
