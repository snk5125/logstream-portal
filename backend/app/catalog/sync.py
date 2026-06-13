"""Background poll that keeps `discovered_sources` current from Cribl.

`sync_once()` runs one discovery→upsert pass and never propagates errors, so the
poll loop survives a transient Cribl/leader outage. `start_poller()` runs it on an
interval in a daemon thread until its stop Event is set (on app shutdown).
"""
import logging
import threading

from app.db import upsert_discovered

logger = logging.getLogger(__name__)


class CatalogSyncService:
    def __init__(self, conn, discovery):
        self._conn = conn
        self._discovery = discovery

    def sync_once(self) -> int:
        try:
            rows = self._discovery.discover()
        except Exception:
            logger.warning("catalog sync: discovery failed", exc_info=True)
            return 0
        n = 0
        for row in rows:
            try:
                upsert_discovered(self._conn, row)
                n += 1
            except Exception:
                logger.warning("catalog sync: upsert failed for %r", row, exc_info=True)
        if n:
            logger.info("catalog sync: %d discovered source(s) upserted", n)
        return n


def start_poller(sync: CatalogSyncService, interval: float,
                 stop_event: threading.Event) -> threading.Thread:
    """Run sync_once() immediately, then every `interval` seconds until stopped."""
    def loop() -> None:
        while not stop_event.is_set():
            sync.sync_once()
            stop_event.wait(interval)

    thread = threading.Thread(target=loop, name="catalog-sync", daemon=True)
    thread.start()
    return thread
