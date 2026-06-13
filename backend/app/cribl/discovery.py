"""Read-only discovery of managed Cribl Edge sources from the co-located leader.

Edge Nodes enroll in managed mode into the single `default_fleet` (Cribl Free
permits one fleet). Each demo source is a `datagen` input whose pre-processing
`pipeline` (`tag_{workload}_{source_name}`) stamps the source identity as literal
Eval fields (account_id, account_alias, environment, workload, source_name).
Discovery reads the fleet's inputs + pipelines from the leader and recovers the
`(account_id, workload, source_name)` tuples so newly-collected sources surface
in the portal catalog.

Best-effort: any failure (unreachable leader, malformed config) yields an empty
list and a logged warning — it never raises, so the catalog keeps serving UC.
"""
import logging

import requests

logger = logging.getLogger(__name__)


def _unquote(value: str) -> str:
    """Cribl Eval values are JS literals; identity tags are single-quoted strings."""
    v = value.strip()
    if len(v) >= 2 and v[0] == v[-1] and v[0] in ("'", '"'):
        return v[1:-1]
    return v


class CriblDiscovery:
    def __init__(self, base_url, fleet, username, password, session=None, timeout=10.0):
        self._base = base_url.rstrip("/")
        self._fleet = fleet
        self._user = username
        self._pw = password
        self._s = session or requests.Session()
        self._timeout = timeout

    def discover(self) -> list[dict]:
        try:
            h = self._login()
            inputs = self._get(f"{self._g()}/system/inputs", h).get("items", [])
            pipelines = {p.get("id"): p
                         for p in self._get(f"{self._g()}/pipelines", h).get("items", [])}
        except (requests.RequestException, KeyError, ValueError, TypeError) as exc:
            logger.warning("cribl discovery failed: %s", exc, exc_info=True)
            return []

        out = []
        for inp in inputs:
            if inp.get("type") != "datagen":
                continue
            pid = inp.get("pipeline") or ""
            if not pid.startswith("tag_"):
                continue
            tags = self._tags_from_pipeline(pipelines.get(pid))
            account_id, workload, source_name = (
                tags.get("account_id"), tags.get("workload"), tags.get("source_name"))
            if not (account_id and workload and source_name):
                logger.warning("cribl discovery: input %s (pipeline %s) missing identity tags",
                               inp.get("id"), pid)
                continue
            out.append({
                "account_id": account_id,
                "account_alias": tags.get("account_alias", account_id),
                "workload": workload,
                "source_name": source_name,
                "environment": tags.get("environment", "prod"),
                "est_volume_per_min": self._eps(inp) * 60,
            })
        return out

    # ── helpers ──
    def _g(self) -> str:
        return f"{self._base}/api/v1/m/{self._fleet}"

    def _login(self) -> dict:
        r = self._s.post(f"{self._base}/api/v1/auth/login",
                         json={"username": self._user, "password": self._pw}, timeout=10)
        r.raise_for_status()
        return {"Authorization": f"Bearer {r.json()['token']}"}

    def _get(self, url, h) -> dict:
        r = self._s.get(url, headers=h, timeout=self._timeout)
        r.raise_for_status()
        return r.json()

    @staticmethod
    def _eps(inp) -> int:
        try:
            return int(inp.get("samples", [{}])[0].get("eventsPerSec") or 0)
        except (IndexError, TypeError, ValueError):
            return 0

    @staticmethod
    def _tags_from_pipeline(pipe) -> dict:
        if not pipe:
            return {}
        tags = {}
        for fn in (pipe.get("conf", {}).get("functions") or []):
            if fn.get("id") != "eval":
                continue
            for entry in (fn.get("conf", {}).get("add") or []):
                name, val = entry.get("name"), entry.get("value")
                if name is not None and val is not None:
                    tags[name] = _unquote(str(val))
        return tags
