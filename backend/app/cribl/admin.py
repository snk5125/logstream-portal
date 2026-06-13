"""Apply fork Routes+Destinations to the Cribl `default` worker group.

A fork mutation: upsert desired fork_* destinations, splice the fork_* routes
into the group's single route table (preserving non-fork routes), commit, deploy
the new commit, and poll until the group's deployedVersion equals it. On any
failure, redeploy the previously deployed version (git-native rollback) and
raise. Only fork_* objects are portal-managed; the static archive route and any
other config is preserved.
"""
import time

import requests


class CriblApplyError(RuntimeError):
    """Cribl rejected the config change or did not converge in time."""


class CriblAdmin:
    def __init__(self, base_url, group, username, password,
                 session=None, poll_interval=1.0, timeout=30.0):
        self._base = base_url.rstrip("/")
        self._group = group
        self._user = username
        self._pw = password
        self._s = session or requests.Session()
        self._poll = poll_interval
        self._timeout = timeout

    def apply(self, routes, destinations) -> None:
        h = self._login()
        g = f"{self._base}/api/v1/m/{self._group}"
        previous = next(iter(self._config_versions(h)), None)
        try:
            # Order matters: create/update outputs first so new routes can
            # reference them, then rewrite the route table, then delete
            # now-unreferenced stale outputs. Deleting an output a route still
            # points at returns 500.
            self._upsert_destinations(g, h, destinations)
            self._reconcile_routes(g, h, routes)
            self._delete_stale_destinations(g, h, destinations)
            commit = self._commit(h)
            self._deploy(h, commit)
            self._await_version(h, commit)
        except CriblApplyError:
            self._rollback(h, previous)
            raise
        except requests.RequestException as exc:
            self._rollback(h, previous)
            raise CriblApplyError(f"cribl apply failed: {exc}") from exc

    # ── auth ──
    def _login(self) -> dict:
        try:
            r = self._s.post(f"{self._base}/api/v1/auth/login",
                             json={"username": self._user, "password": self._pw}, timeout=10)
            r.raise_for_status()
            return {"Authorization": f"Bearer {r.json()['token']}"}
        except (requests.RequestException, KeyError) as exc:
            raise CriblApplyError(f"cribl login failed: {exc}") from exc

    # ── destinations (individual) ──
    def _existing_fork_outputs(self, g, h) -> set:
        return {d["id"] for d in self._get(f"{g}/system/outputs", h).get("items", [])
                if str(d.get("id", "")).startswith("fork_")}

    def _upsert_destinations(self, g, h, destinations):
        existing = self._existing_fork_outputs(g, h)
        for d in destinations:
            if d["id"] in existing:
                self._s.patch(f"{g}/system/outputs/{d['id']}", json=d, headers=h, timeout=10).raise_for_status()
            else:
                self._s.post(f"{g}/system/outputs", json=d, headers=h, timeout=10).raise_for_status()

    def _delete_stale_destinations(self, g, h, destinations):
        desired = {d["id"] for d in destinations}
        for stale in self._existing_fork_outputs(g, h) - desired:
            self._s.delete(f"{g}/system/outputs/{stale}", headers=h, timeout=10).raise_for_status()

    # ── routes (single table) ──
    def _reconcile_routes(self, g, h, routes):
        doc = self._get(f"{g}/routes", h)
        table = doc["items"][0]
        kept = [r for r in table["routes"] if not str(r.get("id", "")).startswith("fork_")]
        table["routes"] = list(routes) + kept
        self._s.patch(f"{g}/routes/default", json=table, headers=h, timeout=10).raise_for_status()

    # ── commit / deploy / verify ──
    def _commit(self, h) -> str:
        r = self._s.post(f"{self._base}/api/v1/version/commit",
                         json={"message": "logstream-portal fork change", "group": self._group},
                         headers=h, timeout=15)
        r.raise_for_status()
        items = r.json().get("items") or []
        if not items:
            raise CriblApplyError("commit returned no commit id")
        return items[0]["commit"]

    def _deploy(self, h, version) -> None:
        r = self._s.patch(f"{self._base}/api/v1/master/groups/{self._group}/deploy",
                          json={"version": version}, headers=h, timeout=15)
        if r.status_code >= 400:
            raise CriblApplyError(f"deploy rejected: HTTP {r.status_code}")

    def _config_versions(self, h) -> set:
        # Cribl versions differ on which field tracks an applied deploy:
        # some populate ``configVersion`` (commit on the leader), others
        # ``deployedVersion`` (worker-applied). Accept a match on either.
        try:
            items = self._get(f"{self._base}/api/v1/master/groups/{self._group}", h).get("items", [])
        except requests.RequestException:
            return set()
        if not items:
            return set()
        g = items[0]
        return {v for v in (g.get("configVersion"), g.get("deployedVersion")) if v}

    def _await_version(self, h, version) -> None:
        deadline = time.monotonic() + self._timeout
        while True:
            if version in self._config_versions(h):
                return
            if time.monotonic() >= deadline:
                break
            time.sleep(self._poll)
        raise CriblApplyError(f"group did not converge to {version} within {self._timeout}s")

    def _rollback(self, h, version) -> None:
        if version is None:
            return
        try:
            self._deploy(h, version)
        except Exception:
            pass

    def _get(self, url, h) -> dict:
        r = self._s.get(url, headers=h, timeout=10)
        r.raise_for_status()
        return r.json()
