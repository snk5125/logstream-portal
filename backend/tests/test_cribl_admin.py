import pytest
import requests

from app.cribl.admin import CriblAdmin, CriblApplyError


class _Resp:
    def __init__(self, status, payload):
        self.status_code, self._p = status, payload

    def json(self):
        return self._p

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeCribl:
    """In-memory stand-in for the Cribl leader REST API (group=default)."""

    def __init__(self, deploy_fails=False, login_fails=False):
        self.fork_routes = []                       # list of fork_* route dicts
        self.other_routes = [{"id": "archive", "filter": "true", "output": "archive"}]
        self.destinations = {"archive": {"id": "archive", "type": "s3"}}
        self.commits = []
        self.deployed_version = "c0"
        self.deploy_fails = deploy_fails
        self.login_fails = login_fails
        self.calls = []

    def _routes_doc(self):
        return {"items": [{"id": "default", "routes": self.other_routes + self.fork_routes}], "count": 1}

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(("POST", url))
        if url.endswith("/auth/login"):
            return _Resp(401 if self.login_fails else 200, {} if self.login_fails else {"token": "t"})
        if url.endswith("/version/commit"):
            cid = f"c{len(self.commits)+1}"
            self.commits.append(cid)
            return _Resp(200, {"items": [{"commit": cid}]})
        if "/system/outputs" in url:        # create destination
            self.destinations[json["id"]] = json
            return _Resp(200, {"items": [json]})
        return _Resp(404, {})

    def patch(self, url, json=None, headers=None, timeout=None):
        self.calls.append(("PATCH", url))
        if url.endswith("/routes/default"):
            self.fork_routes = [r for r in json["routes"] if r["id"].startswith("fork_")]
            self.other_routes = [r for r in json["routes"] if not r["id"].startswith("fork_")]
            return _Resp(200, {"items": [json]})
        if "/system/outputs/" in url:       # update destination
            self.destinations[json["id"]] = json
            return _Resp(200, {"items": [json]})
        if "/deploy" in url:
            if self.deploy_fails:
                return _Resp(500, {})
            self.deployed_version = json["version"]
            return _Resp(200, {"items": [{"id": "default", "deployedVersion": self.deployed_version}]})
        return _Resp(404, {})

    def delete(self, url, headers=None, timeout=None):
        self.calls.append(("DELETE", url))
        oid = url.rstrip("/").split("/")[-1]
        self.destinations.pop(oid, None)
        return _Resp(200, {})

    def get(self, url, headers=None, timeout=None):
        self.calls.append(("GET", url))
        if url.endswith("/routes"):
            return _Resp(200, self._routes_doc())
        if url.endswith("/system/outputs"):
            return _Resp(200, {"items": list(self.destinations.values())})
        if url.endswith("/master/groups/default"):
            return _Resp(200, {"items": [{"id": "default", "deployedVersion": self.deployed_version}]})
        return _Resp(404, {})


def _admin(fake):
    return CriblAdmin("http://leader:9000", "default", "admin", "pw",
                      session=fake, poll_interval=0.001, timeout=0.05)


def test_apply_upserts_route_and_destination_then_commits_and_deploys():
    fake = FakeCribl()
    _admin(fake).apply([{"id": "fork_3", "output": "fork_3_dest"}], [{"id": "fork_3_dest", "type": "kinesis"}])
    assert any(r["id"] == "fork_3" for r in fake.fork_routes)
    assert "fork_3_dest" in fake.destinations
    assert fake.deployed_version == fake.commits[-1]


def test_archive_route_and_destination_preserved():
    fake = FakeCribl()
    _admin(fake).apply([{"id": "fork_3", "output": "fork_3_dest"}], [{"id": "fork_3_dest", "type": "kinesis"}])
    assert any(r["id"] == "archive" for r in fake.other_routes)
    assert "archive" in fake.destinations


def test_stale_fork_objects_removed():
    fake = FakeCribl()
    fake.fork_routes = [{"id": "fork_9", "output": "fork_9_dest"}]
    fake.destinations["fork_9_dest"] = {"id": "fork_9_dest"}
    _admin(fake).apply([{"id": "fork_3", "output": "fork_3_dest"}], [{"id": "fork_3_dest"}])
    assert all(r["id"] != "fork_9" for r in fake.fork_routes)
    assert "fork_9_dest" not in fake.destinations
    assert "fork_3" in [r["id"] for r in fake.fork_routes]


def test_new_destination_created_via_post_existing_via_patch():
    """A brand-new fork output must be created with POST /system/outputs;
    an already-present fork output must be updated with PATCH /system/outputs/{id}.
    (PUT to /system/outputs/{id} is unsupported by the real leader.)"""
    fake = FakeCribl()
    # fork_existing already lives on the leader; fork_new does not.
    fake.destinations["fork_existing"] = {"id": "fork_existing", "type": "kinesis"}
    _admin(fake).apply(
        [],
        [{"id": "fork_existing", "type": "kinesis", "v": 2},
         {"id": "fork_new", "type": "kinesis"}],
    )
    assert ("POST", "http://leader:9000/api/v1/m/default/system/outputs") in fake.calls
    assert ("PATCH", "http://leader:9000/api/v1/m/default/system/outputs/fork_existing") in fake.calls
    # No PUT verb is ever issued.
    assert all(verb != "PUT" for verb, _ in fake.calls)
    assert fake.destinations["fork_existing"]["v"] == 2
    assert "fork_new" in fake.destinations


def test_apply_empty_removes_all_forks_keeps_archive():
    fake = FakeCribl()
    fake.fork_routes = [{"id": "fork_1", "output": "fork_1_dest"}]
    fake.destinations["fork_1_dest"] = {"id": "fork_1_dest"}
    _admin(fake).apply([], [])
    assert fake.fork_routes == []
    assert any(r["id"] == "archive" for r in fake.other_routes)


def test_deploy_failure_rolls_back_and_raises():
    fake = FakeCribl(deploy_fails=True)
    with pytest.raises(CriblApplyError):
        _admin(fake).apply([{"id": "fork_3", "output": "fork_3_dest"}], [{"id": "fork_3_dest"}])


def test_login_failure_raises():
    fake = FakeCribl(login_fails=True)
    with pytest.raises(CriblApplyError):
        _admin(fake).apply([], [])


def test_fork_routes_prepended_before_archive_in_patch_payload():
    """The PATCH body must list fork routes before the final archive route,
    or Cribl's final=true archive route would short-circuit every fork."""
    fake = FakeCribl()
    captured = {}
    real_patch = fake.patch

    def spy(url, json=None, headers=None, timeout=None):
        if url.endswith("/routes/default"):
            captured["routes"] = [r["id"] for r in json["routes"]]
        return real_patch(url, json=json, headers=headers, timeout=timeout)

    fake.patch = spy
    _admin(fake).apply([{"id": "fork_3", "output": "fork_3_dest"}], [{"id": "fork_3_dest"}])
    assert captured["routes"][0] == "fork_3"
    assert "archive" in captured["routes"]
    assert captured["routes"].index("fork_3") < captured["routes"].index("archive")


def test_stale_output_deleted_after_route_rewrite():
    # A removed fork's output must be deleted only AFTER the routes table that
    # referenced it is rewritten, or Cribl returns 500.
    fake = FakeCribl()
    fake.destinations["fork_9_dest"] = {"id": "fork_9_dest"}
    fake.fork_routes = [{"id": "fork_9", "output": "fork_9_dest"}]
    admin = _admin(fake)
    admin.apply([], [])  # desired set is empty -> fork_9 must be removed
    verbs = [(m, u) for (m, u) in fake.calls]
    route_patch = next(i for i, (m, u) in enumerate(verbs)
                       if m == "PATCH" and u.endswith("/routes/default"))
    stale_delete = next(i for i, (m, u) in enumerate(verbs)
                        if m == "DELETE" and u.endswith("/system/outputs/fork_9_dest"))
    assert route_patch < stale_delete
    assert "fork_9_dest" not in fake.destinations
