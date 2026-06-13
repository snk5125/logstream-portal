import requests

from app.cribl.discovery import CriblDiscovery


class _Resp:
    def __init__(self, status, payload):
        self.status_code, self._p = status, payload

    def json(self):
        return self._p

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")


class FakeLeader:
    """In-memory stand-in for the co-located Cribl leader's fleet read API."""

    def __init__(self, inputs, pipelines, login_fails=False, get_raises=False):
        self.inputs = inputs
        self.pipelines = pipelines
        self.login_fails = login_fails
        self.get_raises = get_raises

    def post(self, url, json=None, headers=None, timeout=None):
        if url.endswith("/auth/login"):
            if self.login_fails:
                raise requests.ConnectionError("leader unreachable")
            return _Resp(200, {"token": "t"})
        return _Resp(404, {})

    def get(self, url, headers=None, timeout=None):
        if self.get_raises:
            raise requests.ConnectionError("leader unreachable")
        if url.endswith("/system/inputs"):
            return _Resp(200, {"items": self.inputs})
        if url.endswith("/pipelines"):
            return _Resp(200, {"items": self.pipelines})
        return _Resp(404, {})


def _datagen(workload, source, eps, pipeline=...):
    pl = f"tag_{workload}_{source}" if pipeline is ... else pipeline
    src = {"id": f"ds_{workload}_{source}", "type": "datagen",
           "samples": [{"sample": f"{source}.log", "eventsPerSec": eps}],
           "sendToRoutes": True}
    if pl is not None:
        src["pipeline"] = pl
    return src


def _tagpipe(account_id, alias, workload, source, env="prod"):
    add = [{"name": "account_id", "value": f"'{account_id}'"},
           {"name": "account_alias", "value": f"'{alias}'"},
           {"name": "environment", "value": f"'{env}'"},
           {"name": "workload", "value": f"'{workload}'"},
           {"name": "source_name", "value": f"'{source}'"}]
    return {"id": f"tag_{workload}_{source}",
            "conf": {"functions": [{"id": "eval", "conf": {"add": add}}]}}


def _disc(fake, fleet="default_fleet"):
    return CriblDiscovery("http://localhost:9000", fleet, "admin", "pw", session=fake)


def test_discovers_datagen_sources_with_identity():
    inputs = [
        _datagen("storefront_web", "syslog", 4),
        _datagen("orders_api", "auth_log", 1),
        {"id": "in_system_metrics", "type": "system_metrics"},   # built-in -> filtered
        _datagen("misc", "raw", 2, pipeline="passthru"),         # no tag_ pipeline -> filtered
        _datagen("misc", "raw2", 2, pipeline=None),              # no pipeline -> filtered
    ]
    pipes = [_tagpipe("522412052544", "prod-ecommerce", "storefront_web", "syslog"),
             _tagpipe("522412052544", "prod-ecommerce", "orders_api", "auth_log")]
    out = _disc(FakeLeader(inputs, pipes)).discover()
    by = {(d["workload"], d["source_name"]): d for d in out}
    assert set(by) == {("storefront_web", "syslog"), ("orders_api", "auth_log")}
    d = by[("storefront_web", "syslog")]
    assert d["account_id"] == "522412052544"
    assert d["account_alias"] == "prod-ecommerce"
    assert d["environment"] == "prod"
    assert d["est_volume_per_min"] == 240   # 4 eps * 60


def test_strips_quotes_from_eval_literals():
    [d] = _disc(FakeLeader([_datagen("w", "s", 1)], [_tagpipe("111", "al", "w", "s")])).discover()
    assert d["account_id"] == "111" and d["account_alias"] == "al"


def test_skips_datagen_missing_identity_tags():
    pipe = {"id": "tag_w_s", "conf": {"functions": [
        {"id": "eval", "conf": {"add": [{"name": "environment", "value": "'prod'"}]}}]}}
    assert _disc(FakeLeader([_datagen("w", "s", 1)], [pipe])).discover() == []


def test_missing_pipeline_object_skipped():
    # input references tag_w_s but the pipeline list is empty
    assert _disc(FakeLeader([_datagen("w", "s", 1)], [])).discover() == []


def test_login_failure_returns_empty_never_raises():
    assert _disc(FakeLeader([], [], login_fails=True)).discover() == []


def test_get_failure_returns_empty_never_raises():
    assert _disc(FakeLeader([], [], get_raises=True)).discover() == []
