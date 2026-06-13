from app.db import upsert_discovered
from tests.conftest import AUTH_LOG, IDENTITY_AUTH, SYSLOG, login


def test_personas_lists_seeded_users_without_auth(client):
    resp = client.get("/api/personas")
    assert resp.status_code == 200
    ids = [u["id"] for u in resp.json()]
    assert "dana@app-team" in ids and "admin@platform" in ids


def test_login_sets_cookie_and_me_returns_user(client):
    user = login(client)
    assert user["role"] == "consumer"
    me = client.get("/api/session")
    assert me.status_code == 200
    assert me.json()["id"] == "dana@app-team"


def test_login_unknown_user_is_404(client):
    assert client.post("/api/session", json={"user_id": "ghost@nowhere"}).status_code == 404


def test_me_without_session_is_401(client):
    assert client.get("/api/session").status_code == 401


def test_logout_clears_session(client):
    login(client)
    assert client.delete("/api/session").status_code == 200
    assert client.get("/api/session").status_code == 401


def test_tampered_cookie_is_401(client):
    login(client)
    client.cookies.set("portal_session", "ImRhbmFAYXBwLXRlYW0i.forged")
    assert client.get("/api/session").status_code == 401


# ── catalog ──────────────────────────────────────────────────────────


def test_catalog_requires_auth(client):
    assert client.get("/api/catalog").status_code == 401


def test_catalog_serves_stale_snapshot_with_annotations(client):
    login(client, "admin@platform")  # unscoped, so we see the whole snapshot
    resp = client.get("/api/catalog")
    assert resp.status_code == 200
    tree = resp.json()
    assert tree["stale"] is True  # DownUC forces the bundled-seed fallback path
    assert len(tree["accounts"]) == 2
    first_source = tree["accounts"][0]["workloads"][0]["sources"][0]
    assert first_source["subscriptions"] == []


def test_catalog_shows_my_subscriptions_after_fork(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]})
    tree = client.get("/api/catalog").json()
    web = [w for a in tree["accounts"] for w in a["workloads"] if w["name"] == "storefront_web"][0]
    syslog = [s for s in web["sources"] if s["name"] == "syslog"][0]
    assert syslog["subscriptions"] == [{"stream_id": 1, "stream_name": "s1", "status": "active"}]


def test_discovered_cribl_source_is_sensitive_and_forks_to_pending(client, fakes):
    # A source seen at the Cribl Edge but not yet in UC, in dana's account scope.
    upsert_discovered(fakes["conn"], {
        "account_id": "522412052544", "account_alias": "prod-ecommerce",
        "workload": "storefront_web", "source_name": "new_metric_log",
        "environment": "prod", "est_volume_per_min": 120,
    })
    login(client)  # dana, scoped to 522412052544
    tree = client.get("/api/catalog").json()
    web = [w for a in tree["accounts"] for w in a["workloads"] if w["name"] == "storefront_web"][0]
    disc = [s for s in web["sources"] if s["name"] == "new_metric_log"]
    assert disc, "discovered source should surface in the catalog"
    assert disc[0]["sensitivity"] == "sensitive"
    assert disc[0]["origin"] == "cribl"
    fqn = disc[0]["fqn"]
    assert fqn == "cribl://522412052544/storefront_web/new_metric_log"
    # forking the discovered (sensitive) source lands pending_approval, not active
    resp = client.post("/api/streams",
                       json={"name": "disc1", "type": "kinesis", "source_fqns": [fqn]})
    assert resp.status_code == 201, resp.text
    [src] = resp.json()["sources"]
    assert src["status"] == "pending_approval"
    assert all("new_metric_log" not in r["filter"]
               for r in fakes["pipeline"].applied[-1]["routes"])


# ── streams ──────────────────────────────────────────────────────────


def test_fork_flow_standard_and_sensitive(client, fakes):
    login(client)
    resp = client.post(
        "/api/streams",
        json={"name": "mixed", "type": "kinesis", "source_fqns": [SYSLOG, AUTH_LOG]},
    )
    assert resp.status_code == 201, resp.text
    stream = resp.json()
    assert stream["status"] == "live"
    statuses = {s["source_fqn"]: s["status"] for s in stream["sources"]}
    assert statuses == {SYSLOG: "active", AUTH_LOG: "pending_approval"}
    assert fakes["provisioner"].created == [("kinesis", "logstream-mixed")]
    assert all("auth_log" not in r["filter"] for r in fakes["pipeline"].applied[-1]["routes"])


def test_list_streams_includes_flow_stats(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]})
    [stream] = client.get("/api/streams").json()
    assert stream["flow"] == {"recent_records": 42}


def test_peek_returns_sample_records(client):
    login(client)
    created = client.post(
        "/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    records = client.get(f"/api/streams/{created['id']}/peek").json()
    assert records[0]["workload"] == "storefront_web"


def test_streams_are_private_to_owner(client):
    login(client, "dana@app-team")
    created = client.post(
        "/api/streams", json={"name": "private", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    login(client, "raj@data-sci")
    assert client.get("/api/streams").json() == []
    assert client.get(f"/api/streams/{created['id']}/peek").status_code == 403


def test_add_remove_delete_stream(client, fakes):
    login(client)
    created = client.post(
        "/api/streams", json={"name": "s1", "type": "sqs", "source_fqns": [SYSLOG]}
    ).json()
    sid = created["id"]
    add = client.post(
        f"/api/streams/{sid}/sources",
        json={"source_fqns": ["logging_demo.acct_b__orders_api.syslog"]},
    )
    assert add.status_code == 200
    assert len(add.json()["sources"]) == 2
    from urllib.parse import quote
    rm = client.delete(f"/api/streams/{sid}/sources/{quote(SYSLOG, safe='')}")
    assert rm.status_code == 200
    assert len(rm.json()["sources"]) == 1
    assert client.delete(f"/api/streams/{sid}").status_code == 204
    assert client.get("/api/streams").json() == []
    assert fakes["provisioner"].deleted == [("sqs", "http://q/logstream-s1")]


def test_retry_after_provision_failure(client, fakes):
    login(client)
    fakes["provisioner"].fail = True
    created = client.post(
        "/api/streams", json={"name": "flaky", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    assert created["status"] == "error"
    fakes["provisioner"].fail = False
    retried = client.post(f"/api/streams/{created['id']}/retry")
    assert retried.status_code == 200
    assert retried.json()["status"] == "live"


# ── approvals ────────────────────────────────────────────────────────


def test_approval_queue_visible_to_admin_only(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [AUTH_LOG]})
    assert client.get("/api/approvals").status_code == 403
    login(client, "admin@platform")
    queue = client.get("/api/approvals").json()
    assert len(queue) == 1
    assert queue[0]["source_fqn"] == AUTH_LOG
    assert queue[0]["stream_name"] == "s1"
    assert queue[0]["requested_by"] == "dana@app-team"


def test_approve_activates_source_for_requester(client, fakes):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [AUTH_LOG]})
    login(client, "admin@platform")
    [item] = client.get("/api/approvals").json()
    resp = client.post(f"/api/approvals/{item['id']}", json={"approved": True})
    assert resp.status_code == 200
    assert client.get("/api/approvals").json() == []
    login(client, "dana@app-team")
    [stream] = client.get("/api/streams").json()
    assert stream["sources"][0]["status"] == "active"
    assert any("auth_log" in r["filter"] for r in fakes["pipeline"].applied[-1]["routes"])


def test_reject_marks_source_rejected(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [AUTH_LOG]})
    login(client, "admin@platform")
    [item] = client.get("/api/approvals").json()
    client.post(f"/api/approvals/{item['id']}", json={"approved": False})
    login(client, "dana@app-team")
    [stream] = client.get("/api/streams").json()
    assert stream["sources"][0]["status"] == "rejected"


def test_peek_on_non_live_stream_is_409(client, fakes):
    login(client)
    fakes["provisioner"].fail = True
    created = client.post(
        "/api/streams", json={"name": "broken", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    assert client.get(f"/api/streams/{created['id']}/peek").status_code == 409


def test_unknown_stream_id_is_404(client):
    login(client)
    assert client.get("/api/streams/999/peek").status_code == 404
    assert client.post("/api/streams/999/retry").status_code == 404


def test_mutation_responses_include_flow_key(client):
    login(client)
    created = client.post(
        "/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    assert "flow" in created and created["flow"] is None


def test_access_bundle_owner_only_and_includes_usage(client, fakes):
    login(client)
    created = client.post(
        "/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    bundle = client.get(f"/api/streams/{created['id']}/access-bundle")
    assert bundle.status_code == 200
    body = bundle.json()
    assert body["role_arn"] == created["read_role_arn"]
    assert "assume_role" in body["usage"]
    login(client, "raj@data-sci")
    assert client.get(f"/api/streams/{created['id']}/access-bundle").status_code == 403


def test_access_bundle_404_when_no_role(client, fakes):
    login(client)
    fakes["access_roles"].fail = True
    created = client.post(
        "/api/streams", json={"name": "broken", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    assert created["status"] == "error"
    assert client.get(f"/api/streams/{created['id']}/access-bundle").status_code == 404


# ── account-scoped rbac ──────────────────────────────────────────────


def test_catalog_is_scoped_per_persona(client):
    login(client)  # dana
    accounts = [a["account_id"] for a in client.get("/api/catalog").json()["accounts"]]
    assert accounts == ["522412052544"]
    login(client, "raj@data-sci")
    accounts = [a["account_id"] for a in client.get("/api/catalog").json()["accounts"]]
    assert accounts == ["624627265315"]
    login(client, "admin@platform")
    assert len(client.get("/api/catalog").json()["accounts"]) == 2


def test_cross_scope_fork_rejected_via_api(client):
    login(client)  # dana
    resp = client.post(
        "/api/streams", json={"name": "sneaky", "type": "kinesis", "source_fqns": [IDENTITY_AUTH]}
    )
    assert resp.status_code == 403
