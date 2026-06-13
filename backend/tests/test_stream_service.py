import json
from pathlib import Path

import pytest

from app.config import Settings
from app.cribl.admin import CriblApplyError
from app.db import get_db, init_db
from app.streams.provisioner import ProvisionError
from app.streams.service import StreamService, StreamServiceError

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "catalog_snapshot.json"

SYSLOG = "logging_demo.acct_b__storefront_web.syslog"
AUTH_LOG = "logging_demo.acct_b__orders_api.auth_log"
# identity_svc.auth_log lives in account 624627265315 — out of scope for dana.
IDENTITY_AUTH = "logging_demo.acct_c__identity_svc.auth_log"

DANA = {"id": "dana@app-team", "role": "consumer", "account_scope": "522412052544"}
RAJ = {"id": "raj@data-sci", "role": "consumer", "account_scope": "624627265315"}
ADMIN = {"id": "admin@platform", "role": "admin", "account_scope": None}


class FixtureCatalog:
    def get_tree(self):
        return {**json.loads(FIXTURE.read_text()), "stale": False}


class FakeProvisioner:
    def __init__(self):
        self.created, self.deleted, self.fail = [], [], False

    def create(self, stream_type, name):
        if self.fail:
            raise ProvisionError("boom")
        self.created.append((stream_type, name))
        return name if stream_type == "kinesis" else f"http://q/{name}"

    def delete(self, stream_type, ref):
        self.deleted.append((stream_type, ref))


class FakePipelineAdmin:
    def __init__(self):
        self.applied, self.fail = [], False

    def apply(self, routes, destinations):
        if self.fail:
            raise CriblApplyError("cribl said no")
        self.applied.append({"routes": routes, "destinations": destinations})


class FakeAccessRoles:
    def __init__(self):
        self.created, self.deleted, self.fail = [], [], False

    def create(self, stream_id, stream_name, stream_type, resource_arn, consumer_account_id):
        if self.fail:
            from app.aws.access_roles import AccessRoleError
            raise AccessRoleError("iam boom")
        rec = {"stream_id": stream_id, "stream_name": stream_name,
               "stream_type": stream_type, "resource_arn": resource_arn,
               "consumer_account_id": consumer_account_id}
        self.created.append(rec)
        from app.aws.access_roles import role_name_for
        return {"role_name": role_name_for(stream_id, stream_name),
                "role_arn": f"arn:aws:iam::337394138208:role/logstream/{role_name_for(stream_id, stream_name)}",
                "trust_policy": {}, "permission_policy": {}}

    def delete(self, role_name):
        self.deleted.append(role_name)


@pytest.fixture()
def env(tmp_path):
    conn = get_db(":memory:")
    init_db(conn)
    settings = Settings(
        databricks_host="", databricks_token="", uc_catalog="logging_demo",
        aws_region="us-east-1", data_dir=str(tmp_path), snapshot_seed="",
        static_dir="", session_secret="x",
        cribl_base_url="http://leader:9000", cribl_group="default",
        cribl_username="admin", cribl_password="pw",
        resource_prefix="logstream-", logging_account_id="337394138208",
    )
    provisioner, pipeline, roles = FakeProvisioner(), FakePipelineAdmin(), FakeAccessRoles()
    svc = StreamService(conn, FixtureCatalog(), provisioner, roles, pipeline, settings)
    return svc, provisioner, pipeline, roles


def test_standard_source_activates_immediately(env):
    svc, provisioner, pipeline, _ = env
    stream = svc.create_stream(DANA, "team-a-logs", "kinesis", [SYSLOG])
    assert stream["status"] == "live"
    assert stream["resource_ref"] == "logstream-team-a-logs"
    assert stream["sources"][0]["status"] == "active"
    assert provisioner.created == [("kinesis", "logstream-team-a-logs")]
    last = pipeline.applied[-1]
    assert any(r["id"] == f"fork_{stream['id']}" for r in last["routes"])
    assert any("source_name=='syslog'" in r["filter"] for r in last["routes"])
    # account_id must be carried into the Cribl filter — the whole point of the migration
    assert any("account_id=='522412052544'" in r["filter"] for r in last["routes"])


def test_sensitive_source_pends_and_is_excluded_from_fragment(env):
    svc, _, pipeline, _ = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [SYSLOG, AUTH_LOG])
    by_fqn = {s["source_fqn"]: s["status"] for s in stream["sources"]}
    assert by_fqn == {SYSLOG: "active", AUTH_LOG: "pending_approval"}
    assert all("auth_log" not in r["filter"] for r in pipeline.applied[-1]["routes"])


def test_approval_activates_and_reapplies(env):
    svc, _, pipeline, _ = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [AUTH_LOG])
    assert pipeline.applied[-1]["routes"] == []  # all pending: no routes yet
    pending_id = stream["sources"][0]["id"]
    svc.approve(ADMIN, pending_id, approved=True)
    refreshed = svc.get_stream(DANA, stream["id"])
    assert refreshed["sources"][0]["status"] == "active"
    assert any("auth_log" in r["filter"] for r in pipeline.applied[-1]["routes"])


def test_rejection_records_decision_without_reapply(env):
    svc, _, pipeline, _ = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [AUTH_LOG])
    applies_before = len(pipeline.applied)
    svc.approve(ADMIN, stream["sources"][0]["id"], approved=False)
    refreshed = svc.get_stream(DANA, stream["id"])
    assert refreshed["sources"][0]["status"] == "rejected"
    assert refreshed["sources"][0]["decided_by"] == "admin@platform"
    assert len(pipeline.applied) == applies_before


def test_non_admin_cannot_approve(env):
    svc, _, _, _ = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [AUTH_LOG])
    with pytest.raises(StreamServiceError) as err:
        svc.approve(RAJ, stream["sources"][0]["id"], approved=True)
    assert err.value.status_code == 403


def test_unknown_source_is_404(env):
    svc, _, _, _ = env
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(DANA, "s", "kinesis", ["logging_demo.no.such"])
    assert err.value.status_code == 404


def test_duplicate_stream_name_is_409(env):
    svc, _, _, _ = env
    svc.create_stream(DANA, "dup", "kinesis", [SYSLOG])
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(RAJ, "dup", "sqs", [SYSLOG])
    assert err.value.status_code == 409


def test_ownership_enforced_on_reads_and_mutations(env):
    svc, _, _, _ = env
    stream = svc.create_stream(DANA, "private", "kinesis", [SYSLOG])
    with pytest.raises(StreamServiceError) as err:
        svc.get_stream(RAJ, stream["id"])
    assert err.value.status_code == 403


def test_provision_failure_marks_error_and_retry_recovers(env):
    svc, provisioner, _, _ = env
    provisioner.fail = True
    stream = svc.create_stream(DANA, "flaky", "kinesis", [SYSLOG])
    assert stream["status"] == "error"
    assert stream["last_error"] == "boom"
    assert stream["sources"] == []  # members only attach once resource exists
    provisioner.fail = False
    recovered = svc.retry(DANA, stream["id"])
    assert recovered["status"] == "live"
    assert recovered["resource_ref"] == "logstream-flaky"


def test_add_and_remove_sources_regenerate_routes(env):
    svc, _, pipeline, _ = env
    stream = svc.create_stream(DANA, "grow", "kinesis", [SYSLOG])
    svc.add_sources(DANA, stream["id"], ["logging_demo.acct_b__orders_api.syslog"])
    assert any("workload=='orders_api'" in r["filter"] for r in pipeline.applied[-1]["routes"])
    svc.remove_source(DANA, stream["id"], SYSLOG)
    assert all("workload=='storefront_web'" not in r["filter"] for r in pipeline.applied[-1]["routes"])
    refreshed = svc.get_stream(DANA, stream["id"])
    assert [s["source_fqn"] for s in refreshed["sources"]] == [
        "logging_demo.acct_b__orders_api.syslog"
    ]


def test_add_duplicate_source_is_409(env):
    svc, _, _, _ = env
    stream = svc.create_stream(DANA, "s", "kinesis", [SYSLOG])
    with pytest.raises(StreamServiceError) as err:
        svc.add_sources(DANA, stream["id"], [SYSLOG])
    assert err.value.status_code == 409


def test_delete_stream_reapplies_then_tears_down_resource(env):
    svc, provisioner, pipeline, _ = env
    stream = svc.create_stream(DANA, "gone", "kinesis", [SYSLOG])
    svc.delete_stream(DANA, stream["id"])
    assert all(r["id"] != f"fork_{stream['id']}" for r in pipeline.applied[-1]["routes"])
    assert provisioner.deleted == [("kinesis", "logstream-gone")]
    assert svc.list_streams(DANA) == []


def test_cribl_rejection_surfaces_502_and_flags_stream(env):
    svc, _, pipeline, _ = env
    pipeline.fail = True
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(DANA, "s", "kinesis", [SYSLOG])
    assert err.value.status_code == 502
    pipeline.fail = False
    [stream] = svc.list_streams(DANA)
    assert "cribl said no" in stream["last_error"]


def test_delete_survives_cribl_failure_and_still_tears_down(env):
    svc, provisioner, pipeline, _ = env
    stream = svc.create_stream(DANA, "doomed", "kinesis", [SYSLOG])
    pipeline.fail = True
    svc.delete_stream(DANA, stream["id"])  # must not raise
    assert provisioner.deleted == [("kinesis", "logstream-doomed")]
    assert svc.list_streams(DANA) == []


def test_removing_last_source_drops_fork_from_routes(env):
    svc, _, pipeline, _ = env
    stream = svc.create_stream(DANA, "solo", "kinesis", [SYSLOG])
    svc.remove_source(DANA, stream["id"], SYSLOG)
    last = pipeline.applied[-1]
    assert all(r["id"] != f"fork_{stream['id']}" for r in last["routes"])
    assert last["routes"] == []


def test_create_stream_mints_access_role(env):
    svc, provisioner, pipeline, roles = env
    stream = svc.create_stream(DANA, "team-a-logs", "kinesis", [SYSLOG])
    assert stream["read_role_arn"].endswith("logstream-read-1-team-a-logs")
    assert stream["consumer_account_id"] == "522412052544"
    assert roles.created[0]["consumer_account_id"] == "522412052544"
    assert "kinesis" in roles.created[0]["resource_arn"]


def test_admin_streams_default_consumer_to_logging_account(env):
    svc, _, _, roles = env
    svc.create_stream(ADMIN, "admin-s", "kinesis", [SYSLOG])
    assert roles.created[0]["consumer_account_id"] == "337394138208"


def test_role_failure_marks_error_and_retry_completes(env):
    svc, provisioner, _, roles = env
    roles.fail = True
    stream = svc.create_stream(DANA, "flaky", "kinesis", [SYSLOG])
    assert stream["status"] == "error"
    assert stream["read_role_arn"] is None
    roles.fail = False
    recovered = svc.retry(DANA, stream["id"])
    assert recovered["status"] == "live"
    assert recovered["read_role_arn"] is not None


def test_delete_stream_deletes_role(env):
    svc, _, _, roles = env
    stream = svc.create_stream(DANA, "gone", "kinesis", [SYSLOG])
    svc.delete_stream(DANA, stream["id"])
    assert roles.deleted == ["logstream-read-1-gone"]


def test_delete_after_failed_mint_still_attempts_role_cleanup(env):
    svc, _, _, roles = env
    roles.fail = True
    stream = svc.create_stream(DANA, "halfway", "kinesis", [SYSLOG])
    assert stream["status"] == "error"
    roles.fail = False
    svc.delete_stream(DANA, stream["id"])
    assert roles.deleted == ["logstream-read-1-halfway"]


def test_retry_on_healthy_stream_does_not_remint(env):
    svc, _, _, roles = env
    stream = svc.create_stream(DANA, "healthy", "kinesis", [SYSLOG])
    svc.retry(DANA, stream["id"])
    svc.retry(DANA, stream["id"])
    assert len(roles.created) == 1


def test_fork_outside_scope_is_403(env):
    svc, _, _, _ = env
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(DANA, "sneaky", "kinesis", [IDENTITY_AUTH])
    assert err.value.status_code == 403


def test_admin_can_fork_any_account(env):
    svc, _, _, _ = env
    stream = svc.create_stream(ADMIN, "admin-any", "kinesis", [IDENTITY_AUTH])
    assert stream["status"] == "live"
