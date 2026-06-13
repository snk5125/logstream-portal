import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.catalog.cache import SnapshotCache
from app.catalog.service import CatalogService
from app.catalog.uc_client import CatalogUnavailable
from app.config import Settings
from app.db import get_db
from app.main import create_app

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"

SYSLOG = "logging_demo.acct_b__storefront_web.syslog"
AUTH_LOG = "logging_demo.acct_b__orders_api.auth_log"
# Lives in account 624627265315 — out of scope for dana (522412052544).
IDENTITY_AUTH = "logging_demo.acct_c__identity_svc.auth_log"


class DownUC:
    """Always-unreachable Databricks → app serves the bundled seed snapshot."""

    def list_schemas(self, catalog):
        raise CatalogUnavailable("databricks not reachable in tests")

    def list_tables(self, catalog, schema):
        raise CatalogUnavailable("databricks not reachable in tests")


class FakeProvisioner:
    def __init__(self):
        self.created, self.deleted, self.fail = [], [], False

    def create(self, stream_type, name):
        if self.fail:
            from app.streams.provisioner import ProvisionError
            raise ProvisionError("boom")
        self.created.append((stream_type, name))
        return name if stream_type == "kinesis" else f"http://q/{name}"

    def delete(self, stream_type, ref):
        self.deleted.append((stream_type, ref))


class FakePipelineAdmin:
    def __init__(self):
        self.applied = []

    def apply(self, routes, destinations):
        self.applied.append({"routes": routes, "destinations": destinations})


class FakePeek:
    def peek(self, stream_type, ref, limit=5):
        return [{"message": "hello", "workload": "storefront_web", "source_name": "syslog"}]

    def flow_stats(self, stream_type, ref):
        return {"recent_records": 42}


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
def fakes(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json", seed_path=FIXTURES / "catalog_snapshot.json")
    return {
        "conn": get_db(":memory:"),
        "catalog": CatalogService(DownUC(), cache, "logging_demo"),
        "provisioner": FakeProvisioner(),
        "pipeline": FakePipelineAdmin(),
        "peek": FakePeek(),
        "access_roles": FakeAccessRoles(),
    }


@pytest.fixture()
def client(fakes, tmp_path):
    settings = Settings(
        databricks_host="", databricks_token="", uc_catalog="logging_demo",
        aws_region="us-east-1", data_dir=str(tmp_path), snapshot_seed="",
        static_dir="", session_secret="test-secret",
        cribl_base_url="http://leader:9000", cribl_group="default",
        cribl_username="admin", cribl_password="pw",
        resource_prefix="logstream-", logging_account_id="337394138208",
    )
    app = create_app(settings, services=fakes)
    with TestClient(app) as test_client:
        yield test_client


def login(client, user_id="dana@app-team"):
    resp = client.post("/api/session", json={"user_id": user_id})
    assert resp.status_code == 200, resp.text
    return resp.json()
