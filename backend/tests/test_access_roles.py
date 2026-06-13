import json

import pytest

from app.aws.access_roles import (
    AccessRoleError, AccessRoleService, resource_arn_for, role_name_for,
)


class _NoSuchEntity(Exception):
    pass


class _AlreadyExists(Exception):
    pass


class _Exceptions:
    NoSuchEntityException = _NoSuchEntity
    EntityAlreadyExistsException = _AlreadyExists


class FakeIAM:
    exceptions = _Exceptions()

    def __init__(self, fail=False):
        self.roles, self.policies, self.fail = {}, {}, fail
        self.deleted_roles, self.deleted_policies = [], []

    def create_role(self, RoleName, Path, AssumeRolePolicyDocument, Description):
        if self.fail:
            raise RuntimeError("AccessDenied")
        if RoleName in self.roles:
            raise _AlreadyExists()
        arn = f"arn:aws:iam::337394138208:role{Path}{RoleName}"
        self.roles[RoleName] = {"Arn": arn, "Trust": json.loads(AssumeRolePolicyDocument)}
        return {"Role": {"Arn": arn}}

    def get_role(self, RoleName):
        if RoleName not in self.roles:
            raise _NoSuchEntity()
        return {"Role": {"Arn": self.roles[RoleName]["Arn"]}}

    def put_role_policy(self, RoleName, PolicyName, PolicyDocument):
        self.policies[(RoleName, PolicyName)] = json.loads(PolicyDocument)

    def delete_role_policy(self, RoleName, PolicyName):
        if (RoleName, PolicyName) not in self.policies:
            raise _NoSuchEntity()
        self.deleted_policies.append((RoleName, PolicyName))
        del self.policies[(RoleName, PolicyName)]

    def delete_role(self, RoleName):
        if RoleName not in self.roles:
            raise _NoSuchEntity()
        self.deleted_roles.append(RoleName)
        del self.roles[RoleName]


def test_role_name_truncates_and_is_unique_per_stream():
    assert role_name_for(7, "team-a-orders") == "logstream-read-7-team-a-orders"
    long = role_name_for(123, "x" * 80)
    assert long.startswith("logstream-read-123-")
    assert len(long) <= 64


def test_resource_arn_for_kinesis_and_sqs():
    assert resource_arn_for("kinesis", "logstream-x", "us-east-1", "337394138208") == \
        "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-x"
    assert resource_arn_for(
        "sqs", "https://sqs.us-east-1.amazonaws.com/337394138208/logstream-q",
        "us-east-1", "337394138208",
    ) == "arn:aws:sqs:us-east-1:337394138208:logstream-q"


def test_create_builds_trust_and_scoped_policy():
    iam = FakeIAM()
    svc = AccessRoleService(iam)
    out = svc.create(7, "team-a-orders", "kinesis",
                     "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-team-a-orders",
                     "522412052544")
    assert out["role_name"] == "logstream-read-7-team-a-orders"
    assert out["role_arn"].endswith("/logstream/logstream-read-7-team-a-orders")
    trust = out["trust_policy"]["Statement"][0]
    assert trust["Principal"]["AWS"] == "arn:aws:iam::522412052544:root"
    perm = iam.policies[("logstream-read-7-team-a-orders", "read-access")]
    assert perm["Statement"][0]["Resource"] == \
        "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-team-a-orders"
    assert "kinesis:GetRecords" in perm["Statement"][0]["Action"]


def test_create_is_idempotent_when_role_exists():
    iam = FakeIAM()
    svc = AccessRoleService(iam)
    first = svc.create(7, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
    again = svc.create(7, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
    assert first["role_arn"] == again["role_arn"]


def test_sqs_policy_actions():
    iam = FakeIAM()
    AccessRoleService(iam).create(9, "q", "sqs", "arn:aws:sqs:us-east-1:1:logstream-q", "624627265315")
    perm = iam.policies[("logstream-read-9-q", "read-access")]
    assert set(perm["Statement"][0]["Action"]) == \
        {"sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"}


def test_delete_is_idempotent():
    iam = FakeIAM()
    svc = AccessRoleService(iam)
    svc.create(7, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
    svc.delete("logstream-read-7-s")
    svc.delete("logstream-read-7-s")  # second call must not raise
    assert iam.deleted_roles == ["logstream-read-7-s"]


def test_failures_wrap_in_access_role_error():
    with pytest.raises(AccessRoleError):
        AccessRoleService(FakeIAM(fail=True)).create(
            1, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
