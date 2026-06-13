"""Per-stream cross-account IAM read roles.

Each live stream gets a dedicated role in the logging account that the
owner's workload account may assume to read exactly that stream/queue.
Role lifecycle is 1:1 with the stream (created after provisioning, deleted
with the stream); roles are immutable once created.
"""
import json

ROLE_PATH = "/logstream/"
POLICY_NAME = "read-access"

KINESIS_ACTIONS = [
    "kinesis:GetRecords", "kinesis:GetShardIterator",
    "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards",
]
# Consumers own their queue's consumption; destructive read is intended.
SQS_ACTIONS = ["sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"]


class AccessRoleError(RuntimeError):
    """The IAM role could not be created or deleted."""


def role_name_for(stream_id: int, stream_name: str) -> str:
    # id prefix guarantees uniqueness; truncation keeps IAM's 64-char limit.
    return f"logstream-read-{stream_id}-{stream_name[:40]}"


def resource_arn_for(stream_type: str, resource_ref: str, region: str, account_id: str) -> str:
    if stream_type == "kinesis":
        return f"arn:aws:kinesis:{region}:{account_id}:stream/{resource_ref}"
    queue_name = resource_ref.rstrip("/").split("/")[-1]
    return f"arn:aws:sqs:{region}:{account_id}:{queue_name}"


def trust_policy_for(consumer_account_id: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{consumer_account_id}:root"},
            "Action": "sts:AssumeRole",
        }],
    }


def permission_policy_for(stream_type: str, resource_arn: str) -> dict:
    actions = KINESIS_ACTIONS if stream_type == "kinesis" else SQS_ACTIONS
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": actions, "Resource": resource_arn}],
    }


class AccessRoleService:
    def __init__(self, iam_client):
        self._iam = iam_client

    def create(self, stream_id, stream_name, stream_type, resource_arn,
               consumer_account_id) -> dict:
        name = role_name_for(stream_id, stream_name)
        trust = trust_policy_for(consumer_account_id)
        permission = permission_policy_for(stream_type, resource_arn)
        try:
            try:
                resp = self._iam.create_role(
                    RoleName=name, Path=ROLE_PATH,
                    AssumeRolePolicyDocument=json.dumps(trust),
                    Description=f"logstream-portal read access for stream {stream_id}",
                )
                role_arn = resp["Role"]["Arn"]
            except self._iam.exceptions.EntityAlreadyExistsException:
                role_arn = self._iam.get_role(RoleName=name)["Role"]["Arn"]
            self._iam.put_role_policy(
                RoleName=name, PolicyName=POLICY_NAME,
                PolicyDocument=json.dumps(permission),
            )
        except AccessRoleError:
            raise
        except Exception as exc:
            raise AccessRoleError(str(exc)) from exc
        return {"role_name": name, "role_arn": role_arn,
                "trust_policy": trust, "permission_policy": permission}

    def delete(self, role_name: str) -> None:
        try:
            try:
                self._iam.delete_role_policy(RoleName=role_name, PolicyName=POLICY_NAME)
            except self._iam.exceptions.NoSuchEntityException:
                pass
            try:
                self._iam.delete_role(RoleName=role_name)
            except self._iam.exceptions.NoSuchEntityException:
                pass
        except Exception as exc:
            raise AccessRoleError(str(exc)) from exc
