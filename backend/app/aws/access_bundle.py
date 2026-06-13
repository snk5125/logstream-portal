"""Build the downloadable access bundle for a stream's read role.

Pure function over stored stream state — nothing secret, no credentials;
policies are regenerated with the same builders that created the role.
"""
from app.aws.access_roles import (
    permission_policy_for, resource_arn_for, trust_policy_for,
)


def build_access_bundle(stream: dict, region: str, logging_account_id: str) -> dict:
    arn = resource_arn_for(stream["type"], stream["resource_ref"], region, logging_account_id)
    if stream["type"] == "kinesis":
        read = (
            f"aws kinesis get-shard-iterator --stream-name {stream['resource_ref']}"
            " --shard-id shardId-000000000000 --shard-iterator-type LATEST"
            " --query ShardIterator --output text"
            " | xargs -I{} aws kinesis get-records --shard-iterator {}"
        )
    else:
        read = f"aws sqs receive-message --queue-url {stream['resource_ref']}"
    return {
        "stream": {
            "name": stream["name"], "type": stream["type"],
            "resource": stream["resource_ref"], "resource_arn": arn, "region": region,
        },
        "role_arn": stream["read_role_arn"],
        "consumer_account_id": stream["consumer_account_id"],
        "trust_policy": trust_policy_for(stream["consumer_account_id"]),
        "permission_policy": permission_policy_for(stream["type"], arn),
        "usage": {
            "assume_role": (
                f"aws sts assume-role --role-arn {stream['read_role_arn']}"
                f" --role-session-name read-{stream['name']}"
            ),
            "read": read,
        },
    }
