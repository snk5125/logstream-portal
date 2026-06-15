"""Pure builders for the Cribl Route and Destination objects a fork needs.

Output must be byte-deterministic so golden-file tests can pin it. Values are
validated at construction (the trust boundary for config generation): the
portal interpolates them into Cribl filter expressions, so quotes and control
characters are rejected rather than escaped.
"""
import re
from dataclasses import dataclass, field
from typing import Sequence

_SAFE = re.compile(r"^[A-Za-z0-9_\-.:/@]+$")


def _validate(name: str, value: str) -> None:
    if not _SAFE.match(value):
        raise ValueError(f"{name} {value!r} contains characters unsafe for a Cribl filter")


@dataclass(frozen=True)
class Member:
    account_id: str
    workload: str
    source_name: str

    def __post_init__(self) -> None:
        _validate("account_id", self.account_id)
        _validate("workload", self.workload)
        _validate("source_name", self.source_name)


@dataclass(frozen=True)
class StreamSpec:
    stream_id: int
    stream_type: str  # "kinesis" | "sqs"
    resource_ref: str  # kinesis stream name | sqs queue name
    members: Sequence[Member] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.stream_type not in ("kinesis", "sqs"):
            raise ValueError(f"unsupported stream_type: {self.stream_type!r}")
        for m in self.members:
            if not isinstance(m, Member):
                raise TypeError(f"members must be Member instances, got {type(m)!r}")
        _validate("resource_ref", self.resource_ref)


def _clause(m: Member) -> str:
    return (
        f"(account_id=='{m.account_id}' && workload=='{m.workload}' "
        f"&& source_name=='{m.source_name}')"
    )


def route_for(spec: StreamSpec) -> dict:
    if not spec.members:
        raise ValueError("cannot build a route for a stream with no members (empty filter never matches)")
    ordered = sorted(spec.members, key=lambda m: (m.account_id, m.workload, m.source_name))
    return {
        "id": f"fork_{spec.stream_id}",
        "name": f"fork_{spec.stream_id}",
        "final": False,
        "disabled": False,
        "filter": " || ".join(_clause(m) for m in ordered),
        "pipeline": "passthru",
        "output": f"fork_{spec.stream_id}_dest",
        "description": f"logstream-portal fork for stream {spec.stream_id}",
    }


def destination_for(spec: StreamSpec, region: str = "us-east-1") -> dict:
    desc = f"logstream-portal destination for stream {spec.stream_id}"
    if spec.stream_type == "kinesis":
        return {
            "id": f"fork_{spec.stream_id}_dest", "type": "kinesis",
            "streamName": spec.resource_ref, "region": region,
            "awsAuthenticationMethod": "auto", "format": "json",
            "compression": "none", "description": desc,
        }
    return {
        "id": f"fork_{spec.stream_id}_dest", "type": "sqs",
        "queueName": spec.resource_ref, "region": region,
        "awsAuthenticationMethod": "auto", "format": "json",
        # queueType is required by Cribl's SQS output schema; omitting it makes the
        # reapply PATCH 500 ("should have required property 'queueType'"). Demo
        # queues are standard (the provisioner never creates FIFO).
        "compression": "none", "queueType": "standard", "description": desc,
    }
