import json
from pathlib import Path

import pytest

from app.cribl.objects import Member, StreamSpec, destination_for, route_for

GOLDEN = Path(__file__).parent / "cribl_golden"


def test_route_orders_members_sorted_and_filtered():
    spec = StreamSpec(
        stream_id=3, stream_type="kinesis", resource_ref="logstream-team-a-orders",
        members=(
            Member("522412052544", "orders_api", "syslog"),
            Member("522412052544", "orders_api", "auth_log"),
        ),
    )
    assert route_for(spec) == json.loads((GOLDEN / "route_two_members.json").read_text())


def test_kinesis_destination():
    spec = StreamSpec(3, "kinesis", "logstream-team-a-orders", ())
    assert destination_for(spec) == json.loads((GOLDEN / "dest_kinesis.json").read_text())


def test_sqs_destination():
    spec = StreamSpec(5, "sqs", "logstream-team-b-audit", ())
    assert destination_for(spec) == json.loads((GOLDEN / "dest_sqs.json").read_text())


def test_unsafe_values_rejected():
    with pytest.raises(ValueError):
        Member("522412052544", "orders'api", "syslog")
    with pytest.raises(ValueError):
        StreamSpec(1, "firehose", "x", (Member("1", "w", "s"),))


def test_quote_in_account_id_rejected():
    with pytest.raises(ValueError):
        Member('52"', "w", "s")


def test_route_for_empty_members_rejected():
    spec = StreamSpec(7, "kinesis", "logstream-x", ())
    with pytest.raises(ValueError):
        route_for(spec)


def test_non_member_in_members_rejected():
    with pytest.raises(TypeError):
        StreamSpec(7, "kinesis", "logstream-x", ({"account_id": "1"},))
