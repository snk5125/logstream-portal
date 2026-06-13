import json
from pathlib import Path

from app.aws.access_bundle import build_access_bundle

GOLDEN = Path(__file__).parent / "aws_golden"


def _stream(**over):
    base = {
        "id": 7, "name": "team-a-orders", "type": "kinesis",
        "resource_ref": "logstream-team-a-orders",
        "read_role_arn": "arn:aws:iam::337394138208:role/logstream/logstream-read-7-team-a-orders",
        "consumer_account_id": "522412052544",
    }
    base.update(over)
    return base


def test_kinesis_bundle_matches_golden():
    out = build_access_bundle(_stream(), region="us-east-1", logging_account_id="337394138208")
    assert out == json.loads((GOLDEN / "bundle_kinesis.json").read_text())


def test_sqs_bundle_uses_receive_message():
    out = build_access_bundle(
        _stream(type="sqs",
                resource_ref="https://sqs.us-east-1.amazonaws.com/337394138208/logstream-q"),
        region="us-east-1", logging_account_id="337394138208")
    assert "sqs receive-message" in out["usage"]["read"]
    assert out["permission_policy"]["Statement"][0]["Resource"].startswith("arn:aws:sqs:")
