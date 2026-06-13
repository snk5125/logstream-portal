import json

import pytest

from app.streams.peek import PeekService


class FakeKinesis:
    def __init__(self, records):
        self._records = records

    def describe_stream(self, StreamName):
        return {"StreamDescription": {"Shards": [{"ShardId": "shardId-0"}]}}

    def get_shard_iterator(self, **kwargs):
        assert kwargs["ShardIteratorType"] == "TRIM_HORIZON"
        return {"ShardIterator": "it-0"}

    def get_records(self, ShardIterator, Limit):
        return {
            "Records": [{"Data": json.dumps(r).encode()} for r in self._records],
            "NextShardIterator": None,
            "MillisBehindLatest": 0,
        }


class FakeSQS:
    def __init__(self, bodies, backlog=3):
        self._bodies, self._backlog = bodies, backlog
        self.deleted = []

    def receive_message(self, **kwargs):
        assert kwargs["VisibilityTimeout"] == 0  # peek must not hide messages
        return {"Messages": [{"Body": b} for b in self._bodies]}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        return {"Attributes": {"ApproximateNumberOfMessages": str(self._backlog)}}

    def delete_message(self, **kwargs):
        self.deleted.append(kwargs)


def test_kinesis_peek_returns_last_n_decoded_records():
    records = [{"message": f"m{i}", "workload": "orders_api"} for i in range(8)]
    out = PeekService(FakeKinesis(records), FakeSQS([])).peek("kinesis", "team-a-logs", limit=5)
    assert len(out) == 5
    assert out[-1] == {"message": "m7", "workload": "orders_api"}


def test_sqs_peek_decodes_json_and_never_deletes():
    sqs = FakeSQS([json.dumps({"message": "hi"}), "not-json"])
    out = PeekService(FakeKinesis([]), sqs).peek("sqs", "http://q", limit=5)
    assert out == [{"message": "hi"}, {"raw": "not-json"}]
    assert sqs.deleted == []


def test_flow_stats_kinesis_counts_recent_records():
    records = [{"message": f"m{i}"} for i in range(4)]
    stats = PeekService(FakeKinesis(records), FakeSQS([])).flow_stats("kinesis", "s")
    assert stats == {"recent_records": 4}


def test_flow_stats_sqs_uses_queue_depth():
    stats = PeekService(FakeKinesis([]), FakeSQS([], backlog=12)).flow_stats("sqs", "http://q")
    assert stats == {"recent_records": 12}


def test_unknown_stream_type_raises_value_error():
    svc = PeekService(FakeKinesis([]), FakeSQS([]))
    with pytest.raises(ValueError):
        svc.peek("kafka", "ref")
    with pytest.raises(ValueError):
        svc.flow_stats("kafka", "ref")


NDJSON_BATCH = (
    '{"format":"ndjson","count":2,"size":512}\n'
    '{"_raw":"<25>Jun 12 19:18:36 host1 sshd[1]: line one","account_id":"522412052544",'
    '"workload":"storefront_web","source_name":"syslog"}\n'
    '{"_raw":"<25>Jun 12 19:18:37 host1 sshd[2]: line two","account_id":"522412052544",'
    '"workload":"storefront_web","source_name":"syslog"}'
)


def test_kinesis_peek_unpacks_ndjson_batches():
    kinesis = FakeKinesis([])
    kinesis.get_records = lambda ShardIterator, Limit: {
        "Records": [{"Data": NDJSON_BATCH.encode()}],
        "NextShardIterator": None,
        "MillisBehindLatest": 0,
    }
    out = PeekService(kinesis, FakeSQS([])).peek("kinesis", "s", limit=5)
    assert len(out) == 2
    assert out[0]["workload"] == "storefront_web"
    assert all("format" not in e for e in out)  # header line dropped


def test_batch_with_junk_line_falls_back_to_raw():
    kinesis = FakeKinesis([])
    kinesis.get_records = lambda ShardIterator, Limit: {
        "Records": [{"Data": b'{"format":"ndjson","count":1,"size":9}\nnot-json-line'}],
        "NextShardIterator": None,
        "MillisBehindLatest": 0,
    }
    out = PeekService(kinesis, FakeSQS([])).peek("kinesis", "s", limit=5)
    assert out == [{"raw": "not-json-line"}]


def test_flow_stats_counts_events_not_records():
    kinesis = FakeKinesis([])
    kinesis.get_records = lambda ShardIterator, Limit: {
        "Records": [{"Data": NDJSON_BATCH.encode()}],
        "NextShardIterator": None,
        "MillisBehindLatest": 0,
    }
    stats = PeekService(kinesis, FakeSQS([])).flow_stats("kinesis", "s")
    assert stats == {"recent_records": 2}
