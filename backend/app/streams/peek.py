"""Read-only views into a consumer's stream: sample records and flow stats."""
import json


def _decode_events(data) -> list:
    """Decode one transport record into individual events.

    Cribl destinations write ndjson batches: a header line like
    {"format":"ndjson","count":N,"size":...} followed by one JSON event per
    line. Plain single-JSON records (and non-JSON lines) are handled too.
    """
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="replace")
    events = []
    for line in str(data).splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            events.append({"raw": line})
            continue
        if isinstance(obj, dict) and obj.get("format") == "ndjson" and "_raw" not in obj:
            continue  # batch header
        events.append(obj)
    return events


class PeekService:
    def __init__(self, kinesis_client, sqs_client):
        self._kinesis = kinesis_client
        self._sqs = sqs_client

    def peek(self, stream_type: str, resource_ref: str, limit: int = 5) -> list:
        if stream_type not in ("kinesis", "sqs"):
            raise ValueError(f"unsupported stream_type: {stream_type!r}")
        if stream_type == "kinesis":
            return self._peek_kinesis(resource_ref, limit)
        return self._peek_sqs(resource_ref, limit)

    def _peek_kinesis(self, stream_name: str, limit: int) -> list:
        description = self._kinesis.describe_stream(StreamName=stream_name)
        shard_id = description["StreamDescription"]["Shards"][0]["ShardId"]
        iterator = self._kinesis.get_shard_iterator(
            StreamName=stream_name, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        records = []
        for _ in range(5):  # bounded catch-up through the shard
            out = self._kinesis.get_records(ShardIterator=iterator, Limit=1000)
            records.extend(out["Records"])
            iterator = out.get("NextShardIterator")
            if not iterator or out.get("MillisBehindLatest", 0) == 0:
                break
        events = []
        for r in records:
            events.extend(_decode_events(r["Data"]))
        return events[-limit:]

    def _peek_sqs(self, queue_url: str, limit: int) -> list:
        out = self._sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(limit, 10),
            WaitTimeSeconds=1,
            VisibilityTimeout=0,  # non-destructive: messages stay visible
        )
        events = []
        for m in out.get("Messages", []):
            events.extend(_decode_events(m["Body"]))
        return events[-limit:]

    def flow_stats(self, stream_type: str, resource_ref: str) -> dict:
        if stream_type not in ("kinesis", "sqs"):
            raise ValueError(f"unsupported stream_type: {stream_type!r}")
        if stream_type == "kinesis":
            return {"recent_records": len(self.peek("kinesis", resource_ref, limit=50))}
        attrs = self._sqs.get_queue_attributes(
            QueueUrl=resource_ref, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return {"recent_records": int(attrs["Attributes"]["ApproximateNumberOfMessages"])}
