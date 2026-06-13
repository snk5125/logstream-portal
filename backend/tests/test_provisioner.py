import pytest

from app.streams.provisioner import ProvisionError, Provisioner


class FakeWaiter:
    def __init__(self, log):
        self._log = log

    def wait(self, **kwargs):
        self._log.append(("wait", kwargs))


class FakeKinesis:
    def __init__(self, fail=False, fail_delete=False):
        self.log, self._fail, self._fail_delete = [], fail, fail_delete

    def create_stream(self, **kwargs):
        if self._fail:
            raise RuntimeError("LimitExceededException")
        self.log.append(("create_stream", kwargs))

    def get_waiter(self, name):
        self.log.append(("get_waiter", name))
        return FakeWaiter(self.log)

    def delete_stream(self, **kwargs):
        if self._fail_delete:
            raise RuntimeError("ResourceInUse")
        self.log.append(("delete_stream", kwargs))


class FakeSQS:
    def __init__(self):
        self.log = []

    def create_queue(self, **kwargs):
        self.log.append(("create_queue", kwargs))
        return {"QueueUrl": f"http://localstack:4566/000000000000/{kwargs['QueueName']}"}

    def delete_queue(self, **kwargs):
        self.log.append(("delete_queue", kwargs))


def test_create_kinesis_waits_for_active_and_returns_name():
    kinesis = FakeKinesis()
    ref = Provisioner(kinesis, FakeSQS()).create("kinesis", "team-a-logs")
    assert ref == "team-a-logs"
    ops = [op for op, _ in kinesis.log]
    assert ops == ["create_stream", "get_waiter", "wait"]


def test_create_sqs_returns_queue_url():
    ref = Provisioner(FakeKinesis(), FakeSQS()).create("sqs", "audit-q")
    assert ref == "http://localstack:4566/000000000000/audit-q"


def test_create_failure_wraps_in_provision_error():
    with pytest.raises(ProvisionError):
        Provisioner(FakeKinesis(fail=True), FakeSQS()).create("kinesis", "x")


def test_delete_routes_by_type():
    kinesis, sqs = FakeKinesis(), FakeSQS()
    p = Provisioner(kinesis, sqs)
    p.delete("kinesis", "team-a-logs")
    p.delete("sqs", "http://localstack:4566/000000000000/audit-q")
    assert kinesis.log[-1] == ("delete_stream", {"StreamName": "team-a-logs", "EnforceConsumerDeletion": True})
    assert sqs.log[-1] == ("delete_queue", {"QueueUrl": "http://localstack:4566/000000000000/audit-q"})


def test_unknown_stream_type_raises_value_error():
    p = Provisioner(FakeKinesis(), FakeSQS())
    with pytest.raises(ValueError):
        p.create("firehose", "x")
    with pytest.raises(ValueError):
        p.delete("firehose", "x")


def test_delete_failure_wraps_in_provision_error():
    with pytest.raises(ProvisionError):
        Provisioner(FakeKinesis(fail_delete=True), FakeSQS()).delete("kinesis", "x")
