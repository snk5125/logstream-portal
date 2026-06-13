"""Create/delete the consumer's dedicated stream in (Local)AWS."""


class ProvisionError(RuntimeError):
    """The AWS resource could not be created or deleted."""


class Provisioner:
    def __init__(self, kinesis_client, sqs_client):
        self._kinesis = kinesis_client
        self._sqs = sqs_client

    def create(self, stream_type: str, name: str) -> str:
        """Returns the resource_ref: kinesis stream name or sqs queue URL."""
        if stream_type not in ("kinesis", "sqs"):
            raise ValueError(f"unknown stream_type: {stream_type!r}")
        try:
            if stream_type == "kinesis":
                self._kinesis.create_stream(StreamName=name, ShardCount=1)
                self._kinesis.get_waiter("stream_exists").wait(
                    StreamName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30}
                )
                return name
            return self._sqs.create_queue(QueueName=name)["QueueUrl"]
        except Exception as exc:
            raise ProvisionError(str(exc)) from exc

    def delete(self, stream_type: str, resource_ref: str) -> None:
        if stream_type not in ("kinesis", "sqs"):
            raise ValueError(f"unknown stream_type: {stream_type!r}")
        try:
            if stream_type == "kinesis":
                self._kinesis.delete_stream(
                    StreamName=resource_ref, EnforceConsumerDeletion=True
                )
            else:
                self._sqs.delete_queue(QueueUrl=resource_ref)
        except Exception as exc:
            raise ProvisionError(str(exc)) from exc
