import logging
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.aws.access_bundle import build_access_bundle
from app.routes.deps import current_user
from app.streams.service import StreamServiceError

logger = logging.getLogger(__name__)

router = APIRouter()


def _with_flow(stream: dict) -> dict:
    stream.setdefault("flow", None)
    return stream


class CreateStreamBody(BaseModel):
    name: str
    type: Literal["kinesis", "sqs"]
    source_fqns: list[str]


class AddSourcesBody(BaseModel):
    source_fqns: list[str]


def _call(fn):
    try:
        return fn()
    except StreamServiceError as exc:
        raise HTTPException(exc.status_code, str(exc))


@router.get("/api/streams")
def list_streams(request: Request, user: dict = Depends(current_user)) -> list[dict]:
    streams = _call(lambda: request.app.state.streams.list_streams(user))
    peek = request.app.state.peek
    for stream in streams:
        stream["flow"] = None
        if stream["status"] == "live":
            try:
                stream["flow"] = peek.flow_stats(stream["type"], stream["resource_ref"])
            except Exception:
                logger.debug("flow stats unavailable for stream %s", stream["id"], exc_info=True)
    return streams


@router.post("/api/streams", status_code=201)
def create_stream(
    body: CreateStreamBody, request: Request, user: dict = Depends(current_user)
) -> dict:
    return _with_flow(
        _call(
            lambda: request.app.state.streams.create_stream(
                user, body.name, body.type, body.source_fqns
            )
        )
    )


@router.delete("/api/streams/{stream_id}", status_code=204)
def delete_stream(stream_id: int, request: Request, user: dict = Depends(current_user)) -> None:
    _call(lambda: request.app.state.streams.delete_stream(user, stream_id))


@router.post("/api/streams/{stream_id}/retry")
def retry_stream(stream_id: int, request: Request, user: dict = Depends(current_user)) -> dict:
    return _with_flow(_call(lambda: request.app.state.streams.retry(user, stream_id)))


@router.post("/api/streams/{stream_id}/sources")
def add_sources(
    stream_id: int, body: AddSourcesBody, request: Request, user: dict = Depends(current_user)
) -> dict:
    return _with_flow(
        _call(lambda: request.app.state.streams.add_sources(user, stream_id, body.source_fqns))
    )


@router.delete("/api/streams/{stream_id}/sources/{source_fqn:path}")
def remove_source(
    stream_id: int, source_fqn: str, request: Request, user: dict = Depends(current_user)
) -> dict:
    return _with_flow(
        _call(lambda: request.app.state.streams.remove_source(user, stream_id, source_fqn))
    )


@router.get("/api/streams/{stream_id}/peek")
def peek_stream(stream_id: int, request: Request, user: dict = Depends(current_user)) -> list:
    stream = _call(lambda: request.app.state.streams.get_stream(user, stream_id))
    if stream["status"] != "live":
        raise HTTPException(409, "stream is not live")
    try:
        return request.app.state.peek.peek(stream["type"], stream["resource_ref"])
    except Exception as exc:
        raise HTTPException(502, f"could not read stream: {exc}")


@router.get("/api/streams/{stream_id}/access-bundle")
def access_bundle(stream_id: int, request: Request, user: dict = Depends(current_user)) -> dict:
    stream = _call(lambda: request.app.state.streams.get_stream(user, stream_id))
    if not stream.get("read_role_arn"):
        raise HTTPException(404, "this stream has no access role (it may be in error state)")
    settings = request.app.state.settings
    return build_access_bundle(stream, settings.aws_region, settings.logging_account_id)
