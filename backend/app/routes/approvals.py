from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.routes.deps import admin_user
from app.streams.service import StreamServiceError

router = APIRouter()


class DecisionBody(BaseModel):
    approved: bool


@router.get("/api/approvals")
def list_approvals(request: Request, admin: dict = Depends(admin_user)) -> list[dict]:
    rows = request.app.state.conn.execute(
        "SELECT ss.id, ss.stream_id, s.name AS stream_name, ss.source_fqn,"
        "       ss.requested_by, ss.requested_at"
        " FROM stream_sources ss JOIN streams s ON s.id = ss.stream_id"
        " WHERE ss.status = 'pending_approval' AND s.status != 'deleted'"
        " ORDER BY ss.requested_at, ss.id"
    ).fetchall()
    return [dict(r) for r in rows]


@router.post("/api/approvals/{request_id}")
def decide(
    request_id: int, body: DecisionBody, request: Request, admin: dict = Depends(admin_user)
) -> dict:
    try:
        request.app.state.streams.approve(admin, request_id, body.approved)
    except StreamServiceError as exc:
        raise HTTPException(exc.status_code, str(exc))
    return {"ok": True}
