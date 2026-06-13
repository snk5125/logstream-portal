from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from app.routes.deps import SESSION_COOKIE, current_user, serializer

router = APIRouter()


class LoginBody(BaseModel):
    user_id: str


@router.get("/api/personas")
def personas(request: Request) -> list[dict]:
    rows = request.app.state.conn.execute(
        "SELECT * FROM users ORDER BY role, id"
    ).fetchall()
    return [dict(r) for r in rows]


@router.post("/api/session")
def login(body: LoginBody, request: Request, response: Response) -> dict:
    row = request.app.state.conn.execute(
        "SELECT * FROM users WHERE id = ?", (body.user_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(404, "unknown persona")
    token = serializer(request).dumps(row["id"])
    response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax")
    return dict(row)


@router.get("/api/session")
def me(user: dict = Depends(current_user)) -> dict:
    return user


@router.delete("/api/session")
def logout(response: Response) -> dict:
    response.delete_cookie(SESSION_COOKIE)
    return {"ok": True}
