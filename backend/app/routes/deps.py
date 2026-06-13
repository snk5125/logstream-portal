from fastapi import Depends, HTTPException, Request
from itsdangerous import BadSignature, URLSafeSerializer

SESSION_COOKIE = "portal_session"


def serializer(request: Request) -> URLSafeSerializer:
    return URLSafeSerializer(request.app.state.settings.session_secret, salt="session")


def current_user(request: Request) -> dict:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise HTTPException(401, "not logged in")
    try:
        user_id = serializer(request).loads(token)
    except BadSignature:
        raise HTTPException(401, "invalid session")
    row = request.app.state.conn.execute(
        "SELECT * FROM users WHERE id = ?", (user_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(401, "unknown user")
    return dict(row)


def admin_user(user: dict = Depends(current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(403, "admin only")
    return user
