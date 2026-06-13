from fastapi import APIRouter, Depends, HTTPException, Request

from app.catalog.service import annotate, scope_tree
from app.catalog.uc_client import CatalogUnavailable
from app.routes.deps import current_user

router = APIRouter()


@router.get("/api/catalog")
def get_catalog(request: Request, user: dict = Depends(current_user)) -> dict:
    try:
        tree = request.app.state.catalog.get_tree()
    except CatalogUnavailable as exc:
        raise HTTPException(503, f"catalog unavailable and no cached snapshot: {exc}")
    tree = scope_tree(tree, user.get("account_scope"))
    rows = request.app.state.conn.execute(
        "SELECT ss.source_fqn, ss.status, s.id AS stream_id, s.name AS stream_name"
        " FROM stream_sources ss JOIN streams s ON s.id = ss.stream_id"
        " WHERE s.owner_id = ? AND s.status != 'deleted'",
        (user["id"],),
    ).fetchall()
    return annotate(tree, [dict(r) for r in rows])
