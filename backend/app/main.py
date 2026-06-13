import logging
from pathlib import Path

import boto3
from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.catalog.cache import SnapshotCache
from app.catalog.service import CatalogService
from app.catalog.uc_client import UCClient
from app.config import Settings, load_settings
from app.aws.access_roles import AccessRoleService
from app.cribl.admin import CriblAdmin
from app.db import get_db, init_db
from app.routes import approvals as approvals_routes
from app.routes import catalog as catalog_routes
from app.routes import session as session_routes
from app.routes import streams as streams_routes
from app.streams.peek import PeekService
from app.streams.provisioner import Provisioner
from app.streams.service import StreamService


logger = logging.getLogger(__name__)


def create_app(settings: Settings | None = None, services: dict | None = None) -> FastAPI:
    settings = settings or load_settings()
    services = services or {}
    app = FastAPI(title="LogStream Portal")
    app.state.settings = settings

    conn = services.get("conn")
    if conn is None:
        Path(settings.data_dir).mkdir(parents=True, exist_ok=True)
        conn = get_db(str(Path(settings.data_dir) / "portal.db"))
    init_db(conn)
    app.state.conn = conn

    if "catalog" in services:
        app.state.catalog = services["catalog"]
    else:
        cache = SnapshotCache(
            Path(settings.data_dir) / "catalog_snapshot.json",
            seed_path=Path(settings.snapshot_seed) if settings.snapshot_seed else None,
        )
        uc = UCClient(settings.databricks_host, settings.databricks_token)
        app.state.catalog = CatalogService(uc, cache, settings.uc_catalog)

    # "provisioner", "peek", and "access_roles" must be supplied together, or none.
    if "provisioner" in services:
        app.state.provisioner = services["provisioner"]
        app.state.peek = services["peek"]
        app.state.access_roles = services["access_roles"]
    else:
        kinesis = boto3.client("kinesis", region_name=settings.aws_region)
        sqs = boto3.client("sqs", region_name=settings.aws_region)
        iam = boto3.client("iam", region_name=settings.aws_region)
        app.state.provisioner = Provisioner(kinesis, sqs)
        app.state.peek = PeekService(kinesis, sqs)
        app.state.access_roles = AccessRoleService(iam)

    app.state.pipeline = services.get("pipeline") or CriblAdmin(
        base_url=settings.cribl_base_url, group=settings.cribl_group,
        username=settings.cribl_username, password=settings.cribl_password,
    )
    app.state.streams = StreamService(
        conn, app.state.catalog, app.state.provisioner, app.state.access_roles,
        app.state.pipeline, settings
    )

    app.include_router(session_routes.router)
    app.include_router(catalog_routes.router)
    app.include_router(streams_routes.router)
    app.include_router(approvals_routes.router)

    @app.on_event("startup")
    def resync_pipeline() -> None:
        # Make pipeline routes reflect DB state after restarts. Best-effort: the
        # leader may still be booting; any later mutation re-syncs.
        try:
            app.state.streams.reapply()
        except Exception:
            logger.warning("startup pipeline resync failed", exc_info=True)

    if settings.static_dir and Path(settings.static_dir).is_dir():
        static = Path(settings.static_dir)
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

        @app.get("/{full_path:path}", include_in_schema=False)
        def spa(full_path: str) -> FileResponse:
            # API routes were registered first and win; everything else is the SPA.
            if full_path.startswith("api/"):
                raise HTTPException(404)
            return FileResponse(static / "index.html")

    return app
