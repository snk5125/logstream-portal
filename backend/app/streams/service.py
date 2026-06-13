"""Fork lifecycle: resolve sources, gate by sensitivity, provision, reapply."""
from app.catalog.uc_client import CatalogUnavailable
from app.catalog.service import find_source
from app.cribl.objects import Member, StreamSpec, route_for, destination_for
from app.cribl.admin import CriblApplyError
from app.streams.provisioner import ProvisionError
from app.aws.access_roles import AccessRoleError, resource_arn_for, role_name_for


class StreamServiceError(RuntimeError):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


class StreamService:
    def __init__(self, conn, catalog, provisioner, access_roles, pipeline, settings):
        self._conn = conn
        self._catalog = catalog
        self._provisioner = provisioner
        self._roles = access_roles
        self._pipeline = pipeline
        self._settings = settings

    # ── queries ────────────────────────────────────────────────────────
    def get_stream(self, user: dict, stream_id: int) -> dict:
        row = self._conn.execute(
            "SELECT * FROM streams WHERE id = ? AND status != 'deleted'", (stream_id,)
        ).fetchone()
        if row is None:
            raise StreamServiceError("stream not found", 404)
        if row["owner_id"] != user["id"] and user["role"] != "admin":
            raise StreamServiceError("not your stream", 403)
        stream = dict(row)
        stream["sources"] = [
            dict(r) for r in self._conn.execute(
                "SELECT * FROM stream_sources WHERE stream_id = ? ORDER BY source_fqn",
                (stream_id,),
            )
        ]
        return stream

    def list_streams(self, user: dict) -> list[dict]:
        rows = self._conn.execute(
            "SELECT id FROM streams WHERE owner_id = ? AND status != 'deleted' ORDER BY id",
            (user["id"],),
        ).fetchall()
        return [self.get_stream(user, r["id"]) for r in rows]

    # ── mutations ──────────────────────────────────────────────────────
    def create_stream(self, user, name, stream_type, source_fqns) -> dict:
        if not source_fqns:
            raise StreamServiceError("select at least one source")
        if self._conn.execute(
            "SELECT 1 FROM streams WHERE name = ? AND status != 'deleted'", (name,)
        ).fetchone():
            raise StreamServiceError(f"stream name {name!r} already in use", 409)
        resolved = self._resolve(source_fqns, user.get("account_scope"))
        cur = self._conn.execute(
            "INSERT INTO streams (owner_id, name, type, status) VALUES (?, ?, ?, 'provisioning')",
            (user["id"], name, stream_type),
        )
        stream_id = cur.lastrowid
        try:
            ref = self._provisioner.create(stream_type, self._settings.resource_prefix + name)
        except ProvisionError as exc:
            self._conn.execute(
                "UPDATE streams SET status = 'error', last_error = ? WHERE id = ?",
                (str(exc), stream_id),
            )
            return self.get_stream(user, stream_id)
        self._conn.execute(
            "UPDATE streams SET resource_ref = ?, status = 'live', last_error = NULL WHERE id = ?",
            (ref, stream_id),
        )
        try:
            self._mint_role(stream_id, name, stream_type, ref, user)
        except AccessRoleError as exc:
            self._conn.execute(
                "UPDATE streams SET status = 'error', last_error = ? WHERE id = ?",
                (str(exc), stream_id),
            )
            return self.get_stream(user, stream_id)
        self._insert_members(stream_id, user, resolved)
        self._reapply_or_flag(stream_id)
        return self.get_stream(user, stream_id)

    def add_sources(self, user, stream_id, source_fqns) -> dict:
        stream = self.get_stream(user, stream_id)
        resolved = self._resolve(source_fqns, user.get("account_scope"))
        existing = {s["source_fqn"] for s in stream["sources"]}
        fresh = [r for r in resolved if r["fqn"] not in existing]
        if not fresh:
            raise StreamServiceError("all selected sources are already on this stream", 409)
        self._insert_members(stream_id, user, fresh)
        self._reapply_or_flag(stream_id)
        return self.get_stream(user, stream_id)

    def remove_source(self, user, stream_id, source_fqn) -> dict:
        self.get_stream(user, stream_id)  # ownership check
        cur = self._conn.execute(
            "DELETE FROM stream_sources WHERE stream_id = ? AND source_fqn = ?",
            (stream_id, source_fqn),
        )
        if cur.rowcount == 0:
            raise StreamServiceError("source not on this stream", 404)
        self._reapply_or_flag(stream_id)
        return self.get_stream(user, stream_id)

    def delete_stream(self, user, stream_id) -> None:
        stream = self.get_stream(user, stream_id)
        self._conn.execute("UPDATE streams SET status = 'deleted' WHERE id = ?", (stream_id,))
        # Detach from the pipeline first, then tear down the resource. A Cribl
        # hiccup must not block deletion: regeneration is wholesale from DB, so
        # the next successful reapply drops this fork regardless.
        try:
            self.reapply()
        except CriblApplyError:
            pass
        # Always attempt role deletion: a half-failed mint can leave a role
        # behind with read_role_arn still NULL, and delete() is idempotent.
        try:
            self._roles.delete(role_name_for(stream_id, stream["name"]))
        except AccessRoleError as exc:
            self._conn.execute(
                "UPDATE streams SET last_error = ? WHERE id = ?", (str(exc), stream_id))
        if stream["resource_ref"]:
            try:
                self._provisioner.delete(stream["type"], stream["resource_ref"])
            except ProvisionError as exc:
                # The fork is gone from the pipeline; a lingering resource
                # is harmless. Record it and move on.
                self._conn.execute(
                    "UPDATE streams SET last_error = ? WHERE id = ?", (str(exc), stream_id)
                )

    def retry(self, user, stream_id) -> dict:
        stream = self.get_stream(user, stream_id)
        if stream["resource_ref"] is None:
            try:
                ref = self._provisioner.create(
                    stream["type"], self._settings.resource_prefix + stream["name"]
                )
            except ProvisionError as exc:
                self._conn.execute(
                    "UPDATE streams SET status = 'error', last_error = ? WHERE id = ?",
                    (str(exc), stream_id),
                )
                return self.get_stream(user, stream_id)
            self._conn.execute(
                "UPDATE streams SET resource_ref = ?, status = 'live', last_error = NULL WHERE id = ?",
                (ref, stream_id),
            )
        else:
            self._conn.execute(
                "UPDATE streams SET status = 'live', last_error = NULL WHERE id = ?",
                (stream_id,),
            )
        stream = self.get_stream(user, stream_id)
        if stream["read_role_arn"] is None:
            try:
                self._mint_role(stream_id, stream["name"], stream["type"],
                                stream["resource_ref"], user)
            except AccessRoleError as exc:
                self._conn.execute(
                    "UPDATE streams SET status = 'error', last_error = ? WHERE id = ?",
                    (str(exc), stream_id),
                )
                return self.get_stream(user, stream_id)
        self._reapply_or_flag(stream_id)
        return self.get_stream(user, stream_id)

    def approve(self, admin, request_id, approved: bool) -> None:
        if admin["role"] != "admin":
            raise StreamServiceError("admin only", 403)
        new_status = "active" if approved else "rejected"
        cur = self._conn.execute(
            "UPDATE stream_sources SET status = ?, decided_by = ?, decided_at = datetime('now')"
            " WHERE id = ? AND status = 'pending_approval'",
            (new_status, admin["id"], request_id),
        )
        if cur.rowcount == 0:
            raise StreamServiceError("no such pending request", 404)
        if approved:
            row = self._conn.execute(
                "SELECT stream_id FROM stream_sources WHERE id = ?", (request_id,)
            ).fetchone()
            self._reapply_or_flag(row["stream_id"])

    # ── internals ──────────────────────────────────────────────────────
    def _mint_role(self, stream_id, name, stream_type, resource_ref, user) -> None:
        consumer = user.get("account_scope") or self._settings.logging_account_id
        arn = resource_arn_for(stream_type, resource_ref,
                               self._settings.aws_region, self._settings.logging_account_id)
        created = self._roles.create(stream_id, name, stream_type, arn, consumer)
        self._conn.execute(
            "UPDATE streams SET read_role_arn = ?, consumer_account_id = ? WHERE id = ?",
            (created["role_arn"], consumer, stream_id),
        )

    def _resolve(self, source_fqns, account_scope) -> list[dict]:
        try:
            tree = self._catalog.get_tree()
        except CatalogUnavailable as exc:
            raise StreamServiceError(f"catalog unavailable: {exc}", 503)
        resolved = []
        for fqn in source_fqns:
            found = find_source(tree, fqn)
            if found is None:
                raise StreamServiceError(f"unknown source: {fqn}", 404)
            if account_scope is not None and found["account_id"] != account_scope:
                raise StreamServiceError(
                    f"source {fqn} is outside your account scope", 403)
            resolved.append(found)
        return resolved

    def _insert_members(self, stream_id, user, resolved) -> None:
        for src in resolved:
            # Sensitivity gate, enforced server-side from catalog metadata.
            status = "pending_approval" if src["sensitivity"] == "sensitive" else "active"
            self._conn.execute(
                "INSERT INTO stream_sources"
                " (stream_id, source_fqn, account_id, workload, source_name, status, requested_by)"
                " VALUES (?, ?, ?, ?, ?, ?, ?)",
                (stream_id, src["fqn"], src["account_id"], src["workload_tag"],
                 src["source_name"], status, user["id"]),
            )

    def reapply(self) -> None:
        """Regenerate all fork routes+destinations from DB state and apply."""
        routes, destinations = [], []
        for stream in self._conn.execute("SELECT * FROM streams WHERE status = 'live'").fetchall():
            members = tuple(
                Member(r["account_id"], r["workload"], r["source_name"])
                for r in self._conn.execute(
                    "SELECT account_id, workload, source_name FROM stream_sources"
                    " WHERE stream_id = ? AND status = 'active'"
                    " ORDER BY account_id, workload, source_name",
                    (stream["id"],),
                )
            )
            if members:
                spec = StreamSpec(stream["id"], stream["type"], stream["resource_ref"], members)
                routes.append(route_for(spec))
                destinations.append(destination_for(spec, self._settings.aws_region))
        self._pipeline.apply(routes, destinations)

    def _reapply_or_flag(self, stream_id) -> None:
        try:
            self.reapply()
        except (CriblApplyError, ValueError) as exc:
            self._conn.execute(
                "UPDATE streams SET last_error = ? WHERE id = ?", (str(exc), stream_id)
            )
            raise StreamServiceError(f"could not apply the generated config: {exc}", 502)
