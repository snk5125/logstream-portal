# Cribl integration — license gate result & verified API shapes

## License gate (Task 0) — outcome: **Plan B**

The free Cribl license **prohibits multiple worker groups**:

```
POST /api/v1/master/groups {"id":"logging",...}
→ 500 {"status":"error","message":"Multiple worker groups is prohibited by the current license"}
```

**Consequence:** the portal's fork/logging tier is the leader's single built-in
worker group **`default`**. The collection tier for accounts B and C runs as
**standalone Cribl Edge / single-instance** deployments (each its own free
instance), tagging events and forwarding to the `default` group's TCP ingest.
The portal code is identical to Plan A; only `CRIBL_GROUP=default` and the
seeder/deploy topology differ.

Verified against `cribl/cribl:latest` (single-instance master mode), 2026-06-12.

## Verified REST API (group = `default`)

Base: `http://<leader>:9000/api/v1`

**Auth.** `POST /auth/login` body `{"username","password"}` →
`{"token","forcePasswordChange":bool}`. The token is a bearer JWT (≈1h). The
`forcePasswordChange` flag does not block API use. Header:
`Authorization: Bearer <token>`.

**Routes are a single table object, updated wholesale.**
- `GET /m/default/routes` → `{"items":[{"id":"default","routes":[ {route...}, ... ]}], "count":1}`
- Update: `PATCH /m/default/routes/default` with the **entire** table object
  `{"id":"default","routes":[...]}`. PUT/POST/PATCH at `/m/default/routes`
  (no id) and PUT at `/routes/default` are **not** supported (404 /
  "no handler for create:/routes"). Reconcile = GET table → drop existing
  `fork_*` entries → prepend desired `fork_*` routes → keep the rest (e.g. the
  permanent `archive`/`default` route) → PATCH back.
- A route object: `{"id","name","final":bool,"disabled":bool,"filter":<JS expr>,
  "pipeline":"passthru","output":<dest id>,"description"}`. Use the built-in
  **`passthru`** pipeline so events reach the destination unchanged.

**Destinations (outputs) are individual objects.**
- `GET /m/default/system/outputs` → `{"items":[{dest...}], "count"}`
- Create: `POST /m/default/system/outputs` (body is the dest object)
- Update: `PATCH /m/default/system/outputs/<id>`
- Delete: `DELETE /m/default/system/outputs/<id>`

**Sources (inputs)** mirror outputs at `/m/default/system/inputs[/<id>]`
(POST create, PATCH update). **Pipelines** at `/m/default/pipelines[/<id>]`.

**Commit + deploy.**
- `POST /version/commit` body `{"message","group":"default"}` →
  `{"items":[{"commit":"<sha>", ...}]}`
- `PATCH /master/groups/default/deploy` body `{"version":"<sha>"}` →
  returns the group with `configVersion == <sha>`. (Note: **PATCH**, not POST.)

**Convergence check.** Poll `GET /master/groups/default` →
`items[0].configVersion`. After a successful deploy this equals the committed
sha. `deployedVersion` only populates once a live worker heartbeats the config,
so the portal verifies on **`configVersion`**, which is sufficient proof the
deploy was accepted by the leader.

## Built-in pipelines available
`passthru` (used by fork routes), `devnull`, `main`, plus several packs.

## Implications for the implementation
- `app/cribl/objects.py`: route `pipeline = "passthru"`, `output = fork_<id>_dest`.
- `app/cribl/admin.py`: route reconcile = wholesale-PATCH the `default` routes
  table; destinations = per-id POST/PATCH/DELETE; deploy = PATCH with version;
  convergence = poll `configVersion`.
- `CRIBL_GROUP` default = `default`.

## Live AWS bring-up — validated deltas (2026-06-12)

Discovered while bringing the 3-account stack up; fold these into the IaC/seed
for a clean `make infra-up`:

1. **Logging tier must be leader + worker, both `--network host`.** A single
   `CRIBL_DIST_MODE=master` container manages config but runs no worker, so it
   never binds the `:10300` ingest port (NLB targets stay unhealthy). Run a
   second `CRIBL_DIST_MODE=worker` container enrolled to `tcp://criblmaster@localhost:4200`,
   group `default`, with **`CRIBL_AUTO_PORTS=1`** (else it fights the leader for
   `:9000` → `EADDRINUSE`). Host networking keeps IMDS/instance-role working for
   Kinesis and makes `:4200` localhost-only.
2. **Edge collectors are single-instance** (flat `/api/v1/...` API). Use the
   **Routes** model: datagen sources `sendToRoutes: true` with the tag Eval as
   the source pre-processing `pipeline`, plus one route `filter:"true" →
   to_logging`. Source-level `output` with `sendToRoutes:false` (QuickConnect)
   did NOT deliver events.
3. **Cribl-native forward is type `tcpjson`** (not `tcp`); the logging input
   must also be `tcpjson` on `:10300`.
4. **Datagen sample ids have no extension**: `syslog`, `apache_common`,
   `business_event` (not `*.json`).
5. **Kinesis/SQS destinations: set `compression: "none"`** (done in
   `objects.py`) so consumers read plain JSON, not gzip.
6. **Portal↔Cribl auth**: leader runs default `admin/admin`; set the portal's
   `CRIBL_PASSWORD` to `admin` (the password-rotation API rejected payloads;
   the Cribl API is private/VPC+SSM-only so this is acceptable for the demo).
7. **Known portal polish**: Cribl's Kinesis destination writes ndjson **batches**
   (first line is a `{"format":"ndjson",...}` header). `PeekService` currently
   decodes the whole record as one JSON object and shows the header — it should
   split on newlines and skip the header line to display individual events.
   (Data is correct; verified by reading Kinesis directly.)
