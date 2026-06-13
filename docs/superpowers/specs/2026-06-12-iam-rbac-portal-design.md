# LogStream Portal — Per-Stream IAM Read Roles & Account-Scoped RBAC (Sub-project 2)

**Date:** 2026-06-12
**Status:** Approved design, pending implementation plan
**Builds on:** `2026-06-12-cribl-aws-portal-design.md` (live 3-account Cribl stack)

## Overview

Two product features on the live Cribl-on-AWS portal, plus one carried-over fix:

1. **Per-stream IAM read role with downloadable access bundle.** Every stream a
   user creates gets a dedicated IAM role in the Central Logging Platform
   account that the owner's workload account can assume to read that one
   stream. A **Download access** button on the My Streams card delivers a JSON
   bundle with the role ARN, policies, and runnable CLI snippets.
2. **Account-scoped RBAC for demo personas.** Consumers see and can fork only
   their own account's sources: `dana@app-team` → Workload Account A
   (`522412052544`), `raj@data-sci` → Workload Account B (`624627265315`),
   `admin@platform` → everything. Enforced server-side.
3. **Peek ndjson fix** (first implementation task): Cribl's Kinesis destination
   writes ndjson batches with a header line; `PeekService` must split batches
   and return individual events instead of the current `{"raw": ...}` blobs.

Execution note: implementation is handed to **Opus 4.8** subagents
(`model: "opus"` on implementer dispatches) after plan review, per operator
instruction. Two plan tasks touch the **live** AWS stack (Terraform apply for
portal IAM permissions; end-to-end verification).

## Feature 1 — Per-stream IAM read role

### AccessRoleService (`backend/app/aws/access_roles.py`)

New injected service following the `Provisioner` pattern (boto3 IAM client
injected; `FakeIAM` in tests):

- `create(stream_id, stream_name, stream_type, resource_arn, consumer_account_id) -> dict`
  — creates role `logstream-read-<stream_id>-<stream_name[:40]>` (path
  `/logstream/`; the id prefix guarantees uniqueness and the truncation keeps
  the name under IAM's 64-char limit):
  - **Trust policy:** `Allow sts:AssumeRole` to principal
    `arn:aws:iam::<consumer_account_id>:root`.
  - **Inline policy** (`read-access`), scoped to exactly `resource_arn`:
    - kinesis: `kinesis:GetRecords`, `GetShardIterator`, `DescribeStream`,
      `DescribeStreamSummary`, `ListShards`
    - sqs: `sqs:ReceiveMessage`, `sqs:GetQueueAttributes`, `sqs:DeleteMessage`
      (consumers own their queue's consumption; destructive read is intended)
  - Returns `{role_name, role_arn, trust_policy, permission_policy}`.
- `delete(role_name)` — delete inline policy then role; not-found is success
  (idempotent).
- All boto failures wrap in `AccessRoleError` → the standard stream `error` +
  `last_error` + Retry path.

### Lifecycle (StreamService)

- `streams` table gains `read_role_arn TEXT` and `consumer_account_id TEXT`.
- On create (and on retry after provisioning failure): provision resource →
  create access role → store both columns → Cribl reapply. Role-creation
  failure marks the stream `error` (resource exists; retry re-runs the missing
  steps idempotently).
- `consumer_account_id` = the owner's `account_scope`; for unscoped owners
  (admin), it defaults to the logging account `337394138208`.
- `delete_stream`: Cribl detach (existing) → access-role delete → resource
  teardown. Role-delete failure records `last_error` but does not block
  deletion (same posture as resource teardown).

### Download bundle

- `GET /api/streams/{id}/access-bundle` — owner-only (existing `get_stream`
  ownership check). Returns generated JSON (nothing secret; no credentials):

```json
{
  "stream": {"name": "...", "type": "kinesis", "resource": "...", "region": "us-east-1"},
  "role_arn": "arn:aws:iam::337394138208:role/logstream/logstream-read-7-team-a-orders",
  "consumer_account_id": "522412052544",
  "trust_policy": { ... },
  "permission_policy": { ... },
  "usage": {
    "assume_role": "aws sts assume-role --role-arn ... --role-session-name read-<name>",
    "read": "aws kinesis get-shard-iterator ... && aws kinesis get-records ..."
  }
}
```

  (sqs variant: `aws sqs receive-message --queue-url ...`.) 404 with a clear
  message if the stream has no role yet (e.g. created pre-feature or mid-error).
- **Frontend:** StreamCard gains a **Download access** button (live streams
  with a role): fetches the bundle, saves as `<stream-name>-access.json` via
  blob download. No new dependencies.

### Infra delta (live stack)

Terraform: the portal instance role gains one statement —
`iam:CreateRole, DeleteRole, PutRolePolicy, DeleteRolePolicy, GetRole` on
`arn:aws:iam::337394138208:role/logstream/logstream-read-*` — applied to the
running stack with `terraform apply`. The portal can mint scoped read roles
and nothing else.

## Feature 2 — Account-scoped RBAC

- `users` gains `account_scope TEXT NULL`; seeded:
  `dana@app-team` → `522412052544`, `raj@data-sci` → `624627265315`,
  `admin@platform` → `NULL` (unscoped).
- **Catalog chokepoint:** pure function `scope_tree(tree, account_scope)`
  (catalog service module) returns a filtered copy keeping only matching
  accounts; `NULL` scope → unchanged. Applied in the catalog route before
  annotation. Scoped personas never see out-of-scope accounts.
- **Mutation chokepoint:** `StreamService._resolve()` raises
  `StreamServiceError(403)` when a resolved source's `account_id` is outside
  the requester's scope. Every fork/add-source flows through `_resolve`, so
  this closes the wizard and raw-API paths alike. Peek/remove/delete remain
  owner-gated as today; approvals remain admin-only.
- **Session payload** includes `account_scope`; the UI may show a scope label,
  but enforcement is entirely server-side.

## Carried-over fix — Peek ndjson batches

Cribl's Kinesis destination writes each Kinesis record as an ndjson batch:
first line `{"format":"ndjson","count":N,"size":...}`, then one JSON event per
line. `PeekService`:

- `_decode` becomes batch-aware: split record payload on newlines; drop the
  header line (object with a `format` key and no `_raw`); parse remaining
  lines as events; unparseable lines fall back to `{"raw": line}`.
- `peek()` flattens events across records and returns the latest `limit`
  events. `flow_stats` counts events, not Kinesis records.
- Failing-first test fixture reproduces the exact observed batch format
  (gzip-era records are gone; destinations now write uncompressed).

## Error handling summary

| Failure | Behavior |
|---|---|
| IAM create fails | stream `error` + `last_error`; Retry re-runs missing steps |
| IAM delete fails | stream still deletes; `last_error` records the leak |
| Bundle requested, no role | 404 with explanatory detail |
| Out-of-scope source in fork/add | 403 from `_resolve` |
| Scoped persona's catalog | filtered, never errors |

## Testing

- **Unit:** AccessRoleService against FakeIAM (create payload shapes, delete
  idempotency, error wrap); `scope_tree`; `_resolve` 403; bundle golden JSON;
  ndjson `_decode` (header skip, multi-line, junk line fallback).
- **API integration:** bundle ownership (owner 200 / other 403 / no-role 404);
  catalog per persona (dana sees only A, raj only B, admin both); cross-scope
  fork → 403; stream create persists `read_role_arn`/`consumer_account_id`.
- **Frontend:** StreamCard shows Download access for live streams; click hits
  the bundle URL (component test, existing patterns).
- **Live verification (final task):** dana forks an in-scope source → role
  exists in IAM with correct trust/permissions; dana attempts raj's source via
  raw API → 403; from `--profile seth-demo-b`, `sts assume-role` on the bundle
  ARN then `kinesis get-records` returns events — the cross-account read,
  proven with the minted role.

## Out of scope

- Real authentication / SSO (personas remain).
- Per-source-level grants, external IDs, or role session policies.
- Consumer-side IaC; SIEM wiring (diagram-only consumer).
- Backfilling roles for streams created before this feature (delete/recreate).

## Decisions log

| Decision | Choice |
|---|---|
| Role model | Dedicated role per stream (`logstream-read-<name>`, path `/logstream/`), immutable, 1:1 lifecycle with the stream |
| Trust | Owner's mapped workload account root; admin-owned → logging account |
| Artifact | Generated JSON bundle (ARN, policies, runnable CLI snippets), owner-only endpoint, blob download in UI |
| RBAC mapping | dana→522412052544, raj→624627265315, admin→unscoped; `users.account_scope` |
| Enforcement | Server-side at catalog (scope_tree) + `_resolve` (403); UI cosmetic |
| Peek fix | Batch-aware ndjson decode in PeekService, first plan task |
| Execution | Subagent-driven; implementers on Opus 4.8; two live-AWS tasks |

## As-built amendments

_Deltas observed while executing Tasks 0–8; the design above is otherwise as-shipped._

- **Delete-path role cleanup is name-derived, not state-derived.** `delete_stream`
  always attempts `roles.delete(role_name_for(stream_id, name))` rather than
  keying off `read_role_arn`, so a half-failed mint (role created but
  `read_role_arn` left NULL) still gets cleaned up. `delete()` is idempotent and
  swallows `NoSuchEntity`. (Hardening from code review.)
- **Infra pinning via `ignore_changes`.** The EC2 instances pin
  `ignore_changes = [ami]` and the SSM secret module pins `[value]`, so routine
  `terraform apply` runs do not churn the running hosts on a new Amazon Linux
  AMI release or rotate already-set secrets.
- **Portal→Cribl container networking (found in Task 8 live verification).** The
  portal container runs on the Cribl leader host in the default (bridge)
  network, so `CRIBL_BASE_URL=http://localhost:9000` resolved to the container
  itself and every fork failed with `cribl login failed: Connection refused`.
  Fixed in `user_data_logging.sh.tftpl` by launching the portal with
  `--add-host host.docker.internal:host-gateway` and
  `CRIBL_BASE_URL=http://host.docker.internal:9000`. Requires a portal
  redeploy/replace to take effect on the live host.

### Live verification — additional as-built findings (2026-06-12)

The live acceptance run surfaced several issues (all fixed on the branch / in the
live environment), worth recording for the next deploy:

- **Portal→Cribl container networking:** the userData ran the portal with
  `CRIBL_BASE_URL=http://localhost:9000` under bridge networking; `localhost` is
  the container, not the host. Fixed in `user_data_logging.sh.tftpl` with
  `--add-host host.docker.internal:host-gateway` and
  `CRIBL_BASE_URL=http://host.docker.internal:9000`.
- **Cribl convergence field:** the leader populates `configVersion` (commit on
  the leader) and may leave `deployedVersion` null. `CriblAdmin._await_version`
  now accepts a match on **either** field.
- **Reconcile ordering:** stale fork outputs must be deleted **after** the route
  table that references them is rewritten (deleting a referenced output 500s).
  `apply()` now upserts outputs → rewrites routes → deletes stale outputs.
- **Cribl leader admin credential** drifted from the SSM `cribl_password` during
  sub-project 1; reset to a known value and aligned in SSM. The
  `ignore_changes=[value]` pin on the SSM parameter keeps the operator-managed
  value authoritative.
- **Portal DB volume (RESOLVED):** the portal's SQLite originally lived in the
  container filesystem, so stream rows were lost on every container recreate
  (redeploy). Fixed by attaching a dedicated **5 GB gp3 EBS volume**
  (`aws_ebs_volume.portal_data`, `prevent_destroy`) mounted at
  `/opt/logstream-data` and bind-mounted into the portal container at `/data`.
  The instance ignores `user_data` changes so the mount edit does not force
  replacement; userData formats the volume idempotently (only when it has no
  filesystem). Stream history now survives redeploys and instance replacement.
  Next durability step (multi-instance/HA) is RDS Postgres.

**Acceptance proven live:** dana (scoped to account B) forked a source → portal
minted `logstream-read-<id>-<name>` trusting account B → the consumer assumed
that role from the `seth-demo-b` profile and read real records
(`account_id=522412052544`) from the Kinesis stream → the role was denied on any
other stream → deleting the stream tore down both the role and the stream.
