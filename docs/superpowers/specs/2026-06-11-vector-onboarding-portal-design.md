# LogStream Portal — Self-Service Log Onboarding for a Vector Pipeline

**Date:** 2026-06-11
**Status:** Approved design, pending implementation plan

## Overview

A self-service onboarding portal for a Vector.dev-based ETL log pipeline spanning multiple
cloud accounts. The portal exposes which log sources are collected for each workload and
lets an end user fork selected sources into their own dedicated stream (Kinesis or SQS),
then return to the same interface to manage that fan-out over time (add/remove sources,
delete streams).

This is a **fully working local demo**: portal, Vector fleet, and AWS streaming services
run in one `docker-compose` stack (AWS APIs via LocalStack). The single external
dependency is **Databricks Unity Catalog** (the user's real workspace), which acts as the
read-only source-of-truth catalog of workloads and log sources. Catalog
freshness/synchronization with the actual Vector fleet is treated as a solved problem —
the catalog is assumed accurate.

The portal code lives in `logstream-portal/` within this workspace (it can be split into
its own repo later without changes).

## Goals

- Demonstrate discoverability: "here is everything being collected for your workload."
- Demonstrate self-service forking: select sources → choose Kinesis or SQS → logs flow
  into the consumer's own stream within seconds.
- Demonstrate governance: sensitive sources require platform-admin approval; standard
  sources activate instantly.
- Demonstrate ongoing management: the same UI manages stream membership after creation.
- Exercise a real Unity Catalog integration (Databricks REST API), real Vector configs
  with hot reload, and real AWS API shapes (boto3 → LocalStack).

## Non-Goals

- Production auth (demo uses pick-a-persona login, no passwords/SSO).
- Catalog sync automation (a seed script populates UC once; drift is out of scope).
- Real AWS resources or cross-account IAM.
- Metrics/billing/quotas beyond simple per-stream flow indicators.

## Architecture

### Topology (docker-compose)

```
┌─ docker-compose ────────────────────────────────────────────────┐
│                                                                  │
│  vector-agent-acct-a ──┐                                         │
│  (storefront_web,       ├──► vector-aggregator ──► archive sink  │
│   orders_api)           │      │                  (LocalStack S3)│
│  vector-agent-acct-b ──┘      │ portal-managed fork fragments   │
│  (identity_svc,                ├──► consumer Kinesis streams     │
│   batch_etl)                   └──► consumer SQS queues          │
│                                                                  │
│  localstack (Kinesis, SQS, S3)                                   │
│  portal-api (FastAPI + SQLite)                                   │
│  portal-web (React + Vite + TypeScript; static build served      │
│              by portal-api in the demo)                          │
└──────────────────────────────────────────────────────────────────┘
            │
            └──► Databricks Unity Catalog (user's real workspace,
                 read-only; DATABRICKS_HOST + token via .env)
```

### Fork point: central aggregator (decision)

Account agents ship everything to one central Vector aggregator over Vector's native
protocol. **All forks happen at the aggregator** — a subscription is realized as a
`filter` transform + sink fragment in the aggregator's config. Alternatives considered
and rejected for this demo:

- *Fork at the edge agents:* portal would have to rewrite N configs across N accounts;
  cross-account config management becomes the hard problem.
- *Fork downstream of a central firehose stream:* per-consumer workers re-read the whole
  firehose; extra worker lifecycle to manage.

### Simulated accounts and workloads

Two Vector "agent" containers simulate two AWS accounts. Each generates realistic
system-level logs (via Vector `demo_logs`/file sources shaped as syslog, auth.log, etc.)
and tags every event with `account_id`, `account_alias`, `workload`, `source_name`, and
`environment` via a `remap` transform before shipping to the aggregator.

| Account | Alias | Agent container | Workloads (sources) |
|---|---|---|---|
| `111111111111` | prod-ecommerce | `vector-agent-acct-a` | `storefront_web` (syslog, nginx_access); `orders_api` (syslog, auth_log, app_log) |
| `222222222222` | prod-platform | `vector-agent-acct-b` | `identity_svc` (syslog, auth_log); `batch_etl` (syslog, cron_log) |

All `auth_log` sources are marked `sensitivity=sensitive` in the catalog; all other
sources are `standard`.

### Vector aggregator config layout

- **Static base config:** sources receiving from agents; default `aws_s3` archive sink
  (LocalStack) so the pipeline has a real "everything lands here" path.
- **Watched fragment directory:** the portal writes exactly one config fragment per
  consumer stream. Vector runs with `--watch-config`; writing/removing a fragment is the
  deployment. Fragments are always **fully regenerated from current DB state**
  (idempotent), never incrementally patched.

A fragment contains:
1. A `filter` transform whose condition matches events where
   (`workload`, `source_name`) is in the stream's *active* member set.
2. An `aws_kinesis_streams` or `aws_sqs` sink pointed at the LocalStack endpoint.

## Unity Catalog model

One dedicated catalog in the user's Databricks workspace (name configurable, default
`logging_demo`):

| UC level | Maps to | Example |
|---|---|---|
| Catalog | log pipeline domain | `logging_demo` |
| Schema | workload (account-prefixed) | `acct_a__storefront_web`, `acct_b__identity_svc` |
| Table | one log source | `syslog`, `auth_log`, `nginx_access` |

- **Table columns** describe the event schema (e.g., `timestamp`, `host`, `severity`,
  `message`) so the portal can show what's inside each source.
- **Table properties** carry operational metadata: `sensitivity` (`standard`|`sensitive`),
  `log_type`, `account_id`, `account_alias`, `environment`, `vector_source_id` (the tag
  value the aggregator filters on), `est_volume_per_min`, `description`.
- A one-time `seed_catalog.py` script (Databricks SDK) creates catalog/schemas/tables.
  The portal reads UC via REST and **never writes** to it.

## Portal data model (SQLite)

- `users` — seeded personas: two consumers (e.g., `dana@app-team`, `raj@data-sci`) and
  one platform admin (`admin@platform`). Role: `consumer` | `admin`.
- `streams` — id, owner (user id), name, type (`kinesis`|`sqs`), LocalStack ARN/URL,
  status (`provisioning`|`live`|`error`|`deleted`).
- `stream_sources` — one row per source-membership in a stream: stream id, source FQN
  (UC `catalog.schema.table`), status (`active`|`pending_approval`|`rejected`),
  requested_by/requested_at, decided_by/decided_at.

The admin approvals queue is `stream_sources WHERE status='pending_approval'`. The
catalog view's "Subscribed?" column is computed per-user from these tables.

## Fork mechanics & lifecycle

On fork submission (new stream, or adding sources to an existing one):

1. **Provision (new streams only):** boto3 creates the Kinesis stream or SQS queue in
   LocalStack. Failure → stream status `error`, Retry available, no fragment written.
2. **Gate:** sources with `sensitivity=standard` → `active` immediately;
   `sensitive` → `pending_approval`. Gating is enforced **server-side** by checking the
   UC property at request time (never trusts the client).
3. **Apply:** regenerate the stream's Vector fragment from the set of `active` members
   and write it to the watched directory. Admin approval of a pending source re-runs
   this step. A stream whose members are all pending has no fragment yet.
4. **Verify:** after each fragment write, the portal health-checks the aggregator's
   GraphQL API. If Vector rejected the config, roll the fragment back to the previous
   version and surface the error — a bad generated config can never take down the
   shared pipeline.

Removing a source regenerates the fragment without it. Deleting a stream removes the
fragment, then tears down the LocalStack resource and marks rows `deleted`.

**Live status & peek:** flow badges and a Peek view read the consumer's own stream —
Kinesis `GetRecords` from a latest shard iterator; SQS `ReceiveMessage` without delete.
Peek renders the last few records as formatted JSON.

## API surface (FastAPI, JSON, session cookie)

| Endpoint | Purpose |
|---|---|
| `POST /api/session` | pick-a-persona login → signed session cookie |
| `GET /api/catalog` | accounts → workloads → sources tree from UC, merged with per-user subscription annotations |
| `POST /api/streams` | create stream with initial source selection |
| `DELETE /api/streams/{id}` | delete stream + teardown |
| `GET /api/streams` | my streams, per-source status, flow metrics |
| `POST /api/streams/{id}/sources` | add sources (fan-out management) |
| `DELETE /api/streams/{id}/sources/{fqn}` | remove a source |
| `GET /api/streams/{id}/peek` | sample records from the consumer's stream |
| `GET /api/approvals` | admin: pending queue |
| `POST /api/approvals/{id}` | admin: approve or reject |

## UI (React + Vite + TypeScript)

Blended discover-and-manage layout; top nav: **Catalog · My Streams · Approvals
(admin only) · persona menu**.

- **Catalog (home):** sidebar groups workloads under *Account A / Account B* headers;
  main pane is the selected workload's sources table — name, type, est. volume,
  sensitivity chip, "Subscribed?" column. Checkbox selection → **Fork selected**.
- **Fork wizard (single screen):** destination = ◉ new stream (type Kinesis/SQS + name)
  or ○ add to an existing stream the user owns. Inline warning when the selection
  includes sensitive sources ("will require admin approval; standard sources activate
  immediately"). Submit → lands on My Streams.
- **My Streams:** one card per stream — type, status, member sources each with a status
  chip (`flowing · N/min` | `pending approval` | `rejected`), actions: **+ Add sources**
  (jumps to Catalog with this stream pre-selected as destination), **Remove source**,
  **Peek**, **Delete stream**.
- **Approvals (admin):** pending requests with requester, stream, source, sensitivity
  context; Approve/Reject.
- **Login:** persona picker (seeded users), no passwords.

## Error handling

- **Databricks unreachable:** serve the last-good catalog snapshot (cached to disk on
  every successful read) with a visible "catalog as of <timestamp>" banner. First run
  with no cache → explicit setup error page, not a blank screen.
- **LocalStack provisioning failure:** stream status `error` + Retry; fragments are only
  written after the target resource exists.
- **Vector config rejection:** fragment rollback + surfaced error (see Fork mechanics).
- **Authorization:** sensitive-gating, stream ownership, and admin-only endpoints are all
  enforced server-side.

## Testing

- **Backend unit (pytest, TDD):** golden-file tests for fragment generation (stream +
  active members in → exact Vector TOML out); state-machine tests for the
  subscription/approval lifecycle; UC and boto3 faked at the client boundary.
- **API integration:** FastAPI TestClient + temp SQLite + fakes — full flows: login →
  fork → approve → membership reflected; ownership/role enforcement.
- **End-to-end smoke (`make demo-test`):** bring up compose, seed, fork via API, assert
  records arrive in a LocalStack Kinesis stream within a timeout. The "demo will not
  embarrass me" test.
- **Frontend:** light component tests for the catalog tree and fork wizard only.

## Configuration

- `.env`: `DATABRICKS_HOST`, `DATABRICKS_TOKEN`, `UC_CATALOG_NAME` (default
  `logging_demo`), LocalStack endpoint/region defaults.
- `make` targets: `make up` (compose stack), `make seed` (UC + SQLite personas),
  `make demo-test` (e2e smoke), `make down`.

## Decisions log

| Decision | Choice |
|---|---|
| Demo depth | Fully working local demo (LocalStack), real Databricks UC |
| Catalog | Standalone catalog in user's Databricks workspace; freshness assumed solved |
| Fork point | Central Vector aggregator (route/filter + sink fragments, hot reload) |
| Fulfillment | Instant for standard sources; admin-approval gate for sensitive |
| Identity | Pick-a-persona demo login; consumer + admin roles |
| Stack | FastAPI + SQLite backend; React/Vite/TS frontend |
| UI shape | Blended: catalog-first discovery + My Streams management loop |

## As-built amendments (2026-06-11)

Implementation deviations from this spec, accepted during review:

- **Single fork fragment file.** All forks live in one wholesale-regenerated
  `forks.yaml` (plus a permanent no-op component keeping it valid) instead of
  one fragment file per stream. Vector's `--watch-config` reliably detects
  modifications to existing files; new-file detection is version-sensitive.
- **`vector_source_id` property dropped.** Fork filters match on the
  `workload` + `source_name` tag pair; the seed script writes those two
  properties instead of a separate `vector_source_id`.
- **Flow metrics are per-stream, not per-source.** Stream cards show a
  "N recent records" badge (Kinesis TRIM_HORIZON catch-up count / SQS queue
  depth); per-source chips show membership status only.
- **Vector image pinned to `0.39.0-debian`** — the last series exposing the
  HTTP GraphQL API the portal polls to verify hot reloads; newer images
  moved that API to gRPC.
- **Extra endpoints:** `POST /api/streams/{id}/retry` (recovery from
  provisioning failures), `GET /api/personas`, `GET/DELETE /api/session`.
