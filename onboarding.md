# LogStream Portal — Contributor Onboarding

This document is the fast path to understanding the project well enough to make
changes — on your own or by handing context to an LLM. Read it top to bottom
once; after that the **specs** (intent) and **plans** (step-by-step) are your
reference.

---

## 1. What this is

A **self-service log-onboarding portal**. Platform teams collect system logs
into a central pipeline; application teams use the portal to browse what's being
collected and **fork** the sources they want into their own dedicated stream
(Kinesis or SQS) — without filing tickets. Sensitive sources require a platform
admin's approval. Each forked stream comes with a downloadable, least-privilege
**cross-account IAM read role** so the consuming team can read only their stream.

It runs **live on AWS across three accounts** with **Cribl Stream** as the
pipeline and a real **Databricks Unity Catalog** as the source-of-truth
inventory. It is a demo-grade system (pick-a-persona login, single portal
instance) but every integration is real — real Cribl API, real Kinesis/SQS/IAM,
real PrivateLink, real Unity Catalog.

---

## 2. Architecture at a glance

See `docs/architecture.svg` for the diagram. In words:

```
Workload Account A (522412052544)        Workload Account B (624627265315)   …Account N
  EC2: Cribl Edge collector                EC2: Cribl Edge collector
   datagen → tag(account_id,workload,        (same pattern)
             source_name,environment)
   → tcpjson over PrivateLink ─┐         ┌── PrivateLink ──┘
                               ▼         ▼
Central Logging Platform account (337394138208)
  per-account endpoint service + NLB (principal-allowlisted)  →  :10300
  EC2 "cribl-central": Cribl leader (:9000) + worker (default group) + Portal (:8000)
     worker  ── portal-managed fork routes ──► Kinesis / SQS (+ SIEM)
             ── archive route ──────────────► S3
  Portal ── Cribl leader REST API (commit/deploy) ──┘
  Portal ── reads catalog ──► Databricks Unity Catalog
  ALB (operator-IP allowlisted) ──► Portal
  EBS volume (portal data tier) ──► /data (SQLite)
```

**Key invariants** (don't break these):

- Every event is tagged at the Edge with `account_id`, `account_alias`,
  `environment`, `workload`, `source_name`. A **fork = a Cribl Route** whose
  filter matches `account_id && workload && source_name`, plus a **Destination**
  (Kinesis/SQS). The portal regenerates the *entire* fork set from DB state on
  every change (wholesale reconcile — never incremental patching).
- **Network isolation is enforced**, not narrated: each workload account reaches
  the logging account only through its own PrivateLink endpoint service,
  allowlisted to that account's principal.
- **RBAC is server-side**: a persona's `account_scope` filters the catalog and
  blocks out-of-scope forks. The UI is cosmetic; the API is the wall.
- **Per-stream IAM roles are least-privilege**: trust = the owner's workload
  account only; inline policy = read actions on exactly that one stream ARN.

---

## 3. How the project was built (read this before touching anything)

This codebase was developed with a disciplined **brainstorm → spec → plan →
execute** workflow (the "superpowers" skill set). The artifacts are the most
valuable onboarding resource — they capture *why*, not just *what*:

| Phase | Artifact | What it gives you |
|---|---|---|
| v1 — Vector/local demo | `docs/superpowers/specs/2026-06-11-vector-onboarding-portal-design.md` + `plans/2026-06-11-logstream-portal.md` | The original portal (now superseded at the pipeline layer; the product/UX design still holds) |
| v2 — Cribl on live AWS | `docs/superpowers/specs/2026-06-12-cribl-aws-portal-design.md` + `plans/2026-06-12-cribl-aws-portal.md` | The current 3-account topology, the Vector→Cribl swap |
| v3 — IAM roles + RBAC | `docs/superpowers/specs/2026-06-12-iam-rbac-portal-design.md` + `plans/2026-06-12-iam-rbac-portal.md` | The read-role/bundle feature, account-scoped RBAC, the Peek fix |

**Each spec has an "as-built amendments" / "live verification findings" section
at the bottom** recording the things that only surfaced against the real
environment. Read those — they're where the sharp edges are documented.

`cribl/README.md` records the verified Cribl deployment specifics (leader+worker
topology, `tcpjson`, Edge routes, the REST API contract).

**Convention for new work:** write a short design spec first
(`docs/superpowers/specs/YYYY-MM-DD-<topic>-design.md`), then a step-by-step plan
(`docs/superpowers/plans/…`), then implement TDD with frequent commits. Live-AWS
changes are **plan-gated**: run `terraform plan` and confirm *only* the intended
resources change before applying.

---

## 4. Repo map

```
logstream-portal/
├── onboarding.md            ← you are here
├── README.md                ← user-facing: bring-up, consuming a stream, teardown
├── Makefile                 ← test / infra-up / infra-down[-all] / build-push / seed-*
├── Dockerfile               ← multi-stage: React build → FastAPI runtime
├── architecture.svg         ← (in docs/) the system diagram
├── cribl/README.md          ← Cribl deployment deltas + verified REST API contract
├── fixtures/
│   └── catalog_snapshot.json ← offline catalog fallback; MIRRORS the UC seed
├── scripts/
│   ├── seed_catalog.py      ← seeds Databricks Unity Catalog (one-time)
│   ├── seed_cribl.py        ← seeds the Cribl logging tier + edge collectors
│   ├── build_push_portal.sh ← build + push portal image to ECR
│   └── infra_teardown.sh    ← keep-data vs delete-all teardown (handles prevent_destroy)
├── backend/                 ← FastAPI app (Python 3.11+, sqlite3, boto3, requests)
│   └── app/
│       ├── main.py          ← app factory + DI (services dict overridable in tests)
│       ├── config.py        ← env-driven Settings
│       ├── db.py            ← sqlite schema + idempotent migrations + seeded personas
│       ├── catalog/         ← uc_client (Databricks REST), cache, service (+ scope_tree)
│       ├── streams/         ← service (lifecycle state machine), provisioner, peek
│       ├── cribl/           ← objects (Route/Destination builders), admin (REST client)
│       ├── aws/             ← access_roles (IAM), access_bundle (download payload)
│       └── routes/          ← session, catalog, streams, approvals, deps (auth)
├── frontend/                ← React 18 + TS + Vite (SPA served by FastAPI in prod)
│   └── src/{pages,components}/
└── infra/                   ← Terraform: single root, 3 AWS provider aliases
    └── modules/{network,privatelink,compute,portal_alb,secrets}/
```

The heart of the backend is **`app/streams/service.py`** (`StreamService`): it
orchestrates the whole fork lifecycle — catalog resolution + RBAC check →
sensitivity gate → Kinesis/SQS provisioning → IAM role mint → Cribl reconcile →
and the reverse on delete, all with an error-state + Retry posture. Everything
else hangs off it.

---

## 5. Tech stack & conventions

- **Backend:** FastAPI, `sqlite3` (stdlib, raw SQL), boto3, `requests`,
  itsdangerous (signed-cookie sessions). Services are **dependency-injected**
  through `create_app(settings, services=…)` so tests substitute fakes — there's
  a `Fake*` for every external boundary (IAM, Kinesis/SQS, Cribl, Databricks).
- **Frontend:** React + TypeScript + Vite + vitest. Types in `src/types.ts`
  mirror the backend JSON exactly.
- **Tests:** pure TDD. ~114 backend (pytest) + ~7 frontend (vitest). No network
  in unit tests — every external call goes through an injected client with a
  fake. **`make test` runs both and needs no AWS.**
- **Style:** match surrounding code; small focused files; frequent commits with
  conventional-commit messages (`feat(portal):`, `fix(portal):`, `docs(portal):`).
- **Golden-file tests** pin byte-exact generated artifacts (Cribl objects, the
  access bundle) — if you change a builder, update its golden and confirm the
  diff is what you intend.

---

## 6. Running it

### Locally (no AWS, no cloud) — for code work

```bash
cd logstream-portal/backend
python3 -m venv .venv && source .venv/bin/activate && pip install -e '.[dev]'
pytest -q                       # ~114 passed
cd ../frontend && npm ci && npm test   # ~7 passed
```

The backend runs offline against the bundled `fixtures/catalog_snapshot.json`
(the catalog shows a "stale" banner). There is **no local docker-compose** — the
v1 Vector/LocalStack stack was removed; the system now targets real AWS. For
local API poking you can `uvicorn app.main:create_app --factory` with env vars
pointing at fakes, but most iteration happens through the test suite.

### Live AWS

The full bring-up (Terraform across 3 accounts, image build, Cribl + UC seeding)
is in `README.md` under "Bring it up", and the operational specifics are in
`cribl/README.md`. Teardown:

```bash
make infra-down       # destroy everything BUT the data EBS volume (data kept)
make infra-down-all   # destroy everything; snapshots the volume first
```

(Run teardown from the directory whose `infra/` holds the live Terraform state —
see §8.)

---

## 7. The live environment (as of last session)

- **Accounts:** A `337394138208` (Central Logging Platform / `default` profile),
  B `522412052544` (Workload A, `seth-demo-b`), C/N `624627265315` (Workload B,
  `seth-demo-c`). us-east-1.
- **Portal URL:** an ALB DNS name, **IP-allowlisted to the operator's CIDR**
  (`infra/terraform.tfvars: operator_cidr`). Get it with
  `terraform output -raw portal_url`.
- **Personas (pick-a-persona login, no passwords):** `dana@app-team` (scoped to
  account B / Workload A), `raj@data-sci` (scoped to account C / Workload B),
  `admin@platform` (unscoped, approves sensitive sources).
- **Reaching internals:** the Cribl leader (`:9000`) and the EC2 boxes are
  private — use **SSM** (Session Manager), not SSH. Port-forward for the Cribl
  UI: `aws ssm start-session --target <logging-instance-id>
  --document-name AWS-StartPortForwardingSession
  --parameters '{"portNumber":["9000"],"localPortNumber":["9000"]}'`.
- **Cribl leader login:** `admin` / `admin` (private, VPC + SSM only; aligned to
  the SSM `cribl_password` parameter).
- **Data durability:** the portal's SQLite lives on a dedicated EBS volume
  (`/opt/logstream-data` → container `/data`), so stream history survives
  container recreation and instance replacement.

> The live stack bills ~$6–9/day. If you pull this to work on it and don't need
> the cloud side, stick to `make test` and the specs — you don't need AWS to
> develop the app.

---

## 8. Sharp edges & gotchas (the stuff that bites)

These are distilled from the specs' as-built sections and `cribl/README.md`:

- **Cribl REST API contract** (`app/cribl/admin.py`): outputs are individual
  resources — **POST** the collection to create, **PATCH** `…/outputs/{id}` to
  update, **DELETE** to remove. Routes are a **single ordered doc** — PATCH
  `…/routes/default` with the whole table. Convergence is polled via the group's
  `configVersion` *or* `deployedVersion` (Cribl versions differ). **Order
  matters:** upsert outputs → rewrite routes → delete stale outputs (deleting an
  output a route still references returns 500). Fork routes are `final:false` and
  must precede the catch-all routes.
- **Portal ↔ Cribl networking:** the portal container reaches the leader via
  `host.docker.internal` (host-gateway), not `localhost` (bridge networking).
- **Kinesis/SQS destinations** are created with `compression: none` so consumers
  read plain JSON (and Peek shows real events). Cribl writes **ndjson batches**
  (a `{"format":"ndjson",…}` header line + one event per line) — `PeekService`
  splits these; don't "simplify" it back to single-object decoding.
- **Terraform live changes are plan-gated.** The compute instances
  `ignore_changes = [ami, user_data]` so AMI rolls / userData edits don't force
  replacement; the data EBS volume has `prevent_destroy` (which is why
  `make infra-down` uses `scripts/infra_teardown.sh` to state-rm it first).
- **Terraform state is local** (gitignored) and during development bounced
  between git worktrees. The canonical live state lives in the worktree the
  stack was last applied from. If you inherit this, **consolidate state to one
  location** (or migrate to a remote backend like S3+DynamoDB) before doing
  serious infra work — it's the single biggest fragility.
- **`fixtures/catalog_snapshot.json` mirrors `scripts/seed_catalog.py`** — change
  one, change the other, or the offline fallback drifts from the real UC.

---

## 9. Known limitations & natural next steps

- **Single portal instance, no HA.** The data tier is a single-AZ EBS volume.
  The documented graduation path for multi-instance/HA is **RDS Postgres**
  (replace `sqlite3`; contained change, the SQL is already centralized in
  `db.py`/`service.py`).
- **Pick-a-persona login** is not real auth — wire OIDC/SSO before any real use.
- **Portal DB rows** are not auto-reconciled against AWS if they ever diverge; a
  reconcile/janitor job would clean orphaned roles/streams.
- **Local state** → remote backend (see §8).
- The **Peek-in-UI** path and the **bundle CLI snippets** are the two consumer
  surfaces; the SIEM consumer in the diagram is illustrative (not wired).

---

## 10. Picking this up with an LLM

To get an LLM productive fast, point it at, in order:

1. **This file** (`onboarding.md`) — orientation.
2. The **latest spec** for the area you're touching (`docs/superpowers/specs/`)
   — especially its as-built section — for *intent and constraints*.
3. The matching **plan** (`docs/superpowers/plans/`) for the implementation
   shape and the test patterns.
4. **`app/streams/service.py`** + **`app/main.py`** to see how the pieces wire
   together, and `cribl/README.md` for the pipeline API contract.

Then ask it to **write a short spec → plan → TDD** for the change, keep
`make test` green, and **plan-gate** any `terraform apply`. The codebase rewards
that discipline: every external boundary has a fake, so new logic is unit-
testable without AWS, and the live verification is a thin layer on top.

Good first tasks to learn the system: add a new tagged field end-to-end (Edge
Eval → catalog → fork filter), add an API endpoint with its fake-backed test, or
implement the RDS migration behind the existing `db.py` interface.
