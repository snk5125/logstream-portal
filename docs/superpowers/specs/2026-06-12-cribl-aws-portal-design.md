# LogStream Portal v2 — Cribl Stream in Live AWS (3 Accounts)

**Date:** 2026-06-12
**Status:** Approved design, pending implementation plan
**Supersedes the pipeline layer of:** `2026-06-11-vector-onboarding-portal-design.md`

## Overview

Evolve the LogStream Portal demo from a local Vector/LocalStack stack to **Cribl
Stream running across three live AWS accounts**: two workload accounts generating
and tagging logs, one logging account hosting the Cribl control plane, the fork
point, the consumer streams, and the portal. Workload accounts reach the logging
account exclusively through **per-account PrivateLink endpoint services** —
network isolation is enforced, not narrated.

The portal's product behavior (catalog discovery from Databricks Unity Catalog,
self-service forking, sensitivity approvals, My Streams management, peek) is
unchanged. What changes underneath: forks are realized as **Cribl Routes +
Destinations via the leader's REST API (commit/deploy)** instead of a watched
Vector config file, and the AWS resources are real.

This is sub-project 1 of the v2 roadmap. Sub-project 2 (per-stream IAM read
roles with downloadable artifact, and account-scoped RBAC) follows in its own
spec and builds on the real account IDs established here.

## Goals

- Real multi-account topology: logs physically originate in two AWS accounts
  and centralize in a third, with PrivateLink as the only ingress path.
- Swap the fork mechanic to Cribl's management plane: every portal mutation is
  an API-driven config **commit + deploy** with git-native rollback.
- Keep the entire portal product surface and test suite intact: the
  `StreamService.reapply()` wholesale-regeneration contract survives the swap.
- One-command infrastructure lifecycle: `make infra-up` from zero to demoable,
  `make infra-down` to zero spend.

## Non-Goals (deferred to sub-project 2 or later)

- Per-stream IAM read roles + downloadable artifact ("My Streams" button).
- Account-scoped RBAC for demo personas.
- Cribl.Cloud (the user's cloud org stays untouched; this is self-hosted).
- Production auth on the portal (persona login remains; exposure is mitigated
  by IP allowlisting).
- The local docker-compose/LocalStack/Vector stack — retired by this project.

## Accounts, auth, and mapping

| Account | ID | Role in demo | CLI access |
|---|---|---|---|
| seth-demo-a | `337394138208` | **Logging account**: Cribl leader + `logging` worker group, NLBs/endpoint services, Kinesis/SQS/S3, portal + ALB | `default` profile (IAM user `admin-a`) |
| Seth-Demo-B | `522412052544` | **prod-ecommerce**: `storefront_web`, `orders_api` | `seth-demo-b` profile (IAM user `admin-b`) |
| seth-demo-c | `624627265315` | **prod-platform**: `identity_svc`, `batch_etl` | `seth-demo-c` profile (IAM user `admin-c`) |

All three profiles are configured and verified on this machine. Region:
**us-east-1** everywhere.

**Real account IDs replace the placeholders** (`111111111111` → `522412052544`,
`222222222222` → `624627265315`) in: Cribl Eval tags, the catalog fixture
snapshot, `seed_catalog.py`, and a one-time Unity Catalog re-seed. UC schemas
are renamed to match the real account letters: `acct_b__storefront_web`,
`acct_b__orders_api`, `acct_c__identity_svc`, `acct_c__batch_etl` (the old
`acct_a__*`/`acct_b__*` schemas are dropped from UC during re-seed).

## Topology

```
 Account B 522412052544 (prod-ecommerce)      Account C 624627265315 (prod-platform)
 ┌───────────────────────────────┐            ┌───────────────────────────────┐
 │ EC2 cribl-worker-b (docker)   │            │ EC2 cribl-worker-c (docker)   │
 │  Datagen: storefront_web,     │            │  Datagen: identity_svc,       │
 │           orders_api sources  │            │           batch_etl sources   │
 │  Eval: account_id=5224…, tags │            │  Eval: account_id=6246…, tags │
 │  out → vpce DNS :10300 (data) │            │  out → vpce DNS :10300 (data) │
 │  mgmt → vpce DNS :9000 (enrol)│            │  mgmt → vpce DNS :9000 (enrol)│
 └───────────────┬───────────────┘            └───────────────┬───────────────┘
                 │ interface VPC endpoint                     │ interface VPC endpoint
                 ▼                                            ▼
 Account A 337394138208 (logging)
 ┌──────────────────────────────────────────────────────────────────────────┐
 │ endpoint-service-b (NLB-b) ──┐          ┌── endpoint-service-c (NLB-c)   │
 │   allowlist: 522412052544    │          │     allowlist: 624627265315    │
 │   listeners :10300, :9000    ▼          ▼     listeners :10300, :9000    │
 │              EC2 cribl-central (docker):                                 │
 │                cribl-leader   (API/UI :9000)                             │
 │                cribl-worker-logging (group "logging", ingest :10300)     │
 │                portal         (FastAPI+SPA :8000)                        │
 │              ALB (public, SG allowlists operator IP) → portal :8000      │
 │              Kinesis streams / SQS queues (portal-provisioned, real)     │
 │              S3 log-archive bucket (archive route)                       │
 └──────────────┬───────────────────────────────────────────────────────────┘
                └──► Databricks Unity Catalog (re-seeded with real account IDs)
```

- **Three EC2 instances total** (Amazon Linux 2023 + docker, via user-data):
  one per workload account running that account's collection worker container;
  one in the logging account running leader + logging worker + portal.
- **PrivateLink, enforced isolation:** each workload account gets a dedicated
  NLB + VPC endpoint service in account A, with only that account's principal
  allowlisted, exposing two listeners — `:10300` (Cribl-TCP data) and `:9000`
  (leader management/enrollment). Workload instances have no other route to
  account A. The portal/leader API is never exposed cross-account; the ALB
  exposes only the portal UI/API to the operator's IP.
- Within account A the portal reaches the leader API on the docker network.

## Cribl deployment & license gate

- Self-hosted Cribl Stream (free license), containers from `cribl/cribl`.
- **Plan A:** distributed mode — leader + three named worker groups
  (`acct_b`, `acct_c`, `logging`); collection workers enroll over PrivateLink.
- **Task 0 license gate:** boot the leader and verify the Free license accepts
  multiple worker groups with remote workers. **Plan B fallback:** B/C
  containers run standalone **Cribl Edge** single-instances (free, no
  distributed licensing) with identical Datagen+Eval+Cribl-TCP config, shipping
  to the same `logging`-group ingest. Everything outside this section is
  identical under either plan.
- Cribl admin password: generated at deploy, stored in SSM (SecureString).

## Static Cribl config (seeded, never portal-touched)

`scripts/seed_cribl.py` runs idempotently against the leader API, then
commit + deploy:

- **Collection tier (B and C):** one Datagen source per catalog source (9
  total: storefront_web syslog+nginx_access; orders_api syslog+auth_log+app_log;
  identity_svc syslog+auth_log; batch_etl syslog+cron_log), sample files
  uploaded by the script; per-source Eval pipeline stamping `account_id`
  (real), `account_alias`, `environment`, `workload`, `source_name`; one
  Cribl-TCP destination → that account's endpoint DNS `:10300`.
- **`logging` group:** Cribl-TCP source `:10300`; permanent final **archive
  route** → S3 destination (`log-archive-<acctA>` bucket, signed by the
  instance profile); the portal-managed fork territory below it.

## Portal integration (the module swap)

Deleted: `app/streams/fragments.py`, `app/streams/vector_admin.py`, all
`vector/` configs, the Vector image pin, LocalStack and compose wiring.

Added:

- **`app/cribl/objects.py`** — pure, golden-tested JSON builders:
  - `route_for(stream, members)` → `{id: "fork_<id>", filter: "(account_id=='…'
    && workload=='…' && source_name=='…') || …", output: "fork_<id>_dest",
    final: false, description}` (members sorted; same determinism rules and
    value validation as the Vector renderer).
  - `destination_for(stream)` → Kinesis-Streams or SQS destination JSON
    (region us-east-1, **no static credentials** — the logging worker's
    instance profile signs).
- **`app/cribl/admin.py`** — `CriblAdmin` REST client against the leader:
  - Bearer login (`/api/v1/auth/login`) with credentials from settings/SSM.
  - `apply(routes, destinations)`: read the `logging` group's route table and
    destination list; **wholesale-reconcile every `fork_*` object** to the
    desired set (create/update/delete), leaving non-`fork_*` config untouched;
    record the current commit id; POST commit (message names the change);
    POST deploy to `logging`; poll until the group `configVersion` equals the
    new commit and a worker heartbeats it (timeout configurable). On failure:
    **deploy the previous commit id** (git-native rollback) and raise
    `CriblApplyError`.
- **`StreamService`** — contract unchanged: every mutation calls `reapply()`,
  which derives the full desired route/destination set from DB state (live
  streams' active members only) and calls `CriblAdmin.apply`. The
  `CriblApplyError` → flag `last_error` → HTTP 502 path is preserved verbatim,
  as is the test seam (conftest fake renamed from `vector_admin` to
  `pipeline_admin`).
- **`Provisioner` / `PeekService`** — drop the endpoint override; boto3 uses
  the portal's instance role (Kinesis/SQS scoped by `logstream-*` name prefix).
- **Frontend:** zero changes.

## Infrastructure as code

`logstream-portal/infra/` — single Terraform root, three provider aliases
(profiles `default`, `seth-demo-b`, `seth-demo-c`):

- **module `network`** — dedicated VPC per account (no default-VPC reuse),
  public+private subnets, security groups.
- **module `privatelink`** — per workload account: NLB (listeners 10300/9000,
  targets = cribl-central instance) + endpoint service (acceptance disabled,
  principal allowlist = that account) in A; interface endpoint + private DNS
  in B/C.
- **module `compute`** — 3 EC2s + instance profiles + user-data (docker,
  container launch, SSM-fetched secrets). The portal image is built locally
  from the repo, pushed to an ECR repository in account A, and pulled by the
  logging instance (the repo is private; instances never need git access).
- **module `portal-alb`** — ALB + target group + SG allowlisting
  `var.operator_cidr`.
- Local state (gitignored). `make infra-up` / `make infra-down` wrap
  apply/destroy; destroy returns spend to ~zero.

## Secrets

SSM Parameter Store (SecureString) in account A: `DATABRICKS_HOST`,
`DATABRICKS_TOKEN`, `DATABRICKS_WAREHOUSE_ID`, `CRIBL_ADMIN_PASSWORD`,
`PORTAL_SESSION_SECRET`. The portal container reads them at boot via instance
role. No secrets in images, user-data, or git. (Existing local `.env` stays
for the UC seed script run from this machine.)

## Error handling

Unchanged portal semantics, new failure sources mapped onto them:

- Leader unreachable / commit-deploy failure / convergence timeout →
  `CriblApplyError` → rollback deploy of previous commit → stream
  `last_error` + HTTP 502 (Retry available).
- Databricks unreachable → cached snapshot with stale banner (unchanged).
- Kinesis/SQS provisioning failure → stream `error` + Retry (unchanged).
- Startup resync: best-effort `reapply()` against the leader, logged.

## Testing

- **Unit/golden:** `cribl/objects.py` builders against JSON fixture files;
  `CriblAdmin` against a fake REST session (login, reconcile, commit, deploy,
  convergence, rollback, timeout). Same depth as the modules they replace.
- **Suite parity:** backend pytest + frontend vitest stay green; only the
  pipeline-admin seam and its fakes change.
- **Live smoke `scripts/demo_test_aws.sh`** (the new "demo will not embarrass
  me" gate): portal login → fork `storefront_web/syslog` → poll the real
  Kinesis stream until records arrive → assert the `fork_*` route exists via
  the Cribl API → delete the stream → assert the route is gone and the Kinesis
  stream is deleted.
- **Manual walkthrough:** consumer + approval + manage loops through the ALB
  against live AWS, including the Cribl UI showing portal-created routes.

## Cost & lifecycle

≈ $6–9/day while up (3 EC2, 2 NLB + ALB, 4 interface endpoints, on-demand
Kinesis); ~$0 after `make infra-down`. All IAM access keys used here transited
chat and should be rotated when the build settles.

## Decisions log

| Decision | Choice |
|---|---|
| Sequencing | Cribl + AWS together (sub-project 1); IAM-role download + RBAC next (sub-project 2) |
| Control plane | Self-hosted Cribl leader in logging account (Cribl.Cloud untouched) |
| Worker topology | One worker group per account (Plan A), Edge-sender fallback (Plan B) behind a day-one license gate |
| Networking | Per-workload-account NLB + endpoint service, principal-allowlisted; listeners 10300 (data) + 9000 (mgmt) |
| Compute | 1 EC2 per account; logging EC2 co-hosts leader + logging worker + portal |
| Account mapping | B=prod-ecommerce, C=prod-platform, A=logging |
| Identifiers | Real account IDs in tags, catalog, and UC (re-seed) |
| Fork mechanic | Cribl Routes+Destinations via REST, wholesale `fork_*` reconcile, commit+deploy, rollback = redeploy previous commit |
| Vector | Deleted, not abstracted (git history preserves it) |
| IaC | Terraform single root, 3 provider aliases, local state, make up/down |
| Secrets | SSM Parameter Store SecureString in account A |
| Portal exposure | Public ALB restricted to operator CIDR |
