# LogStream Portal v2 — Cribl on AWS Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the portal's Vector/LocalStack pipeline with Cribl Stream running across three live AWS accounts (two workload + one logging), connected by per-account PrivateLink, with forks realized as Cribl Routes+Destinations via the leader REST API.

**Architecture:** The portal's `StreamService.reapply()` wholesale-regeneration contract is preserved; only the pipeline-admin implementation changes (file+GraphQL → REST commit/deploy). Real Kinesis/SQS/S3 in the logging account replace LocalStack; boto3 uses instance-role credentials. Terraform (single root, three provider aliases) stands up VPCs, PrivateLink endpoint services, three EC2s, an ECR repo, an IP-allowlisted ALB, and SSM secrets.

**Tech Stack:** Python 3.11/FastAPI/pytest (unchanged), Cribl Stream (self-hosted, `cribl/cribl` image), Terraform + AWS provider, boto3, Databricks SDK (seed only), docker, React/Vite (unchanged).

**Spec:** `docs/superpowers/specs/2026-06-12-cribl-aws-portal-design.md`

**Working notes:**
- All paths relative to repo root. Portal backend at `logstream-portal/backend/` (venv at `logstream-portal/backend/.venv`; `source .venv/bin/activate`; pytest from `backend/`).
- AWS profiles already configured + verified: `default` (acct A 337394138208), `seth-demo-b` (522412052544), `seth-demo-c` (624627265315). Region us-east-1.
- This plan **deletes** Vector. Git history preserves it; do not keep a compatibility shim.
- Account IDs are real and load-bearing: A=`337394138208` (logging), B=`522412052544` (prod-ecommerce), C=`624627265315` (prod-platform).

## File Map

```
logstream-portal/
├── backend/app/
│   ├── config.py                      # MODIFY: cribl_* + drop vector/localstack fields
│   ├── cribl/
│   │   ├── __init__.py                # NEW
│   │   ├── objects.py                 # NEW: route_for / destination_for (golden-tested)
│   │   └── admin.py                   # NEW: CriblAdmin REST client (commit/deploy/rollback)
│   ├── streams/
│   │   ├── fragments.py               # DELETE
│   │   ├── vector_admin.py            # DELETE
│   │   ├── service.py                 # MODIFY: reapply() builds cribl objects, calls CriblAdmin
│   │   ├── provisioner.py             # MODIFY: drop endpoint override (instance-role boto3)
│   │   └── peek.py                    # MODIFY: drop endpoint override
│   ├── catalog/                       # unchanged
│   └── main.py                        # MODIFY: build CriblAdmin instead of VectorAdmin
│   └── tests/
│       ├── conftest.py                # MODIFY: FakePipelineAdmin replaces FakeVectorAdmin
│       ├── test_fragments.py          # DELETE
│       ├── test_vector_admin.py       # DELETE
│       ├── cribl_golden/              # NEW: route/destination JSON golden files
│       ├── test_cribl_objects.py      # NEW
│       ├── test_cribl_admin.py        # NEW
│       └── test_stream_service.py     # MODIFY: assert cribl payloads, rename fake
├── fixtures/catalog_snapshot.json     # MODIFY: real account IDs + acct_b/acct_c schemas
├── scripts/
│   ├── seed_catalog.py                # MODIFY: real IDs + schema rename
│   ├── seed_cribl.py                  # NEW: idempotent Cribl API seeding
│   └── demo_test_aws.sh               # NEW: live e2e smoke
├── cribl/
│   ├── seed_spec.json                 # NEW: declarative source/eval/dest definitions seed_cribl reads
│   └── README.md                      # NEW: license-gate + topology notes
├── infra/                             # NEW: Terraform root
│   ├── main.tf  providers.tf  variables.tf  outputs.tf  terraform.tfvars.example
│   ├── modules/network/               # VPC, subnets, SGs (per account)
│   ├── modules/privatelink/           # NLB + endpoint service (A) + interface endpoint (B/C)
│   ├── modules/compute/               # EC2 + instance profiles + user-data + ECR
│   └── modules/portal_alb/            # ALB + operator-CIDR SG
├── Makefile                           # MODIFY: infra-up/down, seed-cribl, demo-test-aws; drop compose
├── docker-compose.yml                 # DELETE (+ vector/, localstack-init/)
└── README.md                          # MODIFY: AWS quick start
```

Two things removed wholesale: `logstream-portal/vector/`, `logstream-portal/localstack-init/`, `logstream-portal/docker-compose.yml`, `logstream-portal/Dockerfile` is **kept** (still builds the portal image for ECR).

---

### Task 0: Local Cribl license gate (decides Plan A vs Plan B before any AWS spend)

**Goal:** Prove the Free license runs leader + ≥1 remote worker group locally, so the AWS topology choice is settled before paying for infrastructure. No portal code yet.

**Files:**
- Create: `logstream-portal/cribl/README.md`

- [ ] **Step 1: Boot a single-instance leader locally and read the license tier**

```bash
docker run -d --name cribl-gate -p 9000:9000 \
  -e CRIBL_DIST_MODE=master cribl/cribl:latest
sleep 25
# Default creds admin/admin; change-password prompt may appear via API.
TOKEN=$(curl -s http://localhost:9000/api/v1/auth/login \
  -H 'Content-Type: application/json' \
  -d '{"username":"admin","password":"admin"}' | python3 -c 'import sys,json; print(json.load(sys.stdin)["token"])')
curl -s http://localhost:9000/api/v1/system/licenses -H "Authorization: Bearer $TOKEN" | python3 -m json.tool
curl -s http://localhost:9000/api/v1/master/groups -H "Authorization: Bearer $TOKEN" | python3 -c 'import sys,json; print("groups:", [g["id"] for g in json.load(sys.stdin)["items"]])'
```

Expected: a license entry (Free/Standard) and the default group list. Record the exact `/api/v1/auth/login` and group payload shapes — Task 2 and Task 6 depend on them.

- [ ] **Step 2: Attempt to create a second worker group via API**

```bash
curl -s -X POST http://localhost:9000/api/v1/master/groups \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"id":"logging","name":"logging","onPrem":true}' -w '\nHTTP %{http_code}\n'
curl -s -X POST http://localhost:9000/api/v1/master/groups \
  -H "Authorization: Bearer $TOKEN" -H 'Content-Type: application/json' \
  -d '{"id":"acct_b","name":"acct_b","onPrem":true}' -w '\nHTTP %{http_code}\n'
```

Expected (decision point):
- **HTTP 200 on both → Plan A** (three named worker groups). Proceed with the spec as written.
- **HTTP 403/license error → Plan B** (collection tier becomes standalone Cribl Edge instances shipping to the single `logging` group). Record this; Task 6 and Task 9 branch on it.

- [ ] **Step 3: Capture the verified API shapes into `logstream-portal/cribl/README.md`**

Write a short doc recording: chosen Plan (A or B), the exact login request/response, the group-create request/response, and the routes + destinations API paths discovered (`/api/v1/m/<group>/routes`, `/api/v1/m/<group>/system/outputs`, `/api/v1/version/commit`, `/api/v1/master/groups/<group>/deploy` — confirm each returns non-404 with the token). This file is the source of truth for Tasks 2 and 6; if a path 404s, find the correct one via `curl .../api/v1/` discovery and document it.

- [ ] **Step 4: Tear down and commit**

```bash
docker rm -f cribl-gate
git add logstream-portal/cribl/README.md
git commit -m "docs(portal): cribl license gate result and verified API shapes"
```

**If Plan B:** add a note to the spec's decisions log via a follow-up commit, and treat every later "worker group acct_b/acct_c" reference as "Edge instance for B/C". The portal code (Tasks 1-5) is identical either way.

---

### Task 1: Cribl object builders (`objects.py`) — golden-tested

**Files:**
- Create: `logstream-portal/backend/app/cribl/__init__.py` (empty)
- Create: `logstream-portal/backend/app/cribl/objects.py`
- Create: `logstream-portal/backend/tests/cribl_golden/route_two_members.json`
- Create: `logstream-portal/backend/tests/cribl_golden/dest_kinesis.json`
- Create: `logstream-portal/backend/tests/cribl_golden/dest_sqs.json`
- Test: `logstream-portal/backend/tests/test_cribl_objects.py`

- [ ] **Step 1: Write golden file `tests/cribl_golden/route_two_members.json`**

```json
{
  "id": "fork_3",
  "name": "fork_3",
  "final": false,
  "disabled": false,
  "filter": "(account_id=='522412052544' && workload=='orders_api' && source_name=='auth_log') || (account_id=='522412052544' && workload=='orders_api' && source_name=='syslog')",
  "pipeline": "devnull",
  "output": "fork_3_dest",
  "description": "logstream-portal fork for stream 3"
}
```

- [ ] **Step 2: Write golden file `tests/cribl_golden/dest_kinesis.json`**

```json
{
  "id": "fork_3_dest",
  "type": "kinesis",
  "streamName": "logstream-team-a-orders",
  "region": "us-east-1",
  "assumeRole": null,
  "awsAuthenticationMethod": "auto",
  "format": "json",
  "description": "logstream-portal destination for stream 3"
}
```

- [ ] **Step 3: Write golden file `tests/cribl_golden/dest_sqs.json`**

```json
{
  "id": "fork_5_dest",
  "type": "sqs",
  "queueName": "logstream-team-b-audit",
  "region": "us-east-1",
  "awsAuthenticationMethod": "auto",
  "format": "json",
  "description": "logstream-portal destination for stream 5"
}
```

- [ ] **Step 4: Write the failing tests** — `tests/test_cribl_objects.py`

```python
import json
from pathlib import Path

import pytest

from app.cribl.objects import Member, StreamSpec, destination_for, route_for

GOLDEN = Path(__file__).parent / "cribl_golden"


def test_route_orders_members_sorted_and_filtered():
    spec = StreamSpec(
        stream_id=3, stream_type="kinesis", resource_ref="logstream-team-a-orders",
        members=(
            Member("522412052544", "orders_api", "syslog"),
            Member("522412052544", "orders_api", "auth_log"),
        ),
    )
    assert route_for(spec) == json.loads((GOLDEN / "route_two_members.json").read_text())


def test_kinesis_destination():
    spec = StreamSpec(3, "kinesis", "logstream-team-a-orders", ())
    assert destination_for(spec) == json.loads((GOLDEN / "dest_kinesis.json").read_text())


def test_sqs_destination():
    spec = StreamSpec(5, "sqs", "logstream-team-b-audit", ())
    assert destination_for(spec) == json.loads((GOLDEN / "dest_sqs.json").read_text())


def test_unsafe_values_rejected():
    with pytest.raises(ValueError):
        Member("522412052544", "orders'api", "syslog")
    with pytest.raises(ValueError):
        StreamSpec(1, "firehose", "x", (Member("1", "w", "s"),))


def test_route_filter_escapes_nothing_but_validates():
    # account_id with a quote must be rejected at Member construction, not silently emitted
    with pytest.raises(ValueError):
        Member('52""', "w", "s")
```

- [ ] **Step 5: Run to verify fail**

Run: `pytest tests/test_cribl_objects.py -q`
Expected: `ModuleNotFoundError: No module named 'app.cribl.objects'`

- [ ] **Step 6: Write `app/cribl/objects.py`**

```python
"""Pure builders for the Cribl Route and Destination objects a fork needs.

Output must be byte-deterministic so golden-file tests can pin it. Values are
validated at construction (same trust boundary as the old Vector renderer):
the portal interpolates them into Cribl filter expressions, so quotes and
control characters are rejected rather than escaped.
"""
import re
from dataclasses import dataclass, field
from typing import Sequence

_SAFE = re.compile(r"^[A-Za-z0-9_\-.:/@]+$")


def _validate(name: str, value: str) -> None:
    if not _SAFE.match(value):
        raise ValueError(f"{name} {value!r} contains characters unsafe for a Cribl filter")


@dataclass(frozen=True)
class Member:
    account_id: str
    workload: str
    source_name: str

    def __post_init__(self) -> None:
        _validate("account_id", self.account_id)
        _validate("workload", self.workload)
        _validate("source_name", self.source_name)


@dataclass(frozen=True)
class StreamSpec:
    stream_id: int
    stream_type: str  # "kinesis" | "sqs"
    resource_ref: str  # kinesis stream name | sqs queue name
    members: Sequence[Member] = field(default_factory=tuple)

    def __post_init__(self) -> None:
        if self.stream_type not in ("kinesis", "sqs"):
            raise ValueError(f"unsupported stream_type: {self.stream_type!r}")
        _validate("resource_ref", self.resource_ref)


def _clause(m: Member) -> str:
    return (
        f"(account_id=='{m.account_id}' && workload=='{m.workload}' "
        f"&& source_name=='{m.source_name}')"
    )


def route_for(spec: StreamSpec) -> dict:
    ordered = sorted(spec.members, key=lambda m: (m.account_id, m.workload, m.source_name))
    return {
        "id": f"fork_{spec.stream_id}",
        "name": f"fork_{spec.stream_id}",
        "final": False,
        "disabled": False,
        "filter": " || ".join(_clause(m) for m in ordered),
        "pipeline": "devnull",
        "output": f"fork_{spec.stream_id}_dest",
        "description": f"logstream-portal fork for stream {spec.stream_id}",
    }


def destination_for(spec: StreamSpec) -> dict:
    base = {
        "id": f"fork_{spec.stream_id}_dest",
        "type": spec.stream_type,
        "region": "us-east-1",
        "awsAuthenticationMethod": "auto",
        "format": "json",
        "description": f"logstream-portal destination for stream {spec.stream_id}",
    }
    if spec.stream_type == "kinesis":
        return {
            "id": base["id"], "type": "kinesis", "streamName": spec.resource_ref,
            "region": "us-east-1", "assumeRole": None,
            "awsAuthenticationMethod": "auto", "format": "json",
            "description": base["description"],
        }
    return {
        "id": base["id"], "type": "sqs", "queueName": spec.resource_ref,
        "region": "us-east-1", "awsAuthenticationMethod": "auto", "format": "json",
        "description": base["description"],
    }
```

- [ ] **Step 7: Run to verify pass**

Run: `pytest tests/test_cribl_objects.py -q`
Expected: `5 passed`. If a golden mismatch occurs, reconcile against the field names/shapes you recorded in `cribl/README.md` from Task 0 — the golden files are the contract, fix whichever side is wrong for the real Cribl API.

- [ ] **Step 8: Commit**

```bash
git add logstream-portal/backend/app/cribl/__init__.py logstream-portal/backend/app/cribl/objects.py logstream-portal/backend/tests/test_cribl_objects.py logstream-portal/backend/tests/cribl_golden
git commit -m "feat(portal): cribl route/destination builders with golden tests"
```

---

### Task 2: CriblAdmin REST client (`admin.py`)

**Files:**
- Create: `logstream-portal/backend/app/cribl/admin.py`
- Test: `logstream-portal/backend/tests/test_cribl_admin.py`

`CriblAdmin.apply(routes, destinations)` reconciles all `fork_*` objects in the
`logging` group to the desired set, commits, deploys, and polls for
convergence; on failure it redeploys the previous commit and raises. The
verification semantic is **exact-match on `fork_*` objects present after
deploy** (mirrors the Vector admin's exact-convergence fix).

- [ ] **Step 1: Write the failing tests** — `tests/test_cribl_admin.py`

```python
import pytest

from app.cribl.admin import CriblAdmin, CriblApplyError


class FakeCribl:
    """In-memory stand-in for the Cribl leader REST API."""

    def __init__(self, deploy_fails=False, login_fails=False):
        self.routes = {}          # id -> route dict (only fork_* tracked by portal)
        self.destinations = {}    # id -> dest dict
        self.commits = []         # list of commit ids
        self.deployed_commit = "c0"
        self.deploy_fails = deploy_fails
        self.login_fails = login_fails
        self.calls = []

    def post(self, url, json=None, headers=None, timeout=None):
        self.calls.append(("POST", url))
        if url.endswith("/auth/login"):
            if self.login_fails:
                return _Resp(401, {})
            return _Resp(200, {"token": "tok-123"})
        if url.endswith("/version/commit"):
            cid = f"c{len(self.commits) + 1}"
            self.commits.append(cid)
            return _Resp(200, {"items": [{"commit": cid}]})
        if "/deploy" in url:
            if self.deploy_fails:
                return _Resp(500, {})
            self.deployed_commit = (json or {}).get("version", self.commits[-1])
            return _Resp(200, {"items": [{"version": self.deployed_commit}]})
        return _Resp(404, {})

    def put(self, url, json=None, headers=None, timeout=None):
        self.calls.append(("PUT", url))
        obj = json
        if "/system/outputs/" in url:
            self.destinations[obj["id"]] = obj
        else:
            self.routes[obj["id"]] = obj
        return _Resp(200, {})

    def delete(self, url, headers=None, timeout=None):
        self.calls.append(("DELETE", url))
        oid = url.rstrip("/").split("/")[-1]
        self.routes.pop(oid, None)
        self.destinations.pop(oid, None)
        return _Resp(200, {})

    def get(self, url, headers=None, timeout=None):
        self.calls.append(("GET", url))
        if url.endswith("/routes"):
            # Cribl returns the whole routes doc; include a non-fork route to prove it's preserved
            items = [{"id": "archive", "filter": "true"}] + list(self.routes.values())
            return _Resp(200, {"items": [{"routes": items}]})
        if url.endswith("/system/outputs"):
            return _Resp(200, {"items": list(self.destinations.values())})
        if "/master/groups/" in url and url.endswith("/logging"):
            return _Resp(200, {"items": [{"id": "logging", "deployedVersion": self.deployed_commit}]})
        return _Resp(404, {})


class _Resp:
    def __init__(self, status, payload):
        self.status_code, self._p = status, payload

    def json(self):
        return self._p

    def raise_for_status(self):
        if self.status_code >= 400:
            import requests
            raise requests.HTTPError(f"HTTP {self.status_code}")


def _admin(session):
    return CriblAdmin(
        base_url="http://leader:9000", group="logging",
        username="admin", password="pw", session=session,
        poll_interval=0.001, timeout=0.05,
    )


def test_apply_creates_routes_and_destinations_then_commits_and_deploys():
    fake = FakeCribl()
    admin = _admin(fake)
    route = {"id": "fork_3", "output": "fork_3_dest", "filter": "x"}
    dest = {"id": "fork_3_dest", "type": "kinesis"}
    admin.apply([route], [dest])
    assert fake.routes == {"fork_3": route}
    assert fake.destinations == {"fork_3_dest": dest}
    assert fake.commits and fake.deployed_commit == fake.commits[-1]


def test_apply_deletes_stale_fork_objects_but_preserves_others():
    fake = FakeCribl()
    fake.routes["fork_9"] = {"id": "fork_9"}
    fake.destinations["fork_9_dest"] = {"id": "fork_9_dest"}
    admin = _admin(fake)
    admin.apply([{"id": "fork_3", "output": "fork_3_dest"}], [{"id": "fork_3_dest"}])
    assert "fork_9" not in fake.routes and "fork_9_dest" not in fake.destinations
    assert "fork_3" in fake.routes


def test_apply_empty_removes_all_fork_objects():
    fake = FakeCribl()
    fake.routes["fork_1"] = {"id": "fork_1"}
    admin = _admin(fake)
    admin.apply([], [])
    assert fake.routes == {}


def test_deploy_failure_rolls_back_to_previous_commit_and_raises():
    fake = FakeCribl(deploy_fails=True)
    admin = _admin(fake)
    with pytest.raises(CriblApplyError):
        admin.apply([{"id": "fork_3", "output": "fork_3_dest"}], [{"id": "fork_3_dest"}])
    # rollback deploy of the previously-deployed commit was attempted
    assert any("/deploy" in u for _, u in fake.calls)


def test_login_failure_raises_cribl_apply_error():
    fake = FakeCribl(login_fails=True)
    admin = _admin(fake)
    with pytest.raises(CriblApplyError):
        admin.apply([], [])
```

- [ ] **Step 2: Run to verify fail**

Run: `pytest tests/test_cribl_admin.py -q`
Expected: `ModuleNotFoundError: No module named 'app.cribl.admin'`

- [ ] **Step 3: Write `app/cribl/admin.py`**

```python
"""Apply fork Routes+Destinations to the Cribl logging worker group.

A fork mutation is: upsert the desired fork_* objects, delete stale ones,
commit, deploy the new commit to the group, and poll until the group reports
the deployed version. On any failure, redeploy the previously deployed commit
(git-native rollback) and raise. Only fork_* objects are touched; the static
archive route and any other config the seed script created are preserved.
"""
import time

import requests


class CriblApplyError(RuntimeError):
    """Cribl rejected the config change or did not converge in time."""


class CriblAdmin:
    def __init__(self, base_url, group, username, password,
                 session=None, poll_interval=1.0, timeout=30.0):
        self._base = base_url.rstrip("/")
        self._group = group
        self._user = username
        self._pw = password
        self._s = session or requests.Session()
        self._poll = poll_interval
        self._timeout = timeout

    # ── auth ───────────────────────────────────────────────────────────
    def _login(self) -> dict:
        try:
            r = self._s.post(
                f"{self._base}/api/v1/auth/login",
                json={"username": self._user, "password": self._pw}, timeout=10,
            )
            r.raise_for_status()
            return {"Authorization": f"Bearer {r.json()['token']}"}
        except (requests.RequestException, KeyError) as exc:
            raise CriblApplyError(f"cribl login failed: {exc}") from exc

    # ── reconcile ──────────────────────────────────────────────────────
    def apply(self, routes: list[dict], destinations: list[dict]) -> None:
        h = self._login()
        g = f"{self._base}/api/v1/m/{self._group}"
        previous = self._deployed_version(h)
        try:
            self._reconcile_destinations(g, h, destinations)
            self._reconcile_routes(g, h, routes)
            commit = self._commit(h)
            self._deploy(h, commit)
            self._await_version(h, commit)
        except CriblApplyError:
            self._rollback(h, previous)
            raise
        except requests.RequestException as exc:
            self._rollback(h, previous)
            raise CriblApplyError(f"cribl apply failed: {exc}") from exc

    def _desired_ids(self, items):
        return {i["id"] for i in items}

    def _reconcile_destinations(self, g, h, destinations):
        existing = {d["id"] for d in self._get(f"{g}/system/outputs", h).get("items", [])
                    if d.get("id", "").startswith("fork_")}
        desired = self._desired_ids(destinations)
        for d in destinations:
            self._s.put(f"{g}/system/outputs/{d['id']}", json=d, headers=h, timeout=10).raise_for_status()
        for stale in existing - desired:
            self._s.delete(f"{g}/system/outputs/{stale}", headers=h, timeout=10)

    def _reconcile_routes(self, g, h, routes):
        doc = self._get(f"{g}/routes", h)
        current = doc["items"][0]["routes"] if doc.get("items") else []
        existing_forks = {r["id"] for r in current if r.get("id", "").startswith("fork_")}
        desired = self._desired_ids(routes)
        for r in routes:
            self._s.put(f"{g}/routes/{r['id']}", json=r, headers=h, timeout=10).raise_for_status()
        for stale in existing_forks - desired:
            self._s.delete(f"{g}/routes/{stale}", headers=h, timeout=10)

    # ── commit / deploy / verify ───────────────────────────────────────
    def _commit(self, h) -> str:
        r = self._s.post(f"{self._base}/api/v1/version/commit",
                         json={"group": self._group, "message": "logstream-portal fork change"},
                         headers=h, timeout=15)
        r.raise_for_status()
        return r.json()["items"][0]["commit"]

    def _deploy(self, h, version) -> None:
        r = self._s.post(f"{self._base}/api/v1/master/groups/{self._group}/deploy",
                         json={"version": version}, headers=h, timeout=15)
        if r.status_code >= 400:
            raise CriblApplyError(f"deploy rejected: HTTP {r.status_code}")

    def _deployed_version(self, h) -> str | None:
        try:
            items = self._get(f"{self._base}/api/v1/master/groups/{self._group}", h).get("items", [])
            return items[0].get("deployedVersion") if items else None
        except requests.RequestException:
            return None

    def _await_version(self, h, version) -> None:
        deadline = time.monotonic() + self._timeout
        while True:
            try:
                if self._deployed_version(h) == version:
                    return
            except requests.RequestException:
                pass
            if time.monotonic() >= deadline:
                break
            time.sleep(self._poll)
        raise CriblApplyError(f"group did not converge to {version} within {self._timeout}s")

    def _rollback(self, h, version) -> None:
        if version is None:
            return
        try:
            self._deploy(h, version)
        except Exception:
            pass  # best-effort; the original error is what we raise

    def _get(self, url, h) -> dict:
        r = self._s.get(url, headers=h, timeout=10)
        r.raise_for_status()
        return r.json()
```

- [ ] **Step 4: Run to verify pass**

Run: `pytest tests/test_cribl_admin.py -q`
Expected: `5 passed`. If a path differs from the real API recorded in Task 0's `cribl/README.md`, update the URL constants here and in the fake to match (keep them identical).

- [ ] **Step 5: Commit**

```bash
git add logstream-portal/backend/app/cribl/admin.py logstream-portal/backend/tests/test_cribl_admin.py
git commit -m "feat(portal): CriblAdmin REST client with commit/deploy/rollback"
```

---

### Task 3: Rewire StreamService + delete Vector + settings

**Files:**
- Modify: `logstream-portal/backend/app/config.py`
- Modify: `logstream-portal/backend/app/streams/service.py`
- Modify: `logstream-portal/backend/app/main.py`
- Modify: `logstream-portal/backend/tests/conftest.py`
- Modify: `logstream-portal/backend/tests/test_stream_service.py`
- Delete: `logstream-portal/backend/app/streams/fragments.py`, `app/streams/vector_admin.py`, `tests/test_fragments.py`, `tests/test_vector_admin.py`

- [ ] **Step 1: Update `config.py`** — replace Vector/LocalStack fields with Cribl fields

Replace the `vector_api_url`, `fragments_path`, `aws_endpoint` fields and their `load_settings` lines. New fields:

```python
    cribl_base_url: str        # http://cribl-leader:9000
    cribl_group: str           # "logging"
    cribl_username: str
    cribl_password: str
    aws_region: str
    data_dir: str
    snapshot_seed: str
    static_dir: str
    session_secret: str
    uc_catalog: str
    databricks_host: str
    databricks_token: str
    resource_prefix: str       # "logstream-"
```

`load_settings()` reads `CRIBL_BASE_URL` (default `http://cribl-leader:9000`),
`CRIBL_GROUP` (default `logging`), `CRIBL_USERNAME` (default `admin`),
`CRIBL_PASSWORD` (default `""`), `RESOURCE_PREFIX` (default `logstream-`),
drops `AWS_ENDPOINT_URL`/`VECTOR_*`/`fragments_path`. Keep `AWS_REGION`,
`PORTAL_DATA_DIR`, `CATALOG_SNAPSHOT_SEED`, `PORTAL_STATIC_DIR`,
`PORTAL_SESSION_SECRET`, `UC_CATALOG_NAME`, `DATABRICKS_HOST`,
`DATABRICKS_TOKEN`.

- [ ] **Step 2: Update `service.py` `reapply()`** to build Cribl objects

Replace the imports `from app.streams.fragments import ...` and
`from app.streams.vector_admin import VectorApplyError` with:

```python
from app.cribl.objects import Member, StreamSpec, route_for, destination_for
from app.cribl.admin import CriblApplyError
```

Replace the body of `reapply()`:

```python
    def reapply(self) -> None:
        """Regenerate all fork routes+destinations from DB state and apply."""
        routes, destinations = [], []
        for stream in self._conn.execute(
            "SELECT * FROM streams WHERE status = 'live'"
        ).fetchall():
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
                destinations.append(destination_for(spec))
        self._pipeline.apply(routes, destinations)
```

Rename the constructor param and attribute `vector_admin`/`self._vector` →
`pipeline`/`self._pipeline`. In `_reapply_or_flag`, change the caught
exception from `VectorApplyError` to `CriblApplyError` (keep the
`ValueError` catch). Everywhere `self._vector` appeared, use `self._pipeline`.

**Note:** this requires `stream_sources.account_id`. The current schema has
`workload`+`source_name` but not `account_id`. Add `account_id TEXT` to the
`stream_sources` table in `db.py` SCHEMA, and set it in `_insert_members`
from the resolved source. Update `find_source` (catalog/service.py) to also
return `account_id` (it has it via the account node). Wire it through:

```python
# catalog/service.py find_source(): add account_id to the returned dict
return {"fqn": fqn, "account_id": account["account_id"],
        "workload_tag": workload["name"], "source_name": src["name"],
        "sensitivity": src["sensitivity"]}
```

```python
# service.py _insert_members(): include account_id
self._conn.execute(
    "INSERT INTO stream_sources (stream_id, source_fqn, account_id, workload,"
    " source_name, status, requested_by) VALUES (?, ?, ?, ?, ?, ?, ?)",
    (stream_id, src["fqn"], src["account_id"], src["workload_tag"],
     src["source_name"], status, user["id"]),
)
```

- [ ] **Step 3: Update `main.py`** — build `CriblAdmin` instead of `VectorAdmin`

Replace the `VectorAdmin` import and construction:

```python
from app.cribl.admin import CriblAdmin
...
app.state.pipeline = services.get("pipeline") or CriblAdmin(
    base_url=settings.cribl_base_url, group=settings.cribl_group,
    username=settings.cribl_username, password=settings.cribl_password,
)
app.state.streams = StreamService(
    conn, app.state.catalog, app.state.provisioner, app.state.pipeline, settings
)
```

- [ ] **Step 4: Update `conftest.py`** — `FakePipelineAdmin` replaces `FakeVectorAdmin`

```python
class FakePipelineAdmin:
    def __init__(self):
        self.applied = []

    def apply(self, routes, destinations):
        self.applied.append({"routes": routes, "destinations": destinations})
```

In the `fakes` fixture, key it as `"pipeline"` (not `"vector_admin"`). Update
the `client` fixture / Settings construction to the new field names
(`cribl_base_url`, `cribl_group`, `cribl_username`, `cribl_password`,
`resource_prefix`, drop `aws_endpoint`/`fragments_path`/`vector_api_url`).

- [ ] **Step 5: Update `test_stream_service.py`** — assert Cribl payloads + add account_id

The fixture catalog already carries account IDs. Change the `FakeVectorAdmin`
to the new shape (records `applied` list of `{routes, destinations}`), and
update assertions that inspected rendered YAML strings to inspect the
structured payload instead. Example transformations:

```python
# was: assert '.source_name == "syslog"' in last["rendered"]
# now:
last = pipeline.applied[-1]
assert any(r["id"] == f"fork_{stream['id']}" for r in last["routes"])
assert any("source_name=='syslog'" in r["filter"] for r in last["routes"])

# sensitive exclusion — was: assert "auth_log" not in last["rendered"]
# now:
assert all("auth_log" not in r["filter"] for r in pipeline.applied[-1]["routes"])

# empty (all pending) — was: assert last["expected"] == ()
# now:
assert pipeline.applied[-1]["routes"] == []
```

Rename the `FakeVectorAdmin` class to `FakePipelineAdmin`, the `vector`
fixture var to `pipeline`, and `VectorApplyError`→`CriblApplyError` in the
rejection test (the fake raises `CriblApplyError` when `fail=True`).

- [ ] **Step 6: Delete the Vector modules and tests**

```bash
git rm logstream-portal/backend/app/streams/fragments.py \
       logstream-portal/backend/app/streams/vector_admin.py \
       logstream-portal/backend/tests/test_fragments.py \
       logstream-portal/backend/tests/test_vector_admin.py \
       logstream-portal/backend/tests/golden/forks_empty.yaml \
       logstream-portal/backend/tests/golden/forks_two_streams.yaml
```

- [ ] **Step 7: Add the `account_id` column migration test to `test_db.py`**

```python
def test_stream_sources_has_account_id(conn):
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(stream_sources)")]
    assert "account_id" in cols
```

- [ ] **Step 8: Run the full backend suite**

Run: `pytest -q`
Expected: all pass. The deleted Vector tests are gone; cribl tests + rewired
stream-service tests carry the coverage. Fix any reference to removed
symbols.

- [ ] **Step 9: Commit**

```bash
git add -A logstream-portal/backend
git commit -m "feat(portal): swap StreamService onto Cribl, delete Vector pipeline"
```

---

### Task 4: De-LocalStack the provisioner and peek (instance-role boto3)

**Files:**
- Modify: `logstream-portal/backend/app/streams/provisioner.py`
- Modify: `logstream-portal/backend/app/streams/peek.py`
- Modify: `logstream-portal/backend/app/main.py`
- Modify: `logstream-portal/backend/tests/conftest.py` (Settings field updates only)

The Provisioner/PeekService logic is unchanged (fakes still inject clients);
only `main.py`'s boto3 client construction drops the LocalStack endpoint and
the `test`/`test` keys so the portal's instance role signs, and stream/queue
names get the `resource_prefix`.

- [ ] **Step 1: Update `main.py` boto3 construction**

Replace the `aws_kwargs` block (which had `endpoint_url` + test creds) with:

```python
        kinesis = boto3.client("kinesis", region_name=settings.aws_region)
        sqs = boto3.client("sqs", region_name=settings.aws_region)
        app.state.provisioner = Provisioner(kinesis, sqs)
        app.state.peek = PeekService(kinesis, sqs)
```

- [ ] **Step 2: Apply the resource prefix in `StreamService.create_stream`**

The stream/queue physical name should be `resource_prefix + name`. In
`service.py create_stream`, change the provisioning call:

```python
            ref = self._provisioner.create(stream_type, self._settings.resource_prefix + name)
```

and `retry()` identically. The stored `name` stays user-facing; `resource_ref`
holds the prefixed physical name (already the pattern). Add a stream-service
test asserting the prefix is applied:

```python
def test_resource_name_is_prefixed(env):
    svc, provisioner, _ = env
    svc.create_stream(DANA, "team-x", "kinesis", [SYSLOG])
    assert provisioner.created == [("kinesis", "logstream-team-x")]
```

(The `env` fixture's Settings needs `resource_prefix="logstream-"`.)

- [ ] **Step 3: Run the suite**

Run: `pytest -q`
Expected: all pass.

- [ ] **Step 4: Commit**

```bash
git add -A logstream-portal/backend
git commit -m "feat(portal): use instance-role boto3 and prefixed resource names"
```

---

### Task 5: Real account IDs in catalog fixture + seed_catalog + UC re-seed

**Files:**
- Modify: `logstream-portal/fixtures/catalog_snapshot.json`
- Modify: `logstream-portal/scripts/seed_catalog.py`
- Test: `logstream-portal/backend/tests/test_catalog_service.py` (FQN updates)

- [ ] **Step 1: Rewrite `fixtures/catalog_snapshot.json`** with real IDs + new schemas

Account `111111111111`→`522412052544` alias `prod-ecommerce`, schemas
`acct_b__storefront_web` / `acct_b__orders_api`; account `222222222222`→
`624627265315` alias `prod-platform`, schemas `acct_c__identity_svc` /
`acct_c__batch_etl`. Every source `fqn` becomes
`logging_demo.<new_schema>.<table>`. Keep all other fields (sensitivity,
volumes, columns, descriptions) identical. (Full JSON: take the existing file,
apply the two account-id swaps and the four schema renames consistently across
all 9 sources and their fqns.)

- [ ] **Step 2: Update test constants** in `test_catalog_service.py`, `conftest.py`, `test_stream_service.py`

The module-level `SYSLOG`/`AUTH_LOG` constants and any inline fqns move to the
new schemas:

```python
SYSLOG = "logging_demo.acct_b__storefront_web.syslog"
AUTH_LOG = "logging_demo.acct_b__orders_api.auth_log"
```

Update `test_find_source_returns_filter_tags` to expect `account_id` in the
returned dict and the `acct_c__identity_svc` fqn. Update the annotate test fqn.

- [ ] **Step 3: Update `seed_catalog.py`** WORKLOADS

Change the two `account_id` values to `522412052544` / `624627265315`, the
`schema` keys to `acct_b__*` / `acct_c__*`, leave everything else. Add a
`DROP SCHEMA IF EXISTS ... CASCADE` for the old `acct_a__*`/`acct_b__*`
(placeholder-era) schemas at the top of `main()` so the re-seed is clean:

```python
    for old in ("acct_a__storefront_web", "acct_a__orders_api",
                "acct_b__identity_svc", "acct_b__batch_etl"):
        sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{old} CASCADE")
```

- [ ] **Step 4: Run the backend suite**

Run: `pytest -q`
Expected: all pass with the new fqns/account ids.

- [ ] **Step 5: Re-seed the real Unity Catalog**

```bash
cd logstream-portal
set -a && source .env && set +a
backend/.venv/bin/python3 scripts/seed_catalog.py
```

Expected: drops the old schemas, recreates `acct_b__*`/`acct_c__*`, final line
`Seeded catalog 'logging_demo': 4 workloads, 9 sources.` Verify via the API:

```bash
set -a && source .env && set +a
curl -s "$DATABRICKS_HOST/api/2.1/unity-catalog/schemas?catalog_name=logging_demo" \
  -H "Authorization: Bearer $DATABRICKS_TOKEN" | python3 -c 'import sys,json; print(sorted(s["name"] for s in json.load(sys.stdin).get("schemas",[])))'
```

Expected: `['acct_b__orders_api', 'acct_b__storefront_web', 'acct_c__batch_etl', 'acct_c__identity_svc', 'information_schema']`

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/fixtures/catalog_snapshot.json logstream-portal/scripts/seed_catalog.py logstream-portal/backend/tests
git commit -m "feat(portal): real account IDs in catalog, fixture, and UC seed"
```

---

### Task 6: Cribl seed spec + idempotent seeder

**Files:**
- Create: `logstream-portal/cribl/seed_spec.json`
- Create: `logstream-portal/scripts/seed_cribl.py`
- Test: `logstream-portal/backend/tests/test_seed_cribl.py` (pure builder portion)

`seed_spec.json` declares, per worker group, the Datagen sources + Eval tag
fields + the Cribl-TCP forward (collection groups) and the archive route
(logging group). `seed_cribl.py` turns it into API calls; the *payload
building* is unit-tested, the *API push* is integration-run in Task 12.

- [ ] **Step 1: Write `cribl/seed_spec.json`** (Plan A: three groups)

```json
{
  "groups": {
    "acct_b": {
      "account_id": "522412052544", "account_alias": "prod-ecommerce",
      "forward_to": "fork-ingest-b:10300",
      "sources": [
        {"workload": "storefront_web", "source_name": "syslog", "eventsPerSec": 4},
        {"workload": "storefront_web", "source_name": "nginx_access", "eventsPerSec": 8},
        {"workload": "orders_api", "source_name": "syslog", "eventsPerSec": 3},
        {"workload": "orders_api", "source_name": "auth_log", "eventsPerSec": 1},
        {"workload": "orders_api", "source_name": "app_log", "eventsPerSec": 2}
      ]
    },
    "acct_c": {
      "account_id": "624627265315", "account_alias": "prod-platform",
      "forward_to": "fork-ingest-c:10300",
      "sources": [
        {"workload": "identity_svc", "source_name": "syslog", "eventsPerSec": 3},
        {"workload": "identity_svc", "source_name": "auth_log", "eventsPerSec": 1},
        {"workload": "batch_etl", "source_name": "syslog", "eventsPerSec": 1},
        {"workload": "batch_etl", "source_name": "cron_log", "eventsPerSec": 1}
      ]
    },
    "logging": {
      "ingest_port": 10300,
      "archive_bucket": "log-archive-337394138208"
    }
  }
}
```

- [ ] **Step 2: Write the failing builder tests** — `tests/test_seed_cribl.py`

```python
import json
from pathlib import Path

from scripts.seed_cribl import build_datagen_source, build_eval_pipeline, build_tcp_output

SPEC = json.loads((Path(__file__).resolve().parents[2] / "cribl" / "seed_spec.json").read_text())


def test_datagen_source_id_and_rate():
    src = build_datagen_source("storefront_web", "syslog", 4)
    assert src["id"] == "ds_storefront_web_syslog"
    assert src["type"] == "datagen"
    assert src["samples"][0]["eventsPerSec"] == 4


def test_eval_pipeline_stamps_all_tag_fields():
    pl = build_eval_pipeline("522412052544", "prod-ecommerce", "orders_api", "auth_log")
    adds = pl["conf"]["functions"][0]["conf"]["add"]
    by = {a["name"]: a["value"] for a in adds}
    assert by["account_id"] == "'522412052544'"
    assert by["workload"] == "'orders_api'"
    assert by["source_name"] == "'auth_log'"
    assert by["account_alias"] == "'prod-ecommerce'"
    assert by["environment"] == "'prod'"


def test_tcp_output_targets_forward_host():
    out = build_tcp_output("fork-ingest-b:10300")
    assert out["type"] == "tcp"
    assert out["host"] == "fork-ingest-b"
    assert out["port"] == 10300
```

- [ ] **Step 3: Run to verify fail**

Run: `pytest tests/test_seed_cribl.py -q`
Expected: `ModuleNotFoundError: No module named 'scripts.seed_cribl'`
(If import path issues: run from `backend/`, and the test adds the repo root to
`sys.path` via the `parents[2]` resolution; if needed add a `conftest.py`
`sys.path` insert for `logstream-portal/`.)

- [ ] **Step 4: Write `scripts/seed_cribl.py`**

```python
"""Seed static Cribl config (sources, eval pipelines, forwards, archive) via API.

Idempotent: every object has a deterministic id and is PUT (upsert). Reads
cribl/seed_spec.json. Env: CRIBL_BASE_URL, CRIBL_USERNAME, CRIBL_PASSWORD.
The builder functions are pure and unit-tested; main() pushes + commits +
deploys each group.
"""
import json
import os
from pathlib import Path

import requests

SPEC = json.loads((Path(__file__).resolve().parent.parent / "cribl" / "seed_spec.json").read_text())
TAG_FIELDS = ("account_id", "account_alias", "environment", "workload", "source_name")


def build_datagen_source(workload: str, source_name: str, eps: int) -> dict:
    return {
        "id": f"ds_{workload}_{source_name}",
        "type": "datagen",
        "samples": [{"sample": f"{source_name}.log", "eventsPerSec": eps}],
        "sendToRoutes": False,
        "pipeline": f"tag_{workload}_{source_name}",
        "output": "to_logging",
    }


def build_eval_pipeline(account_id, alias, workload, source_name) -> dict:
    add = [
        {"name": "account_id", "value": f"'{account_id}'"},
        {"name": "account_alias", "value": f"'{alias}'"},
        {"name": "environment", "value": "'prod'"},
        {"name": "workload", "value": f"'{workload}'"},
        {"name": "source_name", "value": f"'{source_name}'"},
    ]
    return {
        "id": f"tag_{workload}_{source_name}",
        "conf": {"functions": [{"id": "eval", "conf": {"add": add}}]},
    }


def build_tcp_output(forward_to: str) -> dict:
    host, port = forward_to.split(":")
    return {"id": "to_logging", "type": "tcp", "host": host, "port": int(port),
            "sendHeader": False}


def _session_headers(base):
    r = requests.post(f"{base}/api/v1/auth/login",
                      json={"username": os.environ.get("CRIBL_USERNAME", "admin"),
                            "password": os.environ["CRIBL_PASSWORD"]}, timeout=10)
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['token']}"}


def main() -> None:
    base = os.environ.get("CRIBL_BASE_URL", "http://localhost:9000").rstrip("/")
    h = _session_headers(base)

    for group, cfg in SPEC["groups"].items():
        if group == "logging":
            continue
        g = f"{base}/api/v1/m/{group}"
        requests.put(f"{g}/system/outputs/to_logging",
                     json=build_tcp_output(cfg["forward_to"]), headers=h, timeout=10).raise_for_status()
        for s in cfg["sources"]:
            pl = build_eval_pipeline(cfg["account_id"], cfg["account_alias"],
                                     s["workload"], s["source_name"])
            requests.put(f"{g}/pipelines/{pl['id']}", json=pl, headers=h, timeout=10).raise_for_status()
            src = build_datagen_source(s["workload"], s["source_name"], s["eventsPerSec"])
            requests.put(f"{g}/system/inputs/{src['id']}", json=src, headers=h, timeout=10).raise_for_status()
        _commit_deploy(base, group, h)

    # logging group: tcp source + archive route/destination
    lg = f"{base}/api/v1/m/logging"
    requests.put(f"{lg}/system/inputs/from_agents",
                 json={"id": "from_agents", "type": "tcp",
                       "host": "0.0.0.0", "port": SPEC["groups"]["logging"]["ingest_port"]},
                 headers=h, timeout=10).raise_for_status()
    requests.put(f"{lg}/system/outputs/archive",
                 json={"id": "archive", "type": "s3",
                       "bucket": SPEC["groups"]["logging"]["archive_bucket"],
                       "region": "us-east-1", "awsAuthenticationMethod": "auto",
                       "destPath": "raw", "format": "json"},
                 headers=h, timeout=10).raise_for_status()
    requests.put(f"{lg}/routes/archive",
                 json={"id": "archive", "name": "archive", "filter": "true",
                       "final": True, "output": "archive", "pipeline": "devnull"},
                 headers=h, timeout=10).raise_for_status()
    _commit_deploy(base, "logging", h)
    print("Cribl seeded: collection groups + logging archive route.")


def _commit_deploy(base, group, h) -> None:
    c = requests.post(f"{base}/api/v1/version/commit",
                      json={"group": group, "message": f"seed {group}"}, headers=h, timeout=15)
    c.raise_for_status()
    version = c.json()["items"][0]["commit"]
    requests.post(f"{base}/api/v1/master/groups/{group}/deploy",
                  json={"version": version}, headers=h, timeout=15).raise_for_status()


if __name__ == "__main__":
    main()
```

- [ ] **Step 5: Run the builder tests**

Run: `pytest tests/test_seed_cribl.py -q`
Expected: `3 passed`. (The API-push `main()` is exercised live in Task 12.)

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/cribl/seed_spec.json logstream-portal/scripts/seed_cribl.py logstream-portal/backend/tests/test_seed_cribl.py
git commit -m "feat(portal): cribl seed spec and idempotent seeder with builder tests"
```

**Plan B note:** if Task 0 chose Edge, `seed_cribl.py` pushes the acct_b/acct_c
source+eval config to two Edge instances (base URLs from env
`CRIBL_EDGE_B_URL`/`CRIBL_EDGE_C_URL`) instead of worker groups on the leader;
the builder functions are unchanged. Document the chosen path in
`cribl/README.md`.

---

### Task 7: Terraform root + network module

**Files:**
- Create: `logstream-portal/infra/providers.tf`, `variables.tf`, `main.tf`, `outputs.tf`, `terraform.tfvars.example`, `.gitignore`
- Create: `logstream-portal/infra/modules/network/{main.tf,variables.tf,outputs.tf}`

No unit tests (IaC); verification is `terraform validate` + `terraform plan`.

- [ ] **Step 1: `infra/.gitignore`**

```
.terraform/
*.tfstate
*.tfstate.*
terraform.tfvars
.terraform.lock.hcl
```

- [ ] **Step 2: `infra/providers.tf`** — three aliased AWS providers

```hcl
terraform {
  required_version = ">= 1.6"
  required_providers {
    aws = { source = "hashicorp/aws", version = "~> 5.40" }
  }
}

provider "aws" {                       # logging account A (default profile)
  alias   = "logging"
  region  = var.region
  profile = "default"
}

provider "aws" {                       # workload account B
  alias   = "acct_b"
  region  = var.region
  profile = "seth-demo-b"
}

provider "aws" {                       # workload account C
  alias   = "acct_c"
  region  = var.region
  profile = "seth-demo-c"
}
```

- [ ] **Step 3: `infra/variables.tf`**

```hcl
variable "region"        { type = string  default = "us-east-1" }
variable "operator_cidr" { type = string  description = "Your /32 for ALB + SSH access" }
variable "key_name"      { type = string  default = "" description = "Optional EC2 keypair for SSH" }
variable "instance_type" { type = string  default = "t3.small" }
variable "logging_instance_type" { type = string default = "t3.medium" }

variable "account_a" { type = string default = "337394138208" }
variable "account_b" { type = string default = "522412052544" }
variable "account_c" { type = string default = "624627265315" }

variable "vpc_cidrs" {
  type = map(string)
  default = { logging = "10.30.0.0/16", acct_b = "10.31.0.0/16", acct_c = "10.32.0.0/16" }
}
```

- [ ] **Step 4: `infra/modules/network/variables.tf` + `outputs.tf` + `main.tf`**

`variables.tf`:

```hcl
variable "name"        { type = string }
variable "cidr"        { type = string }
variable "operator_cidr" { type = string default = "" }
```

`main.tf`:

```hcl
data "aws_availability_zones" "az" { state = "available" }

resource "aws_vpc" "this" {
  cidr_block           = var.cidr
  enable_dns_support   = true
  enable_dns_hostnames = true
  tags = { Name = "logstream-${var.name}" }
}

resource "aws_internet_gateway" "igw" {
  vpc_id = aws_vpc.this.id
  tags = { Name = "logstream-${var.name}" }
}

resource "aws_subnet" "public" {
  count                   = 2
  vpc_id                  = aws_vpc.this.id
  cidr_block              = cidrsubnet(var.cidr, 8, count.index)
  availability_zone       = data.aws_availability_zones.az.names[count.index]
  map_public_ip_on_launch = true
  tags = { Name = "logstream-${var.name}-public-${count.index}" }
}

resource "aws_route_table" "public" {
  vpc_id = aws_vpc.this.id
  route { cidr_block = "0.0.0.0/0"  gateway_id = aws_internet_gateway.igw.id }
}

resource "aws_route_table_association" "public" {
  count          = 2
  subnet_id      = aws_subnet.public[count.index].id
  route_table_id = aws_route_table.public.id
}

resource "aws_security_group" "instance" {
  name_prefix = "logstream-${var.name}-"
  vpc_id      = aws_vpc.this.id
  egress { from_port = 0 to_port = 0 protocol = "-1" cidr_blocks = ["0.0.0.0/0"] }
  tags = { Name = "logstream-${var.name}" }
}
```

`outputs.tf`:

```hcl
output "vpc_id"      { value = aws_vpc.this.id }
output "subnet_ids"  { value = aws_subnet.public[*].id }
output "sg_id"       { value = aws_security_group.instance.id }
output "cidr"        { value = aws_vpc.this.cidr_block }
```

- [ ] **Step 5: `infra/main.tf`** — instantiate the three networks

```hcl
module "net_logging" {
  source        = "./modules/network"
  providers     = { aws = aws.logging }
  name          = "logging"
  cidr          = var.vpc_cidrs["logging"]
  operator_cidr = var.operator_cidr
}
module "net_b" {
  source    = "./modules/network"
  providers = { aws = aws.acct_b }
  name      = "acct-b"
  cidr      = var.vpc_cidrs["acct_b"]
}
module "net_c" {
  source    = "./modules/network"
  providers = { aws = aws.acct_c }
  name      = "acct-c"
  cidr      = var.vpc_cidrs["acct_c"]
}
```

`infra/outputs.tf`: start with `output "logging_vpc" { value = module.net_logging.vpc_id }`.
`terraform.tfvars.example`: `operator_cidr = "203.0.113.5/32"`.

- [ ] **Step 6: Verify**

```bash
cd logstream-portal/infra
cp terraform.tfvars.example terraform.tfvars   # edit operator_cidr to your real IP/32
terraform init
terraform validate
terraform plan -target=module.net_logging -target=module.net_b -target=module.net_c
```

Expected: `validate` succeeds; `plan` shows 3 VPCs + subnets/IGWs/SGs across the three accounts, no errors. Do NOT apply yet.

- [ ] **Step 7: Commit**

```bash
git add logstream-portal/infra/providers.tf logstream-portal/infra/variables.tf logstream-portal/infra/main.tf logstream-portal/infra/outputs.tf logstream-portal/infra/terraform.tfvars.example logstream-portal/infra/.gitignore logstream-portal/infra/modules/network
git commit -m "feat(infra): terraform root with three-account providers and network module"
```

---

### Task 8: PrivateLink module (NLB + endpoint service per workload account)

**Files:**
- Create: `logstream-portal/infra/modules/privatelink/{main.tf,variables.tf,outputs.tf}`
- Modify: `logstream-portal/infra/main.tf` (instantiate per workload account)

- [ ] **Step 1: `modules/privatelink/variables.tf`**

```hcl
variable "workload_name"        { type = string }          # "acct-b"
variable "logging_vpc_id"       { type = string }
variable "logging_subnet_ids"   { type = list(string) }
variable "logging_instance_id"  { type = string }          # NLB target (cribl-central)
variable "consumer_account_id"  { type = string }          # allowlisted principal
variable "consumer_vpc_id"      { type = string }
variable "consumer_subnet_ids"  { type = list(string) }
variable "consumer_sg_id"       { type = string }
```

- [ ] **Step 2: `modules/privatelink/main.tf`**

```hcl
# --- in the LOGGING account: NLB + endpoint service ---
resource "aws_lb" "nlb" {
  name               = "ls-${var.workload_name}"
  internal           = true
  load_balancer_type = "network"
  subnets            = var.logging_subnet_ids
}

locals { ports = { data = 10300, mgmt = 9000 } }

resource "aws_lb_target_group" "tg" {
  for_each    = local.ports
  name        = "ls-${var.workload_name}-${each.key}"
  port        = each.value
  protocol    = "TCP"
  vpc_id      = var.logging_vpc_id
  target_type = "instance"
}

resource "aws_lb_target_group_attachment" "att" {
  for_each         = aws_lb_target_group.tg
  target_group_arn = each.value.arn
  target_id        = var.logging_instance_id
  port             = local.ports[each.key]
}

resource "aws_lb_listener" "lst" {
  for_each          = aws_lb_target_group.tg
  load_balancer_arn = aws_lb.nlb.arn
  port              = local.ports[each.key]
  protocol          = "TCP"
  default_action { type = "forward"  target_group_arn = each.value.arn }
}

resource "aws_vpc_endpoint_service" "svc" {
  acceptance_required        = false
  network_load_balancer_arns = [aws_lb.nlb.arn]
  allowed_principals         = ["arn:aws:iam::${var.consumer_account_id}:root"]
  tags = { Name = "logstream-${var.workload_name}" }
}

# --- in the CONSUMER (workload) account: interface endpoint ---
resource "aws_security_group" "vpce" {
  provider    = aws.consumer
  name_prefix = "ls-vpce-${var.workload_name}-"
  vpc_id      = var.consumer_vpc_id
  ingress { from_port = 10300 to_port = 10300 protocol = "tcp" cidr_blocks = ["0.0.0.0/0"] }
  ingress { from_port = 9000  to_port = 9000  protocol = "tcp" cidr_blocks = ["0.0.0.0/0"] }
  egress  { from_port = 0 to_port = 0 protocol = "-1" cidr_blocks = ["0.0.0.0/0"] }
}

resource "aws_vpc_endpoint" "ep" {
  provider            = aws.consumer
  vpc_id              = var.consumer_vpc_id
  service_name        = aws_vpc_endpoint_service.svc.service_name
  vpc_endpoint_type   = "Interface"
  subnet_ids          = var.consumer_subnet_ids
  security_group_ids  = [aws_security_group.vpce.id]
  private_dns_enabled = false
}
```

The module needs two providers (logging + consumer). Add to its own
`required_providers` with a `configuration_aliases = [aws, aws.consumer]`
block at the top of `main.tf`:

```hcl
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", configuration_aliases = [aws, aws.consumer] }
  }
}
```

- [ ] **Step 3: `modules/privatelink/outputs.tf`**

```hcl
output "endpoint_dns" { value = aws_vpc_endpoint.ep.dns_entry[0].dns_name }
output "service_name" { value = aws_vpc_endpoint_service.svc.service_name }
```

- [ ] **Step 4: Wire into `infra/main.tf`** (depends on compute's logging instance from Task 9 — declare now, the instance id output is added in Task 9)

```hcl
module "pl_b" {
  source   = "./modules/privatelink"
  providers = { aws = aws.logging, aws.consumer = aws.acct_b }
  workload_name      = "acct-b"
  logging_vpc_id     = module.net_logging.vpc_id
  logging_subnet_ids = module.net_logging.subnet_ids
  logging_instance_id = module.compute.logging_instance_id
  consumer_account_id = var.account_b
  consumer_vpc_id     = module.net_b.vpc_id
  consumer_subnet_ids = module.net_b.subnet_ids
  consumer_sg_id      = module.net_b.sg_id
}
# module "pl_c" — identical with acct_c / net_c / var.account_c
```

- [ ] **Step 5: Verify** (after Task 9 adds `module.compute.logging_instance_id`)

```bash
cd logstream-portal/infra && terraform validate
terraform plan -target=module.pl_b -target=module.pl_c
```

Expected: validate clean; plan shows one NLB + endpoint service per workload
account and one interface endpoint in each consumer account. No apply.

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/infra/modules/privatelink logstream-portal/infra/main.tf
git commit -m "feat(infra): per-account privatelink module (nlb + endpoint service)"
```

---

### Task 9: Compute module (3 EC2s, instance profiles, ECR, user-data)

**Files:**
- Create: `logstream-portal/infra/modules/compute/{main.tf,variables.tf,outputs.tf,user_data_logging.sh.tftpl,user_data_worker.sh.tftpl}`
- Modify: `logstream-portal/infra/main.tf`, `outputs.tf`

- [ ] **Step 1: `modules/compute/variables.tf`**

```hcl
variable "region" { type = string }
variable "logging_vpc_id"     { type = string }
variable "logging_subnet_id"  { type = string }
variable "logging_sg_id"      { type = string }
variable "worker_b" { type = object({ vpc_id=string, subnet_id=string, sg_id=string, endpoint_dns=string }) }
variable "worker_c" { type = object({ vpc_id=string, subnet_id=string, sg_id=string, endpoint_dns=string }) }
variable "instance_type"         { type = string }
variable "logging_instance_type" { type = string }
variable "ecr_repo_url"          { type = string }
variable "key_name"              { type = string default = "" }
```

- [ ] **Step 2: `modules/compute/main.tf`** — instance profiles + the three instances

```hcl
terraform {
  required_providers {
    aws = { source = "hashicorp/aws", configuration_aliases = [aws, aws.acct_b, aws.acct_c] }
  }
}

data "aws_ssm_parameter" "al2023" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-default-x86_64"
}

# ---- logging instance role: kinesis/sqs by prefix, s3 archive, ecr pull, ssm ----
resource "aws_iam_role" "logging" {
  name = "logstream-logging"
  assume_role_policy = jsonencode({ Version = "2012-10-17", Statement = [{
    Effect = "Allow", Principal = { Service = "ec2.amazonaws.com" }, Action = "sts:AssumeRole" }] })
}

resource "aws_iam_role_policy" "logging" {
  role = aws_iam_role.logging.id
  policy = jsonencode({ Version = "2012-10-17", Statement = [
    { Effect = "Allow", Action = ["kinesis:*"], Resource = "arn:aws:kinesis:${var.region}:*:stream/logstream-*" },
    { Effect = "Allow", Action = ["sqs:*"],     Resource = "arn:aws:sqs:${var.region}:*:logstream-*" },
    { Effect = "Allow", Action = ["kinesis:ListStreams","sqs:ListQueues"], Resource = "*" },
    { Effect = "Allow", Action = ["s3:PutObject","s3:ListBucket","s3:CreateBucket"], Resource = ["arn:aws:s3:::log-archive-*","arn:aws:s3:::log-archive-*/*"] },
    { Effect = "Allow", Action = ["ssm:GetParameter","ssm:GetParameters"], Resource = "arn:aws:ssm:${var.region}:*:parameter/logstream/*" },
    { Effect = "Allow", Action = ["ecr:GetAuthorizationToken","ecr:BatchGetImage","ecr:GetDownloadUrlForLayer"], Resource = "*" } ] })
}

resource "aws_iam_instance_profile" "logging" { name = "logstream-logging" role = aws_iam_role.logging.name }

resource "aws_instance" "logging" {
  ami                  = data.aws_ssm_parameter.al2023.value
  instance_type        = var.logging_instance_type
  subnet_id            = var.logging_subnet_id
  vpc_security_group_ids = [var.logging_sg_id]
  iam_instance_profile = aws_iam_instance_profile.logging.name
  key_name             = var.key_name != "" ? var.key_name : null
  user_data = templatefile("${path.module}/user_data_logging.sh.tftpl", {
    region = var.region, ecr_repo_url = var.ecr_repo_url
  })
  tags = { Name = "logstream-cribl-central" }
}
```

Then a `worker` instance in each workload account (provider `aws.acct_b` /
`aws.acct_c`), each with a minimal SSM-only role and
`user_data_worker.sh.tftpl` templated with `endpoint_dns` + `group` (acct_b /
acct_c). Pattern (repeat for c):

```hcl
resource "aws_iam_role" "worker_b" {
  provider = aws.acct_b
  name = "logstream-worker"
  assume_role_policy = jsonencode({ Version="2012-10-17", Statement=[{ Effect="Allow", Principal={Service="ec2.amazonaws.com"}, Action="sts:AssumeRole" }] })
}
resource "aws_iam_instance_profile" "worker_b" { provider = aws.acct_b  name = "logstream-worker"  role = aws_iam_role.worker_b.name }
resource "aws_instance" "worker_b" {
  provider = aws.acct_b
  ami = data.aws_ssm_parameter.al2023.value   # NOTE: AL2023 AMI id differs per account/region — see Step 4
  instance_type = var.instance_type
  subnet_id = var.worker_b.subnet_id
  vpc_security_group_ids = [var.worker_b.sg_id]
  iam_instance_profile = aws_iam_instance_profile.worker_b.name
  user_data = templatefile("${path.module}/user_data_worker.sh.tftpl", {
    endpoint_dns = var.worker_b.endpoint_dns, group = "acct_b"
  })
  tags = { Name = "logstream-worker-acct-b" }
}
```

- [ ] **Step 3: `user_data_logging.sh.tftpl`**

```bash
#!/bin/bash
set -euxo pipefail
dnf install -y docker
systemctl enable --now docker
TOKEN=$(curl -s -X PUT "http://169.254.169.254/latest/api/token" -H "X-aws-ec2-metadata-token-ttl-seconds: 60")
AID=$(curl -s -H "X-aws-ec2-metadata-token: $TOKEN" http://169.254.169.254/latest/dynamic/instance-identity/document | python3 -c 'import sys,json;print(json.load(sys.stdin)["accountId"])')
# Cribl leader + logging worker
docker run -d --name cribl-leader --restart unless-stopped -p 9000:9000 \
  -e CRIBL_DIST_MODE=master cribl/cribl:latest
docker run -d --name cribl-logging --restart unless-stopped -p 10300:10300 \
  -e CRIBL_DIST_MODE=worker -e CRIBL_DIST_MASTER_URL="tcp://criblmaster@localhost:4200" \
  -e CRIBL_GROUP=logging cribl/cribl:latest
# Portal — pull from ECR, secrets from SSM
aws ecr get-login-password --region ${region} | docker login --username AWS --password-stdin ${ecr_repo_url}
DBX_HOST=$(aws ssm get-parameter --with-decryption --name /logstream/databricks_host --query Parameter.Value --output text --region ${region})
DBX_TOKEN=$(aws ssm get-parameter --with-decryption --name /logstream/databricks_token --query Parameter.Value --output text --region ${region})
DBX_WID=$(aws ssm get-parameter --with-decryption --name /logstream/databricks_warehouse_id --query Parameter.Value --output text --region ${region})
CRIBL_PW=$(aws ssm get-parameter --with-decryption --name /logstream/cribl_password --query Parameter.Value --output text --region ${region})
SESS=$(aws ssm get-parameter --with-decryption --name /logstream/session_secret --query Parameter.Value --output text --region ${region})
docker run -d --name portal --restart unless-stopped -p 8000:8000 \
  -e DATABRICKS_HOST="$DBX_HOST" -e DATABRICKS_TOKEN="$DBX_TOKEN" -e UC_CATALOG_NAME=logging_demo \
  -e AWS_REGION=${region} -e RESOURCE_PREFIX=logstream- \
  -e CRIBL_BASE_URL=http://localhost:9000 -e CRIBL_GROUP=logging -e CRIBL_USERNAME=admin -e CRIBL_PASSWORD="$CRIBL_PW" \
  -e PORTAL_SESSION_SECRET="$SESS" -e CATALOG_SNAPSHOT_SEED=/app/fixtures/catalog_snapshot.json \
  ${ecr_repo_url}:latest
```

(Note: the in-container leader↔worker enrollment uses Cribl's master port 4200;
the compose-equivalent wiring is validated in Task 12. If single-instance
distributed-on-one-host needs both leader and worker as one container, Task 12
adjusts — this is the documented integration-risk point.)

- [ ] **Step 4: `user_data_worker.sh.tftpl`**

```bash
#!/bin/bash
set -euxo pipefail
dnf install -y docker
systemctl enable --now docker
# Collection worker enrolls to the leader via the management endpoint, ships data via :10300
docker run -d --name cribl-worker --restart unless-stopped \
  -e CRIBL_DIST_MODE=worker \
  -e CRIBL_DIST_MASTER_URL="tcp://criblmaster@${endpoint_dns}:9000" \
  -e CRIBL_GROUP=${group} cribl/cribl:latest
```

(AMI caveat: `data.aws_ssm_parameter.al2023` resolves in the default provider's
account only. For the worker instances in B/C, add a per-provider
`data "aws_ssm_parameter"` (alias `aws.acct_b`/`aws.acct_c`) and reference it —
the AL2023 SSM public parameter exists in every account/region. Add those two
data sources in Step 2.)

- [ ] **Step 5: ECR repo + outputs.** In `modules/compute/main.tf` add:

```hcl
resource "aws_ecr_repository" "portal" { name = "logstream-portal" force_delete = true }
```

`outputs.tf`:

```hcl
output "logging_instance_id" { value = aws_instance.logging.id }
output "logging_private_ip"  { value = aws_instance.logging.private_ip }
output "ecr_repo_url"        { value = aws_ecr_repository.portal.repository_url }
output "worker_b_id"         { value = aws_instance.worker_b.id }
output "worker_c_id"         { value = aws_instance.worker_c.id }
```

- [ ] **Step 6: Wire `module "compute"` into `infra/main.tf`** with providers
`{ aws = aws.logging, aws.acct_b = aws.acct_b, aws.acct_c = aws.acct_c }`,
passing the network outputs and the privatelink endpoint DNS. Because compute
needs the endpoint DNS and privatelink needs the logging instance id, break the
cycle: privatelink targets the instance by id (compute first), and the worker
user-data gets the endpoint DNS. Resolve by splitting — workers depend on
`module.pl_b.endpoint_dns`; the logging instance does not depend on
privatelink. Terraform resolves this ordering automatically since the worker
instances reference `module.pl_*` outputs and the NLB references
`module.compute.logging_instance_id` (the logging instance is a separate
resource from the workers, so no real cycle).

- [ ] **Step 7: Verify**

```bash
cd logstream-portal/infra && terraform validate && terraform plan
```

Expected: validate clean; full plan shows 3 instances, 3 roles/profiles, 1 ECR
repo, NLBs/endpoint services/endpoints, VPCs. Resource count sane (~40-50). No
apply.

- [ ] **Step 8: Commit**

```bash
git add logstream-portal/infra/modules/compute logstream-portal/infra/main.tf logstream-portal/infra/outputs.tf
git commit -m "feat(infra): compute module — 3 EC2s, instance profiles, ECR, user-data"
```

---

### Task 10: Portal ALB module

**Files:**
- Create: `logstream-portal/infra/modules/portal_alb/{main.tf,variables.tf,outputs.tf}`
- Modify: `logstream-portal/infra/main.tf`, `outputs.tf`

- [ ] **Step 1: `modules/portal_alb/variables.tf`**

```hcl
variable "vpc_id"        { type = string }
variable "subnet_ids"    { type = list(string) }
variable "instance_id"   { type = string }
variable "operator_cidr" { type = string }
```

- [ ] **Step 2: `modules/portal_alb/main.tf`**

```hcl
resource "aws_security_group" "alb" {
  name_prefix = "logstream-alb-"
  vpc_id      = var.vpc_id
  ingress { from_port = 80 to_port = 80 protocol = "tcp" cidr_blocks = [var.operator_cidr] }
  egress  { from_port = 0  to_port = 0  protocol = "-1"  cidr_blocks = ["0.0.0.0/0"] }
}

resource "aws_lb" "portal" {
  name               = "logstream-portal"
  load_balancer_type = "application"
  subnets            = var.subnet_ids
  security_groups    = [aws_security_group.alb.id]
}

resource "aws_lb_target_group" "portal" {
  name     = "logstream-portal"
  port     = 8000
  protocol = "HTTP"
  vpc_id   = var.vpc_id
  health_check { path = "/api/personas" matcher = "200" }
}

resource "aws_lb_target_group_attachment" "portal" {
  target_group_arn = aws_lb_target_group.portal.arn
  target_id        = var.instance_id
  port             = 8000
}

resource "aws_lb_listener" "http" {
  load_balancer_arn = aws_lb.portal.arn
  port              = 80
  protocol          = "HTTP"
  default_action { type = "forward"  target_group_arn = aws_lb_target_group.portal.arn }
}
```

The logging instance SG must allow 8000 from the ALB SG — add an ingress rule
to `module.net_logging`'s SG via an `aws_security_group_rule` in `infra/main.tf`
referencing `module.portal_alb` (source_security_group_id). Also allow
10300/9000 from the NLBs (NLB targets need SG ingress from the VPC CIDR).

- [ ] **Step 3: `outputs.tf`** — `output "portal_url" { value = "http://${aws_lb.portal.dns_name}" }`

- [ ] **Step 4: Wire into `infra/main.tf`** (provider `aws.logging`) + add
`output "portal_url" { value = module.portal_alb.portal_url }` to root outputs.

- [ ] **Step 5: Verify** — `terraform validate && terraform plan`; ALB + TG +
listener + SG present, ingress limited to operator_cidr. No apply.

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/infra/modules/portal_alb logstream-portal/infra/main.tf logstream-portal/infra/outputs.tf
git commit -m "feat(infra): operator-restricted ALB for the portal"
```

---

### Task 11: SSM secret parameters

**Files:**
- Create: `logstream-portal/infra/modules/secrets/{main.tf,variables.tf}`
- Modify: `logstream-portal/infra/main.tf`, `variables.tf`, `terraform.tfvars.example`

- [ ] **Step 1: `modules/secrets/variables.tf`** — five sensitive vars

```hcl
variable "databricks_host"         { type = string }
variable "databricks_token"        { type = string  sensitive = true }
variable "databricks_warehouse_id" { type = string }
variable "cribl_password"          { type = string  sensitive = true }
variable "session_secret"          { type = string  sensitive = true }
```

- [ ] **Step 2: `modules/secrets/main.tf`**

```hcl
locals {
  params = {
    "/logstream/databricks_host"         = var.databricks_host
    "/logstream/databricks_token"        = var.databricks_token
    "/logstream/databricks_warehouse_id" = var.databricks_warehouse_id
    "/logstream/cribl_password"          = var.cribl_password
    "/logstream/session_secret"          = var.session_secret
  }
}
resource "aws_ssm_parameter" "p" {
  for_each = local.params
  name     = each.key
  type     = "SecureString"
  value    = each.value
}
```

- [ ] **Step 3: Add the vars to root `variables.tf`** (sensitive) and wire
`module "secrets"` (provider `aws.logging`). Add the non-secret defaults to
`terraform.tfvars.example` and document that the secret values come from the
local `.env` / are passed via `TF_VAR_*` env, never committed:

```hcl
# terraform.tfvars.example additions (DO NOT put real secrets here):
# pass secrets via environment instead:
#   export TF_VAR_databricks_token=...  TF_VAR_cribl_password=...  TF_VAR_session_secret=...
databricks_host         = "https://dbc-2ef2bfc1-c689.cloud.databricks.com"
databricks_warehouse_id = "0a3fea1c53bea9c6"
```

- [ ] **Step 4: Verify** — `terraform validate`; `terraform plan` (with
`TF_VAR_databricks_token` etc. exported) shows 5 SSM SecureString params, values
not printed. No apply.

- [ ] **Step 5: Commit**

```bash
git add logstream-portal/infra/modules/secrets logstream-portal/infra/main.tf logstream-portal/infra/variables.tf logstream-portal/infra/terraform.tfvars.example
git commit -m "feat(infra): ssm securestring parameters for portal secrets"
```

---

### Task 12: Bring-up orchestration + live deploy + Cribl seed

**Files:**
- Modify: `logstream-portal/Makefile`
- Create: `logstream-portal/scripts/build_push_portal.sh`

**This task applies real infrastructure — run with the user in the loop. Cost starts here.**

- [ ] **Step 1: `scripts/build_push_portal.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."
REPO=$(cd infra && terraform output -raw ecr_repo_url)
REGION=${AWS_REGION:-us-east-1}
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "${REPO%/*}"
docker build -t "$REPO:latest" .
docker push "$REPO:latest"
echo "pushed $REPO:latest"
```

- [ ] **Step 2: Makefile targets** (replace the compose-era ones)

```make
.PHONY: infra-up infra-down build-push seed-cribl seed-uc demo-test-aws test

test:
	cd backend && python3 -m pytest -q
	cd frontend && npm test

infra-up:
	cd infra && terraform init && terraform apply

build-push:
	./scripts/build_push_portal.sh

seed-uc:
	set -a && . ./.env && set +a && backend/.venv/bin/python3 scripts/seed_catalog.py

seed-cribl:
	CRIBL_BASE_URL=$$(cd infra && terraform output -raw cribl_url) \
	CRIBL_USERNAME=admin CRIBL_PASSWORD=$$CRIBL_PW \
	  backend/.venv/bin/python3 scripts/seed_cribl.py

demo-test-aws:
	./scripts/demo_test_aws.sh

infra-down:
	cd infra && terraform destroy
```

(Add `output "cribl_url"` to root outputs = `http://<logging_private_ip>:9000`
reachable via SSM port-forward; the seed runs from an SSM tunnel — Step 5.)

- [ ] **Step 3: Apply core infra (ECR first, so the image exists before instances boot)**

```bash
cd logstream-portal/infra
terraform apply -target=module.compute.aws_ecr_repository.portal   # ECR only
cd .. && make build-push                                            # build+push portal image
cd infra && terraform apply                                        # full apply
```

Expected: ~40-50 resources created across three accounts; root outputs print
`portal_url`, `ecr_repo_url`, instance ids. Note the spend clock starts.

- [ ] **Step 4: Apply secrets** (with secret env vars exported)

```bash
export TF_VAR_databricks_token=$(grep DATABRICKS_TOKEN ../.env | cut -d= -f2)
export TF_VAR_cribl_password='<chosen-strong-pw>'
export TF_VAR_session_secret=$(openssl rand -hex 24)
terraform apply -target=module.secrets
```

- [ ] **Step 5: Seed Cribl over an SSM tunnel + verify enrollment**

```bash
# port-forward 9000 from the logging instance via SSM
INSTANCE=$(cd infra && terraform output -raw logging_instance_id)
aws ssm start-session --target "$INSTANCE" \
  --document-name AWS-StartPortForwardingSession \
  --parameters '{"portNumber":["9000"],"localPortNumber":["9000"]}' &
sleep 5
export CRIBL_BASE_URL=http://localhost:9000 CRIBL_USERNAME=admin CRIBL_PASSWORD="$TF_VAR_cribl_password"
backend/.venv/bin/python3 scripts/seed_cribl.py
# verify both workers enrolled
curl -s http://localhost:9000/api/v1/master/workers -H "Authorization: Bearer $TOKEN" | python3 -c 'import sys,json; print([w["group"] for w in json.load(sys.stdin)["items"]])'
```

Expected: seed prints success; workers list shows `acct_b`, `acct_c`, `logging`.
**This is the integration moment** — if enrollment over PrivateLink fails, debug
SG/listener/endpoint DNS here (the documented risk point). Fix and re-verify
before proceeding.

- [ ] **Step 6: Verify the archive path + portal reachability**

```bash
aws s3 ls s3://log-archive-337394138208/raw/ --recursive | head   # objects flowing
curl -s $(cd infra && terraform output -raw portal_url)/api/personas | python3 -m json.tool
```

Expected: S3 objects under `raw/`; personas JSON (3 users) through the ALB.

- [ ] **Step 7: Commit**

```bash
git add logstream-portal/Makefile logstream-portal/scripts/build_push_portal.sh logstream-portal/infra/outputs.tf
git commit -m "feat(portal): aws bring-up orchestration and cribl seeding"
```

---

### Task 13: Live e2e smoke test

**Files:**
- Create: `logstream-portal/scripts/demo_test_aws.sh`

- [ ] **Step 1: `scripts/demo_test_aws.sh`**

```bash
#!/usr/bin/env bash
# Live e2e: fork a source via the portal → records arrive in real Kinesis → route exists in Cribl → cleanup.
set -euo pipefail
cd "$(dirname "$0")/.."
PORTAL=$(cd infra && terraform output -raw portal_url)
REGION=${AWS_REGION:-us-east-1}
SRC="logging_demo.acct_b__storefront_web.syslog"

JAR=$(mktemp)
curl -fsS -c "$JAR" -X POST "$PORTAL/api/session" -H 'Content-Type: application/json' -d '{"user_id":"dana@app-team"}' >/dev/null
echo "forking $SRC into a new kinesis stream..."
SID=$(curl -fsS -b "$JAR" -X POST "$PORTAL/api/streams" -H 'Content-Type: application/json' \
  -d "{\"name\":\"smoke\",\"type\":\"kinesis\",\"source_fqns\":[\"$SRC\"]}" \
  | python3 -c 'import sys,json; print(json.load(sys.stdin)["id"])')

echo "waiting for records in real Kinesis (logstream-smoke)..."
for _ in $(seq 1 45); do
  N=$(curl -fsS -b "$JAR" "$PORTAL/api/streams" | python3 -c 'import sys,json;s=json.load(sys.stdin)[0];print((s.get("flow") or {}).get("recent_records",0))')
  [ "$N" -gt 0 ] && { echo "PASS: $N records flowing"; break; }
  sleep 4
done
[ "${N:-0}" -gt 0 ] || { echo "FAIL: no records in 180s"; exit 1; }

echo "asserting fork route exists in Cribl..."
curl -fsS "http://localhost:9000/api/v1/m/logging/routes" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; rs=json.load(sys.stdin)['items'][0]['routes']; assert any(r['id']=='fork_${SID}' for r in rs), 'route missing'; print('route fork_${SID} present')"

echo "cleanup..."
curl -fsS -b "$JAR" -X DELETE "$PORTAL/api/streams/$SID" -o /dev/null -w 'delete: %{http_code}\n'
curl -fsS "http://localhost:9000/api/v1/m/logging/routes" -H "Authorization: Bearer $TOKEN" \
  | python3 -c "import sys,json; rs=json.load(sys.stdin)['items'][0]['routes']; assert not any(r['id']=='fork_${SID}' for r in rs), 'route not cleaned'; print('route removed')"
echo "SMOKE PASS"
```

(Requires the SSM 9000 tunnel from Task 12 open and `$TOKEN` exported.)

- [ ] **Step 2: Run it**

```bash
cd logstream-portal && make demo-test-aws
```

Expected: `PASS: N records flowing` → `route fork_<id> present` → `route
removed` → `SMOKE PASS`. Debug from Cribl worker logs / Kinesis metrics on
failure.

- [ ] **Step 3: Commit**

```bash
git add logstream-portal/scripts/demo_test_aws.sh
git commit -m "feat(portal): live aws e2e smoke test"
```

---

### Task 14: Manual walkthrough, README, teardown

**Files:**
- Modify: `logstream-portal/README.md`
- Delete: `logstream-portal/docker-compose.yml`, `logstream-portal/vector/`, `logstream-portal/localstack-init/`

- [ ] **Step 1: Browser walkthrough through the ALB** (`terraform output portal_url`)

1. Login as Dana → Catalog shows prod-ecommerce (522412052544) + prod-platform
   (624627265315), live (non-stale) from UC.
2. Fork orders_api syslog+auth_log → new Kinesis stream → syslog active,
   auth_log pending.
3. Cribl UI (via SSM tunnel to :9000) → logging group routes show `fork_<id>`
   matching only syslog.
4. Admin approves auth_log → route filter now includes auth_log; peek shows
   records tagged `account_id: 522412052544`.
5. Add a storefront source, remove one, delete the stream → routes track each
   change; Kinesis stream list reflects create/delete.

- [ ] **Step 2: Delete the retired local stack**

```bash
git rm -r logstream-portal/docker-compose.yml logstream-portal/vector logstream-portal/localstack-init
```

- [ ] **Step 3: Rewrite `README.md`** — AWS quick start

Cover: prerequisites (3 AWS profiles, Terraform, docker, `.env` with Databricks
creds); `make infra-up` → `make build-push` → `make seed-uc` → SSM tunnel →
`make seed-cribl` → open `portal_url`; personas; the Cribl fork mechanic
(routes+destinations via leader API, commit/deploy); `make demo-test-aws`;
`make infra-down` to stop spend; the PrivateLink topology diagram; the
credential-rotation reminder.

- [ ] **Step 4: Teardown verification**

```bash
cd logstream-portal && make infra-down
# confirm zero remaining billable resources across the three accounts:
for p in default seth-demo-b seth-demo-c; do
  echo "--- $p:"; aws ec2 describe-instances --profile $p --filters Name=instance-state-name,Values=running \
    --query 'Reservations[].Instances[].InstanceId' --output text
done
aws elbv2 describe-load-balancers --query 'LoadBalancers[?starts_with(LoadBalancerName,`logstream`)].LoadBalancerName' --output text
```

Expected: `terraform destroy` completes; no running logstream instances or
ALBs/NLBs remain. (S3 archive bucket + UC schemas persist — pennies; delete
manually if desired.)

- [ ] **Step 5: Commit**

```bash
git add -A logstream-portal
git commit -m "feat(portal): aws readme, retire local compose stack, teardown verified"
```

---

## Self-Review Notes (plan-authoring time)

1. **Spec coverage:** topology + PrivateLink (Tasks 7-8), 3 EC2/instance
   profiles/ECR (Task 9), ALB operator-restricted (Task 10), SSM secrets
   (Task 11), Cribl swap with commit/deploy/rollback (Tasks 1-3), de-LocalStack
   (Task 4), real IDs + UC re-seed (Task 5), Cribl static seed (Task 6), license
   gate Plan A/B (Task 0), live smoke (Task 13), teardown/cost (Tasks 12/14).
   All spec sections map to a task.
2. **Known integration risks flagged inline:** (a) leader↔worker enrollment
   wiring on one host and over PrivateLink (Tasks 9/12 — the explicit debug
   gate); (b) exact Cribl API paths (Task 0 records them; Tasks 2/6 consume
   them; goldens are the contract). These are integration-verified, not
   unit-mockable — expected for live infra.
3. **Type/name consistency:** `Member(account_id, workload, source_name)` and
   `StreamSpec(stream_id, stream_type, resource_ref, members)` are used
   identically in objects.py, service.py, and tests; `fork_<id>` /
   `fork_<id>_dest` id scheme is consistent across objects.py, admin.py
   reconcile, seed routes, and the smoke test; `resource_prefix`
   ("logstream-") is applied in service.py and matched by the IAM policy
   (`logstream-*`) and Kinesis/SQS names in the smoke test.
4. **Placeholder scan:** no TBD/TODO; the one "Plan B" branch (Task 0 outcome)
   is fully specified in Tasks 6/9 rather than deferred.

## Execution

Tasks 0-6 are pure-local TDD (no AWS). Tasks 7-11 are Terraform authored +
`validate`/`plan` only (no spend). Tasks 12-14 apply real infrastructure and
must run with the user in the loop; `make infra-down` returns spend to ~zero.
