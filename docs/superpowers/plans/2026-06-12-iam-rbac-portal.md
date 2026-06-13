# Per-Stream IAM Read Roles & Account-Scoped RBAC Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking. **Operator instruction: implementer subagents run on Opus 4.8 (`model: "opus"`).**

**Goal:** Every stream gets a dedicated cross-account IAM read role with a downloadable access bundle; personas are scoped to their workload account server-side; Peek correctly decodes Cribl's ndjson batches.

**Architecture:** One new injected service (`AccessRoleService`, the `Provisioner` pattern) wired into the existing create/retry/delete lifecycle; two server-side RBAC chokepoints (`scope_tree` in the catalog route, a 403 in `StreamService._resolve`); a pure bundle builder behind an owner-only route; one Terraform statement on the portal instance role, applied to the live stack; live cross-account verification at the end.

**Tech Stack:** Python/FastAPI/pytest, boto3 IAM, React/TS, Terraform. Live AWS (3 accounts, profiles `default`/`seth-demo-b`/`seth-demo-c`).

**Spec:** `docs/superpowers/specs/2026-06-12-iam-rbac-portal-design.md`

**Working notes for the executor:**
- Work in a NEW worktree `feature/iam-rbac` off `main` (use superpowers:using-git-worktrees; `.worktrees/` is the established location). Do NOT touch `.worktrees/cribl-aws` except where Task 7 says so — it holds the live Terraform state.
- Backend venv: `logstream-portal/backend/.venv` exists only in the main checkout; create one in the worktree (`python3 -m venv .venv && pip install -e '.[dev]'`). Pytest from `backend/`. Frontend: `npm ci` in the worktree.
- Current suite on main: **84 backend + 5 frontend** tests. Keep it green; every task ends committed.
- BEFORE Task 4, read `logstream-portal/fixtures/catalog_snapshot.json` and confirm which `account_id` each workload carries (expected: storefront_web/orders_api → `522412052544`, identity_svc/batch_etl → `624627265315`). The RBAC seeds and tests below assume that mapping — if the fixture differs, the fixture is the truth; adjust the constants, not the fixture.
- Live resources: portal EC2 `i-08d23780caa239892` (acct A/default profile), ALB `http://logstream-portal-1037867087.us-east-1.elb.amazonaws.com`, region us-east-1, logging account `337394138208`. SSM (not SSH) for instance commands; the session-manager-plugin lives at `~/bin`.

## File Map

```
logstream-portal/
├── backend/app/
│   ├── aws/
│   │   ├── __init__.py                # NEW
│   │   ├── access_roles.py            # NEW: AccessRoleService + resource_arn_for + role naming
│   │   └── access_bundle.py           # NEW: pure build_access_bundle()
│   ├── streams/
│   │   ├── peek.py                    # MODIFY: ndjson batch decode
│   │   └── service.py                 # MODIFY: role lifecycle + scope check in _resolve
│   ├── catalog/service.py             # MODIFY: scope_tree()
│   ├── routes/
│   │   ├── catalog.py                 # MODIFY: apply scope_tree
│   │   └── streams.py                 # MODIFY: access-bundle route; pass scope into service
│   ├── config.py                      # MODIFY: logging_account_id
│   ├── db.py                          # MODIFY: new columns + idempotent migration + persona scopes
│   └── main.py                        # MODIFY: wire AccessRoleService (iam client)
├── backend/tests/
│   ├── test_peek.py                   # MODIFY: ndjson batch tests
│   ├── test_access_roles.py           # NEW
│   ├── test_access_bundle.py          # NEW (golden: tests/aws_golden/bundle_kinesis.json)
│   ├── test_catalog_service.py        # MODIFY: scope_tree tests
│   ├── test_stream_service.py         # MODIFY: role lifecycle + 403 scope tests
│   ├── test_db.py                     # MODIFY: migration/persona-scope tests
│   ├── test_api.py                    # MODIFY: bundle + RBAC integration tests
│   └── conftest.py                    # MODIFY: FakeIAM / roles fake in services dict
├── frontend/src/
│   ├── types.ts                       # MODIFY: read_role_arn, account_scope
│   ├── components/StreamCard.tsx      # MODIFY: Download access button
│   ├── components/StreamCard.test.tsx # NEW
│   └── App.tsx                        # MODIFY: scope chip in nav
└── infra/modules/compute/             # MODIFY: portal role IAM statement (executor locates the policy file)
```

---

### Task 0: Worktree + Peek ndjson fix

**Files:**
- Modify: `logstream-portal/backend/app/streams/peek.py`
- Test: `logstream-portal/backend/tests/test_peek.py`

- [ ] **Step 1: Create the worktree and baseline**

```bash
cd /Users/skimmel/custom-claude-skills
git worktree add .worktrees/iam-rbac -b feature/iam-rbac
cd .worktrees/iam-rbac/logstream-portal/backend
python3 -m venv .venv && source .venv/bin/activate && pip install -q -e '.[dev]'
pytest -q          # expect: 84 passed
cd ../frontend && npm ci --silent && npm test   # expect: 5 passed
```

- [ ] **Step 2: Write the failing batch-decode tests** — append to `tests/test_peek.py`

```python
NDJSON_BATCH = (
    '{"format":"ndjson","count":2,"size":512}\n'
    '{"_raw":"<25>Jun 12 19:18:36 host1 sshd[1]: line one","account_id":"522412052544",'
    '"workload":"storefront_web","source_name":"syslog"}\n'
    '{"_raw":"<25>Jun 12 19:18:37 host1 sshd[2]: line two","account_id":"522412052544",'
    '"workload":"storefront_web","source_name":"syslog"}'
)


def test_kinesis_peek_unpacks_ndjson_batches():
    kinesis = FakeKinesis([])
    kinesis.get_records = lambda ShardIterator, Limit: {
        "Records": [{"Data": NDJSON_BATCH.encode()}],
        "NextShardIterator": None,
        "MillisBehindLatest": 0,
    }
    out = PeekService(kinesis, FakeSQS([])).peek("kinesis", "s", limit=5)
    assert len(out) == 2
    assert out[0]["workload"] == "storefront_web"
    assert all("format" not in e for e in out)  # header line dropped


def test_batch_with_junk_line_falls_back_to_raw():
    kinesis = FakeKinesis([])
    kinesis.get_records = lambda ShardIterator, Limit: {
        "Records": [{"Data": b'{"format":"ndjson","count":1,"size":9}\nnot-json-line'}],
        "NextShardIterator": None,
        "MillisBehindLatest": 0,
    }
    out = PeekService(kinesis, FakeSQS([])).peek("kinesis", "s", limit=5)
    assert out == [{"raw": "not-json-line"}]


def test_flow_stats_counts_events_not_records():
    kinesis = FakeKinesis([])
    kinesis.get_records = lambda ShardIterator, Limit: {
        "Records": [{"Data": NDJSON_BATCH.encode()}],
        "NextShardIterator": None,
        "MillisBehindLatest": 0,
    }
    stats = PeekService(kinesis, FakeSQS([])).flow_stats("kinesis", "s")
    assert stats == {"recent_records": 2}
```

- [ ] **Step 3: Run to verify fail** — `pytest tests/test_peek.py -q` → the three new tests fail (single-object decode returns the header as one event / `{"raw": ...}` blob).

- [ ] **Step 4: Rewrite the decode in `app/streams/peek.py`**

Replace `_decode` with a batch-aware decoder and use it in both peeks:

```python
def _decode_events(data) -> list:
    """Decode one transport record into individual events.

    Cribl destinations write ndjson batches: a header line like
    {"format":"ndjson","count":N,"size":...} followed by one JSON event per
    line. Plain single-JSON records (and non-JSON lines) are handled too.
    """
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="replace")
    events = []
    for line in str(data).splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            events.append({"raw": line})
            continue
        if isinstance(obj, dict) and obj.get("format") == "ndjson" and "_raw" not in obj:
            continue  # batch header
        events.append(obj)
    return events
```

In `_peek_kinesis`: replace the final return with

```python
        events = []
        for r in records:
            events.extend(_decode_events(r["Data"]))
        return events[-limit:]
```

In `_peek_sqs`: replace the return with

```python
        events = []
        for m in out.get("Messages", []):
            events.extend(_decode_events(m["Body"]))
        return events[-limit:]
```

Delete the old `_decode`. (`flow_stats` needs no change — it counts `peek()` results, which are now events.)

- [ ] **Step 5: Run** — `pytest tests/test_peek.py -q` → all pass (old tests still pass: a plain JSON record decodes to one event; "not-json" body → `{"raw": ...}`). Full suite: `pytest -q` → 87 passed.

- [ ] **Step 6: Commit** — `git add -A logstream-portal/backend && git commit -m "fix(portal): peek decodes cribl ndjson batches into individual events"`

---

### Task 1: AccessRoleService

**Files:**
- Create: `logstream-portal/backend/app/aws/__init__.py` (empty)
- Create: `logstream-portal/backend/app/aws/access_roles.py`
- Test: `logstream-portal/backend/tests/test_access_roles.py`

- [ ] **Step 1: Write the failing tests** — `tests/test_access_roles.py`

```python
import json

import pytest

from app.aws.access_roles import (
    AccessRoleError, AccessRoleService, resource_arn_for, role_name_for,
)


class _NoSuchEntity(Exception):
    pass


class _AlreadyExists(Exception):
    pass


class _Exceptions:
    NoSuchEntityException = _NoSuchEntity
    EntityAlreadyExistsException = _AlreadyExists


class FakeIAM:
    exceptions = _Exceptions()

    def __init__(self, fail=False):
        self.roles, self.policies, self.fail = {}, {}, fail
        self.deleted_roles, self.deleted_policies = [], []

    def create_role(self, RoleName, Path, AssumeRolePolicyDocument, Description):
        if self.fail:
            raise RuntimeError("AccessDenied")
        if RoleName in self.roles:
            raise _AlreadyExists()
        arn = f"arn:aws:iam::337394138208:role{Path}{RoleName}"
        self.roles[RoleName] = {"Arn": arn, "Trust": json.loads(AssumeRolePolicyDocument)}
        return {"Role": {"Arn": arn}}

    def get_role(self, RoleName):
        if RoleName not in self.roles:
            raise _NoSuchEntity()
        return {"Role": {"Arn": self.roles[RoleName]["Arn"]}}

    def put_role_policy(self, RoleName, PolicyName, PolicyDocument):
        self.policies[(RoleName, PolicyName)] = json.loads(PolicyDocument)

    def delete_role_policy(self, RoleName, PolicyName):
        if (RoleName, PolicyName) not in self.policies:
            raise _NoSuchEntity()
        self.deleted_policies.append((RoleName, PolicyName))
        del self.policies[(RoleName, PolicyName)]

    def delete_role(self, RoleName):
        if RoleName not in self.roles:
            raise _NoSuchEntity()
        self.deleted_roles.append(RoleName)
        del self.roles[RoleName]


def test_role_name_truncates_and_is_unique_per_stream():
    assert role_name_for(7, "team-a-orders") == "logstream-read-7-team-a-orders"
    long = role_name_for(123, "x" * 80)
    assert long.startswith("logstream-read-123-")
    assert len(long) <= 64


def test_resource_arn_for_kinesis_and_sqs():
    assert resource_arn_for("kinesis", "logstream-x", "us-east-1", "337394138208") == \
        "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-x"
    assert resource_arn_for(
        "sqs", "https://sqs.us-east-1.amazonaws.com/337394138208/logstream-q",
        "us-east-1", "337394138208",
    ) == "arn:aws:sqs:us-east-1:337394138208:logstream-q"


def test_create_builds_trust_and_scoped_policy():
    iam = FakeIAM()
    svc = AccessRoleService(iam)
    out = svc.create(7, "team-a-orders", "kinesis",
                     "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-team-a-orders",
                     "522412052544")
    assert out["role_name"] == "logstream-read-7-team-a-orders"
    assert out["role_arn"].endswith("/logstream/logstream-read-7-team-a-orders")
    trust = out["trust_policy"]["Statement"][0]
    assert trust["Principal"]["AWS"] == "arn:aws:iam::522412052544:root"
    perm = iam.policies[("logstream-read-7-team-a-orders", "read-access")]
    assert perm["Statement"][0]["Resource"] == \
        "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-team-a-orders"
    assert "kinesis:GetRecords" in perm["Statement"][0]["Action"]


def test_create_is_idempotent_when_role_exists():
    iam = FakeIAM()
    svc = AccessRoleService(iam)
    first = svc.create(7, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
    again = svc.create(7, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
    assert first["role_arn"] == again["role_arn"]


def test_sqs_policy_actions():
    iam = FakeIAM()
    AccessRoleService(iam).create(9, "q", "sqs", "arn:aws:sqs:us-east-1:1:logstream-q", "624627265315")
    perm = iam.policies[("logstream-read-9-q", "read-access")]
    assert set(perm["Statement"][0]["Action"]) == \
        {"sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"}


def test_delete_is_idempotent():
    iam = FakeIAM()
    svc = AccessRoleService(iam)
    svc.create(7, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
    svc.delete("logstream-read-7-s")
    svc.delete("logstream-read-7-s")  # second call must not raise
    assert iam.deleted_roles == ["logstream-read-7-s"]


def test_failures_wrap_in_access_role_error():
    with pytest.raises(AccessRoleError):
        AccessRoleService(FakeIAM(fail=True)).create(
            1, "s", "kinesis", "arn:aws:kinesis:us-east-1:1:stream/s", "522412052544")
```

- [ ] **Step 2: Run to verify fail** — `pytest tests/test_access_roles.py -q` → `ModuleNotFoundError: No module named 'app.aws'`

- [ ] **Step 3: Write `app/aws/access_roles.py`**

```python
"""Per-stream cross-account IAM read roles.

Each live stream gets a dedicated role in the logging account that the
owner's workload account may assume to read exactly that stream/queue.
Role lifecycle is 1:1 with the stream (created after provisioning, deleted
with the stream); roles are immutable once created.
"""
import json

ROLE_PATH = "/logstream/"
POLICY_NAME = "read-access"

KINESIS_ACTIONS = [
    "kinesis:GetRecords", "kinesis:GetShardIterator",
    "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards",
]
# Consumers own their queue's consumption; destructive read is intended.
SQS_ACTIONS = ["sqs:ReceiveMessage", "sqs:GetQueueAttributes", "sqs:DeleteMessage"]


class AccessRoleError(RuntimeError):
    """The IAM role could not be created or deleted."""


def role_name_for(stream_id: int, stream_name: str) -> str:
    # id prefix guarantees uniqueness; truncation keeps IAM's 64-char limit.
    return f"logstream-read-{stream_id}-{stream_name[:40]}"


def resource_arn_for(stream_type: str, resource_ref: str, region: str, account_id: str) -> str:
    if stream_type == "kinesis":
        return f"arn:aws:kinesis:{region}:{account_id}:stream/{resource_ref}"
    queue_name = resource_ref.rstrip("/").split("/")[-1]
    return f"arn:aws:sqs:{region}:{account_id}:{queue_name}"


def trust_policy_for(consumer_account_id: str) -> dict:
    return {
        "Version": "2012-10-17",
        "Statement": [{
            "Effect": "Allow",
            "Principal": {"AWS": f"arn:aws:iam::{consumer_account_id}:root"},
            "Action": "sts:AssumeRole",
        }],
    }


def permission_policy_for(stream_type: str, resource_arn: str) -> dict:
    actions = KINESIS_ACTIONS if stream_type == "kinesis" else SQS_ACTIONS
    return {
        "Version": "2012-10-17",
        "Statement": [{"Effect": "Allow", "Action": actions, "Resource": resource_arn}],
    }


class AccessRoleService:
    def __init__(self, iam_client):
        self._iam = iam_client

    def create(self, stream_id, stream_name, stream_type, resource_arn,
               consumer_account_id) -> dict:
        name = role_name_for(stream_id, stream_name)
        trust = trust_policy_for(consumer_account_id)
        permission = permission_policy_for(stream_type, resource_arn)
        try:
            try:
                resp = self._iam.create_role(
                    RoleName=name, Path=ROLE_PATH,
                    AssumeRolePolicyDocument=json.dumps(trust),
                    Description=f"logstream-portal read access for stream {stream_id}",
                )
                role_arn = resp["Role"]["Arn"]
            except self._iam.exceptions.EntityAlreadyExistsException:
                role_arn = self._iam.get_role(RoleName=name)["Role"]["Arn"]
            self._iam.put_role_policy(
                RoleName=name, PolicyName=POLICY_NAME,
                PolicyDocument=json.dumps(permission),
            )
        except AccessRoleError:
            raise
        except Exception as exc:
            raise AccessRoleError(str(exc)) from exc
        return {"role_name": name, "role_arn": role_arn,
                "trust_policy": trust, "permission_policy": permission}

    def delete(self, role_name: str) -> None:
        try:
            try:
                self._iam.delete_role_policy(RoleName=role_name, PolicyName=POLICY_NAME)
            except self._iam.exceptions.NoSuchEntityException:
                pass
            try:
                self._iam.delete_role(RoleName=role_name)
            except self._iam.exceptions.NoSuchEntityException:
                pass
        except Exception as exc:
            raise AccessRoleError(str(exc)) from exc
```

- [ ] **Step 4: Run** — `pytest tests/test_access_roles.py -q` → `7 passed`. Full suite green.

- [ ] **Step 5: Commit** — `git add -A logstream-portal/backend && git commit -m "feat(portal): access role service minting per-stream cross-account read roles"`

---

### Task 2: DB migration + persona scopes + StreamService lifecycle

**Files:**
- Modify: `logstream-portal/backend/app/db.py`
- Modify: `logstream-portal/backend/app/config.py`
- Modify: `logstream-portal/backend/app/streams/service.py`
- Modify: `logstream-portal/backend/app/main.py`
- Modify: `logstream-portal/backend/tests/conftest.py`
- Test: `logstream-portal/backend/tests/test_db.py`, `tests/test_stream_service.py`

- [ ] **Step 1: Failing tests.** Append to `tests/test_db.py`:

```python
def test_new_columns_and_scopes_migrated(conn):
    stream_cols = [r["name"] for r in conn.execute("PRAGMA table_info(streams)")]
    assert "read_role_arn" in stream_cols and "consumer_account_id" in stream_cols
    user_cols = [r["name"] for r in conn.execute("PRAGMA table_info(users)")]
    assert "account_scope" in user_cols
    scopes = {r["id"]: r["account_scope"] for r in conn.execute("SELECT id, account_scope FROM users")}
    assert scopes["dana@app-team"] == "522412052544"
    assert scopes["raj@data-sci"] == "624627265315"
    assert scopes["admin@platform"] is None


def test_migration_is_idempotent_on_existing_db(tmp_path):
    # Simulate the live volume: a DB created by the OLD schema, then migrated.
    import sqlite3
    path = str(tmp_path / "old.db")
    old = sqlite3.connect(path)
    old.execute("CREATE TABLE users (id TEXT PRIMARY KEY, display_name TEXT NOT NULL,"
                " team TEXT NOT NULL, role TEXT NOT NULL)")
    old.execute("INSERT INTO users VALUES ('dana@app-team','Dana','team-a','consumer')")
    old.commit(); old.close()
    conn2 = get_db(path)
    init_db(conn2)
    init_db(conn2)  # twice: still fine
    row = conn2.execute("SELECT account_scope FROM users WHERE id='dana@app-team'").fetchone()
    assert row["account_scope"] == "522412052544"
```

Append to `tests/test_stream_service.py` (the `env` fixture gains a roles fake — see Step 4 for the fixture change; tests reference `roles`):

```python
def test_create_stream_mints_access_role(env):
    svc, provisioner, pipeline, roles = env
    stream = svc.create_stream(DANA, "team-a-logs", "kinesis", [SYSLOG])
    assert stream["read_role_arn"].endswith("logstream-read-1-team-a-logs")
    assert stream["consumer_account_id"] == "522412052544"
    assert roles.created[0]["consumer_account_id"] == "522412052544"
    assert "kinesis" in roles.created[0]["resource_arn"]


def test_admin_streams_default_consumer_to_logging_account(env):
    svc, _, _, roles = env
    svc.create_stream(ADMIN, "admin-s", "kinesis", [SYSLOG])
    assert roles.created[0]["consumer_account_id"] == "337394138208"


def test_role_failure_marks_error_and_retry_completes(env):
    svc, provisioner, _, roles = env
    roles.fail = True
    stream = svc.create_stream(DANA, "flaky", "kinesis", [SYSLOG])
    assert stream["status"] == "error"
    assert stream["read_role_arn"] is None
    roles.fail = False
    recovered = svc.retry(DANA, stream["id"])
    assert recovered["status"] == "live"
    assert recovered["read_role_arn"] is not None


def test_delete_stream_deletes_role(env):
    svc, _, _, roles = env
    stream = svc.create_stream(DANA, "gone", "kinesis", [SYSLOG])
    svc.delete_stream(DANA, stream["id"])
    assert roles.deleted == ["logstream-read-1-gone"]
```

- [ ] **Step 2: Run to verify fail** — `pytest tests/test_db.py tests/test_stream_service.py -q` → failures (missing columns; env fixture shape).

- [ ] **Step 3: Implement db.py + config.py**

`db.py` — extend `PERSONAS` tuples with a scope, add migration helper, and update `init_db`:

```python
PERSONAS = [
    ("admin@platform", "Alex Romero", "platform", "admin", None),
    ("dana@app-team", "Dana Whitfield", "team-a", "consumer", "522412052544"),
    ("raj@data-sci", "Raj Patel", "data-sci", "consumer", "624627265315"),
]


def _ensure_column(conn, table: str, column: str, decl: str) -> None:
    cols = [r["name"] for r in conn.execute(f"PRAGMA table_info({table})")]
    if column not in cols:
        conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {decl}")


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.execute("PRAGMA foreign_keys = ON")  # executescript can reset session state
    # Idempotent migrations for databases created by earlier schema versions
    # (the live portal volume persists across deploys).
    _ensure_column(conn, "streams", "read_role_arn", "TEXT")
    _ensure_column(conn, "streams", "consumer_account_id", "TEXT")
    _ensure_column(conn, "users", "account_scope", "TEXT")
    conn.executemany(
        "INSERT OR IGNORE INTO users (id, display_name, team, role, account_scope)"
        " VALUES (?, ?, ?, ?, ?)",
        PERSONAS,
    )
    for user_id, _, _, _, scope in PERSONAS:
        conn.execute("UPDATE users SET account_scope = ? WHERE id = ?", (scope, user_id))
```

Also add the two columns to the `streams` CREATE TABLE in `SCHEMA` (fresh DBs) and `account_scope TEXT` to `users` — keep `_ensure_column` for pre-existing DBs. **Check `test_init_db_seeds_personas`:** it indexes `p[0]`/`p[3]` — still valid with 5-tuples; fix only if it fails.

`config.py` — add field `logging_account_id: str` and in `load_settings()`:
`logging_account_id=env("LOGGING_ACCOUNT_ID", "337394138208"),`

- [ ] **Step 4: Wire the lifecycle in `service.py` + fakes**

`StreamService.__init__` gains an `access_roles` param (after `provisioner`): store as `self._roles`. In `create_stream`, after the resource_ref UPDATE and before `_insert_members`:

```python
        try:
            self._mint_role(stream_id, name, stream_type, ref, user)
        except AccessRoleError as exc:
            self._conn.execute(
                "UPDATE streams SET status = 'error', last_error = ? WHERE id = ?",
                (str(exc), stream_id),
            )
            return self.get_stream(user, stream_id)
```

Add the helper + import (`from app.aws.access_roles import AccessRoleError, resource_arn_for, role_name_for`):

```python
    def _mint_role(self, stream_id, name, stream_type, resource_ref, user) -> None:
        consumer = user.get("account_scope") or self._settings.logging_account_id
        arn = resource_arn_for(stream_type, resource_ref,
                               self._settings.aws_region, self._settings.logging_account_id)
        created = self._roles.create(stream_id, name, stream_type, arn, consumer)
        self._conn.execute(
            "UPDATE streams SET read_role_arn = ?, consumer_account_id = ? WHERE id = ?",
            (created["role_arn"], consumer, stream_id),
        )
```

In `retry`, after the resource_ref branch and before `_reapply_or_flag`:

```python
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
```

In `delete_stream`, after the Cribl reapply try/except and before the provisioner teardown:

```python
        if stream["read_role_arn"]:
            try:
                self._roles.delete(role_name_for(stream_id, stream["name"]))
            except AccessRoleError as exc:
                self._conn.execute(
                    "UPDATE streams SET last_error = ? WHERE id = ?", (str(exc), stream_id))
```

`main.py`: build the IAM client next to kinesis/sqs (`boto3.client("iam", region_name=settings.aws_region)` — IAM is global; no endpoint), `app.state.access_roles = services.get("access_roles") or AccessRoleService(iam)`, pass into `StreamService(...)`. Note: when `services` overrides provisioner/peek, also expect `services["access_roles"]`.

`conftest.py` + `test_stream_service.py` fixtures — add:

```python
class FakeAccessRoles:
    def __init__(self):
        self.created, self.deleted, self.fail = [], [], False

    def create(self, stream_id, stream_name, stream_type, resource_arn, consumer_account_id):
        if self.fail:
            from app.aws.access_roles import AccessRoleError
            raise AccessRoleError("iam boom")
        rec = {"stream_id": stream_id, "stream_name": stream_name,
               "stream_type": stream_type, "resource_arn": resource_arn,
               "consumer_account_id": consumer_account_id}
        self.created.append(rec)
        from app.aws.access_roles import role_name_for
        return {"role_name": role_name_for(stream_id, stream_name),
                "role_arn": f"arn:aws:iam::337394138208:role/logstream/{role_name_for(stream_id, stream_name)}",
                "trust_policy": {}, "permission_policy": {}}

    def delete(self, role_name):
        self.deleted.append(role_name)
```

`env` fixture returns `(svc, provisioner, pipeline, roles)` — update ALL existing unpackings in `test_stream_service.py` (mechanical: `svc, provisioner, vector = env` style lines gain a fourth name). Conftest `fakes` dict gains `"access_roles": FakeAccessRoles()`. Settings constructions gain `logging_account_id="337394138208"`. ADMIN constant: ensure `{"id": "admin@platform", "role": "admin"}` exists; user dicts in tests gain `"account_scope"` keys (`DANA`: `"522412052544"`, `RAJ`: `"624627265315"`, `ADMIN`: `None`).

- [ ] **Step 5: Run full suite** — `pytest -q` → all pass (87 + new ≈ 93).
- [ ] **Step 6: Commit** — `git add -A logstream-portal/backend && git commit -m "feat(portal): per-stream access role lifecycle with idempotent db migration"`

---

### Task 3: Access bundle builder + route + Download button

**Files:**
- Create: `logstream-portal/backend/app/aws/access_bundle.py`
- Create: `logstream-portal/backend/tests/aws_golden/bundle_kinesis.json`
- Test: `logstream-portal/backend/tests/test_access_bundle.py`; append to `tests/test_api.py`
- Modify: `logstream-portal/backend/app/routes/streams.py`
- Modify: `logstream-portal/frontend/src/types.ts`, `src/components/StreamCard.tsx`
- Create: `logstream-portal/frontend/src/components/StreamCard.test.tsx`

- [ ] **Step 1: Golden** — `tests/aws_golden/bundle_kinesis.json`:

```json
{
  "stream": {
    "name": "team-a-orders",
    "type": "kinesis",
    "resource": "logstream-team-a-orders",
    "resource_arn": "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-team-a-orders",
    "region": "us-east-1"
  },
  "role_arn": "arn:aws:iam::337394138208:role/logstream/logstream-read-7-team-a-orders",
  "consumer_account_id": "522412052544",
  "trust_policy": {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Principal": {"AWS": "arn:aws:iam::522412052544:root"},
        "Action": "sts:AssumeRole"
      }
    ]
  },
  "permission_policy": {
    "Version": "2012-10-17",
    "Statement": [
      {
        "Effect": "Allow",
        "Action": ["kinesis:GetRecords", "kinesis:GetShardIterator", "kinesis:DescribeStream", "kinesis:DescribeStreamSummary", "kinesis:ListShards"],
        "Resource": "arn:aws:kinesis:us-east-1:337394138208:stream/logstream-team-a-orders"
      }
    ]
  },
  "usage": {
    "assume_role": "aws sts assume-role --role-arn arn:aws:iam::337394138208:role/logstream/logstream-read-7-team-a-orders --role-session-name read-team-a-orders",
    "read": "aws kinesis get-shard-iterator --stream-name logstream-team-a-orders --shard-id shardId-000000000000 --shard-iterator-type LATEST --query ShardIterator --output text | xargs -I{} aws kinesis get-records --shard-iterator {}"
  }
}
```

- [ ] **Step 2: Failing tests** — `tests/test_access_bundle.py`:

```python
import json
from pathlib import Path

from app.aws.access_bundle import build_access_bundle

GOLDEN = Path(__file__).parent / "aws_golden"


def _stream(**over):
    base = {
        "id": 7, "name": "team-a-orders", "type": "kinesis",
        "resource_ref": "logstream-team-a-orders",
        "read_role_arn": "arn:aws:iam::337394138208:role/logstream/logstream-read-7-team-a-orders",
        "consumer_account_id": "522412052544",
    }
    base.update(over)
    return base


def test_kinesis_bundle_matches_golden():
    out = build_access_bundle(_stream(), region="us-east-1", logging_account_id="337394138208")
    assert out == json.loads((GOLDEN / "bundle_kinesis.json").read_text())


def test_sqs_bundle_uses_receive_message():
    out = build_access_bundle(
        _stream(type="sqs",
                resource_ref="https://sqs.us-east-1.amazonaws.com/337394138208/logstream-q"),
        region="us-east-1", logging_account_id="337394138208")
    assert "sqs receive-message" in out["usage"]["read"]
    assert out["permission_policy"]["Statement"][0]["Resource"].startswith("arn:aws:sqs:")
```

Append to `tests/test_api.py`:

```python
def test_access_bundle_owner_only_and_404_without_role(client, fakes):
    login(client)
    created = client.post(
        "/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    bundle = client.get(f"/api/streams/{created['id']}/access-bundle")
    assert bundle.status_code == 200
    body = bundle.json()
    assert body["role_arn"] == created["read_role_arn"]
    assert "assume_role" in body["usage"]
    login(client, "raj@data-sci")
    assert client.get(f"/api/streams/{created['id']}/access-bundle").status_code == 403
```

(The no-role 404 path is covered in stream-service tests via the error state; an API-level check: set `fakes["access_roles"].fail = True`, create a stream → error status → bundle returns 404.)

- [ ] **Step 3: Implement.** `app/aws/access_bundle.py`:

```python
"""Build the downloadable access bundle for a stream's read role.

Pure function over stored stream state — nothing secret, no credentials;
policies are regenerated with the same builders that created the role.
"""
from app.aws.access_roles import (
    permission_policy_for, resource_arn_for, trust_policy_for,
)


def build_access_bundle(stream: dict, region: str, logging_account_id: str) -> dict:
    arn = resource_arn_for(stream["type"], stream["resource_ref"], region, logging_account_id)
    if stream["type"] == "kinesis":
        read = (
            f"aws kinesis get-shard-iterator --stream-name {stream['resource_ref']}"
            " --shard-id shardId-000000000000 --shard-iterator-type LATEST"
            " --query ShardIterator --output text"
            " | xargs -I{} aws kinesis get-records --shard-iterator {}"
        )
    else:
        read = f"aws sqs receive-message --queue-url {stream['resource_ref']}"
    return {
        "stream": {
            "name": stream["name"], "type": stream["type"],
            "resource": stream["resource_ref"], "resource_arn": arn, "region": region,
        },
        "role_arn": stream["read_role_arn"],
        "consumer_account_id": stream["consumer_account_id"],
        "trust_policy": trust_policy_for(stream["consumer_account_id"]),
        "permission_policy": permission_policy_for(stream["type"], arn),
        "usage": {
            "assume_role": (
                f"aws sts assume-role --role-arn {stream['read_role_arn']}"
                f" --role-session-name read-{stream['name']}"
            ),
            "read": read,
        },
    }
```

Route in `routes/streams.py`:

```python
from app.aws.access_bundle import build_access_bundle

@router.get("/api/streams/{stream_id}/access-bundle")
def access_bundle(stream_id: int, request: Request, user: dict = Depends(current_user)) -> dict:
    stream = _call(lambda: request.app.state.streams.get_stream(user, stream_id))
    if not stream.get("read_role_arn"):
        raise HTTPException(404, "this stream has no access role (it may be in error state)")
    settings = request.app.state.settings
    return build_access_bundle(stream, settings.aws_region, settings.logging_account_id)
```

- [ ] **Step 4: Frontend.** `types.ts` `Stream` gains `read_role_arn?: string | null; consumer_account_id?: string | null`. `StreamCard.tsx` — add in the `.row` of buttons (after Peek):

```tsx
        <button
          onClick={downloadAccess}
          disabled={stream.status !== 'live' || !stream.read_role_arn}
        >
          Download access
        </button>
```

and the handler inside the component:

```tsx
  async function downloadAccess() {
    try {
      const bundle = await api<unknown>(`/api/streams/${stream.id}/access-bundle`)
      const blob = new Blob([JSON.stringify(bundle, null, 2)], { type: 'application/json' })
      const url = URL.createObjectURL(blob)
      const a = document.createElement('a')
      a.href = url
      a.download = `${stream.name}-access.json`
      a.click()
      URL.revokeObjectURL(url)
    } catch (e) {
      setError(String((e as Error).message ?? e))
    }
  }
```

`StreamCard.test.tsx`:

```tsx
import { render, screen } from '@testing-library/react'
import type { Stream } from '../types'
import StreamCard from './StreamCard'

const base: Stream = {
  id: 1, name: 's1', type: 'kinesis', status: 'live', sources: [],
  read_role_arn: 'arn:aws:iam::337394138208:role/logstream/logstream-read-1-s1',
}

it('enables Download access for live streams with a role', () => {
  render(<StreamCard stream={base} onChanged={() => {}} onAddSources={() => {}} onPeek={() => {}} />)
  expect(screen.getByText('Download access')).toBeEnabled()
})

it('disables Download access when no role exists', () => {
  render(<StreamCard stream={{ ...base, read_role_arn: null }} onChanged={() => {}}
    onAddSources={() => {}} onPeek={() => {}} />)
  expect(screen.getByText('Download access')).toBeDisabled()
})
```

- [ ] **Step 5: Run** — backend `pytest -q` all pass; frontend `npm run build && npm test` (7 tests).
- [ ] **Step 6: Commit** — `git add -A logstream-portal && git commit -m "feat(portal): downloadable access bundle with per-stream read role"`

---

### Task 4: Account-scoped RBAC

**Files:**
- Modify: `logstream-portal/backend/app/catalog/service.py` (add `scope_tree`)
- Modify: `logstream-portal/backend/app/routes/catalog.py`
- Modify: `logstream-portal/backend/app/streams/service.py` (`_resolve` scope check)
- Test: `tests/test_catalog_service.py`, `tests/test_stream_service.py`, `tests/test_api.py`

(First: confirm fixture `account_id`s per the working note. Constants below assume dana↔522412052544 owns storefront_web/orders_api and raj↔624627265315 owns identity_svc/batch_etl. Add a cross-scope FQN constant to conftest: `IDENTITY_AUTH = "logging_demo.<identity_svc schema>.auth_log"` using the actual schema name from the fixture.)

- [ ] **Step 1: Failing tests.** `tests/test_catalog_service.py`:

```python
from app.catalog.service import scope_tree


def test_scope_tree_filters_to_matching_account():
    tree = json.loads(FIXTURE.read_text())
    scoped = scope_tree(tree, "522412052544")
    assert [a["account_id"] for a in scoped["accounts"]] == ["522412052544"]
    assert "as_of" in scoped


def test_scope_tree_none_returns_everything_unchanged():
    tree = json.loads(FIXTURE.read_text())
    assert scope_tree(tree, None) == tree
```

`tests/test_stream_service.py`:

```python
def test_fork_outside_scope_is_403(env):
    svc, _, _, _ = env
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(DANA, "sneaky", "kinesis", [IDENTITY_AUTH])
    assert err.value.status_code == 403


def test_admin_can_fork_any_account(env):
    svc, _, _, _ = env
    stream = svc.create_stream(ADMIN, "admin-any", "kinesis", [IDENTITY_AUTH])
    assert stream["status"] == "live"
```

`tests/test_api.py`:

```python
def test_catalog_is_scoped_per_persona(client):
    login(client)  # dana
    accounts = [a["account_id"] for a in client.get("/api/catalog").json()["accounts"]]
    assert accounts == ["522412052544"]
    login(client, "raj@data-sci")
    accounts = [a["account_id"] for a in client.get("/api/catalog").json()["accounts"]]
    assert accounts == ["624627265315"]
    login(client, "admin@platform")
    assert len(client.get("/api/catalog").json()["accounts"]) == 2


def test_cross_scope_fork_rejected_via_api(client):
    login(client)  # dana
    resp = client.post(
        "/api/streams", json={"name": "sneaky", "type": "kinesis", "source_fqns": [IDENTITY_AUTH]}
    )
    assert resp.status_code == 403
```

- [ ] **Step 2: Run to verify fail.**

- [ ] **Step 3: Implement.** `catalog/service.py` module function:

```python
def scope_tree(tree: dict, account_scope: str | None) -> dict:
    """Return the tree filtered to one account; None scope = unscoped (admin)."""
    if account_scope is None:
        return tree
    return {
        **tree,
        "accounts": [a for a in tree["accounts"] if a["account_id"] == account_scope],
    }
```

`routes/catalog.py` — after `get_tree()`: `tree = scope_tree(tree, user.get("account_scope"))` (import alongside `annotate`).

`streams/service.py` `_resolve` — signature becomes `_resolve(self, source_fqns, account_scope)`; after the unknown-source 404 check:

```python
            if account_scope is not None and found["account_id"] != account_scope:
                raise StreamServiceError(
                    f"source {fqn} is outside your account scope", 403)
```

Callers pass `user.get("account_scope")` (create_stream, add_sources).

- [ ] **Step 4: Run full suites** (backend + frontend) — green.
- [ ] **Step 5: Commit** — `git add -A logstream-portal && git commit -m "feat(portal): account-scoped rbac enforced in catalog and fork paths"`

---

### Task 5: Scope label in UI

**Files:**
- Modify: `logstream-portal/frontend/src/types.ts` (`User` gains `account_scope?: string | null`)
- Modify: `logstream-portal/frontend/src/App.tsx`

- [ ] **Step 1:** In the topnav, after the display name span:

```tsx
          {user.account_scope && (
            <span className="chip">scope: {user.account_scope}</span>
          )}
```

- [ ] **Step 2:** `npm run build && npm test` → green. Commit: `git add -A logstream-portal/frontend && git commit -m "feat(portal): show account scope in nav"`

---

### Task 6: Terraform — portal IAM permission (live stack)

**Files:**
- Modify: `logstream-portal/infra/modules/compute/` — the file defining the portal instance role's policy (locate with `grep -rl "kinesis:" logstream-portal/infra/modules/compute/`).

- [ ] **Step 1:** Add to the portal role policy document (HCL, matching the file's existing style):

```hcl
statement {
  sid    = "MintStreamReadRoles"
  effect = "Allow"
  actions = [
    "iam:CreateRole", "iam:DeleteRole", "iam:GetRole",
    "iam:PutRolePolicy", "iam:DeleteRolePolicy",
  ]
  resources = ["arn:aws:iam::337394138208:role/logstream/logstream-read-*"]
}
```

(If the file uses inline JSON policies instead of `data "aws_iam_policy_document"`, add the equivalent JSON statement.)

- [ ] **Step 2: Apply against the LIVE state.** The live Terraform state lives in `.worktrees/cribl-aws/logstream-portal/infra/`. Copy state + vars into this worktree's infra dir, then plan/apply from here (this worktree's infra has the new statement):

```bash
cd .worktrees/iam-rbac/logstream-portal/infra
cp ../../../cribl-aws/logstream-portal/infra/terraform.tfstate .
cp ../../../cribl-aws/logstream-portal/infra/terraform.tfvars .
terraform init
terraform plan    # MUST show only the IAM policy change (1 to change); if anything
                  # else shows, STOP and report — do not apply a surprise diff.
terraform apply -auto-approve
# Move the canonical state forward: copy terraform.tfstate BACK to the cribl-aws
# worktree so there is exactly one live-state location until branches merge.
cp terraform.tfstate ../../../cribl-aws/logstream-portal/infra/terraform.tfstate
```

- [ ] **Step 3:** Verify: `aws iam simulate-principal-policy` is overkill — instead confirm with `aws iam get-role-policy --role-name <portal role name from terraform output/state> ...` or rely on Task 8's live test. Commit the .tf change: `git add -A logstream-portal/infra && git commit -m "feat(infra): allow portal to mint scoped logstream-read roles"`

---

### Task 7: Deploy the new portal build (live)

- [ ] **Step 1:** Build & push from the worktree root: `cd .worktrees/iam-rbac/logstream-portal && ./scripts/build_push_portal.sh` (uses ECR URL from the infra outputs — run after Task 6 so state is local; if `terraform output` fails, get the repo URL with `aws ecr describe-repositories --repository-names logstream-portal --query 'repositories[0].repositoryUri' --output text`).
- [ ] **Step 2:** Restart the portal container on `i-08d23780caa239892` via SSM. First recover the original `docker run` flags: `aws ec2 describe-instance-attribute --instance-id i-08d23780caa239892 --attribute userData --query 'UserData.Value' --output text | base64 -d | grep -A5 'docker run.*portal'`. Then send an SSM shell script that does: ECR login → `docker pull <repo>:latest` → `docker rm -f portal` → re-run with the exact same flags. Confirm `curl -s <ALB>/api/personas` returns 3 personas and the response for dana now includes `account_scope`.
- [ ] **Step 3:** The live SQLite volume persists — `init_db`'s migration runs at startup and must have added the new columns. Verify via the API: `POST /api/session` as dana → response contains `"account_scope": "522412052544"`. If it doesn't, check portal logs via SSM (`docker logs portal --tail 50`).

---

### Task 8: Live verification + docs

- [ ] **Step 1: Live walkthrough (scripted curls from this machine, cookie jar per persona):**
  1. dana: catalog shows ONLY account 522412052544; raj: only 624627265315; admin: both.
  2. dana forks `storefront_web/syslog` → stream live, `read_role_arn` populated; `aws iam get-role --role-name logstream-read-<id>-<name>` shows path `/logstream/`, trust principal `arn:aws:iam::522412052544:root`.
  3. dana attempts an identity_svc FQN via raw POST → 403.
  4. Download bundle (`GET .../access-bundle`) → run its own snippets with the consumer account's credentials:
     `aws sts assume-role --profile seth-demo-b --role-arn <bundle.role_arn> --role-session-name read-test` → export the temp creds → `aws kinesis get-records ...` returns records. **This is the acceptance gate: a workload account reading its stream through the minted role.**
  5. Peek in the API now returns individual tagged events (no `{"raw": ...}`, no ndjson header) — confirms Task 0 against live data.
  6. Delete the test stream → `aws iam get-role` now fails with NoSuchEntity (role cleaned up).
- [ ] **Step 2: Docs.** README: add a "Consuming your stream" section (Download access → assume role → read; note SQS read is destructive by design). Spec: append an as-built amendments section if anything deviated. Update `docs/architecture.svg` only if the user asks (the diagram already shows consumers).
- [ ] **Step 3: Commit docs; run both full suites one final time; report.**

---

## Self-Review Notes

1. **Spec coverage:** Peek fix (T0), AccessRoleService incl. naming/ARN helpers (T1), lifecycle + migration + persona scopes (T2), bundle + route + button (T3), RBAC chokepoints + session payload (T4 — payload is automatic since routes return `dict(row)`), scope chip (T5), infra statement (T6), deploy (T7), live verification incl. the cross-account read and role-deletion check, plus docs (T8). Error-handling table maps to T2 (create/delete failures), T3 (404), T4 (403).
2. **Placeholders:** Task 6 names the file by grep (executor locates it — the exact filename exists only in the repo; instruction is concrete). Task 7's docker-run recovery is a concrete command sequence. No TBDs.
3. **Type consistency:** `AccessRoleService.create(stream_id, stream_name, stream_type, resource_arn, consumer_account_id)` used identically in service.py, fakes, and tests; `role_name_for`/`resource_arn_for`/`trust_policy_for`/`permission_policy_for` shared by bundle builder so golden bundles match role creation; `env` fixture 4-tuple updated everywhere in T2.
4. **Live-stack safety:** Terraform plan-gate ("only the IAM change or STOP"), state copied back to the canonical worktree, migration is idempotent against the persisted volume, and the executor never works inside `.worktrees/cribl-aws` except the state copy.

## Execution

Subagent-driven, fresh implementer per task with **`model: "opus"`** (operator instruction), spec review then code-quality review per task as established. Tasks 6–8 touch live AWS from this machine's profiles; they are sequential and must not run in parallel with anything.
