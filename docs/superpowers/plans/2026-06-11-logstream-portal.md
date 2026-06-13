# LogStream Portal Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A fully working local demo of a self-service log onboarding portal: browse a Unity Catalog inventory of Vector-collected log sources, fork selected sources into your own Kinesis/SQS stream (LocalStack), with admin approval gating for sensitive sources.

**Architecture:** Two Vector "agent" containers (simulating AWS accounts A/B) ship tagged demo logs to a central Vector aggregator. The portal (FastAPI + SQLite) reads the source inventory from Databricks Unity Catalog (read-only, disk-cached), provisions streams in LocalStack via boto3, and realizes each fork by wholesale-regenerating one watched Vector config fragment (`forks.yaml`) — Vector hot-reloads it. React/Vite frontend served by FastAPI.

**Tech Stack:** Python 3.11+, FastAPI, sqlite3 (stdlib), boto3, requests, itsdangerous, pytest; React 18 + TypeScript + Vite + vitest; Vector (timberio/vector docker image); LocalStack; docker-compose; databricks-sdk (seed script only).

**Spec:** `docs/superpowers/specs/2026-06-11-vector-onboarding-portal-design.md`

**Working directory note:** All paths below are relative to the repo root. The portal lives in `logstream-portal/`. All `pytest` commands run from `logstream-portal/backend/`; all `npm` commands from `logstream-portal/frontend/`; `docker compose` from `logstream-portal/`.

**One deliberate refinement vs the spec:** the spec said "one fragment file per stream." Implementation uses **one regenerated `forks.yaml` containing all forks** (plus a permanent no-op component so the file is always valid). Rationale: Vector's `--watch-config` reliably detects modifications to existing files, but new-file detection in a `--config-dir` is version-sensitive; a single always-present file sidesteps that class of demo failure entirely and is even more faithful to the "always regenerated wholesale from DB state, never patched" idempotency rule.

---

## File Map

```
logstream-portal/
├── Makefile
├── docker-compose.yml
├── Dockerfile
├── .env.example
├── README.md
├── localstack-init/
│   └── init-aws.sh                  # create archive S3 bucket
├── vector/
│   ├── agent-acct-a/agent.yaml      # demo_logs sources + tags for account A
│   ├── agent-acct-b/agent.yaml      # demo_logs sources + tags for account B
│   └── aggregator/
│       ├── aggregator.yaml          # base config: vector source, tagged transform, S3 archive
│       └── fragments/forks.yaml     # committed noop bootstrap; overwritten by portal at runtime
├── fixtures/
│   └── catalog_snapshot.json        # bundled catalog snapshot (UC-offline fallback + tests)
├── scripts/
│   ├── seed_catalog.py              # one-time UC seeding via Databricks SQL warehouse
│   └── demo_test.sh                 # e2e smoke: compose up → fork → records arrive
├── backend/
│   ├── pyproject.toml
│   ├── app/
│   │   ├── __init__.py
│   │   ├── config.py                # Settings dataclass from env
│   │   ├── db.py                    # sqlite schema + persona seed
│   │   ├── main.py                  # create_app factory, wiring, SPA static serving
│   │   ├── catalog/
│   │   │   ├── __init__.py
│   │   │   ├── uc_client.py         # UC REST client (requests)
│   │   │   ├── cache.py             # SnapshotCache (disk + bundled seed)
│   │   │   └── service.py           # tree build, find_source, annotate
│   │   ├── streams/
│   │   │   ├── __init__.py
│   │   │   ├── fragments.py         # deterministic forks.yaml renderer (golden-tested)
│   │   │   ├── provisioner.py       # boto3 create/delete Kinesis/SQS
│   │   │   ├── vector_admin.py      # write fragment, verify via GraphQL, rollback
│   │   │   ├── service.py           # StreamService: lifecycle + approval state machine
│   │   │   └── peek.py              # PeekService: sample records + flow stats
│   │   └── routes/
│   │       ├── __init__.py
│   │       ├── deps.py              # current_user / admin_user dependencies
│   │       ├── session.py           # personas, login/logout
│   │       ├── catalog.py           # GET /api/catalog
│   │       ├── streams.py           # stream CRUD, sources, peek, retry
│   │       └── approvals.py         # admin queue + decisions
│   └── tests/
│       ├── conftest.py              # fakes + app/client fixtures
│       ├── golden/
│       │   ├── forks_empty.yaml
│       │   └── forks_two_streams.yaml
│       ├── test_smoke.py
│       ├── test_db.py
│       ├── test_fragments.py
│       ├── test_uc_client.py
│       ├── test_catalog_service.py
│       ├── test_provisioner.py
│       ├── test_vector_admin.py
│       ├── test_stream_service.py
│       └── test_api.py
└── frontend/
    ├── package.json
    ├── vite.config.ts
    ├── tsconfig.json
    ├── index.html
    └── src/
        ├── main.tsx
        ├── App.tsx
        ├── api.ts
        ├── types.ts
        ├── styles.css
        ├── test-setup.ts
        ├── pages/
        │   ├── LoginPage.tsx
        │   ├── CatalogPage.tsx
        │   ├── StreamsPage.tsx
        │   └── ApprovalsPage.tsx
        └── components/
            ├── SourcesTable.tsx
            ├── SourcesTable.test.tsx
            ├── ForkWizard.tsx
            ├── ForkWizard.test.tsx
            ├── StreamCard.tsx
            └── PeekModal.tsx
```

---

### Task 0: Project scaffolding

**Files:**
- Create: `logstream-portal/backend/pyproject.toml`
- Create: `logstream-portal/backend/app/__init__.py`, `app/catalog/__init__.py`, `app/streams/__init__.py`, `app/routes/__init__.py`
- Create: `logstream-portal/backend/tests/test_smoke.py`
- Create: `logstream-portal/.env.example`
- Create: `logstream-portal/Makefile`

- [ ] **Step 1: Create directory tree and empty `__init__.py` files**

```bash
mkdir -p logstream-portal/backend/app/{catalog,streams,routes} \
         logstream-portal/backend/tests/golden \
         logstream-portal/vector/{agent-acct-a,agent-acct-b,aggregator/fragments} \
         logstream-portal/{fixtures,scripts,localstack-init} \
         logstream-portal/frontend/src/{pages,components}
touch logstream-portal/backend/app/__init__.py \
      logstream-portal/backend/app/catalog/__init__.py \
      logstream-portal/backend/app/streams/__init__.py \
      logstream-portal/backend/app/routes/__init__.py
```

- [ ] **Step 2: Write `logstream-portal/backend/pyproject.toml`**

```toml
[project]
name = "logstream-portal"
version = "0.1.0"
requires-python = ">=3.11"
dependencies = [
    "fastapi>=0.111",
    "uvicorn[standard]>=0.30",
    "itsdangerous>=2.1",
    "requests>=2.31",
    "boto3>=1.34",
    "pyyaml>=6.0",
]

[project.optional-dependencies]
dev = ["pytest>=8.0", "httpx>=0.27"]

[build-system]
requires = ["setuptools>=68"]
build-backend = "setuptools.build_meta"

[tool.setuptools.packages.find]
include = ["app*"]
```

- [ ] **Step 3: Write `logstream-portal/backend/tests/test_smoke.py`**

```python
def test_app_package_imports():
    import app  # noqa: F401
```

- [ ] **Step 4: Install and run the smoke test**

Run: `cd logstream-portal/backend && python3 -m venv .venv && source .venv/bin/activate && pip install -e '.[dev]' && pytest -q`
Expected: `1 passed`
(All later pytest steps assume this venv is active.)

- [ ] **Step 5: Write `logstream-portal/.env.example`**

```bash
# Databricks workspace that hosts the Unity Catalog inventory
DATABRICKS_HOST=https://dbc-xxxxxxxx-xxxx.cloud.databricks.com
DATABRICKS_TOKEN=dapiXXXXXXXXXXXXXXXXXXXXXXXXXXXX
# SQL warehouse used only by scripts/seed_catalog.py
DATABRICKS_WAREHOUSE_ID=abcdef1234567890
UC_CATALOG_NAME=logging_demo
PORTAL_SESSION_SECRET=demo-secret-change-me
```

- [ ] **Step 6: Write `logstream-portal/Makefile`**

```make
.PHONY: up down seed test demo-test

up:
	docker compose up -d --build

down:
	docker compose down -v

seed:
	python3 scripts/seed_catalog.py

test:
	cd backend && python3 -m pytest -q
	cd frontend && npm test

demo-test:
	./scripts/demo_test.sh
```

- [ ] **Step 7: Add venv to gitignore and commit**

Append to the repo root `.gitignore`:

```
logstream-portal/backend/.venv/
logstream-portal/frontend/node_modules/
logstream-portal/frontend/dist/
```

```bash
git add logstream-portal .gitignore
git commit -m "feat(portal): scaffold logstream-portal project skeleton"
```

---

### Task 1: Settings and database layer

**Files:**
- Create: `logstream-portal/backend/app/config.py`
- Create: `logstream-portal/backend/app/db.py`
- Test: `logstream-portal/backend/tests/test_db.py`

- [ ] **Step 1: Write the failing tests**

`logstream-portal/backend/tests/test_db.py`:

```python
import pytest

from app.config import load_settings
from app.db import PERSONAS, get_db, init_db


@pytest.fixture()
def conn():
    c = get_db(":memory:")
    init_db(c)
    return c


def test_settings_defaults(monkeypatch):
    for var in ("DATABRICKS_HOST", "UC_CATALOG_NAME", "PORTAL_DATA_DIR"):
        monkeypatch.delenv(var, raising=False)
    s = load_settings()
    assert s.uc_catalog == "logging_demo"
    assert s.aws_region == "us-east-1"
    assert s.data_dir == "/data"


def test_settings_reads_env(monkeypatch):
    monkeypatch.setenv("UC_CATALOG_NAME", "other_cat")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "s3kr1t")
    s = load_settings()
    assert s.uc_catalog == "other_cat"
    assert s.session_secret == "s3kr1t"


def test_init_db_seeds_personas(conn):
    rows = conn.execute("SELECT id, role FROM users ORDER BY id").fetchall()
    assert [(r["id"], r["role"]) for r in rows] == sorted(
        [(p[0], p[3]) for p in PERSONAS]
    )


def test_init_db_is_idempotent(conn):
    init_db(conn)  # second run must not raise or duplicate
    assert conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"] == len(PERSONAS)


def test_stream_sources_unique_per_stream(conn):
    conn.execute(
        "INSERT INTO streams(owner_id, name, type) VALUES ('dana@app-team', 's1', 'kinesis')"
    )
    ins = (
        "INSERT INTO stream_sources(stream_id, source_fqn, workload, source_name, status, requested_by)"
        " VALUES (1, 'c.s.t', 'w', 'syslog', 'active', 'dana@app-team')"
    )
    conn.execute(ins)
    import sqlite3

    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(ins)
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_db.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.config'`

- [ ] **Step 3: Write `logstream-portal/backend/app/config.py`**

```python
import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    databricks_host: str
    databricks_token: str
    uc_catalog: str
    aws_endpoint: str
    aws_region: str
    data_dir: str
    fragments_path: str
    vector_api_url: str
    snapshot_seed: str
    static_dir: str
    session_secret: str


def load_settings() -> Settings:
    env = os.environ.get
    return Settings(
        databricks_host=env("DATABRICKS_HOST", ""),
        databricks_token=env("DATABRICKS_TOKEN", ""),
        uc_catalog=env("UC_CATALOG_NAME", "logging_demo"),
        aws_endpoint=env("AWS_ENDPOINT_URL", "http://localstack:4566"),
        aws_region=env("AWS_REGION", "us-east-1"),
        data_dir=env("PORTAL_DATA_DIR", "/data"),
        fragments_path=env("VECTOR_FRAGMENTS_PATH", "/vector-fragments/forks.yaml"),
        vector_api_url=env("VECTOR_API_URL", "http://vector-aggregator:8686/graphql"),
        snapshot_seed=env("CATALOG_SNAPSHOT_SEED", ""),
        static_dir=env("PORTAL_STATIC_DIR", ""),
        session_secret=env("PORTAL_SESSION_SECRET", "demo-secret-change-me"),
    )
```

- [ ] **Step 4: Write `logstream-portal/backend/app/db.py`**

```python
import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  team TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('consumer', 'admin'))
);
CREATE TABLE IF NOT EXISTS streams (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  owner_id TEXT NOT NULL REFERENCES users(id),
  name TEXT NOT NULL,
  type TEXT NOT NULL CHECK (type IN ('kinesis', 'sqs')),
  resource_ref TEXT,
  status TEXT NOT NULL DEFAULT 'provisioning'
    CHECK (status IN ('provisioning', 'live', 'error', 'deleted')),
  last_error TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS stream_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stream_id INTEGER NOT NULL REFERENCES streams(id),
  source_fqn TEXT NOT NULL,
  workload TEXT NOT NULL,
  source_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'pending_approval', 'rejected')),
  requested_by TEXT NOT NULL REFERENCES users(id),
  requested_at TEXT NOT NULL DEFAULT (datetime('now')),
  decided_by TEXT REFERENCES users(id),
  decided_at TEXT,
  UNIQUE (stream_id, source_fqn)
);
"""

PERSONAS = [
    ("admin@platform", "Alex Romero", "platform", "admin"),
    ("dana@app-team", "Dana Whitfield", "team-a", "consumer"),
    ("raj@data-sci", "Raj Patel", "data-sci", "consumer"),
]


def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


def init_db(conn: sqlite3.Connection) -> None:
    conn.executescript(SCHEMA)
    conn.executemany(
        "INSERT OR IGNORE INTO users (id, display_name, team, role) VALUES (?, ?, ?, ?)",
        PERSONAS,
    )
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `pytest tests/test_db.py -q`
Expected: `5 passed`

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/backend/app/config.py logstream-portal/backend/app/db.py logstream-portal/backend/tests/test_db.py
git commit -m "feat(portal): settings loader and sqlite schema with seeded personas"
```

---

### Task 2: Vector fork-fragment renderer (golden-file TDD)

**Files:**
- Create: `logstream-portal/backend/app/streams/fragments.py`
- Create: `logstream-portal/backend/tests/golden/forks_empty.yaml`
- Create: `logstream-portal/backend/tests/golden/forks_two_streams.yaml`
- Test: `logstream-portal/backend/tests/test_fragments.py`

- [ ] **Step 1: Write golden file `tests/golden/forks_empty.yaml`** (exact content, trailing newline included)

```yaml
# GENERATED BY logstream-portal -- DO NOT EDIT
# Regenerated wholesale from portal DB state on every change.
transforms:
  forks_noop:
    type: filter
    inputs: ["tagged"]
    condition: "false"
sinks:
  forks_noop_sink:
    type: blackhole
    inputs: ["forks_noop"]
    print_interval_secs: 0
```

- [ ] **Step 2: Write golden file `tests/golden/forks_two_streams.yaml`**

```yaml
# GENERATED BY logstream-portal -- DO NOT EDIT
# Regenerated wholesale from portal DB state on every change.
transforms:
  forks_noop:
    type: filter
    inputs: ["tagged"]
    condition: "false"
  fork_3_filter:
    type: filter
    inputs: ["tagged"]
    condition: '(.workload == "orders_api" && .source_name == "auth_log") || (.workload == "orders_api" && .source_name == "syslog")'
  fork_5_filter:
    type: filter
    inputs: ["tagged"]
    condition: '(.workload == "identity_svc" && .source_name == "auth_log")'
sinks:
  forks_noop_sink:
    type: blackhole
    inputs: ["forks_noop"]
    print_interval_secs: 0
  fork_3_sink:
    type: aws_kinesis_streams
    stream_name: "team-a-logs"
    inputs: ["fork_3_filter"]
    region: "us-east-1"
    endpoint: "http://localstack:4566"
    auth:
      access_key_id: "test"
      secret_access_key: "test"
    encoding:
      codec: json
  fork_5_sink:
    type: aws_sqs
    queue_url: "http://localstack:4566/000000000000/audit-q"
    inputs: ["fork_5_filter"]
    region: "us-east-1"
    endpoint: "http://localstack:4566"
    auth:
      access_key_id: "test"
      secret_access_key: "test"
    encoding:
      codec: json
```

- [ ] **Step 3: Write the failing tests** — `tests/test_fragments.py`

```python
from pathlib import Path

import yaml

from app.streams.fragments import ForkSpec, Member, render_forks_config

GOLDEN = Path(__file__).parent / "golden"
ENDPOINT, REGION = "http://localstack:4566", "us-east-1"


def test_no_forks_renders_noop_only():
    rendered = render_forks_config([], ENDPOINT, REGION)
    assert rendered == (GOLDEN / "forks_empty.yaml").read_text()
    assert yaml.safe_load(rendered)  # stays parseable YAML


def test_kinesis_and_sqs_forks_render_deterministically():
    forks = [
        # deliberately out of order: renderer must sort by stream_id
        ForkSpec(
            5, "sqs", "http://localstack:4566/000000000000/audit-q",
            (Member("identity_svc", "auth_log"),),
        ),
        ForkSpec(
            3, "kinesis", "team-a-logs",
            # deliberately unsorted members: renderer must sort them too
            (Member("orders_api", "syslog"), Member("orders_api", "auth_log")),
        ),
    ]
    rendered = render_forks_config(forks, ENDPOINT, REGION)
    assert rendered == (GOLDEN / "forks_two_streams.yaml").read_text()
    parsed = yaml.safe_load(rendered)
    assert set(parsed["sinks"]) == {"forks_noop_sink", "fork_3_sink", "fork_5_sink"}
    assert parsed["sinks"]["fork_3_sink"]["stream_name"] == "team-a-logs"
```

- [ ] **Step 4: Run tests to verify they fail**

Run: `pytest tests/test_fragments.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.streams.fragments'`

- [ ] **Step 5: Write `app/streams/fragments.py`**

```python
"""Render the aggregator fork fragment (forks.yaml) from portal state.

The file is always regenerated wholesale and must be byte-deterministic
so golden-file tests can assert exact output. The permanent no-op
filter/blackhole pair keeps the file a valid Vector config even when no
forks are active.
"""
from dataclasses import dataclass
from typing import Iterable, Sequence

HEADER = (
    "# GENERATED BY logstream-portal -- DO NOT EDIT\n"
    "# Regenerated wholesale from portal DB state on every change.\n"
)


@dataclass(frozen=True)
class Member:
    workload: str
    source_name: str


@dataclass(frozen=True)
class ForkSpec:
    stream_id: int
    stream_type: str  # "kinesis" | "sqs"
    resource_ref: str  # kinesis stream name | sqs queue url
    members: tuple[Member, ...]  # active members only; never empty


def _condition(members: Iterable[Member]) -> str:
    ordered = sorted(members, key=lambda m: (m.workload, m.source_name))
    return " || ".join(
        f'(.workload == "{m.workload}" && .source_name == "{m.source_name}")'
        for m in ordered
    )


def _filter_block(fork: ForkSpec, input_id: str) -> str:
    return (
        f"  fork_{fork.stream_id}_filter:\n"
        "    type: filter\n"
        f'    inputs: ["{input_id}"]\n'
        f"    condition: '{_condition(fork.members)}'\n"
    )


def _sink_block(fork: ForkSpec, endpoint: str, region: str) -> str:
    if fork.stream_type == "kinesis":
        head = (
            f"  fork_{fork.stream_id}_sink:\n"
            "    type: aws_kinesis_streams\n"
            f'    stream_name: "{fork.resource_ref}"\n'
        )
    else:
        head = (
            f"  fork_{fork.stream_id}_sink:\n"
            "    type: aws_sqs\n"
            f'    queue_url: "{fork.resource_ref}"\n'
        )
    return head + (
        f'    inputs: ["fork_{fork.stream_id}_filter"]\n'
        f'    region: "{region}"\n'
        f'    endpoint: "{endpoint}"\n'
        "    auth:\n"
        '      access_key_id: "test"\n'
        '      secret_access_key: "test"\n'
        "    encoding:\n"
        "      codec: json\n"
    )


def render_forks_config(
    forks: Sequence[ForkSpec],
    aws_endpoint: str,
    aws_region: str,
    input_id: str = "tagged",
) -> str:
    ordered = sorted(forks, key=lambda f: f.stream_id)
    out = [HEADER, "transforms:\n"]
    out.append(
        "  forks_noop:\n"
        "    type: filter\n"
        f'    inputs: ["{input_id}"]\n'
        '    condition: "false"\n'
    )
    for fork in ordered:
        out.append(_filter_block(fork, input_id))
    out.append("sinks:\n")
    out.append(
        "  forks_noop_sink:\n"
        "    type: blackhole\n"
        '    inputs: ["forks_noop"]\n'
        "    print_interval_secs: 0\n"
    )
    for fork in ordered:
        out.append(_sink_block(fork, aws_endpoint, aws_region))
    return "".join(out)
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_fragments.py -q`
Expected: `2 passed`
If a golden mismatch occurs, diff carefully — fix the golden file only if the renderer output is genuinely correct YAML for Vector (sorted forks, sorted members, quoting as shown).

- [ ] **Step 7: Commit**

```bash
git add logstream-portal/backend/app/streams/fragments.py logstream-portal/backend/tests/test_fragments.py logstream-portal/backend/tests/golden
git commit -m "feat(portal): deterministic Vector fork-fragment renderer with golden tests"
```

---

### Task 3: Unity Catalog REST client

**Files:**
- Create: `logstream-portal/backend/app/catalog/uc_client.py`
- Test: `logstream-portal/backend/tests/test_uc_client.py`

- [ ] **Step 1: Write the failing tests** — `tests/test_uc_client.py`

```python
import pytest
import requests

from app.catalog.uc_client import CatalogUnavailable, UCClient


class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload, self.status_code = payload, status

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}")

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, payloads=None, error=None):
        self.calls, self._payloads, self._error = [], list(payloads or []), error

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls.append({"url": url, "params": params, "headers": headers})
        if self._error:
            raise self._error
        return FakeResponse(self._payloads.pop(0))


def test_list_schemas_hits_uc_endpoint_with_bearer_token():
    session = FakeSession(payloads=[{"schemas": [{"name": "acct_a__orders_api"}]}])
    client = UCClient("https://dbc-1.cloud.databricks.com/", "tok123", session=session)
    schemas = client.list_schemas("logging_demo")
    assert schemas == [{"name": "acct_a__orders_api"}]
    call = session.calls[0]
    assert call["url"] == "https://dbc-1.cloud.databricks.com/api/2.1/unity-catalog/schemas"
    assert call["params"] == {"catalog_name": "logging_demo"}
    assert call["headers"]["Authorization"] == "Bearer tok123"


def test_list_tables_passes_schema_and_returns_empty_when_missing_key():
    session = FakeSession(payloads=[{}])
    client = UCClient("https://h", "t", session=session)
    assert client.list_tables("logging_demo", "acct_a__orders_api") == []
    assert session.calls[0]["params"] == {
        "catalog_name": "logging_demo",
        "schema_name": "acct_a__orders_api",
    }


def test_network_error_raises_catalog_unavailable():
    session = FakeSession(error=requests.ConnectionError("no route"))
    client = UCClient("https://h", "t", session=session)
    with pytest.raises(CatalogUnavailable):
        client.list_schemas("logging_demo")


def test_http_error_raises_catalog_unavailable():
    session = FakeSession(payloads=[])
    session._error = None
    session._payloads = []

    class ErrSession(FakeSession):
        def get(self, url, params=None, headers=None, timeout=None):
            return FakeResponse({}, status=403)

    client = UCClient("https://h", "t", session=ErrSession())
    with pytest.raises(CatalogUnavailable):
        client.list_schemas("logging_demo")
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_uc_client.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.catalog.uc_client'`

- [ ] **Step 3: Write `app/catalog/uc_client.py`**

```python
"""Thin read-only client for the Unity Catalog REST API."""
import requests


class CatalogUnavailable(RuntimeError):
    """Databricks could not be reached or refused the request."""


class UCClient:
    def __init__(self, host: str, token: str, session=None):
        self._base = host.rstrip("/")
        self._token = token
        self._session = session or requests.Session()

    def _get(self, path: str, params: dict) -> dict:
        try:
            resp = self._session.get(
                self._base + path,
                params=params,
                headers={"Authorization": f"Bearer {self._token}"},
                timeout=10,
            )
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as exc:
            raise CatalogUnavailable(str(exc)) from exc

    def list_schemas(self, catalog: str) -> list[dict]:
        payload = self._get("/api/2.1/unity-catalog/schemas", {"catalog_name": catalog})
        return payload.get("schemas", [])

    def list_tables(self, catalog: str, schema: str) -> list[dict]:
        payload = self._get(
            "/api/2.1/unity-catalog/tables",
            {"catalog_name": catalog, "schema_name": schema},
        )
        return payload.get("tables", [])
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_uc_client.py -q`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add logstream-portal/backend/app/catalog/uc_client.py logstream-portal/backend/tests/test_uc_client.py
git commit -m "feat(portal): Unity Catalog REST client with unavailability error mapping"
```

---

### Task 4: Snapshot cache, catalog service, and bundled fixture snapshot

**Files:**
- Create: `logstream-portal/backend/app/catalog/cache.py`
- Create: `logstream-portal/backend/app/catalog/service.py`
- Create: `logstream-portal/fixtures/catalog_snapshot.json`
- Test: `logstream-portal/backend/tests/test_catalog_service.py`

- [ ] **Step 1: Write the bundled fixture `logstream-portal/fixtures/catalog_snapshot.json`**

This mirrors exactly what `seed_catalog.py` (Task 13) creates in Databricks. It doubles as the UC-offline fallback in the running demo and the catalog used by all backend tests. Columns are the same four for every source: `ts timestamp`, `host string`, `severity string`, `message string`.

```json
{
  "as_of": "2026-06-11T00:00:00+00:00",
  "accounts": [
    {
      "account_id": "111111111111",
      "account_alias": "prod-ecommerce",
      "workloads": [
        {
          "name": "orders_api",
          "schema": "acct_a__orders_api",
          "environment": "prod",
          "sources": [
            {"fqn": "logging_demo.acct_a__orders_api.app_log", "name": "app_log", "log_type": "application", "sensitivity": "standard", "est_volume_per_min": 600, "description": "Structured application logs", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]},
            {"fqn": "logging_demo.acct_a__orders_api.auth_log", "name": "auth_log", "log_type": "system", "sensitivity": "sensitive", "est_volume_per_min": 300, "description": "SSH/sudo/PAM auth events", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]},
            {"fqn": "logging_demo.acct_a__orders_api.syslog", "name": "syslog", "log_type": "system", "sensitivity": "standard", "est_volume_per_min": 900, "description": "Host syslog from orders API tier", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]}
          ]
        },
        {
          "name": "storefront_web",
          "schema": "acct_a__storefront_web",
          "environment": "prod",
          "sources": [
            {"fqn": "logging_demo.acct_a__storefront_web.nginx_access", "name": "nginx_access", "log_type": "web", "sensitivity": "standard", "est_volume_per_min": 2400, "description": "Nginx access logs from storefront web tier", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]},
            {"fqn": "logging_demo.acct_a__storefront_web.syslog", "name": "syslog", "log_type": "system", "sensitivity": "standard", "est_volume_per_min": 1200, "description": "Host syslog from storefront web tier", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]}
          ]
        }
      ]
    },
    {
      "account_id": "222222222222",
      "account_alias": "prod-platform",
      "workloads": [
        {
          "name": "batch_etl",
          "schema": "acct_b__batch_etl",
          "environment": "prod",
          "sources": [
            {"fqn": "logging_demo.acct_b__batch_etl.cron_log", "name": "cron_log", "log_type": "system", "sensitivity": "standard", "est_volume_per_min": 120, "description": "Cron scheduler logs from ETL hosts", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]},
            {"fqn": "logging_demo.acct_b__batch_etl.syslog", "name": "syslog", "log_type": "system", "sensitivity": "standard", "est_volume_per_min": 300, "description": "Host syslog from batch ETL hosts", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]}
          ]
        },
        {
          "name": "identity_svc",
          "schema": "acct_b__identity_svc",
          "environment": "prod",
          "sources": [
            {"fqn": "logging_demo.acct_b__identity_svc.auth_log", "name": "auth_log", "log_type": "system", "sensitivity": "sensitive", "est_volume_per_min": 450, "description": "SSH/sudo/PAM auth events", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]},
            {"fqn": "logging_demo.acct_b__identity_svc.syslog", "name": "syslog", "log_type": "system", "sensitivity": "standard", "est_volume_per_min": 800, "description": "Host syslog from identity service", "columns": [{"name": "ts", "type": "timestamp"}, {"name": "host", "type": "string"}, {"name": "severity", "type": "string"}, {"name": "message", "type": "string"}]}
          ]
        }
      ]
    }
  ]
}
```

- [ ] **Step 2: Write the failing tests** — `tests/test_catalog_service.py`

```python
import json
from pathlib import Path

import pytest

from app.catalog.cache import SnapshotCache
from app.catalog.service import CatalogService, annotate, find_source
from app.catalog.uc_client import CatalogUnavailable

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "catalog_snapshot.json"


class FakeUC:
    """Returns UC-API-shaped payloads for one account/workload/source."""

    def list_schemas(self, catalog):
        return [{"name": "acct_a__orders_api"}, {"name": "information_schema"}]

    def list_tables(self, catalog, schema):
        if schema == "information_schema":
            raise AssertionError("information_schema must be skipped")
        return [
            {
                "name": "auth_log",
                "comment": "SSH/sudo/PAM auth events",
                "properties": {
                    "sensitivity": "sensitive", "log_type": "system",
                    "account_id": "111111111111", "account_alias": "prod-ecommerce",
                    "workload": "orders_api", "environment": "prod",
                    "est_volume_per_min": "300",
                },
                "columns": [{"name": "ts", "type_text": "timestamp"}],
            },
            {
                "name": "syslog",
                "comment": "Host syslog",
                "properties": {
                    "sensitivity": "standard", "log_type": "system",
                    "account_id": "111111111111", "account_alias": "prod-ecommerce",
                    "workload": "orders_api", "environment": "prod",
                    "est_volume_per_min": "900",
                },
                "columns": [{"name": "ts", "type_text": "timestamp"}],
            },
        ]


class DownUC:
    def list_schemas(self, catalog):
        raise CatalogUnavailable("databricks unreachable")

    def list_tables(self, catalog, schema):
        raise CatalogUnavailable("databricks unreachable")


def test_builds_tree_grouped_by_account_and_saves_snapshot(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json")
    tree = CatalogService(FakeUC(), cache, "logging_demo").get_tree()
    assert tree["stale"] is False
    assert tree["as_of"]
    [account] = tree["accounts"]
    assert account["account_id"] == "111111111111"
    [workload] = account["workloads"]
    assert workload["name"] == "orders_api"
    assert [s["name"] for s in workload["sources"]] == ["auth_log", "syslog"]
    src = workload["sources"][0]
    assert src["fqn"] == "logging_demo.acct_a__orders_api.auth_log"
    assert src["sensitivity"] == "sensitive"
    assert src["est_volume_per_min"] == 300
    assert (tmp_path / "snap.json").exists()


def test_falls_back_to_cached_snapshot_when_uc_down(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json")
    CatalogService(FakeUC(), cache, "logging_demo").get_tree()  # warm the cache
    tree = CatalogService(DownUC(), cache, "logging_demo").get_tree()
    assert tree["stale"] is True
    assert tree["accounts"][0]["account_id"] == "111111111111"


def test_falls_back_to_bundled_seed_when_no_local_cache(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json", seed_path=FIXTURE)
    tree = CatalogService(DownUC(), cache, "logging_demo").get_tree()
    assert tree["stale"] is True
    assert len(tree["accounts"]) == 2


def test_raises_when_uc_down_and_no_cache_at_all(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json")
    with pytest.raises(CatalogUnavailable):
        CatalogService(DownUC(), cache, "logging_demo").get_tree()


def test_find_source_returns_filter_tags():
    tree = json.loads(FIXTURE.read_text())
    found = find_source(tree, "logging_demo.acct_b__identity_svc.auth_log")
    assert found == {
        "fqn": "logging_demo.acct_b__identity_svc.auth_log",
        "workload_tag": "identity_svc",
        "source_name": "auth_log",
        "sensitivity": "sensitive",
    }
    assert find_source(tree, "logging_demo.nope.nope") is None


def test_annotate_merges_subscriptions_without_mutating_input():
    tree = json.loads(FIXTURE.read_text())
    rows = [{
        "source_fqn": "logging_demo.acct_a__orders_api.syslog",
        "stream_id": 7, "stream_name": "team-a-logs", "status": "active",
    }]
    out = annotate(tree, rows)
    orders = out["accounts"][0]["workloads"][0]
    syslog = [s for s in orders["sources"] if s["name"] == "syslog"][0]
    assert syslog["subscriptions"] == [
        {"stream_id": 7, "stream_name": "team-a-logs", "status": "active"}
    ]
    auth = [s for s in orders["sources"] if s["name"] == "auth_log"][0]
    assert auth["subscriptions"] == []
    assert "subscriptions" not in tree["accounts"][0]["workloads"][0]["sources"][0]
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_catalog_service.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.catalog.cache'`

- [ ] **Step 4: Write `app/catalog/cache.py`**

```python
import json
from pathlib import Path


class SnapshotCache:
    """Last-good catalog snapshot on disk, with an optional bundled seed."""

    def __init__(self, path: Path, seed_path: Path | None = None):
        self._path = Path(path)
        self._seed = Path(seed_path) if seed_path else None

    def save(self, snapshot: dict) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(snapshot, indent=2))

    def load(self) -> dict | None:
        for candidate in (self._path, self._seed):
            if candidate and candidate.exists():
                return json.loads(candidate.read_text())
        return None
```

- [ ] **Step 5: Write `app/catalog/service.py`**

```python
import json
from datetime import datetime, timezone

from app.catalog.cache import SnapshotCache
from app.catalog.uc_client import CatalogUnavailable


def find_source(tree: dict, fqn: str) -> dict | None:
    """Resolve a source FQN to the tags the fork filter needs."""
    for account in tree["accounts"]:
        for workload in account["workloads"]:
            for src in workload["sources"]:
                if src["fqn"] == fqn:
                    return {
                        "fqn": fqn,
                        "workload_tag": workload["name"],
                        "source_name": src["name"],
                        "sensitivity": src["sensitivity"],
                    }
    return None


def annotate(tree: dict, sub_rows: list[dict]) -> dict:
    """Return a deep copy of the tree with per-user subscription refs on each source."""
    by_fqn: dict[str, list[dict]] = {}
    for row in sub_rows:
        by_fqn.setdefault(row["source_fqn"], []).append(
            {"stream_id": row["stream_id"], "stream_name": row["stream_name"], "status": row["status"]}
        )
    out = json.loads(json.dumps(tree))
    for account in out["accounts"]:
        for workload in account["workloads"]:
            for src in workload["sources"]:
                src["subscriptions"] = by_fqn.get(src["fqn"], [])
    return out


class CatalogService:
    def __init__(self, uc_client, cache: SnapshotCache, catalog_name: str):
        self._uc = uc_client
        self._cache = cache
        self._catalog_name = catalog_name

    def get_tree(self) -> dict:
        try:
            accounts = self._build()
        except CatalogUnavailable:
            snap = self._cache.load()
            if snap is None:
                raise
            return {**snap, "stale": True}
        snap = {
            "as_of": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "accounts": accounts,
        }
        self._cache.save(snap)
        return {**snap, "stale": False}

    def _build(self) -> list[dict]:
        accounts: dict[str, dict] = {}
        for schema in self._uc.list_schemas(self._catalog_name):
            schema_name = schema["name"]
            if schema_name == "information_schema":
                continue
            tables = self._uc.list_tables(self._catalog_name, schema_name)
            if not tables:
                continue
            props = tables[0].get("properties") or {}
            account_id = props.get("account_id", "unknown")
            account = accounts.setdefault(account_id, {
                "account_id": account_id,
                "account_alias": props.get("account_alias", account_id),
                "workloads": [],
            })
            workload = {
                "name": props.get("workload", schema_name.split("__", 1)[-1]),
                "schema": schema_name,
                "environment": props.get("environment", "prod"),
                "sources": [],
            }
            for table in tables:
                tprops = table.get("properties") or {}
                workload["sources"].append({
                    "fqn": f'{self._catalog_name}.{schema_name}.{table["name"]}',
                    "name": table["name"],
                    "log_type": tprops.get("log_type", "system"),
                    "sensitivity": tprops.get("sensitivity", "standard"),
                    "est_volume_per_min": int(tprops.get("est_volume_per_min", 0)),
                    "description": table.get("comment", ""),
                    "columns": [
                        {"name": c["name"], "type": c.get("type_text", "")}
                        for c in table.get("columns", [])
                    ],
                })
            workload["sources"].sort(key=lambda s: s["name"])
            account["workloads"].append(workload)
        out = sorted(accounts.values(), key=lambda a: a["account_id"])
        for account in out:
            account["workloads"].sort(key=lambda w: w["name"])
        return out
```

- [ ] **Step 6: Run tests to verify they pass**

Run: `pytest tests/test_catalog_service.py -q`
Expected: `6 passed`

- [ ] **Step 7: Commit**

```bash
git add logstream-portal/backend/app/catalog logstream-portal/fixtures/catalog_snapshot.json logstream-portal/backend/tests/test_catalog_service.py
git commit -m "feat(portal): catalog service with UC tree build, disk cache, and bundled snapshot"
```

---

### Task 5: Stream/queue provisioner (LocalStack via boto3)

**Files:**
- Create: `logstream-portal/backend/app/streams/provisioner.py`
- Test: `logstream-portal/backend/tests/test_provisioner.py`

- [ ] **Step 1: Write the failing tests** — `tests/test_provisioner.py`

```python
import pytest

from app.streams.provisioner import ProvisionError, Provisioner


class FakeWaiter:
    def __init__(self, log):
        self._log = log

    def wait(self, **kwargs):
        self._log.append(("wait", kwargs))


class FakeKinesis:
    def __init__(self, fail=False):
        self.log, self._fail = [], fail

    def create_stream(self, **kwargs):
        if self._fail:
            raise RuntimeError("LimitExceededException")
        self.log.append(("create_stream", kwargs))

    def get_waiter(self, name):
        self.log.append(("get_waiter", name))
        return FakeWaiter(self.log)

    def delete_stream(self, **kwargs):
        self.log.append(("delete_stream", kwargs))


class FakeSQS:
    def __init__(self):
        self.log = []

    def create_queue(self, **kwargs):
        self.log.append(("create_queue", kwargs))
        return {"QueueUrl": f"http://localstack:4566/000000000000/{kwargs['QueueName']}"}

    def delete_queue(self, **kwargs):
        self.log.append(("delete_queue", kwargs))


def test_create_kinesis_waits_for_active_and_returns_name():
    kinesis = FakeKinesis()
    ref = Provisioner(kinesis, FakeSQS()).create("kinesis", "team-a-logs")
    assert ref == "team-a-logs"
    ops = [op for op, _ in kinesis.log]
    assert ops == ["create_stream", "get_waiter", "wait"]


def test_create_sqs_returns_queue_url():
    ref = Provisioner(FakeKinesis(), FakeSQS()).create("sqs", "audit-q")
    assert ref == "http://localstack:4566/000000000000/audit-q"


def test_create_failure_wraps_in_provision_error():
    with pytest.raises(ProvisionError):
        Provisioner(FakeKinesis(fail=True), FakeSQS()).create("kinesis", "x")


def test_delete_routes_by_type():
    kinesis, sqs = FakeKinesis(), FakeSQS()
    p = Provisioner(kinesis, sqs)
    p.delete("kinesis", "team-a-logs")
    p.delete("sqs", "http://localstack:4566/000000000000/audit-q")
    assert kinesis.log[-1] == ("delete_stream", {"StreamName": "team-a-logs", "EnforceConsumerDeletion": True})
    assert sqs.log[-1] == ("delete_queue", {"QueueUrl": "http://localstack:4566/000000000000/audit-q"})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_provisioner.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.streams.provisioner'`

- [ ] **Step 3: Write `app/streams/provisioner.py`**

```python
"""Create/delete the consumer's dedicated stream in (Local)AWS."""


class ProvisionError(RuntimeError):
    """The AWS resource could not be created or deleted."""


class Provisioner:
    def __init__(self, kinesis_client, sqs_client):
        self._kinesis = kinesis_client
        self._sqs = sqs_client

    def create(self, stream_type: str, name: str) -> str:
        """Returns the resource_ref: kinesis stream name or sqs queue URL."""
        try:
            if stream_type == "kinesis":
                self._kinesis.create_stream(StreamName=name, ShardCount=1)
                self._kinesis.get_waiter("stream_exists").wait(
                    StreamName=name, WaiterConfig={"Delay": 1, "MaxAttempts": 30}
                )
                return name
            return self._sqs.create_queue(QueueName=name)["QueueUrl"]
        except Exception as exc:
            raise ProvisionError(str(exc)) from exc

    def delete(self, stream_type: str, resource_ref: str) -> None:
        try:
            if stream_type == "kinesis":
                self._kinesis.delete_stream(
                    StreamName=resource_ref, EnforceConsumerDeletion=True
                )
            else:
                self._sqs.delete_queue(QueueUrl=resource_ref)
        except Exception as exc:
            raise ProvisionError(str(exc)) from exc
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_provisioner.py -q`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add logstream-portal/backend/app/streams/provisioner.py logstream-portal/backend/tests/test_provisioner.py
git commit -m "feat(portal): kinesis/sqs provisioner with error wrapping"
```

---

### Task 6: Vector admin — write fragment, verify via GraphQL, roll back

**Files:**
- Create: `logstream-portal/backend/app/streams/vector_admin.py`
- Test: `logstream-portal/backend/tests/test_vector_admin.py`

- [ ] **Step 1: Write the failing tests** — `tests/test_vector_admin.py`

```python
import json

import pytest
import requests

from app.streams.vector_admin import VectorAdmin, VectorApplyError


class FakeGraphQL:
    """Pretends to be requests.Session against Vector's GraphQL API."""

    def __init__(self, component_ids=None, error=None):
        self.component_ids = set(component_ids or [])
        self._error = error
        self.calls = 0

    def post(self, url, data=None, headers=None, timeout=None):
        self.calls += 1
        if self._error:
            raise self._error

        class Resp:
            status_code = 200

            def raise_for_status(self):
                pass

            def json(inner):
                return {"data": {"components": {"edges": [
                    {"node": {"componentId": cid}} for cid in sorted(self.component_ids)
                ]}}}

        return Resp()


def _admin(tmp_path, session, timeout=0.05):
    return VectorAdmin(
        str(tmp_path / "forks.yaml"), "http://vector:8686/graphql",
        session=session, poll_interval=0.01, timeout=timeout,
    )


def test_apply_writes_file_and_succeeds_when_components_appear(tmp_path):
    session = FakeGraphQL(component_ids=["fork_1_sink", "forks_noop_sink"])
    admin = _admin(tmp_path, session)
    admin.apply("rendered: yaml\n", ["fork_1_sink"])
    assert (tmp_path / "forks.yaml").read_text() == "rendered: yaml\n"
    assert session.calls >= 1


def test_apply_with_no_expected_sinks_just_needs_healthy_api(tmp_path):
    admin = _admin(tmp_path, FakeGraphQL(component_ids=[]))
    admin.apply("rendered: yaml\n", [])  # removal case: nothing to wait for
    assert (tmp_path / "forks.yaml").read_text() == "rendered: yaml\n"


def test_apply_rolls_back_previous_content_on_timeout(tmp_path):
    path = tmp_path / "forks.yaml"
    path.write_text("previous: yaml\n")
    admin = _admin(tmp_path, FakeGraphQL(component_ids=["forks_noop_sink"]))
    with pytest.raises(VectorApplyError):
        admin.apply("bad: yaml\n", ["fork_9_sink"])
    assert path.read_text() == "previous: yaml\n"


def test_apply_rolls_back_when_api_unreachable(tmp_path):
    path = tmp_path / "forks.yaml"
    path.write_text("previous: yaml\n")
    admin = _admin(tmp_path, FakeGraphQL(error=requests.ConnectionError("down")))
    with pytest.raises(VectorApplyError):
        admin.apply("new: yaml\n", ["fork_1_sink"])
    assert path.read_text() == "previous: yaml\n"


def test_first_write_with_no_previous_keeps_new_file_on_failure(tmp_path):
    # Nothing to roll back to; the noop-only file is still valid for Vector.
    admin = _admin(tmp_path, FakeGraphQL(error=requests.ConnectionError("down")))
    with pytest.raises(VectorApplyError):
        admin.apply("new: yaml\n", ["fork_1_sink"])
    assert (tmp_path / "forks.yaml").read_text() == "new: yaml\n"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_vector_admin.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.streams.vector_admin'`

- [ ] **Step 3: Write `app/streams/vector_admin.py`**

```python
"""Apply a rendered fork fragment and verify Vector actually loaded it.

Vector keeps running its old config when a reloaded file is invalid, so
"rollback" here means restoring the previous file content (so disk state
matches what Vector is actually running) and surfacing the error.
"""
import json
import time
from pathlib import Path

import requests


class VectorApplyError(RuntimeError):
    """Vector did not load the expected components in time."""


_COMPONENTS_QUERY = json.dumps(
    {"query": "{ components(first: 500) { edges { node { componentId } } } }"}
)


class VectorAdmin:
    def __init__(
        self,
        fragments_path: str,
        api_url: str,
        session=None,
        poll_interval: float = 0.5,
        timeout: float = 15.0,
    ):
        self._path = Path(fragments_path)
        self._api_url = api_url
        self._session = session or requests.Session()
        self._poll_interval = poll_interval
        self._timeout = timeout

    def _component_ids(self) -> set[str]:
        resp = self._session.post(
            self._api_url,
            data=_COMPONENTS_QUERY,
            headers={"Content-Type": "application/json"},
            timeout=3,
        )
        resp.raise_for_status()
        edges = resp.json()["data"]["components"]["edges"]
        return {edge["node"]["componentId"] for edge in edges}

    def apply(self, rendered: str, expected_sink_ids: list[str]) -> None:
        previous = self._path.read_text() if self._path.exists() else None
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(rendered)

        deadline = time.monotonic() + self._timeout
        while time.monotonic() < deadline:
            try:
                ids = self._component_ids()
                if all(sink_id in ids for sink_id in expected_sink_ids):
                    return
            except requests.RequestException:
                pass  # aggregator may be mid-reload; keep polling
            time.sleep(self._poll_interval)

        if previous is not None:
            self._path.write_text(previous)
        raise VectorApplyError(
            f"Vector did not load {expected_sink_ids!r} within {self._timeout}s"
        )
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_vector_admin.py -q`
Expected: `5 passed`

- [ ] **Step 5: Commit**

```bash
git add logstream-portal/backend/app/streams/vector_admin.py logstream-portal/backend/tests/test_vector_admin.py
git commit -m "feat(portal): vector admin with GraphQL verification and fragment rollback"
```

---

### Task 7: Peek service (sample records + flow stats)

**Files:**
- Create: `logstream-portal/backend/app/streams/peek.py`
- Test: `logstream-portal/backend/tests/test_peek.py`

- [ ] **Step 1: Write the failing tests** — `tests/test_peek.py`

```python
import json

from app.streams.peek import PeekService


class FakeKinesis:
    def __init__(self, records):
        self._records = records

    def describe_stream(self, StreamName):
        return {"StreamDescription": {"Shards": [{"ShardId": "shardId-0"}]}}

    def get_shard_iterator(self, **kwargs):
        assert kwargs["ShardIteratorType"] == "TRIM_HORIZON"
        return {"ShardIterator": "it-0"}

    def get_records(self, ShardIterator, Limit):
        return {
            "Records": [{"Data": json.dumps(r).encode()} for r in self._records],
            "NextShardIterator": None,
            "MillisBehindLatest": 0,
        }


class FakeSQS:
    def __init__(self, bodies, backlog=3):
        self._bodies, self._backlog = bodies, backlog
        self.deleted = []

    def receive_message(self, **kwargs):
        assert kwargs["VisibilityTimeout"] == 0  # peek must not hide messages
        return {"Messages": [{"Body": b} for b in self._bodies]}

    def get_queue_attributes(self, QueueUrl, AttributeNames):
        return {"Attributes": {"ApproximateNumberOfMessages": str(self._backlog)}}

    def delete_message(self, **kwargs):
        self.deleted.append(kwargs)


def test_kinesis_peek_returns_last_n_decoded_records():
    records = [{"message": f"m{i}", "workload": "orders_api"} for i in range(8)]
    out = PeekService(FakeKinesis(records), FakeSQS([])).peek("kinesis", "team-a-logs", limit=5)
    assert len(out) == 5
    assert out[-1] == {"message": "m7", "workload": "orders_api"}


def test_sqs_peek_decodes_json_and_never_deletes():
    sqs = FakeSQS([json.dumps({"message": "hi"}), "not-json"])
    out = PeekService(FakeKinesis([]), sqs).peek("sqs", "http://q", limit=5)
    assert out == [{"message": "hi"}, {"raw": "not-json"}]
    assert sqs.deleted == []


def test_flow_stats_kinesis_counts_recent_records():
    records = [{"message": f"m{i}"} for i in range(4)]
    stats = PeekService(FakeKinesis(records), FakeSQS([])).flow_stats("kinesis", "s")
    assert stats == {"recent_records": 4}


def test_flow_stats_sqs_uses_queue_depth():
    stats = PeekService(FakeKinesis([]), FakeSQS([], backlog=12)).flow_stats("sqs", "http://q")
    assert stats == {"recent_records": 12}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_peek.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.streams.peek'`

- [ ] **Step 3: Write `app/streams/peek.py`**

```python
"""Read-only views into a consumer's stream: sample records and flow stats."""
import json


def _decode(data):
    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="replace")
    try:
        return json.loads(data)
    except (json.JSONDecodeError, TypeError):
        return {"raw": data}


class PeekService:
    def __init__(self, kinesis_client, sqs_client):
        self._kinesis = kinesis_client
        self._sqs = sqs_client

    def peek(self, stream_type: str, resource_ref: str, limit: int = 5) -> list:
        if stream_type == "kinesis":
            return self._peek_kinesis(resource_ref, limit)
        return self._peek_sqs(resource_ref, limit)

    def _peek_kinesis(self, stream_name: str, limit: int) -> list:
        description = self._kinesis.describe_stream(StreamName=stream_name)
        shard_id = description["StreamDescription"]["Shards"][0]["ShardId"]
        iterator = self._kinesis.get_shard_iterator(
            StreamName=stream_name, ShardId=shard_id, ShardIteratorType="TRIM_HORIZON"
        )["ShardIterator"]
        records = []
        for _ in range(5):  # bounded catch-up through the shard
            out = self._kinesis.get_records(ShardIterator=iterator, Limit=1000)
            records.extend(out["Records"])
            iterator = out.get("NextShardIterator")
            if not iterator or out.get("MillisBehindLatest", 0) == 0:
                break
        return [_decode(r["Data"]) for r in records[-limit:]]

    def _peek_sqs(self, queue_url: str, limit: int) -> list:
        out = self._sqs.receive_message(
            QueueUrl=queue_url,
            MaxNumberOfMessages=min(limit, 10),
            WaitTimeSeconds=1,
            VisibilityTimeout=0,  # non-destructive: messages stay visible
        )
        return [_decode(m["Body"]) for m in out.get("Messages", [])]

    def flow_stats(self, stream_type: str, resource_ref: str) -> dict:
        if stream_type == "kinesis":
            return {"recent_records": len(self.peek("kinesis", resource_ref, limit=50))}
        attrs = self._sqs.get_queue_attributes(
            QueueUrl=resource_ref, AttributeNames=["ApproximateNumberOfMessages"]
        )
        return {"recent_records": int(attrs["Attributes"]["ApproximateNumberOfMessages"])}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_peek.py -q`
Expected: `4 passed`

- [ ] **Step 5: Commit**

```bash
git add logstream-portal/backend/app/streams/peek.py logstream-portal/backend/tests/test_peek.py
git commit -m "feat(portal): non-destructive peek and flow stats for kinesis/sqs"
```

---

### Task 8: StreamService — fork lifecycle and approval state machine

**Files:**
- Create: `logstream-portal/backend/app/streams/service.py`
- Test: `logstream-portal/backend/tests/test_stream_service.py`

The service is the heart of the portal: it resolves catalog sources, applies the sensitivity gate, provisions resources, and regenerates the Vector fragment from DB state after every mutation.

- [ ] **Step 1: Write the failing tests** — `tests/test_stream_service.py`

```python
import json
from pathlib import Path

import pytest

from app.config import Settings
from app.db import get_db, init_db
from app.streams.provisioner import ProvisionError
from app.streams.service import StreamService, StreamServiceError
from app.streams.vector_admin import VectorApplyError

FIXTURE = Path(__file__).resolve().parents[2] / "fixtures" / "catalog_snapshot.json"

SYSLOG = "logging_demo.acct_a__storefront_web.syslog"
AUTH_LOG = "logging_demo.acct_a__orders_api.auth_log"

DANA = {"id": "dana@app-team", "role": "consumer"}
RAJ = {"id": "raj@data-sci", "role": "consumer"}
ADMIN = {"id": "admin@platform", "role": "admin"}


class FixtureCatalog:
    def get_tree(self):
        return {**json.loads(FIXTURE.read_text()), "stale": False}


class FakeProvisioner:
    def __init__(self):
        self.created, self.deleted, self.fail = [], [], False

    def create(self, stream_type, name):
        if self.fail:
            raise ProvisionError("boom")
        self.created.append((stream_type, name))
        return name if stream_type == "kinesis" else f"http://q/{name}"

    def delete(self, stream_type, ref):
        self.deleted.append((stream_type, ref))


class FakeVectorAdmin:
    def __init__(self):
        self.applied, self.fail = [], False

    def apply(self, rendered, expected):
        if self.fail:
            raise VectorApplyError("vector said no")
        self.applied.append({"rendered": rendered, "expected": tuple(expected)})


@pytest.fixture()
def env(tmp_path):
    conn = get_db(":memory:")
    init_db(conn)
    settings = Settings(
        databricks_host="", databricks_token="", uc_catalog="logging_demo",
        aws_endpoint="http://localstack:4566", aws_region="us-east-1",
        data_dir=str(tmp_path), fragments_path=str(tmp_path / "forks.yaml"),
        vector_api_url="http://v:8686/graphql", snapshot_seed="",
        static_dir="", session_secret="x",
    )
    provisioner, vector = FakeProvisioner(), FakeVectorAdmin()
    svc = StreamService(conn, FixtureCatalog(), provisioner, vector, settings)
    return svc, provisioner, vector


def test_standard_source_activates_immediately(env):
    svc, provisioner, vector = env
    stream = svc.create_stream(DANA, "team-a-logs", "kinesis", [SYSLOG])
    assert stream["status"] == "live"
    assert stream["resource_ref"] == "team-a-logs"
    assert stream["sources"][0]["status"] == "active"
    assert provisioner.created == [("kinesis", "team-a-logs")]
    last = vector.applied[-1]
    assert f'fork_{stream["id"]}_sink' in last["expected"]
    assert '.source_name == "syslog"' in last["rendered"]


def test_sensitive_source_pends_and_is_excluded_from_fragment(env):
    svc, _, vector = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [SYSLOG, AUTH_LOG])
    by_fqn = {s["source_fqn"]: s["status"] for s in stream["sources"]}
    assert by_fqn == {SYSLOG: "active", AUTH_LOG: "pending_approval"}
    assert "auth_log" not in vector.applied[-1]["rendered"]


def test_approval_activates_and_reapplies(env):
    svc, _, vector = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [AUTH_LOG])
    assert vector.applied[-1]["expected"] == ()  # all pending: no sink yet
    pending_id = stream["sources"][0]["id"]
    svc.approve(ADMIN, pending_id, approved=True)
    refreshed = svc.get_stream(DANA, stream["id"])
    assert refreshed["sources"][0]["status"] == "active"
    assert "auth_log" in vector.applied[-1]["rendered"]


def test_rejection_records_decision_without_reapply(env):
    svc, _, vector = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [AUTH_LOG])
    applies_before = len(vector.applied)
    svc.approve(ADMIN, stream["sources"][0]["id"], approved=False)
    refreshed = svc.get_stream(DANA, stream["id"])
    assert refreshed["sources"][0]["status"] == "rejected"
    assert refreshed["sources"][0]["decided_by"] == "admin@platform"
    assert len(vector.applied) == applies_before


def test_non_admin_cannot_approve(env):
    svc, _, _ = env
    stream = svc.create_stream(DANA, "mixed", "kinesis", [AUTH_LOG])
    with pytest.raises(StreamServiceError) as err:
        svc.approve(RAJ, stream["sources"][0]["id"], approved=True)
    assert err.value.status_code == 403


def test_unknown_source_is_404(env):
    svc, _, _ = env
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(DANA, "s", "kinesis", ["logging_demo.no.such"])
    assert err.value.status_code == 404


def test_duplicate_stream_name_is_409(env):
    svc, _, _ = env
    svc.create_stream(DANA, "dup", "kinesis", [SYSLOG])
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(RAJ, "dup", "sqs", [SYSLOG])
    assert err.value.status_code == 409


def test_ownership_enforced_on_reads_and_mutations(env):
    svc, _, _ = env
    stream = svc.create_stream(DANA, "private", "kinesis", [SYSLOG])
    with pytest.raises(StreamServiceError) as err:
        svc.get_stream(RAJ, stream["id"])
    assert err.value.status_code == 403


def test_provision_failure_marks_error_and_retry_recovers(env):
    svc, provisioner, _ = env
    provisioner.fail = True
    stream = svc.create_stream(DANA, "flaky", "kinesis", [SYSLOG])
    assert stream["status"] == "error"
    assert stream["last_error"] == "boom"
    assert stream["sources"] == []  # members only attach once resource exists
    provisioner.fail = False
    recovered = svc.retry(DANA, stream["id"])
    assert recovered["status"] == "live"
    assert recovered["resource_ref"] == "flaky"


def test_add_and_remove_sources_regenerate_fragment(env):
    svc, _, vector = env
    stream = svc.create_stream(DANA, "grow", "kinesis", [SYSLOG])
    svc.add_sources(DANA, stream["id"], ["logging_demo.acct_a__orders_api.syslog"])
    assert '.workload == "orders_api"' in vector.applied[-1]["rendered"]
    svc.remove_source(DANA, stream["id"], SYSLOG)
    assert '.workload == "storefront_web"' not in vector.applied[-1]["rendered"]
    refreshed = svc.get_stream(DANA, stream["id"])
    assert [s["source_fqn"] for s in refreshed["sources"]] == [
        "logging_demo.acct_a__orders_api.syslog"
    ]


def test_add_duplicate_source_is_409(env):
    svc, _, _ = env
    stream = svc.create_stream(DANA, "s", "kinesis", [SYSLOG])
    with pytest.raises(StreamServiceError) as err:
        svc.add_sources(DANA, stream["id"], [SYSLOG])
    assert err.value.status_code == 409


def test_delete_stream_reapplies_then_tears_down_resource(env):
    svc, provisioner, vector = env
    stream = svc.create_stream(DANA, "gone", "kinesis", [SYSLOG])
    svc.delete_stream(DANA, stream["id"])
    assert "gone" not in vector.applied[-1]["rendered"]
    assert provisioner.deleted == [("kinesis", "gone")]
    assert svc.list_streams(DANA) == []


def test_vector_rejection_surfaces_502_and_flags_stream(env):
    svc, _, vector = env
    vector.fail = True
    with pytest.raises(StreamServiceError) as err:
        svc.create_stream(DANA, "s", "kinesis", [SYSLOG])
    assert err.value.status_code == 502
    vector.fail = False
    [stream] = svc.list_streams(DANA)
    assert "vector said no" in stream["last_error"]
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_stream_service.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.streams.service'`

- [ ] **Step 3: Write `app/streams/service.py`**

```python
"""Fork lifecycle: resolve sources, gate by sensitivity, provision, reapply."""
from app.catalog.uc_client import CatalogUnavailable
from app.catalog.service import find_source
from app.streams.fragments import ForkSpec, Member, render_forks_config
from app.streams.provisioner import ProvisionError
from app.streams.vector_admin import VectorApplyError


class StreamServiceError(RuntimeError):
    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


class StreamService:
    def __init__(self, conn, catalog, provisioner, vector_admin, settings):
        self._conn = conn
        self._catalog = catalog
        self._provisioner = provisioner
        self._vector = vector_admin
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
        resolved = self._resolve(source_fqns)
        cur = self._conn.execute(
            "INSERT INTO streams (owner_id, name, type, status) VALUES (?, ?, ?, 'provisioning')",
            (user["id"], name, stream_type),
        )
        stream_id = cur.lastrowid
        try:
            ref = self._provisioner.create(stream_type, name)
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
        self._insert_members(stream_id, user, resolved)
        self._reapply_or_flag(stream_id)
        return self.get_stream(user, stream_id)

    def add_sources(self, user, stream_id, source_fqns) -> dict:
        stream = self.get_stream(user, stream_id)
        resolved = self._resolve(source_fqns)
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
        # Detach from the pipeline first, then tear down the resource.
        self.reapply()
        if stream["resource_ref"]:
            try:
                self._provisioner.delete(stream["type"], stream["resource_ref"])
            except ProvisionError as exc:
                # The fork is gone from the pipeline; a lingering LocalStack
                # resource is harmless. Record it and move on.
                self._conn.execute(
                    "UPDATE streams SET last_error = ? WHERE id = ?", (str(exc), stream_id)
                )

    def retry(self, user, stream_id) -> dict:
        stream = self.get_stream(user, stream_id)
        if stream["resource_ref"] is None:
            try:
                ref = self._provisioner.create(stream["type"], stream["name"])
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
    def _resolve(self, source_fqns) -> list[dict]:
        try:
            tree = self._catalog.get_tree()
        except CatalogUnavailable as exc:
            raise StreamServiceError(f"catalog unavailable: {exc}", 503)
        resolved = []
        for fqn in source_fqns:
            found = find_source(tree, fqn)
            if found is None:
                raise StreamServiceError(f"unknown source: {fqn}", 404)
            resolved.append(found)
        return resolved

    def _insert_members(self, stream_id, user, resolved) -> None:
        for src in resolved:
            # Sensitivity gate, enforced server-side from catalog metadata.
            status = "pending_approval" if src["sensitivity"] == "sensitive" else "active"
            self._conn.execute(
                "INSERT INTO stream_sources"
                " (stream_id, source_fqn, workload, source_name, status, requested_by)"
                " VALUES (?, ?, ?, ?, ?, ?)",
                (stream_id, src["fqn"], src["workload_tag"], src["source_name"], status, user["id"]),
            )

    def reapply(self) -> None:
        """Regenerate forks.yaml purely from current DB state and push it."""
        forks = []
        for stream in self._conn.execute("SELECT * FROM streams WHERE status = 'live'").fetchall():
            members = tuple(
                Member(r["workload"], r["source_name"])
                for r in self._conn.execute(
                    "SELECT workload, source_name FROM stream_sources"
                    " WHERE stream_id = ? AND status = 'active'"
                    " ORDER BY workload, source_name",
                    (stream["id"],),
                )
            )
            if members:
                forks.append(ForkSpec(stream["id"], stream["type"], stream["resource_ref"], members))
        rendered = render_forks_config(forks, self._settings.aws_endpoint, self._settings.aws_region)
        self._vector.apply(rendered, [f"fork_{f.stream_id}_sink" for f in forks])

    def _reapply_or_flag(self, stream_id) -> None:
        try:
            self.reapply()
        except VectorApplyError as exc:
            self._conn.execute(
                "UPDATE streams SET last_error = ? WHERE id = ?", (str(exc), stream_id)
            )
            raise StreamServiceError(f"vector rejected the generated config: {exc}", 502)
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `pytest tests/test_stream_service.py -q`
Expected: `13 passed`

- [ ] **Step 5: Run the whole backend suite**

Run: `pytest -q`
Expected: all tests pass, none skipped unexpectedly.

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/backend/app/streams/service.py logstream-portal/backend/tests/test_stream_service.py
git commit -m "feat(portal): stream service with sensitivity gate, approvals, and fragment reapply"
```

---

### Task 9: App factory, auth dependencies, and session routes

**Files:**
- Create: `logstream-portal/backend/app/routes/deps.py`
- Create: `logstream-portal/backend/app/routes/session.py`
- Create: `logstream-portal/backend/app/main.py`
- Test: `logstream-portal/backend/tests/conftest.py`, `logstream-portal/backend/tests/test_api.py` (session tests only in this task)

- [ ] **Step 1: Write `tests/conftest.py`** — shared fakes and app/client fixtures for all API tests

```python
import json
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from app.catalog.cache import SnapshotCache
from app.catalog.service import CatalogService
from app.catalog.uc_client import CatalogUnavailable
from app.config import Settings
from app.db import get_db
from app.main import create_app

FIXTURES = Path(__file__).resolve().parents[2] / "fixtures"

SYSLOG = "logging_demo.acct_a__storefront_web.syslog"
AUTH_LOG = "logging_demo.acct_a__orders_api.auth_log"


class DownUC:
    """Always-unreachable Databricks → app serves the bundled seed snapshot."""

    def list_schemas(self, catalog):
        raise CatalogUnavailable("databricks not reachable in tests")

    def list_tables(self, catalog, schema):
        raise CatalogUnavailable("databricks not reachable in tests")


class FakeProvisioner:
    def __init__(self):
        self.created, self.deleted, self.fail = [], [], False

    def create(self, stream_type, name):
        if self.fail:
            from app.streams.provisioner import ProvisionError
            raise ProvisionError("boom")
        self.created.append((stream_type, name))
        return name if stream_type == "kinesis" else f"http://q/{name}"

    def delete(self, stream_type, ref):
        self.deleted.append((stream_type, ref))


class FakeVectorAdmin:
    def __init__(self):
        self.applied = []

    def apply(self, rendered, expected):
        self.applied.append({"rendered": rendered, "expected": tuple(expected)})


class FakePeek:
    def peek(self, stream_type, ref, limit=5):
        return [{"message": "hello", "workload": "storefront_web", "source_name": "syslog"}]

    def flow_stats(self, stream_type, ref):
        return {"recent_records": 42}


@pytest.fixture()
def fakes(tmp_path):
    cache = SnapshotCache(tmp_path / "snap.json", seed_path=FIXTURES / "catalog_snapshot.json")
    return {
        "conn": get_db(":memory:"),
        "catalog": CatalogService(DownUC(), cache, "logging_demo"),
        "provisioner": FakeProvisioner(),
        "vector_admin": FakeVectorAdmin(),
        "peek": FakePeek(),
    }


@pytest.fixture()
def client(fakes, tmp_path):
    settings = Settings(
        databricks_host="", databricks_token="", uc_catalog="logging_demo",
        aws_endpoint="http://localstack:4566", aws_region="us-east-1",
        data_dir=str(tmp_path), fragments_path=str(tmp_path / "forks.yaml"),
        vector_api_url="http://v:8686/graphql", snapshot_seed="",
        static_dir="", session_secret="test-secret",
    )
    app = create_app(settings, services=fakes)
    with TestClient(app) as test_client:
        yield test_client


def login(client, user_id="dana@app-team"):
    resp = client.post("/api/session", json={"user_id": user_id})
    assert resp.status_code == 200, resp.text
    return resp.json()
```

- [ ] **Step 2: Write the session tests** — start `tests/test_api.py`

```python
from tests.conftest import AUTH_LOG, SYSLOG, login


def test_personas_lists_seeded_users_without_auth(client):
    resp = client.get("/api/personas")
    assert resp.status_code == 200
    ids = [u["id"] for u in resp.json()]
    assert "dana@app-team" in ids and "admin@platform" in ids


def test_login_sets_cookie_and_me_returns_user(client):
    user = login(client)
    assert user["role"] == "consumer"
    me = client.get("/api/session")
    assert me.status_code == 200
    assert me.json()["id"] == "dana@app-team"


def test_login_unknown_user_is_404(client):
    assert client.post("/api/session", json={"user_id": "ghost@nowhere"}).status_code == 404


def test_me_without_session_is_401(client):
    assert client.get("/api/session").status_code == 401


def test_logout_clears_session(client):
    login(client)
    assert client.delete("/api/session").status_code == 200
    assert client.get("/api/session").status_code == 401


def test_tampered_cookie_is_401(client):
    login(client)
    client.cookies.set("portal_session", "ImRhbmFAYXBwLXRlYW0i.forged")
    assert client.get("/api/session").status_code == 401
```

Note: `tests/__init__.py` is not needed — run pytest from `backend/` and the `from tests.conftest import` line works because pytest adds rootdir to `sys.path` (`rootdir` contains the `tests` package directory with `conftest.py`). If the import fails in your environment, add an empty `tests/__init__.py`.

- [ ] **Step 3: Run tests to verify they fail**

Run: `pytest tests/test_api.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'app.main'`

- [ ] **Step 4: Write `app/routes/deps.py`**

```python
from fastapi import Depends, HTTPException, Request
from itsdangerous import BadSignature, URLSafeSerializer

SESSION_COOKIE = "portal_session"


def serializer(request: Request) -> URLSafeSerializer:
    return URLSafeSerializer(request.app.state.settings.session_secret, salt="session")


def current_user(request: Request) -> dict:
    token = request.cookies.get(SESSION_COOKIE)
    if not token:
        raise HTTPException(401, "not logged in")
    try:
        user_id = serializer(request).loads(token)
    except BadSignature:
        raise HTTPException(401, "invalid session")
    row = request.app.state.conn.execute(
        "SELECT * FROM users WHERE id = ?", (user_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(401, "unknown user")
    return dict(row)


def admin_user(user: dict = Depends(current_user)) -> dict:
    if user["role"] != "admin":
        raise HTTPException(403, "admin only")
    return user
```

- [ ] **Step 5: Write `app/routes/session.py`**

```python
from fastapi import APIRouter, Depends, HTTPException, Request, Response
from pydantic import BaseModel

from app.routes.deps import SESSION_COOKIE, current_user, serializer

router = APIRouter()


class LoginBody(BaseModel):
    user_id: str


@router.get("/api/personas")
def personas(request: Request) -> list[dict]:
    rows = request.app.state.conn.execute(
        "SELECT * FROM users ORDER BY role, id"
    ).fetchall()
    return [dict(r) for r in rows]


@router.post("/api/session")
def login(body: LoginBody, request: Request, response: Response) -> dict:
    row = request.app.state.conn.execute(
        "SELECT * FROM users WHERE id = ?", (body.user_id,)
    ).fetchone()
    if row is None:
        raise HTTPException(404, "unknown persona")
    token = serializer(request).dumps(row["id"])
    response.set_cookie(SESSION_COOKIE, token, httponly=True, samesite="lax")
    return dict(row)


@router.get("/api/session")
def me(user: dict = Depends(current_user)) -> dict:
    return user


@router.delete("/api/session")
def logout(response: Response) -> dict:
    response.delete_cookie(SESSION_COOKIE)
    return {"ok": True}
```

- [ ] **Step 6: Write `app/main.py`** (routers for catalog/streams/approvals arrive in Task 10; import them here already — Task 10 creates the modules, so until then create minimal placeholder routers in those files as part of THIS step to keep the app importable: each file contains just `from fastapi import APIRouter` and `router = APIRouter()`)

`app/routes/catalog.py`, `app/routes/streams.py`, `app/routes/approvals.py` (placeholder content for now):

```python
from fastapi import APIRouter

router = APIRouter()
```

`app/main.py`:

```python
from pathlib import Path

import boto3
from fastapi import FastAPI
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.catalog.cache import SnapshotCache
from app.catalog.service import CatalogService
from app.catalog.uc_client import UCClient
from app.config import Settings, load_settings
from app.db import get_db, init_db
from app.routes import approvals as approvals_routes
from app.routes import catalog as catalog_routes
from app.routes import session as session_routes
from app.routes import streams as streams_routes
from app.streams.peek import PeekService
from app.streams.provisioner import Provisioner
from app.streams.service import StreamService
from app.streams.vector_admin import VectorAdmin


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

    if "provisioner" in services:
        app.state.provisioner = services["provisioner"]
        app.state.peek = services["peek"]
    else:
        aws_kwargs = dict(
            endpoint_url=settings.aws_endpoint,
            region_name=settings.aws_region,
            aws_access_key_id="test",
            aws_secret_access_key="test",
        )
        kinesis = boto3.client("kinesis", **aws_kwargs)
        sqs = boto3.client("sqs", **aws_kwargs)
        app.state.provisioner = Provisioner(kinesis, sqs)
        app.state.peek = PeekService(kinesis, sqs)

    app.state.vector_admin = services.get("vector_admin") or VectorAdmin(
        settings.fragments_path, settings.vector_api_url
    )
    app.state.streams = StreamService(
        conn, app.state.catalog, app.state.provisioner, app.state.vector_admin, settings
    )

    app.include_router(session_routes.router)
    app.include_router(catalog_routes.router)
    app.include_router(streams_routes.router)
    app.include_router(approvals_routes.router)

    @app.on_event("startup")
    def resync_fragment() -> None:
        # Make forks.yaml reflect DB state after restarts. Best-effort: the
        # aggregator may still be booting; any later mutation re-syncs.
        try:
            app.state.streams.reapply()
        except Exception:
            pass

    if settings.static_dir and Path(settings.static_dir).is_dir():
        static = Path(settings.static_dir)
        app.mount("/assets", StaticFiles(directory=static / "assets"), name="assets")

        @app.get("/{full_path:path}", include_in_schema=False)
        def spa(full_path: str) -> FileResponse:
            # API routes were registered first and win; everything else is the SPA.
            return FileResponse(static / "index.html")

    return app
```

- [ ] **Step 7: Run tests to verify they pass**

Run: `pytest tests/test_api.py -q`
Expected: `6 passed`

- [ ] **Step 8: Commit**

```bash
git add logstream-portal/backend/app/main.py logstream-portal/backend/app/routes logstream-portal/backend/tests/conftest.py logstream-portal/backend/tests/test_api.py
git commit -m "feat(portal): app factory with service injection, signed-cookie sessions"
```

---

### Task 10: Catalog, streams, and approvals routes (API integration)

**Files:**
- Modify: `logstream-portal/backend/app/routes/catalog.py` (replace placeholder)
- Modify: `logstream-portal/backend/app/routes/streams.py` (replace placeholder)
- Modify: `logstream-portal/backend/app/routes/approvals.py` (replace placeholder)
- Test: append to `logstream-portal/backend/tests/test_api.py`

- [ ] **Step 1: Append the failing integration tests to `tests/test_api.py`**

```python
# ── catalog ──────────────────────────────────────────────────────────


def test_catalog_requires_auth(client):
    assert client.get("/api/catalog").status_code == 401


def test_catalog_serves_stale_snapshot_with_annotations(client):
    login(client)
    resp = client.get("/api/catalog")
    assert resp.status_code == 200
    tree = resp.json()
    assert tree["stale"] is True  # DownUC forces the bundled-seed fallback path
    assert len(tree["accounts"]) == 2
    first_source = tree["accounts"][0]["workloads"][0]["sources"][0]
    assert first_source["subscriptions"] == []


def test_catalog_shows_my_subscriptions_after_fork(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]})
    tree = client.get("/api/catalog").json()
    web = [w for a in tree["accounts"] for w in a["workloads"] if w["name"] == "storefront_web"][0]
    syslog = [s for s in web["sources"] if s["name"] == "syslog"][0]
    assert syslog["subscriptions"] == [{"stream_id": 1, "stream_name": "s1", "status": "active"}]


# ── streams ──────────────────────────────────────────────────────────


def test_fork_flow_standard_and_sensitive(client, fakes):
    login(client)
    resp = client.post(
        "/api/streams",
        json={"name": "mixed", "type": "kinesis", "source_fqns": [SYSLOG, AUTH_LOG]},
    )
    assert resp.status_code == 201, resp.text
    stream = resp.json()
    assert stream["status"] == "live"
    statuses = {s["source_fqn"]: s["status"] for s in stream["sources"]}
    assert statuses == {SYSLOG: "active", AUTH_LOG: "pending_approval"}
    assert fakes["provisioner"].created == [("kinesis", "mixed")]
    assert "auth_log" not in fakes["vector_admin"].applied[-1]["rendered"]


def test_list_streams_includes_flow_stats(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]})
    [stream] = client.get("/api/streams").json()
    assert stream["flow"] == {"recent_records": 42}


def test_peek_returns_sample_records(client):
    login(client)
    created = client.post(
        "/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    records = client.get(f"/api/streams/{created['id']}/peek").json()
    assert records[0]["workload"] == "storefront_web"


def test_streams_are_private_to_owner(client):
    login(client, "dana@app-team")
    created = client.post(
        "/api/streams", json={"name": "private", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    login(client, "raj@data-sci")
    assert client.get("/api/streams").json() == []
    assert client.get(f"/api/streams/{created['id']}/peek").status_code == 403


def test_add_remove_delete_stream(client, fakes):
    login(client)
    created = client.post(
        "/api/streams", json={"name": "s1", "type": "sqs", "source_fqns": [SYSLOG]}
    ).json()
    sid = created["id"]
    add = client.post(
        f"/api/streams/{sid}/sources",
        json={"source_fqns": ["logging_demo.acct_a__orders_api.syslog"]},
    )
    assert add.status_code == 200
    assert len(add.json()["sources"]) == 2
    from urllib.parse import quote
    rm = client.delete(f"/api/streams/{sid}/sources/{quote(SYSLOG, safe='')}")
    assert rm.status_code == 200
    assert len(rm.json()["sources"]) == 1
    assert client.delete(f"/api/streams/{sid}").status_code == 204
    assert client.get("/api/streams").json() == []
    assert fakes["provisioner"].deleted == [("sqs", "http://q/s1")]


def test_retry_after_provision_failure(client, fakes):
    login(client)
    fakes["provisioner"].fail = True
    created = client.post(
        "/api/streams", json={"name": "flaky", "type": "kinesis", "source_fqns": [SYSLOG]}
    ).json()
    assert created["status"] == "error"
    fakes["provisioner"].fail = False
    retried = client.post(f"/api/streams/{created['id']}/retry")
    assert retried.status_code == 200
    assert retried.json()["status"] == "live"


# ── approvals ────────────────────────────────────────────────────────


def test_approval_queue_visible_to_admin_only(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [AUTH_LOG]})
    assert client.get("/api/approvals").status_code == 403
    login(client, "admin@platform")
    queue = client.get("/api/approvals").json()
    assert len(queue) == 1
    assert queue[0]["source_fqn"] == AUTH_LOG
    assert queue[0]["stream_name"] == "s1"
    assert queue[0]["requested_by"] == "dana@app-team"


def test_approve_activates_source_for_requester(client, fakes):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [AUTH_LOG]})
    login(client, "admin@platform")
    [item] = client.get("/api/approvals").json()
    resp = client.post(f"/api/approvals/{item['id']}", json={"approved": True})
    assert resp.status_code == 200
    assert client.get("/api/approvals").json() == []
    login(client, "dana@app-team")
    [stream] = client.get("/api/streams").json()
    assert stream["sources"][0]["status"] == "active"
    assert "auth_log" in fakes["vector_admin"].applied[-1]["rendered"]


def test_reject_marks_source_rejected(client):
    login(client)
    client.post("/api/streams", json={"name": "s1", "type": "kinesis", "source_fqns": [AUTH_LOG]})
    login(client, "admin@platform")
    [item] = client.get("/api/approvals").json()
    client.post(f"/api/approvals/{item['id']}", json={"approved": False})
    login(client, "dana@app-team")
    [stream] = client.get("/api/streams").json()
    assert stream["sources"][0]["status"] == "rejected"
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `pytest tests/test_api.py -q`
Expected: FAIL — the new tests 404 (placeholder routers have no routes); the 6 session tests still pass.

- [ ] **Step 3: Replace `app/routes/catalog.py`**

```python
from fastapi import APIRouter, Depends, HTTPException, Request

from app.catalog.service import annotate
from app.catalog.uc_client import CatalogUnavailable
from app.routes.deps import current_user

router = APIRouter()


@router.get("/api/catalog")
def get_catalog(request: Request, user: dict = Depends(current_user)) -> dict:
    try:
        tree = request.app.state.catalog.get_tree()
    except CatalogUnavailable as exc:
        raise HTTPException(503, f"catalog unavailable and no cached snapshot: {exc}")
    rows = request.app.state.conn.execute(
        "SELECT ss.source_fqn, ss.status, s.id AS stream_id, s.name AS stream_name"
        " FROM stream_sources ss JOIN streams s ON s.id = ss.stream_id"
        " WHERE s.owner_id = ? AND s.status != 'deleted'",
        (user["id"],),
    ).fetchall()
    return annotate(tree, [dict(r) for r in rows])
```

- [ ] **Step 4: Replace `app/routes/streams.py`**

```python
from typing import Literal

from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.routes.deps import current_user
from app.streams.service import StreamServiceError

router = APIRouter()


class CreateStreamBody(BaseModel):
    name: str
    type: Literal["kinesis", "sqs"]
    source_fqns: list[str]


class AddSourcesBody(BaseModel):
    source_fqns: list[str]


def _call(fn):
    try:
        return fn()
    except StreamServiceError as exc:
        raise HTTPException(exc.status_code, str(exc))


@router.get("/api/streams")
def list_streams(request: Request, user: dict = Depends(current_user)) -> list[dict]:
    streams = _call(lambda: request.app.state.streams.list_streams(user))
    peek = request.app.state.peek
    for stream in streams:
        stream["flow"] = None
        if stream["status"] == "live":
            try:
                stream["flow"] = peek.flow_stats(stream["type"], stream["resource_ref"])
            except Exception:
                pass  # flow stats are decorative; never fail the listing
    return streams


@router.post("/api/streams", status_code=201)
def create_stream(
    body: CreateStreamBody, request: Request, user: dict = Depends(current_user)
) -> dict:
    return _call(
        lambda: request.app.state.streams.create_stream(
            user, body.name, body.type, body.source_fqns
        )
    )


@router.delete("/api/streams/{stream_id}", status_code=204)
def delete_stream(stream_id: int, request: Request, user: dict = Depends(current_user)) -> None:
    _call(lambda: request.app.state.streams.delete_stream(user, stream_id))


@router.post("/api/streams/{stream_id}/retry")
def retry_stream(stream_id: int, request: Request, user: dict = Depends(current_user)) -> dict:
    return _call(lambda: request.app.state.streams.retry(user, stream_id))


@router.post("/api/streams/{stream_id}/sources")
def add_sources(
    stream_id: int, body: AddSourcesBody, request: Request, user: dict = Depends(current_user)
) -> dict:
    return _call(
        lambda: request.app.state.streams.add_sources(user, stream_id, body.source_fqns)
    )


@router.delete("/api/streams/{stream_id}/sources/{source_fqn:path}")
def remove_source(
    stream_id: int, source_fqn: str, request: Request, user: dict = Depends(current_user)
) -> dict:
    return _call(
        lambda: request.app.state.streams.remove_source(user, stream_id, source_fqn)
    )


@router.get("/api/streams/{stream_id}/peek")
def peek_stream(stream_id: int, request: Request, user: dict = Depends(current_user)) -> list:
    stream = _call(lambda: request.app.state.streams.get_stream(user, stream_id))
    if stream["status"] != "live":
        raise HTTPException(409, "stream is not live")
    try:
        return request.app.state.peek.peek(stream["type"], stream["resource_ref"])
    except Exception as exc:
        raise HTTPException(502, f"could not read stream: {exc}")
```

- [ ] **Step 5: Replace `app/routes/approvals.py`**

```python
from fastapi import APIRouter, Depends, HTTPException, Request
from pydantic import BaseModel

from app.routes.deps import admin_user
from app.streams.service import StreamServiceError

router = APIRouter()


class DecisionBody(BaseModel):
    approved: bool


@router.get("/api/approvals")
def list_approvals(request: Request, admin: dict = Depends(admin_user)) -> list[dict]:
    rows = request.app.state.conn.execute(
        "SELECT ss.id, ss.stream_id, s.name AS stream_name, ss.source_fqn,"
        "       ss.requested_by, ss.requested_at"
        " FROM stream_sources ss JOIN streams s ON s.id = ss.stream_id"
        " WHERE ss.status = 'pending_approval' AND s.status != 'deleted'"
        " ORDER BY ss.requested_at, ss.id"
    ).fetchall()
    return [dict(r) for r in rows]


@router.post("/api/approvals/{request_id}")
def decide(
    request_id: int, body: DecisionBody, request: Request, admin: dict = Depends(admin_user)
) -> dict:
    try:
        request.app.state.streams.approve(admin, request_id, body.approved)
    except StreamServiceError as exc:
        raise HTTPException(exc.status_code, str(exc))
    return {"ok": True}
```

- [ ] **Step 6: Run the full backend suite**

Run: `pytest -q`
Expected: all tests pass (session, catalog, streams, approvals, plus all unit suites).

- [ ] **Step 7: Commit**

```bash
git add logstream-portal/backend/app/routes logstream-portal/backend/tests/test_api.py
git commit -m "feat(portal): catalog, streams, and approvals API routes"
```

---

### Task 11: Vector fleet configs (agents A/B, aggregator, bootstrap fragment)

**Files:**
- Create: `logstream-portal/vector/agent-acct-a/agent.yaml`
- Create: `logstream-portal/vector/agent-acct-b/agent.yaml`
- Create: `logstream-portal/vector/aggregator/aggregator.yaml`
- Create: `logstream-portal/vector/aggregator/fragments/forks.yaml`

Every event is tagged at the agent with the same five fields the catalog promises (`account_id`, `account_alias`, `environment`, `workload`, `source_name`) — the aggregator's fork filters match on `workload` + `source_name`.

- [ ] **Step 1: Write `vector/agent-acct-a/agent.yaml`**

```yaml
# Simulates AWS Account A (111111111111, prod-ecommerce).
sources:
  storefront_web_syslog:
    type: demo_logs
    format: syslog
    interval: 0.5
  storefront_web_nginx_access:
    type: demo_logs
    format: apache_common
    interval: 0.25
  orders_api_syslog:
    type: demo_logs
    format: syslog
    interval: 0.7
  orders_api_auth_log:
    type: demo_logs
    format: shuffle
    interval: 2
    lines:
      - "sshd[2103]: Failed password for invalid user admin from 203.0.113.7 port 51122 ssh2"
      - "sshd[2742]: Accepted publickey for deploy from 10.0.4.21 port 40022 ssh2"
      - "sudo: pam_unix(sudo:session): session opened for user root by deploy(uid=1002)"
      - "sshd[3017]: Invalid user oracle from 198.51.100.23 port 44210"
  orders_api_app_log:
    type: demo_logs
    format: json
    interval: 1

transforms:
  tag_storefront_web_syslog:
    type: remap
    inputs: ["storefront_web_syslog"]
    source: |
      .account_id = "111111111111"
      .account_alias = "prod-ecommerce"
      .environment = "prod"
      .workload = "storefront_web"
      .source_name = "syslog"
  tag_storefront_web_nginx_access:
    type: remap
    inputs: ["storefront_web_nginx_access"]
    source: |
      .account_id = "111111111111"
      .account_alias = "prod-ecommerce"
      .environment = "prod"
      .workload = "storefront_web"
      .source_name = "nginx_access"
  tag_orders_api_syslog:
    type: remap
    inputs: ["orders_api_syslog"]
    source: |
      .account_id = "111111111111"
      .account_alias = "prod-ecommerce"
      .environment = "prod"
      .workload = "orders_api"
      .source_name = "syslog"
  tag_orders_api_auth_log:
    type: remap
    inputs: ["orders_api_auth_log"]
    source: |
      .account_id = "111111111111"
      .account_alias = "prod-ecommerce"
      .environment = "prod"
      .workload = "orders_api"
      .source_name = "auth_log"
  tag_orders_api_app_log:
    type: remap
    inputs: ["orders_api_app_log"]
    source: |
      .account_id = "111111111111"
      .account_alias = "prod-ecommerce"
      .environment = "prod"
      .workload = "orders_api"
      .source_name = "app_log"

sinks:
  to_aggregator:
    type: vector
    inputs: ["tag_*"]
    address: "vector-aggregator:6000"
```

- [ ] **Step 2: Write `vector/agent-acct-b/agent.yaml`**

```yaml
# Simulates AWS Account B (222222222222, prod-platform).
sources:
  identity_svc_syslog:
    type: demo_logs
    format: syslog
    interval: 0.8
  identity_svc_auth_log:
    type: demo_logs
    format: shuffle
    interval: 1.5
    lines:
      - "sshd[418]: Accepted password for svc-identity from 10.1.2.8 port 50122 ssh2"
      - "sshd[562]: Failed password for root from 192.0.2.41 port 60317 ssh2"
      - "sudo: pam_unix(sudo:auth): authentication failure; logname=ops uid=1004"
      - "sshd[610]: pam_unix(sshd:session): session closed for user svc-identity"
  batch_etl_syslog:
    type: demo_logs
    format: syslog
    interval: 2
  batch_etl_cron_log:
    type: demo_logs
    format: shuffle
    interval: 5
    lines:
      - "CRON[1882]: (etl) CMD (/opt/batch/run_etl.sh daily)"
      - "CRON[1990]: (etl) CMD (/opt/batch/compact_partitions.sh)"
      - "CRON[2034]: (root) CMD (logrotate /etc/logrotate.conf)"

transforms:
  tag_identity_svc_syslog:
    type: remap
    inputs: ["identity_svc_syslog"]
    source: |
      .account_id = "222222222222"
      .account_alias = "prod-platform"
      .environment = "prod"
      .workload = "identity_svc"
      .source_name = "syslog"
  tag_identity_svc_auth_log:
    type: remap
    inputs: ["identity_svc_auth_log"]
    source: |
      .account_id = "222222222222"
      .account_alias = "prod-platform"
      .environment = "prod"
      .workload = "identity_svc"
      .source_name = "auth_log"
  tag_batch_etl_syslog:
    type: remap
    inputs: ["batch_etl_syslog"]
    source: |
      .account_id = "222222222222"
      .account_alias = "prod-platform"
      .environment = "prod"
      .workload = "batch_etl"
      .source_name = "syslog"
  tag_batch_etl_cron_log:
    type: remap
    inputs: ["batch_etl_cron_log"]
    source: |
      .account_id = "222222222222"
      .account_alias = "prod-platform"
      .environment = "prod"
      .workload = "batch_etl"
      .source_name = "cron_log"

sinks:
  to_aggregator:
    type: vector
    inputs: ["tag_*"]
    address: "vector-aggregator:6000"
```

- [ ] **Step 3: Write `vector/aggregator/aggregator.yaml`**

```yaml
# Central aggregator: receives from all account agents, archives everything,
# and hosts the portal-managed fork fragments (--config-dir fragments/).
api:
  enabled: true
  address: "0.0.0.0:8686"

sources:
  from_agents:
    type: vector
    address: "0.0.0.0:6000"

transforms:
  tagged:
    type: remap
    inputs: ["from_agents"]
    source: |
      .ingested_at = now()

sinks:
  archive:
    type: aws_s3
    inputs: ["tagged"]
    bucket: "log-archive"
    key_prefix: "raw/%Y-%m-%d/"
    region: "us-east-1"
    endpoint: "http://localstack:4566"
    force_path_style: true
    auth:
      access_key_id: "test"
      secret_access_key: "test"
    encoding:
      codec: json
```

- [ ] **Step 4: Write the bootstrap `vector/aggregator/fragments/forks.yaml`**

This committed file must exist before the aggregator first boots so `--watch-config` has a file to watch; the portal overwrites it at runtime (expect it to show as modified in `git status` while the demo runs — that's by design; `git update-index --skip-worktree` it if the noise bothers you). Content is exactly the renderer's empty output:

```yaml
# GENERATED BY logstream-portal -- DO NOT EDIT
# Regenerated wholesale from portal DB state on every change.
transforms:
  forks_noop:
    type: filter
    inputs: ["tagged"]
    condition: "false"
sinks:
  forks_noop_sink:
    type: blackhole
    inputs: ["forks_noop"]
    print_interval_secs: 0
```

- [ ] **Step 5: Validate all four configs with Vector itself**

Run (from `logstream-portal/`):

```bash
docker run --rm -v "$PWD/vector:/v:ro" timberio/vector:latest-debian \
  validate --no-environment /v/agent-acct-a/agent.yaml
docker run --rm -v "$PWD/vector:/v:ro" timberio/vector:latest-debian \
  validate --no-environment /v/agent-acct-b/agent.yaml
docker run --rm -v "$PWD/vector:/v:ro" timberio/vector:latest-debian \
  validate --no-environment /v/aggregator/aggregator.yaml /v/aggregator/fragments/forks.yaml
```

Expected: `Validated` for each invocation. If `tag_*` glob inputs are rejected by your Vector version, replace the glob with the explicit list of transform names.

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/vector
git commit -m "feat(portal): vector agent/aggregator configs with bootstrap fork fragment"
```

---

### Task 12: docker-compose, Dockerfile, LocalStack init

**Files:**
- Create: `logstream-portal/docker-compose.yml`
- Create: `logstream-portal/Dockerfile`
- Create: `logstream-portal/localstack-init/init-aws.sh`

- [ ] **Step 1: Write `localstack-init/init-aws.sh`** (LocalStack runs everything in `/etc/localstack/init/ready.d/` once services are up)

```bash
#!/usr/bin/env bash
set -euo pipefail
awslocal s3 mb s3://log-archive
echo "log-archive bucket ready"
```

Run: `chmod +x logstream-portal/localstack-init/init-aws.sh`

- [ ] **Step 2: Write `logstream-portal/Dockerfile`** (multi-stage: frontend build → backend runtime; the frontend exists from Task 14 onward — building before then uses the placeholder Vite scaffold, which is fine)

```dockerfile
FROM node:20-slim AS web
WORKDIR /web
COPY frontend/package.json frontend/package-lock.json ./
RUN npm ci
COPY frontend/ ./
RUN npm run build

FROM python:3.12-slim
WORKDIR /app
COPY backend/ ./backend/
RUN pip install --no-cache-dir ./backend
COPY fixtures/ ./fixtures/
COPY --from=web /web/dist ./static
ENV PORTAL_STATIC_DIR=/app/static
EXPOSE 8000
CMD ["uvicorn", "app.main:create_app", "--factory", "--host", "0.0.0.0", "--port", "8000"]
```

- [ ] **Step 3: Write `logstream-portal/docker-compose.yml`**

```yaml
services:
  localstack:
    image: localstack/localstack:3
    environment:
      - SERVICES=kinesis,sqs,s3
    ports:
      - "4566:4566"
    volumes:
      - ./localstack-init:/etc/localstack/init/ready.d:ro

  vector-aggregator:
    image: timberio/vector:latest-debian
    command:
      - "--config"
      - "/etc/vector/aggregator.yaml"
      - "--config-dir"
      - "/etc/vector/fragments"
      - "--watch-config"
    volumes:
      - ./vector/aggregator/aggregator.yaml:/etc/vector/aggregator.yaml:ro
      - ./vector/aggregator/fragments:/etc/vector/fragments
    ports:
      - "8686:8686"
    depends_on:
      - localstack

  vector-agent-acct-a:
    image: timberio/vector:latest-debian
    command: ["--config", "/etc/vector/agent.yaml"]
    volumes:
      - ./vector/agent-acct-a/agent.yaml:/etc/vector/agent.yaml:ro
    depends_on:
      - vector-aggregator

  vector-agent-acct-b:
    image: timberio/vector:latest-debian
    command: ["--config", "/etc/vector/agent.yaml"]
    volumes:
      - ./vector/agent-acct-b/agent.yaml:/etc/vector/agent.yaml:ro
    depends_on:
      - vector-aggregator

  portal:
    build: .
    ports:
      - "8000:8000"
    env_file:
      - .env
    environment:
      - AWS_ENDPOINT_URL=http://localstack:4566
      - AWS_REGION=us-east-1
      - VECTOR_API_URL=http://vector-aggregator:8686/graphql
      - VECTOR_FRAGMENTS_PATH=/vector-fragments/forks.yaml
      - PORTAL_DATA_DIR=/data
      - CATALOG_SNAPSHOT_SEED=/app/fixtures/catalog_snapshot.json
    volumes:
      - ./vector/aggregator/fragments:/vector-fragments
      - portal-data:/data

volumes:
  portal-data: {}
```

Note: `.env` must exist (`cp .env.example .env`) even if you leave the Databricks values as placeholders — the portal then runs on the bundled snapshot with the stale banner, which is exactly the offline-demo mode the e2e test uses.

- [ ] **Step 4: Pin the Vector image**

Run: `docker compose pull vector-aggregator && docker image inspect timberio/vector:latest-debian --format '{{index .RepoTags 0}} {{.Created}}'`
Then replace all three `timberio/vector:latest-debian` references in `docker-compose.yml` (and the validate commands you use locally) with the current explicit version tag (e.g. `timberio/vector:0.4X.Y-debian` — check `docker run --rm timberio/vector:latest-debian --version`). Reproducible demos beat floating tags.

- [ ] **Step 5: Boot the stack and verify the pipeline end-to-end manually**

```bash
cd logstream-portal
cp .env.example .env   # placeholders fine for this check
docker compose up -d --build
sleep 20
# Aggregator API is up and serving components:
curl -s http://localhost:8686/graphql -H 'Content-Type: application/json' \
  -d '{"query":"{ components(first: 100) { edges { node { componentId } } } }"}' | python3 -m json.tool
# Archive sink is receiving (objects appear after ~1 min batch flush):
docker compose exec localstack awslocal s3 ls s3://log-archive/ --recursive | head
```

Expected: GraphQL response lists `from_agents`, `tagged`, `archive`, `forks_noop`, `forks_noop_sink`; S3 listing shows objects under `raw/`.
(`portal` may crash-loop until the frontend exists in Task 14 — that's fine for this step; check `docker compose logs portal` only for import errors, not build errors.)

- [ ] **Step 6: Commit**

```bash
git add logstream-portal/docker-compose.yml logstream-portal/Dockerfile logstream-portal/localstack-init
git commit -m "feat(portal): docker-compose stack with localstack and vector fleet"
```

---

### Task 13: Unity Catalog seed script

**Files:**
- Create: `logstream-portal/scripts/seed_catalog.py`

- [ ] **Step 1: Write `scripts/seed_catalog.py`**

```python
"""Seed the demo log-source inventory into Databricks Unity Catalog.

One-time setup. Requires: pip install databricks-sdk
Env: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
     UC_CATALOG_NAME (optional, default logging_demo)

The created tables are metadata-only (no rows): their columns document the
event schema and their TBLPROPERTIES carry the operational metadata the
portal reads. This script is the source of truth that
fixtures/catalog_snapshot.json mirrors — keep them in sync.
"""
import os

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState

CATALOG = os.environ.get("UC_CATALOG_NAME", "logging_demo")

BASE_COLUMNS = (
    "ts TIMESTAMP COMMENT 'event time', "
    "host STRING COMMENT 'origin host', "
    "severity STRING COMMENT 'syslog severity', "
    "message STRING COMMENT 'raw log line'"
)

WORKLOADS = [
    {
        "schema": "acct_a__storefront_web", "workload": "storefront_web",
        "account_id": "111111111111", "account_alias": "prod-ecommerce",
        "sources": [
            {"name": "syslog", "log_type": "system", "sensitivity": "standard",
             "volume": 1200, "comment": "Host syslog from storefront web tier"},
            {"name": "nginx_access", "log_type": "web", "sensitivity": "standard",
             "volume": 2400, "comment": "Nginx access logs from storefront web tier"},
        ],
    },
    {
        "schema": "acct_a__orders_api", "workload": "orders_api",
        "account_id": "111111111111", "account_alias": "prod-ecommerce",
        "sources": [
            {"name": "syslog", "log_type": "system", "sensitivity": "standard",
             "volume": 900, "comment": "Host syslog from orders API tier"},
            {"name": "auth_log", "log_type": "system", "sensitivity": "sensitive",
             "volume": 300, "comment": "SSH/sudo/PAM auth events"},
            {"name": "app_log", "log_type": "application", "sensitivity": "standard",
             "volume": 600, "comment": "Structured application logs"},
        ],
    },
    {
        "schema": "acct_b__identity_svc", "workload": "identity_svc",
        "account_id": "222222222222", "account_alias": "prod-platform",
        "sources": [
            {"name": "syslog", "log_type": "system", "sensitivity": "standard",
             "volume": 800, "comment": "Host syslog from identity service"},
            {"name": "auth_log", "log_type": "system", "sensitivity": "sensitive",
             "volume": 450, "comment": "SSH/sudo/PAM auth events"},
        ],
    },
    {
        "schema": "acct_b__batch_etl", "workload": "batch_etl",
        "account_id": "222222222222", "account_alias": "prod-platform",
        "sources": [
            {"name": "syslog", "log_type": "system", "sensitivity": "standard",
             "volume": 300, "comment": "Host syslog from batch ETL hosts"},
            {"name": "cron_log", "log_type": "system", "sensitivity": "standard",
             "volume": 120, "comment": "Cron scheduler logs from ETL hosts"},
        ],
    },
]


def main() -> None:
    workspace = WorkspaceClient()  # reads DATABRICKS_HOST / DATABRICKS_TOKEN
    warehouse = os.environ["DATABRICKS_WAREHOUSE_ID"]

    def sql(statement: str) -> None:
        result = workspace.statement_execution.execute_statement(
            warehouse_id=warehouse, statement=statement, wait_timeout="50s"
        )
        if result.status.state != StatementState.SUCCEEDED:
            raise SystemExit(f"FAILED: {statement}\n{result.status}")
        print(f"ok: {statement[:80]}")

    sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    for wl in WORKLOADS:
        sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{wl['schema']}")
        for src in wl["sources"]:
            sql(
                f"CREATE TABLE IF NOT EXISTS {CATALOG}.{wl['schema']}.{src['name']}"
                f" ({BASE_COLUMNS}) COMMENT '{src['comment']}' TBLPROPERTIES ("
                f"'sensitivity' = '{src['sensitivity']}',"
                f" 'log_type' = '{src['log_type']}',"
                f" 'account_id' = '{wl['account_id']}',"
                f" 'account_alias' = '{wl['account_alias']}',"
                f" 'workload' = '{wl['workload']}',"
                f" 'environment' = 'prod',"
                f" 'source_name' = '{src['name']}',"
                f" 'est_volume_per_min' = '{src['volume']}')"
            )
    total = sum(len(wl["sources"]) for wl in WORKLOADS)
    print(f"Seeded catalog {CATALOG!r}: {len(WORKLOADS)} workloads, {total} sources.")


if __name__ == "__main__":
    main()
```

- [ ] **Step 2: Run it against the real workspace** (requires `.env` values exported; skip in CI)

```bash
cd logstream-portal
set -a && source .env && set +a
pip install databricks-sdk
python3 scripts/seed_catalog.py
```

Expected: `ok:` line per statement, final `Seeded catalog 'logging_demo': 4 workloads, 9 sources.`
Then verify the live read path: restart the portal (`docker compose restart portal`), log in, and the catalog banner should disappear (fresh `as_of`, `stale: false`).

- [ ] **Step 3: Commit**

```bash
git add logstream-portal/scripts/seed_catalog.py
git commit -m "feat(portal): unity catalog seed script for demo inventory"
```

---

### Task 14: Frontend scaffold — Vite app, API client, types, shell, login

**Files:**
- Create: `logstream-portal/frontend/package.json`, `vite.config.ts`, `tsconfig.json`, `index.html`
- Create: `logstream-portal/frontend/src/main.tsx`, `src/api.ts`, `src/types.ts`, `src/styles.css`, `src/test-setup.ts`, `src/App.tsx`, `src/pages/LoginPage.tsx`

- [ ] **Step 1: Write `frontend/package.json`**

```json
{
  "name": "logstream-portal-web",
  "private": true,
  "version": "0.1.0",
  "type": "module",
  "scripts": {
    "dev": "vite",
    "build": "vite build",
    "test": "vitest run"
  },
  "dependencies": {
    "react": "^18.3.0",
    "react-dom": "^18.3.0",
    "react-router-dom": "^6.26.0"
  },
  "devDependencies": {
    "@testing-library/jest-dom": "^6.4.0",
    "@testing-library/react": "^16.0.0",
    "@types/react": "^18.3.0",
    "@types/react-dom": "^18.3.0",
    "@vitejs/plugin-react": "^4.3.0",
    "jsdom": "^25.0.0",
    "typescript": "^5.5.0",
    "vite": "^5.4.0",
    "vitest": "^2.0.0"
  }
}
```

- [ ] **Step 2: Write `frontend/vite.config.ts`**

```ts
/// <reference types="vitest" />
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  plugins: [react()],
  server: { proxy: { '/api': 'http://localhost:8000' } },
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: './src/test-setup.ts',
  },
})
```

- [ ] **Step 3: Write `frontend/tsconfig.json`**

```json
{
  "compilerOptions": {
    "target": "ES2020",
    "lib": ["ES2020", "DOM", "DOM.Iterable"],
    "module": "ESNext",
    "moduleResolution": "bundler",
    "jsx": "react-jsx",
    "strict": true,
    "skipLibCheck": true,
    "noEmit": true,
    "types": ["vitest/globals", "@testing-library/jest-dom"]
  },
  "include": ["src"]
}
```

- [ ] **Step 4: Write `frontend/index.html`**

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <meta name="viewport" content="width=device-width, initial-scale=1.0" />
    <title>LogStream Portal</title>
  </head>
  <body>
    <div id="root"></div>
    <script type="module" src="/src/main.tsx"></script>
  </body>
</html>
```

- [ ] **Step 5: Write `frontend/src/test-setup.ts`**

```ts
import '@testing-library/jest-dom'
```

- [ ] **Step 6: Write `frontend/src/api.ts`**

```ts
export async function api<T>(path: string, opts?: RequestInit): Promise<T> {
  const res = await fetch(path, {
    headers: { 'Content-Type': 'application/json' },
    credentials: 'same-origin',
    ...opts,
  })
  if (!res.ok) {
    const detail = await res
      .json()
      .then((b: { detail?: string }) => b.detail)
      .catch(() => undefined)
    throw new Error(detail ?? `${res.status} ${res.statusText}`)
  }
  if (res.status === 204) return undefined as T
  return res.json() as Promise<T>
}
```

- [ ] **Step 7: Write `frontend/src/types.ts`**

```ts
export interface User {
  id: string
  display_name: string
  team: string
  role: 'consumer' | 'admin'
}

export interface SubRef {
  stream_id: number
  stream_name: string
  status: string
}

export interface Source {
  fqn: string
  name: string
  log_type: string
  sensitivity: 'standard' | 'sensitive'
  est_volume_per_min: number
  description: string
  columns: { name: string; type: string }[]
  subscriptions: SubRef[]
}

export interface Workload {
  name: string
  schema: string
  environment: string
  sources: Source[]
}

export interface Account {
  account_id: string
  account_alias: string
  workloads: Workload[]
}

export interface Catalog {
  as_of: string
  stale: boolean
  accounts: Account[]
}

export interface StreamSource {
  id: number
  source_fqn: string
  workload: string
  source_name: string
  status: 'active' | 'pending_approval' | 'rejected'
}

export interface Stream {
  id: number
  name: string
  type: 'kinesis' | 'sqs'
  status: 'provisioning' | 'live' | 'error' | 'deleted'
  last_error?: string | null
  sources: StreamSource[]
  flow?: { recent_records: number } | null
}

export interface Approval {
  id: number
  stream_id: number
  stream_name: string
  source_fqn: string
  requested_by: string
  requested_at: string
}
```

- [ ] **Step 8: Write `frontend/src/styles.css`**

```css
:root {
  --bg: #f6f7f9; --panel: #ffffff; --ink: #1d2433; --muted: #69707f;
  --line: #d9dde4; --brand: #3056d3; --ok: #1f8a4c; --warn: #b97d10; --err: #c0392b;
  font-family: -apple-system, "Segoe UI", Roboto, Helvetica, Arial, sans-serif;
}
* { box-sizing: border-box; }
body { margin: 0; background: var(--bg); color: var(--ink); }
.app { max-width: 1100px; margin: 0 auto; padding: 0 16px 48px; }
.topnav { display: flex; align-items: center; gap: 16px; padding: 12px 0; border-bottom: 1px solid var(--line); margin-bottom: 20px; }
.topnav .brand { font-weight: 700; }
.topnav a { color: var(--ink); text-decoration: none; }
.topnav a:hover { color: var(--brand); }
.topnav .spacer { flex: 1; }
button { cursor: pointer; border: 1px solid var(--line); background: var(--panel); border-radius: 6px; padding: 6px 12px; font-size: 14px; }
button.primary { background: var(--brand); border-color: var(--brand); color: white; }
button.danger { color: var(--err); border-color: var(--err); }
button.link { border: none; background: none; color: var(--brand); padding: 0 4px; }
button:disabled { opacity: 0.5; cursor: not-allowed; }
.muted { color: var(--muted); }
.small { font-size: 12px; }
.error { color: var(--err); }
.chip { display: inline-block; border: 1px solid var(--line); border-radius: 10px; padding: 0 8px; font-size: 12px; margin-left: 6px; }
.chip.ok { border-color: var(--ok); color: var(--ok); }
.chip.warn { border-color: var(--warn); color: var(--warn); }
.chip.err { border-color: var(--err); color: var(--err); }
.banner { background: #fdf6e3; border: 1px solid var(--warn); border-radius: 6px; padding: 8px 12px; margin: 10px 0; font-size: 13px; }
.banner.warn { background: #fff4e0; }
.login { max-width: 420px; margin: 80px auto; text-align: center; }
.persona { display: block; width: 100%; text-align: left; margin: 8px 0; padding: 12px; }
.catalog { display: flex; gap: 20px; align-items: flex-start; }
.catalog aside { width: 240px; flex-shrink: 0; }
.account-header { font-size: 12px; text-transform: uppercase; color: var(--muted); margin: 14px 0 6px; }
.wl { display: block; width: 100%; text-align: left; margin: 2px 0; border: none; background: none; padding: 6px 10px; border-radius: 6px; }
.wl.active { background: var(--brand); color: white; }
.catalog main { flex: 1; }
table.sources { width: 100%; border-collapse: collapse; background: var(--panel); border: 1px solid var(--line); border-radius: 6px; margin-bottom: 14px; }
table.sources th, table.sources td { text-align: left; padding: 8px 10px; border-bottom: 1px solid var(--line); font-size: 14px; }
.card { background: var(--panel); border: 1px solid var(--line); border-radius: 8px; padding: 14px; margin-bottom: 12px; }
.card-head { display: flex; align-items: center; gap: 8px; }
.row { display: flex; gap: 8px; margin-top: 10px; }
.modal-backdrop { position: fixed; inset: 0; background: rgba(20, 25, 35, 0.45); display: flex; align-items: center; justify-content: center; }
.modal { background: var(--panel); border-radius: 10px; padding: 20px; width: 440px; max-height: 80vh; overflow: auto; }
.modal.wide { width: 640px; }
.modal label { display: block; margin: 10px 0 4px; }
.indent { margin-left: 22px; display: flex; gap: 8px; margin-top: 6px; }
.modal input[type="text"], .modal input:not([type]), .modal select { padding: 6px 8px; border: 1px solid var(--line); border-radius: 6px; font-size: 14px; }
pre { background: #f0f2f5; border-radius: 6px; padding: 8px; font-size: 12px; overflow-x: auto; }
```

- [ ] **Step 9: Write `frontend/src/main.tsx`**

```tsx
import React from 'react'
import ReactDOM from 'react-dom/client'
import { BrowserRouter } from 'react-router-dom'
import App from './App'
import './styles.css'

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode>
    <BrowserRouter>
      <App />
    </BrowserRouter>
  </React.StrictMode>,
)
```

- [ ] **Step 10: Write `frontend/src/App.tsx`** (Catalog/Streams/Approvals pages arrive in Tasks 15–17; create placeholder components in this step so the app compiles: each page file default-exports a component returning `<p>…coming soon…</p>`)

```tsx
import { useEffect, useState } from 'react'
import { Link, Navigate, Route, Routes, useLocation } from 'react-router-dom'
import { api } from './api'
import type { User } from './types'
import ApprovalsPage from './pages/ApprovalsPage'
import CatalogPage from './pages/CatalogPage'
import LoginPage from './pages/LoginPage'
import StreamsPage from './pages/StreamsPage'

export default function App() {
  const [user, setUser] = useState<User | null>(null)
  const [loading, setLoading] = useState(true)
  const location = useLocation()

  useEffect(() => {
    api<User>('/api/session')
      .then(setUser)
      .catch(() => setUser(null))
      .finally(() => setLoading(false))
  }, [])

  if (loading) return <p className="muted">Loading…</p>
  if (!user && location.pathname !== '/login') return <Navigate to="/login" />

  return (
    <div className="app">
      {user && (
        <nav className="topnav">
          <span className="brand">LogStream Portal</span>
          <Link to="/">Catalog</Link>
          <Link to="/streams">My Streams</Link>
          {user.role === 'admin' && <Link to="/approvals">Approvals</Link>}
          <span className="spacer" />
          <span className="muted">{user.display_name} · {user.team}</span>
          <button onClick={() => api('/api/session', { method: 'DELETE' }).then(() => setUser(null))}>
            Sign out
          </button>
        </nav>
      )}
      <Routes>
        <Route path="/login" element={<LoginPage onLogin={setUser} />} />
        <Route path="/" element={<CatalogPage />} />
        <Route path="/streams" element={<StreamsPage />} />
        <Route path="/approvals" element={<ApprovalsPage />} />
      </Routes>
    </div>
  )
}
```

- [ ] **Step 11: Write `frontend/src/pages/LoginPage.tsx`**

```tsx
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api'
import type { User } from '../types'

export default function LoginPage({ onLogin }: { onLogin: (u: User) => void }) {
  const [personas, setPersonas] = useState<User[]>([])
  const [error, setError] = useState('')
  const navigate = useNavigate()

  useEffect(() => {
    api<User[]>('/api/personas').then(setPersonas).catch(e => setError(String(e.message ?? e)))
  }, [])

  async function pick(id: string) {
    const user = await api<User>('/api/session', {
      method: 'POST',
      body: JSON.stringify({ user_id: id }),
    })
    onLogin(user)
    navigate('/')
  }

  return (
    <div className="login">
      <h1>LogStream Portal</h1>
      <p className="muted">Pick a persona to continue (demo login)</p>
      {error && <p className="error">{error}</p>}
      {personas.map(p => (
        <button key={p.id} className="persona" onClick={() => pick(p.id)}>
          <b>{p.display_name}</b> — {p.id}
          <span className={p.role === 'admin' ? 'chip warn' : 'chip'}>{p.role}</span>
        </button>
      ))}
    </div>
  )
}
```

Placeholder pages (replaced in Tasks 15–17) — `src/pages/CatalogPage.tsx`, `src/pages/StreamsPage.tsx`, `src/pages/ApprovalsPage.tsx`, each:

```tsx
export default function CatalogPage() {
  return <p className="muted">Coming soon.</p>
}
```

(adjust the function name per file: `StreamsPage`, `ApprovalsPage`)

- [ ] **Step 12: Install, build, and commit**

Run: `cd logstream-portal/frontend && npm install && npm run build`
Expected: `vite build` completes, `dist/` created; `package-lock.json` generated.

```bash
git add logstream-portal/frontend
git commit -m "feat(portal): frontend scaffold with persona login and app shell"
```

---

### Task 15: Catalog page with account-grouped sidebar and sources table

**Files:**
- Create: `logstream-portal/frontend/src/components/SourcesTable.tsx`
- Test: `logstream-portal/frontend/src/components/SourcesTable.test.tsx`
- Modify: `logstream-portal/frontend/src/pages/CatalogPage.tsx` (replace placeholder)

- [ ] **Step 1: Write the failing component test** — `src/components/SourcesTable.test.tsx`

```tsx
import { fireEvent, render, screen } from '@testing-library/react'
import type { Source } from '../types'
import SourcesTable from './SourcesTable'

const sources: Source[] = [
  {
    fqn: 'c.s.syslog', name: 'syslog', log_type: 'system', sensitivity: 'standard',
    est_volume_per_min: 900, description: 'host syslog', columns: [], subscriptions: [],
  },
  {
    fqn: 'c.s.auth_log', name: 'auth_log', log_type: 'system', sensitivity: 'sensitive',
    est_volume_per_min: 300, description: 'auth events', columns: [],
    subscriptions: [{ stream_id: 1, stream_name: 'ops', status: 'active' }],
  },
]

it('renders sources with sensitivity chips and subscription refs', () => {
  render(<SourcesTable sources={sources} checked={new Set()} onToggle={() => {}} />)
  expect(screen.getByText('syslog')).toBeInTheDocument()
  expect(screen.getByText('sensitive')).toBeInTheDocument()
  expect(screen.getByText('ops (active)')).toBeInTheDocument()
})

it('reports toggles through the callback', () => {
  const onToggle = vi.fn()
  render(<SourcesTable sources={sources} checked={new Set()} onToggle={onToggle} />)
  fireEvent.click(screen.getByLabelText('select syslog'))
  expect(onToggle).toHaveBeenCalledWith('c.s.syslog')
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd logstream-portal/frontend && npm test`
Expected: FAIL — cannot resolve `./SourcesTable`

- [ ] **Step 3: Write `src/components/SourcesTable.tsx`**

```tsx
import type { Source } from '../types'

export default function SourcesTable({ sources, checked, onToggle }: {
  sources: Source[]
  checked: Set<string>
  onToggle: (fqn: string) => void
}) {
  return (
    <table className="sources">
      <thead>
        <tr>
          <th />
          <th>Source</th>
          <th>Type</th>
          <th>Est. volume</th>
          <th>Sensitivity</th>
          <th>Subscribed</th>
        </tr>
      </thead>
      <tbody>
        {sources.map(s => (
          <tr key={s.fqn}>
            <td>
              <input
                type="checkbox"
                aria-label={`select ${s.name}`}
                checked={checked.has(s.fqn)}
                onChange={() => onToggle(s.fqn)}
              />
            </td>
            <td>
              <b>{s.name}</b>
              <div className="muted small">{s.description}</div>
            </td>
            <td>{s.log_type}</td>
            <td>{s.est_volume_per_min}/min</td>
            <td>
              <span className={s.sensitivity === 'sensitive' ? 'chip warn' : 'chip'}>
                {s.sensitivity}
              </span>
            </td>
            <td>
              {s.subscriptions.length === 0
                ? '—'
                : s.subscriptions.map(x => `${x.stream_name} (${x.status})`).join(', ')}
            </td>
          </tr>
        ))}
      </tbody>
    </table>
  )
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `npm test`
Expected: SourcesTable tests pass.

- [ ] **Step 5: Replace `src/pages/CatalogPage.tsx`** (ForkWizard import lands in Task 16 — add a placeholder `src/components/ForkWizard.tsx` in this step that renders nothing so the page compiles)

Placeholder `src/components/ForkWizard.tsx` (replaced in Task 16):

```tsx
import type { Source, Stream } from '../types'

export default function ForkWizard(_props: {
  sources: Source[]
  streams: Stream[]
  presetStreamId?: number
  onClose: () => void
  onDone: () => void
}) {
  return null
}
```

`src/pages/CatalogPage.tsx`:

```tsx
import { useEffect, useState } from 'react'
import { useNavigate, useSearchParams } from 'react-router-dom'
import { api } from '../api'
import ForkWizard from '../components/ForkWizard'
import SourcesTable from '../components/SourcesTable'
import type { Catalog, Stream } from '../types'

export default function CatalogPage() {
  const [catalog, setCatalog] = useState<Catalog | null>(null)
  const [streams, setStreams] = useState<Stream[]>([])
  const [error, setError] = useState('')
  const [selected, setSelected] = useState({ account: 0, workload: 0 })
  const [checked, setChecked] = useState<Set<string>>(new Set())
  const [wizardOpen, setWizardOpen] = useState(false)
  const [params] = useSearchParams()
  const navigate = useNavigate()
  const destId = params.get('dest') ? Number(params.get('dest')) : undefined

  function load() {
    api<Catalog>('/api/catalog').then(setCatalog).catch(e => setError(String(e.message ?? e)))
    api<Stream[]>('/api/streams').then(setStreams).catch(() => {})
  }
  useEffect(load, [])

  if (error) return <p className="error">{error}</p>
  if (!catalog) return <p className="muted">Loading catalog…</p>

  const workload = catalog.accounts[selected.account]?.workloads[selected.workload]
  const allSources = catalog.accounts.flatMap(a => a.workloads.flatMap(w => w.sources))
  const checkedSources = allSources.filter(s => checked.has(s.fqn))

  function toggle(fqn: string) {
    setChecked(prev => {
      const next = new Set(prev)
      if (next.has(fqn)) next.delete(fqn)
      else next.add(fqn)
      return next
    })
  }

  return (
    <div>
      {catalog.stale && (
        <div className="banner">
          Catalog as of {catalog.as_of} — Databricks unreachable, showing cached snapshot.
        </div>
      )}
      {destId !== undefined && (
        <div className="banner">
          Adding sources to stream #{destId} — select below and hit Fork.
        </div>
      )}
      <div className="catalog">
        <aside>
          {catalog.accounts.map((account, ai) => (
            <div key={account.account_id}>
              <div className="account-header">
                {account.account_alias} · {account.account_id}
              </div>
              {account.workloads.map((w, wi) => (
                <button
                  key={w.schema}
                  className={ai === selected.account && wi === selected.workload ? 'wl active' : 'wl'}
                  onClick={() => setSelected({ account: ai, workload: wi })}
                >
                  {w.name}
                </button>
              ))}
            </div>
          ))}
        </aside>
        <main>
          {workload && (
            <>
              <h2>
                {workload.name} <span className="muted small">({workload.environment})</span>
              </h2>
              <SourcesTable sources={workload.sources} checked={checked} onToggle={toggle} />
            </>
          )}
          <button className="primary" disabled={checked.size === 0} onClick={() => setWizardOpen(true)}>
            Fork {checked.size} selected →
          </button>
        </main>
      </div>
      {wizardOpen && (
        <ForkWizard
          sources={checkedSources}
          streams={streams}
          presetStreamId={destId}
          onClose={() => setWizardOpen(false)}
          onDone={() => navigate('/streams')}
        />
      )}
    </div>
  )
}
```

- [ ] **Step 6: Verify build and tests**

Run: `npm run build && npm test`
Expected: build succeeds; all frontend tests pass.

- [ ] **Step 7: Commit**

```bash
git add logstream-portal/frontend/src
git commit -m "feat(portal): catalog page with account-grouped sidebar and sources table"
```

---

### Task 16: Fork wizard

**Files:**
- Modify: `logstream-portal/frontend/src/components/ForkWizard.tsx` (replace placeholder)
- Test: `logstream-portal/frontend/src/components/ForkWizard.test.tsx`

- [ ] **Step 1: Write the failing tests** — `src/components/ForkWizard.test.tsx`

```tsx
import { fireEvent, render, screen, waitFor } from '@testing-library/react'
import type { Source } from '../types'
import ForkWizard from './ForkWizard'

const sources: Source[] = [
  {
    fqn: 'c.s.syslog', name: 'syslog', log_type: 'system', sensitivity: 'standard',
    est_volume_per_min: 900, description: '', columns: [], subscriptions: [],
  },
  {
    fqn: 'c.s.auth_log', name: 'auth_log', log_type: 'system', sensitivity: 'sensitive',
    est_volume_per_min: 300, description: '', columns: [], subscriptions: [],
  },
]

afterEach(() => vi.unstubAllGlobals())

it('warns when the selection includes sensitive sources', () => {
  render(<ForkWizard sources={sources} streams={[]} onClose={() => {}} onDone={() => {}} />)
  expect(screen.getByText(/require admin approval/i)).toBeInTheDocument()
})

it('posts a new-stream payload and calls onDone', async () => {
  const fetchMock = vi.fn().mockResolvedValue({
    ok: true,
    status: 201,
    json: async () => ({}),
  })
  vi.stubGlobal('fetch', fetchMock)
  const onDone = vi.fn()
  render(<ForkWizard sources={sources} streams={[]} onClose={() => {}} onDone={onDone} />)
  fireEvent.change(screen.getByPlaceholderText('stream name'), { target: { value: 'team-a-logs' } })
  fireEvent.click(screen.getByText('Submit'))
  await waitFor(() => expect(onDone).toHaveBeenCalled())
  expect(fetchMock).toHaveBeenCalledWith('/api/streams', expect.objectContaining({ method: 'POST' }))
  const body = JSON.parse((fetchMock.mock.calls[0][1] as RequestInit).body as string)
  expect(body).toEqual({ name: 'team-a-logs', type: 'kinesis', source_fqns: ['c.s.syslog', 'c.s.auth_log'] })
})

it('posts to the existing stream when preset', async () => {
  const fetchMock = vi.fn().mockResolvedValue({ ok: true, status: 200, json: async () => ({}) })
  vi.stubGlobal('fetch', fetchMock)
  const streams = [{
    id: 7, name: 'ops', type: 'kinesis' as const, status: 'live' as const, sources: [],
  }]
  render(
    <ForkWizard sources={[sources[0]]} streams={streams} presetStreamId={7}
      onClose={() => {}} onDone={() => {}} />,
  )
  fireEvent.click(screen.getByText('Submit'))
  await waitFor(() => expect(fetchMock).toHaveBeenCalled())
  expect(fetchMock.mock.calls[0][0]).toBe('/api/streams/7/sources')
})
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `npm test`
Expected: ForkWizard tests FAIL (placeholder renders null).

- [ ] **Step 3: Replace `src/components/ForkWizard.tsx`**

```tsx
import { useState } from 'react'
import { api } from '../api'
import type { Source, Stream } from '../types'

export default function ForkWizard({ sources, streams, presetStreamId, onClose, onDone }: {
  sources: Source[]
  streams: Stream[]
  presetStreamId?: number
  onClose: () => void
  onDone: () => void
}) {
  const existing = streams.filter(s => s.status !== 'deleted')
  const [mode, setMode] = useState<'new' | 'existing'>(presetStreamId ? 'existing' : 'new')
  const [type, setType] = useState<'kinesis' | 'sqs'>('kinesis')
  const [name, setName] = useState('')
  const [streamId, setStreamId] = useState<number | undefined>(presetStreamId ?? existing[0]?.id)
  const [error, setError] = useState('')
  const [busy, setBusy] = useState(false)
  const sensitive = sources.filter(s => s.sensitivity === 'sensitive')

  async function submit() {
    setBusy(true)
    setError('')
    const source_fqns = sources.map(s => s.fqn)
    try {
      if (mode === 'new') {
        await api('/api/streams', {
          method: 'POST',
          body: JSON.stringify({ name, type, source_fqns }),
        })
      } else {
        await api(`/api/streams/${streamId}/sources`, {
          method: 'POST',
          body: JSON.stringify({ source_fqns }),
        })
      }
      onDone()
    } catch (e) {
      setError(String((e as Error).message ?? e))
    } finally {
      setBusy(false)
    }
  }

  return (
    <div className="modal-backdrop">
      <div className="modal">
        <h3>Fork {sources.length} source{sources.length > 1 ? 's' : ''}</h3>
        <p className="muted small">{sources.map(s => s.name).join(', ')}</p>

        <label>
          <input type="radio" checked={mode === 'new'} onChange={() => setMode('new')} />
          {' '}New stream
        </label>
        {mode === 'new' && (
          <div className="indent">
            <select value={type} onChange={e => setType(e.target.value as 'kinesis' | 'sqs')}>
              <option value="kinesis">Kinesis</option>
              <option value="sqs">SQS</option>
            </select>
            <input placeholder="stream name" value={name} onChange={e => setName(e.target.value)} />
          </div>
        )}

        <label>
          <input
            type="radio"
            checked={mode === 'existing'}
            disabled={existing.length === 0}
            onChange={() => setMode('existing')}
          />
          {' '}Add to existing stream
        </label>
        {mode === 'existing' && (
          <div className="indent">
            <select value={streamId} onChange={e => setStreamId(Number(e.target.value))}>
              {existing.map(s => (
                <option key={s.id} value={s.id}>{s.name} ({s.type})</option>
              ))}
            </select>
          </div>
        )}

        {sensitive.length > 0 && (
          <div className="banner warn">
            <b>{sensitive.map(s => s.name).join(', ')}</b>{' '}
            {sensitive.length > 1 ? 'are' : 'is'} sensitive and will require admin approval.
            Standard sources activate immediately.
          </div>
        )}
        {error && <p className="error">{error}</p>}

        <div className="row">
          <button
            className="primary"
            disabled={busy || (mode === 'new' && !name) || (mode === 'existing' && !streamId)}
            onClick={submit}
          >
            Submit
          </button>
          <button onClick={onClose}>Cancel</button>
        </div>
      </div>
    </div>
  )
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `npm test`
Expected: all frontend tests pass.

- [ ] **Step 5: Commit**

```bash
git add logstream-portal/frontend/src/components
git commit -m "feat(portal): fork wizard with new/existing destination and sensitivity warning"
```

---

### Task 17: My Streams page, peek modal, and approvals page

**Files:**
- Create: `logstream-portal/frontend/src/components/StreamCard.tsx`
- Create: `logstream-portal/frontend/src/components/PeekModal.tsx`
- Modify: `logstream-portal/frontend/src/pages/StreamsPage.tsx` (replace placeholder)
- Modify: `logstream-portal/frontend/src/pages/ApprovalsPage.tsx` (replace placeholder)

- [ ] **Step 1: Write `src/components/StreamCard.tsx`**

```tsx
import { api } from '../api'
import type { Stream } from '../types'

const STATUS_CHIP: Record<string, string> = {
  active: 'chip ok',
  pending_approval: 'chip warn',
  rejected: 'chip err',
}

export default function StreamCard({ stream, onChanged, onAddSources, onPeek }: {
  stream: Stream
  onChanged: () => void
  onAddSources: () => void
  onPeek: () => void
}) {
  async function removeSource(fqn: string) {
    await api(`/api/streams/${stream.id}/sources/${encodeURIComponent(fqn)}`, { method: 'DELETE' })
    onChanged()
  }

  async function deleteStream() {
    if (!confirm(`Delete stream "${stream.name}" and its ${stream.type} resource?`)) return
    await api(`/api/streams/${stream.id}`, { method: 'DELETE' })
    onChanged()
  }

  async function retry() {
    await api(`/api/streams/${stream.id}/retry`, { method: 'POST' })
    onChanged()
  }

  return (
    <div className="card">
      <div className="card-head">
        <b>{stream.name}</b>
        <span className="chip">{stream.type}</span>
        <span className={stream.status === 'live' ? 'chip ok' : 'chip warn'}>{stream.status}</span>
        {stream.flow && (
          <span className="muted small">{stream.flow.recent_records} recent records</span>
        )}
      </div>
      {stream.last_error && (
        <p className="error small">
          {stream.last_error} <button onClick={retry}>Retry</button>
        </p>
      )}
      <ul>
        {stream.sources.map(src => (
          <li key={src.id}>
            {src.workload}/{src.source_name}
            <span className={STATUS_CHIP[src.status] ?? 'chip'}>{src.status}</span>
            <button className="link" onClick={() => removeSource(src.source_fqn)}>remove</button>
          </li>
        ))}
      </ul>
      <div className="row">
        <button onClick={onAddSources}>+ Add sources</button>
        <button onClick={onPeek} disabled={stream.status !== 'live'}>Peek</button>
        <button className="danger" onClick={deleteStream}>Delete stream</button>
      </div>
    </div>
  )
}
```

- [ ] **Step 2: Write `src/components/PeekModal.tsx`**

```tsx
import { useEffect, useState } from 'react'
import { api } from '../api'
import type { Stream } from '../types'

export default function PeekModal({ stream, onClose }: { stream: Stream; onClose: () => void }) {
  const [records, setRecords] = useState<unknown[] | null>(null)
  const [error, setError] = useState('')

  useEffect(() => {
    api<unknown[]>(`/api/streams/${stream.id}/peek`)
      .then(setRecords)
      .catch(e => setError(String((e as Error).message ?? e)))
  }, [stream.id])

  return (
    <div className="modal-backdrop" onClick={onClose}>
      <div className="modal wide" onClick={e => e.stopPropagation()}>
        <h3>Peek: {stream.name}</h3>
        {error && <p className="error">{error}</p>}
        {!records && !error && <p className="muted">Reading stream…</p>}
        {records && records.length === 0 && (
          <p className="muted">No records yet — logs flow in within a few seconds of activation.</p>
        )}
        {records && records.map((r, i) => (
          <pre key={i}>{JSON.stringify(r, null, 2)}</pre>
        ))}
        <button onClick={onClose}>Close</button>
      </div>
    </div>
  )
}
```

- [ ] **Step 3: Replace `src/pages/StreamsPage.tsx`**

```tsx
import { useEffect, useState } from 'react'
import { useNavigate } from 'react-router-dom'
import { api } from '../api'
import PeekModal from '../components/PeekModal'
import StreamCard from '../components/StreamCard'
import type { Stream } from '../types'

export default function StreamsPage() {
  const [streams, setStreams] = useState<Stream[] | null>(null)
  const [peeking, setPeeking] = useState<Stream | null>(null)
  const navigate = useNavigate()

  function load() {
    api<Stream[]>('/api/streams').then(setStreams)
  }
  useEffect(load, [])

  if (!streams) return <p className="muted">Loading…</p>

  return (
    <div>
      <h2>My Streams</h2>
      {streams.length === 0 && (
        <p className="muted">No streams yet — fork some sources from the Catalog.</p>
      )}
      {streams.map(s => (
        <StreamCard
          key={s.id}
          stream={s}
          onChanged={load}
          onAddSources={() => navigate(`/?dest=${s.id}`)}
          onPeek={() => setPeeking(s)}
        />
      ))}
      {peeking && <PeekModal stream={peeking} onClose={() => setPeeking(null)} />}
    </div>
  )
}
```

- [ ] **Step 4: Replace `src/pages/ApprovalsPage.tsx`**

```tsx
import { useEffect, useState } from 'react'
import { api } from '../api'
import type { Approval } from '../types'

export default function ApprovalsPage() {
  const [items, setItems] = useState<Approval[] | null>(null)
  const [error, setError] = useState('')

  function load() {
    api<Approval[]>('/api/approvals').then(setItems).catch(e => setError(String(e.message ?? e)))
  }
  useEffect(load, [])

  async function decide(id: number, approved: boolean) {
    await api(`/api/approvals/${id}`, { method: 'POST', body: JSON.stringify({ approved }) })
    load()
  }

  if (error) return <p className="error">{error}</p>
  if (!items) return <p className="muted">Loading…</p>

  return (
    <div>
      <h2>Pending approvals</h2>
      {items.length === 0 && <p className="muted">Queue is empty.</p>}
      {items.map(a => (
        <div className="card" key={a.id}>
          <b>{a.source_fqn}</b> → stream <b>{a.stream_name}</b>
          <div className="muted small">
            requested by {a.requested_by} at {a.requested_at}
          </div>
          <div className="row">
            <button className="primary" onClick={() => decide(a.id, true)}>Approve</button>
            <button className="danger" onClick={() => decide(a.id, false)}>Reject</button>
          </div>
        </div>
      ))}
    </div>
  )
}
```

- [ ] **Step 5: Build, test, commit**

Run: `npm run build && npm test`
Expected: build succeeds, all frontend tests pass.

```bash
git add logstream-portal/frontend/src
git commit -m "feat(portal): my-streams management, peek modal, and approvals queue"
```

---

### Task 18: Full-stack manual verification

No new files — this task verifies the whole demo loop through the real stack (with or without live Databricks; without, the stale banner shows and everything else behaves identically).

- [ ] **Step 1: Rebuild and restart the stack**

Run (from `logstream-portal/`): `docker compose up -d --build`
Expected: all five services running (`docker compose ps`); portal healthy: `curl -s localhost:8000/api/personas | python3 -m json.tool` lists 3 personas.

- [ ] **Step 2: Walk the consumer happy path in a browser** — http://localhost:8000

1. Log in as **Dana** → Catalog shows *prod-ecommerce (111111111111)* and *prod-platform (222222222222)* groups in the sidebar.
2. Select `orders_api`, check `syslog` + `auth_log` → Fork → new Kinesis stream `team-a-orders` → Submit.
3. My Streams: `team-a-orders` is **live**; `syslog` **active**, `auth_log` **pending_approval**.
4. Peek shows JSON records tagged `"workload": "orders_api", "source_name": "syslog", "account_id": "111111111111"` within ~10 seconds.
5. Verify the fragment on disk: `cat vector/aggregator/fragments/forks.yaml` contains `fork_1_filter` matching only `syslog` (not `auth_log`).

- [ ] **Step 3: Walk the approval path**

1. Sign out, log in as **admin@platform** → Approvals shows Dana's `auth_log` request.
2. Approve it. Sign back in as Dana → `auth_log` now **active**; `forks.yaml` condition now includes `auth_log`; Peek soon shows auth-style lines.

- [ ] **Step 4: Walk the manage-fan-out loop**

1. As Dana on My Streams → **+ Add sources** on `team-a-orders` → lands on Catalog with the "Adding sources to stream #1" banner → check `storefront_web/nginx_access` → Fork → Submit (existing stream preselected).
2. Remove `orders_api/syslog` from the stream; `forks.yaml` updates accordingly.
3. Delete the stream; `forks.yaml` drops the fork; `docker compose exec localstack awslocal kinesis list-streams` no longer lists it.

- [ ] **Step 5: Commit any fixes found**

If any step failed, fix the bug (writing a regression test where the bug was in portal logic), then:

```bash
git add -A logstream-portal
git commit -m "fix(portal): issues found in full-stack verification"
```

---

### Task 19: End-to-end smoke script and README

**Files:**
- Create: `logstream-portal/scripts/demo_test.sh`
- Create: `logstream-portal/README.md`

- [ ] **Step 1: Write `scripts/demo_test.sh`** — the "demo will not embarrass me" test

```bash
#!/usr/bin/env bash
# E2E smoke: compose up → login → fork a source into Kinesis → records arrive.
# Runs fully offline (bundled catalog snapshot); Databricks creds not required.
set -euo pipefail
cd "$(dirname "$0")/.."

cleanup() { docker compose down -v >/dev/null 2>&1 || true; }
trap cleanup EXIT

[ -f .env ] || cp .env.example .env

docker compose up -d --build

echo "waiting for portal..."
for _ in $(seq 1 60); do
  curl -fsS localhost:8000/api/personas >/dev/null 2>&1 && break
  sleep 2
done
curl -fsS localhost:8000/api/personas >/dev/null || { echo "FAIL: portal never came up"; exit 1; }

JAR=$(mktemp)
curl -fsS -c "$JAR" -X POST localhost:8000/api/session \
  -H 'Content-Type: application/json' \
  -d '{"user_id":"dana@app-team"}' >/dev/null

echo "forking storefront_web/syslog into a new kinesis stream..."
curl -fsS -b "$JAR" -X POST localhost:8000/api/streams \
  -H 'Content-Type: application/json' \
  -d '{"name":"smoke-stream","type":"kinesis","source_fqns":["logging_demo.acct_a__storefront_web.syslog"]}' \
  | python3 -m json.tool

echo "waiting for records to flow..."
for _ in $(seq 1 45); do
  COUNT=$(curl -fsS -b "$JAR" localhost:8000/api/streams | python3 -c '
import json, sys
streams = json.load(sys.stdin)
flow = (streams[0].get("flow") or {}) if streams else {}
print(flow.get("recent_records", 0))')
  if [ "$COUNT" -gt 0 ]; then
    echo "PASS: $COUNT records flowing into smoke-stream"
    exit 0
  fi
  sleep 2
done

echo "FAIL: no records arrived in smoke-stream within 90s"
docker compose logs --tail 50 vector-aggregator portal
exit 1
```

Run: `chmod +x logstream-portal/scripts/demo_test.sh`

- [ ] **Step 2: Run it**

Run: `cd logstream-portal && make demo-test`
Expected: `PASS: N records flowing into smoke-stream` (typically within 20–40s of the fork). On failure the script dumps aggregator/portal logs — debug from there before proceeding.

- [ ] **Step 3: Write `logstream-portal/README.md`**

```markdown
# LogStream Portal

Self-service log onboarding for a Vector.dev pipeline spanning multiple AWS
accounts. Browse the Unity Catalog inventory of collected log sources, fork
the ones you want into your own Kinesis stream or SQS queue, and manage that
fan-out over time. Sensitive sources require platform-admin approval.

Everything runs locally in docker-compose (AWS via LocalStack) except the
catalog, which reads from a real Databricks Unity Catalog workspace — with a
bundled snapshot fallback so the demo also works fully offline.

## Quick start

    cp .env.example .env        # add Databricks creds, or leave placeholders
                                # for offline mode (stale-catalog banner)
    make up                     # build + start the stack
    open http://localhost:8000  # log in as dana@app-team

Personas: `dana@app-team`, `raj@data-sci` (consumers), `admin@platform`
(approves sensitive-source requests).

## Seeding Unity Catalog (one-time, optional)

    pip install databricks-sdk
    set -a && source .env && set +a
    make seed

Creates catalog `logging_demo` with one schema per workload
(`acct_a__storefront_web`, `acct_a__orders_api`, `acct_b__identity_svc`,
`acct_b__batch_etl`) and one table per log source, carrying sensitivity and
routing metadata in TBLPROPERTIES. `fixtures/catalog_snapshot.json` mirrors
this inventory — keep them in sync if you change either.

## How a fork works

1. Portal creates your Kinesis stream / SQS queue in LocalStack.
2. Standard sources activate immediately; sensitive ones await admin approval.
3. Portal regenerates `vector/aggregator/fragments/forks.yaml` (a filter
   transform + sink per stream) purely from DB state; the Vector aggregator
   hot-reloads it (`--watch-config`) and verifies via its GraphQL API.

Note: `forks.yaml` is committed as a noop bootstrap and overwritten at
runtime, so it shows as modified while the demo runs. That's by design.

## Tests

    make test       # backend pytest + frontend vitest
    make demo-test  # full e2e: compose up → fork → records arrive

## Architecture

See `docs/superpowers/specs/2026-06-11-vector-onboarding-portal-design.md`
(repo root) for the approved design, including the decision log.
```

- [ ] **Step 4: Commit**

```bash
git add logstream-portal/scripts/demo_test.sh logstream-portal/README.md
git commit -m "feat(portal): e2e smoke test and README"
```

---

## Self-Review Notes (performed at plan-writing time)

1. **Spec coverage:** topology/compose (Tasks 11–12), UC model + seed (Tasks 4, 13), portal data model (Task 1), fork mechanics + lifecycle + gating + retry (Tasks 2, 5–8), API surface incl. peek/retry (Tasks 9–10), blended UI with all four pages and the add-to-existing loop (Tasks 14–17), error handling (stale-catalog banner Task 15/4, provisioning retry Task 8/10, Vector rollback Task 6), testing strategy incl. golden files and e2e smoke (Tasks 2, 19). The spec's "Retry button" implied an endpoint not in the spec's API table — added as `POST /api/streams/{id}/retry`.
2. **Known deviation:** single regenerated `forks.yaml` instead of per-stream fragment files (rationale in header).
3. **Type consistency check:** `resource_ref` (not `arn`) everywhere; `ForkSpec(stream_id, stream_type, resource_ref, members)` matches between fragments.py, service.py, and tests; `find_source` returns `workload_tag`/`source_name`/`sensitivity` consumed identically in `_insert_members`; frontend `Stream`/`Source` types match API JSON shapes produced by the routes.
4. **Sequencing note:** Tasks 9/14/15 create placeholder modules that later tasks replace — each task still leaves the suite green and the app importable/buildable.

---

## Execution

Run tasks strictly in order; every task ends with a green test suite and a commit. Tasks 12, 13, 18, and 19 need Docker (and 13 needs real Databricks creds); all other tasks are pure local TDD.

