import sqlite3

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
    assert s.cribl_base_url == "http://localhost:9000"
    assert s.resource_prefix == "logstream-"


def test_settings_reads_env(monkeypatch):
    monkeypatch.setenv("UC_CATALOG_NAME", "other_cat")
    monkeypatch.setenv("PORTAL_SESSION_SECRET", "s3kr1t")
    monkeypatch.setenv("CRIBL_BASE_URL", "http://cribl:9000")
    s = load_settings()
    assert s.uc_catalog == "other_cat"
    assert s.session_secret == "s3kr1t"
    assert s.cribl_base_url == "http://cribl:9000"


def test_init_db_seeds_personas(conn):
    rows = conn.execute("SELECT id, role FROM users ORDER BY id").fetchall()
    assert [(r["id"], r["role"]) for r in rows] == sorted(
        [(p[0], p[3]) for p in PERSONAS]
    )


def test_init_db_is_idempotent(conn):
    init_db(conn)  # second run must not raise or duplicate
    assert conn.execute("SELECT COUNT(*) AS n FROM users").fetchone()["n"] == len(PERSONAS)


def test_stream_sources_has_account_id(conn):
    cols = [r["name"] for r in conn.execute("PRAGMA table_info(stream_sources)")]
    assert "account_id" in cols


def test_stream_sources_unique_per_stream(conn):
    conn.execute(
        "INSERT INTO streams(owner_id, name, type) VALUES ('dana@app-team', 's1', 'kinesis')"
    )
    ins = (
        "INSERT INTO stream_sources(stream_id, source_fqn, account_id, workload, source_name, status, requested_by)"
        " VALUES (1, 'c.s.t', '522412052544', 'w', 'syslog', 'active', 'dana@app-team')"
    )
    conn.execute(ins)
    with pytest.raises(sqlite3.IntegrityError):
        conn.execute(ins)


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
