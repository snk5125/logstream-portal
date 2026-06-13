import sqlite3

SCHEMA = """
CREATE TABLE IF NOT EXISTS users (
  id TEXT PRIMARY KEY,
  display_name TEXT NOT NULL,
  team TEXT NOT NULL,
  role TEXT NOT NULL CHECK (role IN ('consumer', 'admin')),
  account_scope TEXT
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
  read_role_arn TEXT,
  consumer_account_id TEXT,
  created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE TABLE IF NOT EXISTS stream_sources (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  stream_id INTEGER NOT NULL REFERENCES streams(id),
  source_fqn TEXT NOT NULL,
  account_id TEXT NOT NULL,
  workload TEXT NOT NULL,
  source_name TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('active', 'pending_approval', 'rejected')),
  requested_by TEXT NOT NULL REFERENCES users(id),
  requested_at TEXT NOT NULL DEFAULT (datetime('now')),
  decided_by TEXT REFERENCES users(id),
  decided_at TEXT,
  UNIQUE (stream_id, source_fqn)
);
CREATE UNIQUE INDEX IF NOT EXISTS idx_streams_name_alive
  ON streams(name) WHERE status != 'deleted';
"""

PERSONAS = [
    ("admin@platform", "Alex Romero", "platform", "admin", None),
    ("dana@app-team", "Dana Whitfield", "team-a", "consumer", "522412052544"),
    ("raj@data-sci", "Raj Patel", "data-sci", "consumer", "624627265315"),
]


def get_db(path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(path, check_same_thread=False, isolation_level=None)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA foreign_keys = ON")
    return conn


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
