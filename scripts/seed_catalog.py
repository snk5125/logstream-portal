"""Seed the demo log-source inventory into Databricks Unity Catalog.

One-time setup. Requires: pip install databricks-sdk
Env: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_WAREHOUSE_ID,
     UC_CATALOG_NAME (optional, default logging_demo)

The created tables are metadata-only (no rows): their columns document the
event schema and their TBLPROPERTIES carry the operational metadata the
portal reads. This script is the source of truth that
fixtures/catalog_snapshot.json mirrors -- keep them in sync.
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
        "schema": "acct_b__storefront_web", "workload": "storefront_web",
        "account_id": "522412052544", "account_alias": "prod-ecommerce",
        "sources": [
            {"name": "syslog", "log_type": "system", "sensitivity": "standard",
             "volume": 1200, "comment": "Host syslog from storefront web tier"},
            {"name": "nginx_access", "log_type": "web", "sensitivity": "standard",
             "volume": 2400, "comment": "Nginx access logs from storefront web tier"},
        ],
    },
    {
        "schema": "acct_b__orders_api", "workload": "orders_api",
        "account_id": "522412052544", "account_alias": "prod-ecommerce",
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
        "schema": "acct_c__identity_svc", "workload": "identity_svc",
        "account_id": "624627265315", "account_alias": "prod-platform",
        "sources": [
            {"name": "syslog", "log_type": "system", "sensitivity": "standard",
             "volume": 800, "comment": "Host syslog from identity service"},
            {"name": "auth_log", "log_type": "system", "sensitivity": "sensitive",
             "volume": 450, "comment": "SSH/sudo/PAM auth events"},
        ],
    },
    {
        "schema": "acct_c__batch_etl", "workload": "batch_etl",
        "account_id": "624627265315", "account_alias": "prod-platform",
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

    for old in ("acct_a__storefront_web", "acct_a__orders_api",
                "acct_b__identity_svc", "acct_b__batch_etl"):
        sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{old} CASCADE")

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
