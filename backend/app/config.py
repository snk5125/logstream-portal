import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    databricks_host: str
    databricks_token: str
    uc_catalog: str
    aws_region: str
    data_dir: str
    snapshot_seed: str
    static_dir: str
    session_secret: str
    cribl_base_url: str
    cribl_group: str
    cribl_username: str
    cribl_password: str
    cribl_fleet: str
    cribl_sync_interval: int
    resource_prefix: str
    logging_account_id: str


def load_settings() -> Settings:
    env = os.environ.get
    return Settings(
        databricks_host=env("DATABRICKS_HOST", ""),
        databricks_token=env("DATABRICKS_TOKEN", ""),
        uc_catalog=env("UC_CATALOG_NAME", "logging_demo"),
        aws_region=env("AWS_REGION", "us-east-1"),
        data_dir=env("PORTAL_DATA_DIR", "/data"),
        snapshot_seed=env("CATALOG_SNAPSHOT_SEED", ""),
        static_dir=env("PORTAL_STATIC_DIR", ""),
        session_secret=env("PORTAL_SESSION_SECRET", "demo-secret-change-me"),
        cribl_base_url=env("CRIBL_BASE_URL", "http://localhost:9000"),
        cribl_group=env("CRIBL_GROUP", "default"),
        cribl_username=env("CRIBL_USERNAME", "admin"),
        cribl_password=env("CRIBL_PASSWORD", ""),
        cribl_fleet=env("CRIBL_FLEET", "default_fleet"),
        cribl_sync_interval=int(env("CRIBL_SYNC_INTERVAL_SECONDS", "0")),
        resource_prefix=env("RESOURCE_PREFIX", "logstream-"),
        logging_account_id=env("LOGGING_ACCOUNT_ID", "337394138208"),
    )
