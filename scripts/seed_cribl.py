"""Seed static Cribl config: collection-tier Edge instances + logging-tier archive.

Idempotent: deterministic ids, upserts. Pure builders are unit-tested; main()
pushes to the logging group (default) on the leader and to each Edge instance.
Env: CRIBL_BASE_URL (leader), CRIBL_USERNAME, CRIBL_PASSWORD,
     CRIBL_EDGE_B_URL, CRIBL_EDGE_C_URL (Edge instances).
Endpoints for the Edge push are validated during the live AWS bring-up.
"""
import json
import os
from pathlib import Path

import requests

SPEC = json.loads((Path(__file__).resolve().parent.parent / "cribl" / "seed_spec.json").read_text())


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
    return {"id": f"tag_{workload}_{source_name}",
            "conf": {"functions": [{"id": "eval", "conf": {"add": add}}]}}


def build_tcp_output(forward_to: str) -> dict:
    host, port = forward_to.rsplit(":", 1)
    return {"id": "to_logging", "type": "tcp", "host": host, "port": int(port), "sendHeader": False}


def build_s3_destination(bucket: str) -> dict:
    return {"id": "archive", "type": "s3", "bucket": bucket, "region": "us-east-1",
            "awsAuthenticationMethod": "auto", "destPath": "raw", "format": "json"}


def build_archive_route() -> dict:
    return {"id": "archive", "name": "archive", "filter": "true", "final": True,
            "output": "archive", "pipeline": "passthru", "description": "archive all to s3"}


def _headers(base):
    r = requests.post(f"{base}/api/v1/auth/login",
                      json={"username": os.environ.get("CRIBL_USERNAME", "admin"),
                            "password": os.environ["CRIBL_PASSWORD"]}, timeout=10)
    r.raise_for_status()
    return {"Authorization": f"Bearer {r.json()['token']}"}


def _commit_deploy(base, group, h):
    c = requests.post(f"{base}/api/v1/version/commit",
                      json={"message": f"seed {group}", "group": group}, headers=h, timeout=15)
    c.raise_for_status()
    version = c.json()["items"][0]["commit"]
    requests.patch(f"{base}/api/v1/master/groups/{group}/deploy",
                   json={"version": version}, headers=h, timeout=15).raise_for_status()


def seed_logging(base, h):
    """Logging tier on the leader's `default` group: TCP source + archive dest + archive route."""
    group = SPEC["logging"]["group"]
    g = f"{base}/api/v1/m/{group}"
    requests.post(f"{g}/system/inputs",
                  json={"id": "from_agents", "type": "tcp", "host": "0.0.0.0",
                        "port": SPEC["logging"]["ingest_port"]}, headers=h, timeout=10).raise_for_status()
    requests.post(f"{g}/system/outputs",
                  json=build_s3_destination(SPEC["logging"]["archive_bucket"]), headers=h, timeout=10).raise_for_status()
    # splice the archive route into the route table (keep any existing fork_* / other routes)
    doc = requests.get(f"{g}/routes", headers=h, timeout=10).json()
    table = doc["items"][0]
    table["routes"] = [r for r in table["routes"] if r["id"] != "archive"] + [build_archive_route()]
    requests.patch(f"{g}/routes/default", json=table, headers=h, timeout=10).raise_for_status()
    _commit_deploy(base, group, h)


def seed_edge(edge_base, forward_to, cfg, h):
    """Collection tier: a standalone Edge single-instance (no /m/<group>/ prefix)."""
    b = f"{edge_base}/api/v1"
    requests.post(f"{b}/system/outputs", json=build_tcp_output(forward_to), headers=h, timeout=10).raise_for_status()
    for s in cfg["sources"]:
        pl = build_eval_pipeline(cfg["account_id"], cfg["account_alias"], s["workload"], s["source_name"])
        requests.post(f"{b}/pipelines", json=pl, headers=h, timeout=10).raise_for_status()
        src = build_datagen_source(s["workload"], s["source_name"], s["eventsPerSec"])
        requests.post(f"{b}/system/inputs", json=src, headers=h, timeout=10).raise_for_status()


def main() -> None:
    base = os.environ.get("CRIBL_BASE_URL", "http://localhost:9000").rstrip("/")
    h = _headers(base)
    seed_logging(base, h)
    edge_urls = {"acct_b": os.environ.get("CRIBL_EDGE_B_URL"),
                 "acct_c": os.environ.get("CRIBL_EDGE_C_URL")}
    fwd = os.environ.get("LOGGING_INGEST_HOST", "localhost:10300")
    for name, cfg in SPEC["edges"].items():
        url = edge_urls.get(name)
        if not url:
            print(f"skip {name}: no edge URL env"); continue
        seed_edge(url.rstrip("/"), fwd, cfg, _headers(url.rstrip("/")))
    print("Cribl seeded: logging archive route + edge collection config.")


if __name__ == "__main__":
    main()
