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


# Each logical source maps to a built-in Cribl datagen sample (valid sample ids,
# no extension — see the live deltas in cribl/README.md).
SAMPLE_MAP = {
    "syslog": "syslog", "auth_log": "syslog", "cron_log": "syslog",
    "nginx_access": "apache_common", "app_log": "business_event",
}


def build_datagen_source(workload: str, source_name: str, eps: int) -> dict:
    # sendToRoutes=True with the tag Eval as the source pre-processing pipeline,
    # then a catch-all route forwards to to_logging (validated live; source-level
    # QuickConnect output did not deliver events).
    return {
        "id": f"ds_{workload}_{source_name}",
        "type": "datagen",
        "samples": [{"sample": SAMPLE_MAP.get(source_name, "syslog"), "eventsPerSec": eps}],
        "sendToRoutes": True,
        "pipeline": f"tag_{workload}_{source_name}",
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
    return {"id": "to_logging", "type": "tcpjson", "host": host, "port": int(port)}


def build_fleet_route() -> dict:
    """Catch-all route: every tagged datagen event forwards to the logging tier."""
    return {"id": "to_logging", "name": "to_logging", "filter": "true", "final": True,
            "output": "to_logging", "pipeline": "passthru",
            "description": "forward all tagged events to the logging tier"}


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


def _upsert(coll_url, obj, h):
    """POST a config object; if it already exists, PATCH it by id (idempotent)."""
    r = requests.post(coll_url, json=obj, headers=h, timeout=10)
    if r.status_code == 409 or (r.status_code >= 400 and "already exists" in r.text.lower()):
        requests.patch(f"{coll_url}/{obj['id']}", json=obj, headers=h, timeout=10).raise_for_status()
    else:
        r.raise_for_status()


def seed_fleet(base, h, fleet, forward_to, accounts):
    """Managed-Edge collection config on the leader's single fleet (/m/<fleet>).

    Pushes the shared tcpjson forward output, then each account's datagen sources +
    tag Eval pipelines (account identity is a literal per source), then a catch-all
    route → to_logging. Edge Nodes enrolled into <fleet> pull this config. The portal
    discovers these inputs from the leader. Idempotent (deterministic ids; upsert).
    """
    g = f"{base}/api/v1/m/{fleet}"
    _upsert(f"{g}/system/outputs", build_tcp_output(forward_to), h)
    for name in accounts:
        cfg = SPEC["edges"][name]
        for s in cfg["sources"]:
            _upsert(f"{g}/pipelines",
                    build_eval_pipeline(cfg["account_id"], cfg["account_alias"],
                                        s["workload"], s["source_name"]), h)
            _upsert(f"{g}/system/inputs",
                    build_datagen_source(s["workload"], s["source_name"], s["eventsPerSec"]), h)
    doc = requests.get(f"{g}/routes", headers=h, timeout=10).json()
    table = doc["items"][0]
    table["routes"] = [r for r in table["routes"] if r["id"] != "to_logging"] + [build_fleet_route()]
    requests.patch(f"{g}/routes/default", json=table, headers=h, timeout=10).raise_for_status()
    _commit_deploy(base, fleet, h)


def main() -> None:
    base = os.environ.get("CRIBL_BASE_URL", "http://localhost:9000").rstrip("/")
    h = _headers(base)
    seed_logging(base, h)
    fleet = os.environ.get("CRIBL_FLEET")
    if fleet:
        # Managed-Edge model: seed the leader's single fleet. FLEET_ACCOUNTS is a
        # comma list of SPEC["edges"] keys (e.g. "acct_b"); FLEET_FORWARD_HOST is the
        # logging-tier ingest reachable from the enrolled Edge (its endpoint DNS:10300).
        accounts = [a for a in os.environ.get("FLEET_ACCOUNTS", "").split(",") if a]
        seed_fleet(base, h, fleet, os.environ["FLEET_FORWARD_HOST"], accounts)
        print(f"Cribl seeded: logging archive + fleet {fleet} ({', '.join(accounts)}).")
        return
    # Legacy standalone-Edge model (each Edge configured via its own flat API).
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
