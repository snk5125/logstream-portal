import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
from scripts.seed_cribl import (
    build_archive_route, build_datagen_source, build_eval_pipeline,
    build_fleet_route, build_s3_destination, build_tcp_output,
)

SPEC = json.loads((ROOT / "cribl" / "seed_spec.json").read_text())


def test_datagen_source_id_and_rate():
    src = build_datagen_source("storefront_web", "syslog", 4)
    assert src["id"] == "ds_storefront_web_syslog"
    assert src["type"] == "datagen"
    assert src["samples"][0]["eventsPerSec"] == 4
    # sendToRoutes + tag pre-processing pipeline; events forward via the route table
    assert src["sendToRoutes"] is True
    assert src["pipeline"] == "tag_storefront_web_syslog"
    assert src["samples"][0]["sample"] == "syslog"  # valid built-in sample id


def test_datagen_maps_unknown_source_to_a_valid_sample():
    src = build_datagen_source("orders_api", "app_log", 2)
    assert src["samples"][0]["sample"] == "business_event"


def test_eval_pipeline_stamps_all_tag_fields():
    pl = build_eval_pipeline("522412052544", "prod-ecommerce", "orders_api", "auth_log")
    adds = pl["conf"]["functions"][0]["conf"]["add"]
    by = {a["name"]: a["value"] for a in adds}
    assert by["account_id"] == "'522412052544'"
    assert by["account_alias"] == "'prod-ecommerce'"
    assert by["environment"] == "'prod'"
    assert by["workload"] == "'orders_api'"
    assert by["source_name"] == "'auth_log'"


def test_tcp_output_targets_forward_host():
    out = build_tcp_output("fork-ingest-b:10300")
    assert out["type"] == "tcpjson"   # Cribl-native forward (validated live)
    assert out["host"] == "fork-ingest-b"
    assert out["port"] == 10300


def test_fleet_route_is_final_catch_all_to_logging():
    r = build_fleet_route()
    assert r["filter"] == "true"
    assert r["final"] is True
    assert r["output"] == "to_logging"
    assert r["pipeline"] == "passthru"


def test_s3_destination_bucket_and_auto_auth():
    d = build_s3_destination("log-archive-337394138208")
    assert d["id"] == "archive"
    assert d["type"] == "s3"
    assert d["bucket"] == "log-archive-337394138208"
    assert d["awsAuthenticationMethod"] == "auto"


def test_archive_route_is_final_catch_all():
    r = build_archive_route()
    assert r["id"] == "archive"
    assert r["filter"] == "true"
    assert r["final"] is True
    assert r["output"] == "archive"
