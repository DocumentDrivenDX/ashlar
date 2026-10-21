# Databricks notebook source
# One task invocation consumes one privately delivered, digest-bound Truss page.
# Secret values are read into memory only and are never printed or returned.

import json
from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_trial import (
    ManagedCurrentTrialConfig,
    run_managed_current_trial_page,
)

dbutils.widgets.text("private_config_scope", "")
dbutils.widgets.text("private_config_key", "")
dbutils.widgets.text("private_page_scope", "")
dbutils.widgets.text("private_page_key", "")


def _secret(scope_widget, key_widget):
    scope = dbutils.widgets.get(scope_widget)
    key = dbutils.widgets.get(key_widget)
    if not scope or not key or any(c in scope + key for c in "\r\n\0"):
        raise ValueError("Private trial input reference required")
    return dbutils.secrets.get(scope=scope, key=key)


private_config = json.loads(_secret("private_config_scope", "private_config_key"))
private_page = json.loads(_secret("private_page_scope", "private_page_key"))
if set(private_config) != {
    "catalog", "schema", "stream", "predecessor", "schedule_sha256", "schedule_step",
    "stage_id", "schema_revisions_json",
    "materialized_at", "expected_environment", "maximum_task_seconds",
    "estimated_total_dbu", "authorized_total_dbu", "estimated_total_cost_usd", "reserve_dbu_for_cleanup",
    "reserve_usd_for_cleanup", "cost_estimate_includes_startup_driver_and_cleanup",
    "create_schema", "preferred_budget_exception", "retries", "max_concurrent_runs",
    "dedicated_serverless", "predictive_optimization_disabled", "protocol_profile",
}:
    raise ValueError("Closed private managed-current trial configuration required")
if set(private_page) != {"binding"} or type(private_page["binding"]) is not dict:
    raise ValueError("Exactly one private original source page bundle required")

protocol = private_config.pop("protocol_profile")
if type(protocol) is not dict or set(protocol) != {
    "identity", "reader_versions", "writer_versions", "features"
}:
    raise ValueError("Exact selected native Delta reader protocol required")
protocol_profile = ReaderProtocolProfile(
    protocol["identity"], tuple(protocol["reader_versions"]),
    tuple(protocol["writer_versions"]), frozenset(protocol["features"]),
)
config = ManagedCurrentTrialConfig(**private_config)
if config.expected_environment["protocol"] != protocol_profile.identity:
    raise ValueError("Selected runtime and native protocol profile differ")

result = run_managed_current_trial_page(
    spark,
    config=config,
    binding=private_page["binding"],
    protocol_profile=protocol_profile,
)
# The returned destination receipt contains the durable four-role manifest
# vector and six-role install inventory, but never the source page/cursor.
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
