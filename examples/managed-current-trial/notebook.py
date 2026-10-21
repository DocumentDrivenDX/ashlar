# Databricks notebook source
# One task invocation consumes one privately delivered, digest-bound Truss page.
# Secret values are read into memory only and are never printed or returned.

import json
from ashlar.protocol import ReaderProtocolProfile
from ashlar_host.managed_current_trial import ManagedCurrentTrialConfig
from ashlar_host.managed_current_campaign import run_managed_current_campaign
from umf_access.types import StrictModel

dbutils.widgets.text("private_config_scope", "")
dbutils.widgets.text("private_config_key", "")
dbutils.widgets.text("private_page_scope", "")
dbutils.widgets.text("private_page_key", "")
dbutils.widgets.text("private_oracle_scope", "")
dbutils.widgets.text("private_oracle_key", "")
dbutils.widgets.text("job_run_id", "{{job.run_id}}")
dbutils.widgets.text("task_run_id", "{{task.run_id}}")


def _secret(scope_widget, key_widget):
    scope = dbutils.widgets.get(scope_widget)
    key = dbutils.widgets.get(key_widget)
    if not scope or not key or any(c in scope + key for c in "\r\n\0"):
        raise ValueError("Private trial input reference required")
    return dbutils.secrets.get(scope=scope, key=key)


private_config = json.loads(_secret("private_config_scope", "private_config_key"))
private_page = json.loads(_secret("private_page_scope", "private_page_key"))
private_oracle = json.loads(_secret("private_oracle_scope", "private_oracle_key"))
required_config = {
    "catalog", "schema", "stream", "predecessor", "schedule_sha256", "schedule_step",
    "stage_id", "schema_revisions_json",
    "materialized_at", "expected_environment", "maximum_task_seconds",
    "carrier_profile",
    "estimated_total_dbu", "authorized_total_dbu", "estimated_total_cost_usd", "reserve_dbu_for_cleanup",
    "reserve_usd_for_cleanup", "cost_estimate_includes_startup_driver_and_cleanup",
    "create_schema", "preferred_budget_exception", "retries", "max_concurrent_runs",
    "dedicated_serverless", "predictive_optimization_disabled", "protocol_profile",
}
if set(private_config) not in (required_config, required_config | {"publication_profile"}):
    raise ValueError("Closed private managed-current trial configuration required")
if set(private_page) != {"binding"} or type(private_page["binding"]) is not dict:
    raise ValueError("Exactly one private original source page bundle required")
if set(private_oracle) != {"source_oracle"} or type(private_oracle["source_oracle"]) is not dict:
    raise ValueError("Exactly one private digest-bound external Truss source oracle required")

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
if config.carrier_profile != "ashlar-delta/0.4-xxhash64":
    raise ValueError("Fresh managed-current functional campaign requires the selected xxhash64 carrier profile")
if config.expected_environment["protocol"] != protocol_profile.identity:
    raise ValueError("Selected runtime and native protocol profile differ")

class EmptyPageParameters(StrictModel):
    pass


class PersonPage(StrictModel):
    person_key: str
    person_name: str


job_run_id = dbutils.widgets.get("job_run_id")
task_run_id = dbutils.widgets.get("task_run_id")
if not job_run_id.isdecimal() or not task_run_id.isdecimal():
    raise ValueError("Actual Jobs job-run and task-run IDs are required")

import weft

result = run_managed_current_campaign(
    spark,
    config=config,
    binding=private_page["binding"],
    protocol_profile=protocol_profile,
    source_oracle_input=private_oracle["source_oracle"],
    compiler=weft,
    page_parameters=EmptyPageParameters,
    page_result=PersonPage,
    grouped_sql=("SELECT p.person_key, COUNT(*) AS paths, "
        "COUNT_DISTINCT_PATH_TARGETS(x) AS targets FROM Person p "
        "CROSS JOIN EXPAND_PATHS(p.knows, located) AS x GROUP BY p.person_key "
        "HAVING COUNT(*) > 0 ORDER BY p.person_key"),
    negative_sql=("SELECT p.person_key, COUNT(*) AS paths, "
        "COUNT_DISTINCT_PATH_TARGETS(x) AS targets FROM Person p "
        "CROSS JOIN EXPAND_PATHS(p.knows, located) AS x GROUP BY p.person_key "
        "HAVING COUNT(*) > 2 ORDER BY p.person_key"),
    page_sql="SELECT p.person_key,p.person_name FROM Person p",
    run_id=int(job_run_id), task_receipt_id=task_run_id,
)
# The result includes the durable destination vector and four exact query
# cases. The source page, source oracle, raw cursors, credentials and page
# continuation bytes remain private to their respective task owners.
dbutils.notebook.exit(json.dumps(result, ensure_ascii=False, separators=(",", ":")))
