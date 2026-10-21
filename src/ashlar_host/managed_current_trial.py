# SPDX-License-Identifier: MIT OR Apache-2.0
"""Fail-closed entrypoints for one bounded managed-current trial stage.

The Databricks notebook supplies its injected SparkSession and one privately
delivered, digest-bound Truss page. This module does not create jobs, obtain
credentials, fetch a source page, or acknowledge it.
"""
from dataclasses import dataclass
import json
import math
import re

from .managed_current_core import ManagedCurrentCoreConfig

_IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
_COST_CEILING_USD = 100.0
_PREFERRED_CEILING_USD = 10.0
_REQUIRED_RUNTIME_KEYS = {
    "runtimeName", "sparkVersion", "sparkBuildText", "runtimeTag", "principal",
    "catalog", "schema", "protocol",
}


def _require(value, reason="Managed current trial configuration refused"):
    if not value:
        raise ValueError(reason)


@dataclass(frozen=True)
class ManagedCurrentTrialConfig:
    """Small closed contract for a single dedicated serverless Jobs task.

    Values are supplied privately at task start. No field contains a token or
    password. The opaque source page is passed separately and is never included
    in reprs, notebook widgets, logs, or the returned destination receipt.
    """

    catalog: str
    schema: str
    stream: str
    predecessor: str
    schedule_sha256: str
    schedule_step: int
    stage_id: str
    schema_revisions_json: str
    materialized_at: str
    expected_environment: dict
    maximum_task_seconds: int
    estimated_total_dbu: float
    authorized_total_dbu: float
    estimated_total_cost_usd: float
    reserve_dbu_for_cleanup: float
    reserve_usd_for_cleanup: float
    cost_estimate_includes_startup_driver_and_cleanup: bool
    create_schema: bool
    preferred_budget_exception: str | None = None
    retries: int = 0
    max_concurrent_runs: int = 1
    dedicated_serverless: bool = True
    predictive_optimization_disabled: bool = False

    def validate(self):
        _require(type(self.catalog) is str and _IDENT.fullmatch(self.catalog)
                 and type(self.schema) is str and _IDENT.fullmatch(self.schema),
                 "Exact selected managed catalog and new schema names required")
        _require(self.catalog == "client_dev",
                 "The current functional trial is scoped to client_dev")
        _require(type(self.stream) is str and self.stream and "\0" not in self.stream
                 and type(self.predecessor) is str and self.predecessor and "\0" not in self.predecessor,
                 "Original source stream and predecessor required")
        _require(type(self.schedule_sha256) is str
                 and re.fullmatch(r"[0-9a-f]{64}", self.schedule_sha256)
                 and type(self.schedule_step) is int and self.schedule_step >= 1
                 and type(self.stage_id) is str and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,63}", self.stage_id),
                 "Frozen versioned request schedule digest and step identity required")
        _require(type(self.schema_revisions_json) is str and self.schema_revisions_json
                 and type(self.materialized_at) is str and re.fullmatch(r"0|[1-9][0-9]{0,18}", self.materialized_at)
                 and int(self.materialized_at) < 2**63,
                 "Canonical source revisions and bounded materialization timestamp required")
        try:
            revisions = json.loads(self.schema_revisions_json)
        except (TypeError, json.JSONDecodeError):
            _require(False, "Canonical source revision vector required")
        _require(type(revisions) is dict and bool(revisions)
                 and json.dumps(revisions, sort_keys=True, separators=(",", ":")) == self.schema_revisions_json,
                 "Canonical source revision vector required")
        _require(type(self.expected_environment) is dict
                 and set(self.expected_environment) == _REQUIRED_RUNTIME_KEYS
                 and self.expected_environment.get("runtimeName") == "databricks-serverless"
                 and self.expected_environment.get("catalog") == self.catalog
                 and self.expected_environment.get("schema") == self.schema
                 and all(value is None or (type(value) is str and value and "\0" not in value)
                         for value in self.expected_environment.values())
                 and all(type(self.expected_environment[key]) is str and self.expected_environment[key]
                         for key in ("runtimeName", "sparkVersion", "principal", "catalog", "schema", "protocol")),
                 "Exact selected serverless runtime, principal, namespace, and native protocol required")
        _require(type(self.maximum_task_seconds) is int and 1 <= self.maximum_task_seconds <= 7200,
                 "Finite dedicated task timeout required")
        _require(type(self.estimated_total_dbu) in (int, float) and math.isfinite(self.estimated_total_dbu)
                 and self.estimated_total_dbu > 0
                 and type(self.authorized_total_dbu) in (int, float) and math.isfinite(self.authorized_total_dbu)
                 and self.authorized_total_dbu > 0
                 and type(self.reserve_dbu_for_cleanup) in (int, float)
                 and math.isfinite(self.reserve_dbu_for_cleanup)
                 and self.reserve_dbu_for_cleanup >= 0
                 and self.estimated_total_dbu + self.reserve_dbu_for_cleanup <= self.authorized_total_dbu,
                 "Finite approved DBU envelope including cleanup reserve required")
        _require(type(self.estimated_total_cost_usd) in (int, float)
                 and type(self.reserve_usd_for_cleanup) in (int, float)
                 and math.isfinite(self.estimated_total_cost_usd)
                 and math.isfinite(self.reserve_usd_for_cleanup)
                 and self.cost_estimate_includes_startup_driver_and_cleanup is True
                 and self.estimated_total_cost_usd > 0 and self.reserve_usd_for_cleanup >= 0
                 and self.estimated_total_cost_usd + self.reserve_usd_for_cleanup <= _COST_CEILING_USD,
                 "Estimated total including startup, driver, and cleanup must remain below the authorized $100 ceiling")
        if self.estimated_total_cost_usd + self.reserve_usd_for_cleanup > _PREFERRED_CEILING_USD:
            _require(type(self.preferred_budget_exception) is str and self.preferred_budget_exception.strip()
                     and len(self.preferred_budget_exception.encode("utf-8")) <= 1024
                     and "\0" not in self.preferred_budget_exception,
                     "An estimate above the preferred $10 budget needs an explicit owner rationale")
        else:
            _require(self.preferred_budget_exception is None,
                     "Preferred-budget exception must be omitted for estimates at or below $10")
        _require(self.retries == 0 and self.max_concurrent_runs == 1 and self.dedicated_serverless is True,
                 "One dedicated, single-concurrency serverless task with no retries required")
        _require(self.predictive_optimization_disabled is False,
                 "Predictive optimization must remain enabled")
        _require(type(self.create_schema) is bool, "Explicit fresh-schema creation decision required")
        return None

    def core_config(self):
        self.validate()
        return ManagedCurrentCoreConfig(
            catalog=self.catalog,
            schema=self.schema,
            backend03={"surface": "Backend03", "backendId": "ashlar.databricks.managed-uc-spark",
                       "backendVersion": "0.1.0-managed-current-core-candidate",
                       "targetProfile": "ashlar-managed-current-core/0.1",
                       "weftInterface": "weft-compile/0.4.1", "weftDialect": "weft-sql/0.4.1"},
            backend02={"surface": "Backend02", "backendId": "ashlar.databricks.managed-uc-spark",
                       "backendVersion": "0.1.0-managed-current-core-pages-candidate",
                       "targetProfile": "ashlar-managed-current-core/0.1",
                       "weftInterface": "weft-compile/0.2.7", "weftDialect": "weft-sql/0.2.0"},
            preferred_cost_usd=10,
            hard_cost_usd=100,
        )


def _quote_namespace(catalog, schema):
    _require(_IDENT.fullmatch(catalog) is not None and _IDENT.fullmatch(schema) is not None)
    return "`" + catalog + "`.`" + schema + "`"


def _schema_exists(spark, catalog, schema):
    rows = spark.sql(
        "SELECT schema_name FROM system.information_schema.schemata "
        "WHERE catalog_name=:catalog AND schema_name=:schema",
        args={"catalog": catalog, "schema": schema},
    ).collect()
    _require(len(rows) <= 1, "Ambiguous Unity Catalog schema inventory")
    return bool(rows)


def run_managed_current_trial_page(spark, *, config, binding, protocol_profile):
    """Run exactly one original source page and return its destination receipt.

    The external source holder must retain this result, independently reopen
    the exact UC manifest/vector, and only then ACK the original source page.
    The helper never retries a page or source ACK.
    """
    from .managed_current_publication import run_managed_current_publication_task
    _require(type(config) is ManagedCurrentTrialConfig,
             "Closed managed current trial configuration required")
    config.validate()
    _require(type(binding) is dict and binding.get("profile") == "ashlar-managed-current-core-binding/0.1"
             and binding.get("status") == "preinstallation-source-bundle",
             "One digest-bound original current Truss source bundle required")
    exists = _schema_exists(spark, config.catalog, config.schema)
    if config.create_schema:
        _require(not exists, "Requested new managed trial schema already exists")
        spark.sql("CREATE SCHEMA " + _quote_namespace(config.catalog, config.schema)).collect()
        _require(_schema_exists(spark, config.catalog, config.schema),
                 "Fresh managed trial schema was not observed after creation")
    else:
        _require(exists, "Existing per-run trial schema required for this page")
    result = run_managed_current_publication_task(
        spark,
        binding=binding,
        core_config=config.core_config(),
        protocol_profile=protocol_profile,
        stream=config.stream,
        predecessor=config.predecessor,
        schema_revisions_json=config.schema_revisions_json,
        materialized_at=config.materialized_at,
        expected_environment=config.expected_environment,
    )
    return {
        "profile": "ashlar-managed-current-trial-step/0.1",
        "scheduleSha256": config.schedule_sha256,
        "scheduleStep": config.schedule_step,
        "stageId": config.stage_id,
        "task": result,
    }


def cleanup_managed_current_trial_schema(spark, *, catalog, schema, expected_principal,
                                         expected_inventory):
    """Delete only the complete six-role installation owned by this trial.

    This is intentionally a separate, explicit terminal call after result and
    billing receipts are retained by the operator. Unknown tables, mismatched
    UUIDs, or owner drift refuse cleanup; the schema is never dropped CASCADE.
    """
    from .managed_current_core import ROLES
    _require(type(catalog) is str and catalog == "client_dev"
             and _IDENT.fullmatch(catalog) and type(schema) is str and _IDENT.fullmatch(schema)
             and type(expected_principal) is str and expected_principal,
             "Exact owned trial schema and principal required for cleanup")
    _require(type(expected_inventory) is dict and set(expected_inventory) == set(ROLES)
             and all(type(item) is dict and set(item) == {"uuid", "owner"}
                     and type(item["uuid"]) is str and item["uuid"]
                     and item["owner"] == expected_principal for item in expected_inventory.values()),
             "Complete six-role trial UUID and owner receipt required")
    namespace = _quote_namespace(catalog, schema)
    try:
        tables = {row["tableName"] for row in spark.sql("SHOW TABLES IN " + namespace).collect()}
    except Exception:
        _require(not _schema_exists(spark, catalog, schema), "Could not verify trial schema inventory")
        return {"schemaAbsent": True, "dropped": []}
    _require(tables == set(ROLES), "Unknown or missing schema objects; refusing cleanup")
    actual = {}
    for role in sorted(ROLES):
        name = namespace + ".`" + role + "`"
        detail = spark.sql("DESCRIBE DETAIL " + name).first()
        owner = spark.sql(
            "SELECT table_owner FROM system.information_schema.tables "
            "WHERE table_catalog=:catalog AND table_schema=:schema AND table_name=:name",
            args={"catalog": catalog, "schema": schema, "name": role},
        ).collect()
        _require(detail is not None and len(owner) == 1
                 and detail["id"] == expected_inventory[role]["uuid"]
                 and owner[0]["table_owner"] == expected_principal,
                 "Trial object UUID or owner changed; refusing cleanup")
        actual[role] = detail["id"]
    dropped = []
    for role in sorted(ROLES, reverse=True):
        name = namespace + ".`" + role + "`"
        detail = spark.sql("DESCRIBE DETAIL " + name).first()
        _require(detail is not None and detail["id"] == expected_inventory[role]["uuid"],
                 "Trial object was replaced during cleanup")
        spark.sql("DROP TABLE " + name).collect()
        _require(not spark.sql("SHOW TABLES IN " + namespace + " LIKE '" + role + "'").collect(),
                 "Trial table absence was not verified")
        dropped.append(role)
    _require(not spark.sql("SHOW TABLES IN " + namespace).collect(),
             "Trial schema contains objects after owned table cleanup")
    spark.sql("DROP SCHEMA " + namespace).collect()
    _require(not _schema_exists(spark, catalog, schema), "Trial schema absence was not verified")
    return {"schemaAbsent": True, "dropped": dropped, "uuids": actual}
