# Databricks notebook source
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Read-only Databricks serverless runtime identity probe; no catalog effects."""
import json

version = spark.version
try:
    build_text = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
except Exception:
    build_text = None
try:
    runtime_tag = spark.conf.get("spark.databricks.clusterUsageTags.runtimeTag")
except Exception:
    runtime_tag = None
try:
    identity = spark.sql("SELECT current_user() AS principal, current_catalog() AS catalog, "
                         "current_schema() AS schema").first()
except Exception:
    identity = None

result = {
    "runtimeName": "databricks-serverless",
    "sparkVersion": version,
    "sparkBuildText": build_text,
    "runtimeTag": runtime_tag,
    "principal": None if identity is None else identity["principal"],
    "catalog": None if identity is None else identity["catalog"],
    "schema": None if identity is None else identity["schema"],
    "capabilities": {"unityCatalog": True, "serverless": True},
    "sideEffects": [],
}
dbutils.notebook.exit(json.dumps(result, sort_keys=True, separators=(",", ":")))
