# Databricks notebook source
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Read-only Databricks serverless runtime identity probe; no catalog effects."""
import json
import uuid

dbutils.widgets.text("protocol_identity", "")
protocol_identity = dbutils.widgets.get("protocol_identity")

version = spark.version
try:
    build_text = spark.sql("SELECT version() AS sparkBuildText").first()["sparkBuildText"]
except Exception:
    build_text = None
try:
    runtime_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
except Exception:
    runtime_tag = None
try:
    identity = spark.sql("SELECT current_user() AS principal, current_catalog() AS catalog, "
                         "current_schema() AS schema").first()
except Exception:
    identity = None

# Qualify the Spark Connect per-session tag and interrupt APIs with one tiny,
# completed query. This does not test cancellation of a long-running operation.
probe_session = spark.newSession()
required_methods = ("addTag", "getTags", "removeTag", "interruptTag", "sql", "stop")
if probe_session is spark or any(not callable(getattr(probe_session, name, None))
                                 for name in required_methods):
    raise RuntimeError("Distinct Spark Connect session and tag/cancellation APIs required")
original_tags = probe_session.getTags()
if type(original_tags) is not set or any(type(tag) is not str for tag in original_tags):
    raise RuntimeError("Spark Connect getTags() must return a text set")
tag = "ashlar-runtime-probe-" + uuid.uuid4().hex
tagged_rows = None
interrupt_result = None
try:
    probe_session.addTag(tag)
    if tag not in probe_session.getTags():
        raise RuntimeError("Spark Connect failed to retain an operation tag")
    tagged_rows = probe_session.sql("SELECT 1 AS probe_value").collect()
    if len(tagged_rows) != 1 or int(tagged_rows[0]["probe_value"]) != 1:
        raise RuntimeError("Bounded one-row tagged Spark SQL probe failed")
    interrupt_result = probe_session.interruptTag(tag)
    if type(interrupt_result) is not list or any(type(value) is not str for value in interrupt_result):
        raise RuntimeError("Spark Connect interruptTag() response is not a list of operation IDs")
finally:
    try:
        probe_session.removeTag(tag)
    except Exception:
        if tag in probe_session.getTags():
            raise
try:
    final_tags = probe_session.getTags()
    if type(final_tags) is not set or final_tags != original_tags:
        raise RuntimeError("Spark Connect tag cleanup did not restore the original tag set")
finally:
    # A child Connect session is owned by this probe; release it without
    # stopping the serverless job's parent SparkSession.
    probe_session.stop()

result = {
    "runtimeName": "databricks-serverless",
    "sparkVersion": version,
    "sparkBuildText": build_text,
    "runtimeTag": runtime_tag,
    "principal": None if identity is None else identity["principal"],
    "catalog": None if identity is None else identity["catalog"],
    "schema": None if identity is None else identity["schema"],
    "protocol": protocol_identity,
    "serverlessApiProbe": {
        "profile": "ashlar-spark-connect-tag-api-probe/0.1",
        "newSessionDistinct": probe_session is not spark,
        "tagAddedAndObserved": True,
        "taggedQueryRowCount": len(tagged_rows),
        "taggedQueryValue": int(tagged_rows[0]["probe_value"]),
        "interruptTagResponseType": "list",
        "interruptTagResponseCount": len(interrupt_result),
        "tagRemoved": True,
        "preexistingTagsRestored": True,
        "operationTerminationClaimed": False,
    },
    "sideEffects": [],
}
dbutils.notebook.exit(json.dumps(result, sort_keys=True, separators=(",", ":")))
