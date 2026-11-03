# Databricks notebook source
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Read-only Databricks serverless runtime identity probe; no catalog effects."""
import json
import queue
import threading
import time
import uuid

dbutils.widgets.text("protocol_identity", "")
protocol_identity = dbutils.widgets.get("protocol_identity")

probe_deadline = time.monotonic() + 45

def bounded_tagged_query(sql, action="first"):
    tag_value = "ashlar-runtime-probe-" + uuid.uuid4().hex
    result = queue.Queue(maxsize=1)
    def invoke():
        original = None
        value = None
        error = None
        success = False
        try:
            original = spark.getTags()
            if type(original) is not set or any(type(value) is not str for value in original):
                raise RuntimeError("Spark Connect getTags() must return a text set")
            spark.addTag(tag_value)
            if tag_value not in spark.getTags():
                raise RuntimeError("Spark Connect failed to retain a thread-local operation tag")
            value = getattr(spark.sql(sql), action)()
            success = True
        except BaseException as caught:
            error = caught
        finally:
            try:
                if original is not None and tag_value in spark.getTags():
                    spark.removeTag(tag_value)
                if original is not None and spark.getTags() != original:
                    raise RuntimeError("Thread-local tag cleanup did not restore prior tags")
            except BaseException as cleanup_error:
                if error is None:
                    error = cleanup_error
                success = False
        result.put((success, value, original, error))
    worker = threading.Thread(target=invoke, name="ashlar-runtime-probe-sql", daemon=True)
    worker.start()
    worker.join(max(0, probe_deadline - time.monotonic()))
    if worker.is_alive():
        try:
            spark.interruptTag(tag_value)
        except Exception:
            pass
        raise RuntimeError("Tagged serverless SQL exceeded the probe deadline; interrupt requested, completion unknown")
    success, value, original, error = result.get_nowait()
    if error is not None:
        raise error
    return value

version = spark.version
try:
    build_text = bounded_tagged_query("SELECT version() AS sparkBuildText")["sparkBuildText"]
except Exception:
    build_text = None
try:
    runtime_tag = spark.conf.get("spark.databricks.clusterUsageTags.sparkVersion")
except Exception:
    runtime_tag = None
try:
    identity = bounded_tagged_query("SELECT current_user() AS principal, current_catalog() AS catalog, "
                                    "current_schema() AS schema")
except Exception:
    identity = None

# Serverless Jobs injects the task's Spark Connect session. Spark tags are
# thread-local, so bind, observe, query and restore them on the same worker
# thread. This probes one completed query; it does not claim interruption
# terminates an in-flight remote operation.
required_methods = ("addTag", "getTags", "removeTag", "interruptTag", "sql")
if any(not callable(getattr(spark, name, None)) for name in required_methods):
    raise RuntimeError("Injected task Spark session lacks the required tag/query APIs")
tag = "ashlar-runtime-probe-" + uuid.uuid4().hex
result_queue = queue.Queue(maxsize=1)

def tagged_query():
    try:
        original_tags = spark.getTags()
        if type(original_tags) is not set or any(type(value) is not str for value in original_tags):
            raise RuntimeError("Spark Connect getTags() must return a text set")
        spark.addTag(tag)
        if tag not in spark.getTags():
            raise RuntimeError("Spark Connect failed to retain a thread-local operation tag")
        rows = spark.sql("SELECT 1 AS probe_value").collect()
        if len(rows) != 1 or int(rows[0]["probe_value"]) != 1:
            raise RuntimeError("Bounded one-row tagged Spark SQL probe failed")
        interrupted = spark.interruptTag(tag)
        if type(interrupted) is not list or any(type(value) is not str for value in interrupted):
            raise RuntimeError("Spark Connect interruptTag() response is not a list of operation IDs")
        spark.removeTag(tag)
        if spark.getTags() != original_tags:
            raise RuntimeError("Thread-local tag cleanup did not restore prior tags")
        result_queue.put((rows, interrupted, None))
    except BaseException as error:
        try:
            if tag in spark.getTags():
                spark.removeTag(tag)
        except BaseException:
            pass
        result_queue.put((None, None, error))

query_thread = threading.Thread(target=tagged_query, name="ashlar-runtime-tag-probe", daemon=True)
query_thread.start()
query_thread.join(max(0, probe_deadline - time.monotonic()))
if query_thread.is_alive():
    try:
        interrupt_result = spark.interruptTag(tag)
    except Exception:
        interrupt_result = None
    raise RuntimeError("Tagged query exceeded 15 seconds; interrupt requested, completion unknown")
tagged_rows, interrupt_result, query_error = result_queue.get_nowait()
if query_error is not None:
    raise query_error

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
        "profile": "ashlar-spark-connect-tag-api-probe/0.2",
        "providedJobSessionBorrowed": True,
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
