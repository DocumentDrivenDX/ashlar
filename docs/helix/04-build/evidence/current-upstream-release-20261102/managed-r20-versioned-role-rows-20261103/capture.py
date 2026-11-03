# Databricks notebook source
"""Read-only, receipt-pinned six-role R20 row/schema capture."""
import base64
import datetime
import decimal
import json
import time

PROFILE = "ashlar-managed-a1-r20-versioned-role-rows/0.1"
RECEIPT_SHA256 = "9b90f21685e92e8e236b632d779978e0c66840248bcff4da7545c23802d18b78"
RECEIPT_LOCAL_PATH = "/private/var/folders/z0/kv_wlyps77b_txh08w55z99w0000gn/T/ashlar-a1-recovery-mddw1v9u/a0_publisher_result.bin"
PUBLICATION_ID = "ashlar-current-incremental:39f0c463cf28854ed0b37d277b9c6be68fc15ab5d3b344f486ed2b5561618374"
PINS = {"edge_current":{"name":"client_dev.ashlar_a1_release_20261103_serverless_r1.edge_current","uuid":"7bdb2ea3-1137-467b-9e78-f94cef34b76f","version":0},"object_current":{"name":"client_dev.ashlar_a1_release_20261103_serverless_r1.object_current","uuid":"159db8f0-ab8f-4a36-a40e-267ae003640a","version":0},"publication_attempt_phase":{"name":"client_dev.ashlar_a1_release_20261103_serverless_r1.publication_attempt_phase","uuid":"f873e663-2509-4cd0-aa2a-87237d507254","version":5},"publication_manifest":{"name":"client_dev.ashlar_a1_release_20261103_serverless_r1.publication_manifest","uuid":"9f185334-d4c3-466d-ac2a-2652c53a5ea5","version":2},"tombstone":{"name":"client_dev.ashlar_a1_release_20261103_serverless_r1.tombstone","uuid":"c7b24be8-dab0-4831-be1f-dcc9eecb06c9","version":0},"whole_source_history":{"name":"client_dev.ashlar_a1_release_20261103_serverless_r1.whole_source_history","uuid":"6a09f8ee-5866-4bf7-b77f-2672f091fcb2","version":0}}
MAX_ROWS_PER_ROLE = 128
MAX_OUTPUT_BYTES = 2 * 1024 * 1024
MAX_BODY_SECONDS = 150


def _native(value):
    if value is None or type(value) in (str, int, bool):
        return value
    if type(value) is float:
        if value != value or value in (float("inf"), float("-inf")):
            raise ValueError("Non-finite native float is not serializable")
        return value
    if isinstance(value, datetime.datetime):
        return {"$native_type": "datetime", "isoformat": value.isoformat(),
                "fold": value.fold}
    if isinstance(value, datetime.date):
        return {"$native_type": "date", "isoformat": value.isoformat()}
    if isinstance(value, decimal.Decimal):
        return {"$native_type": "decimal", "text": str(value)}
    if isinstance(value, (bytes, bytearray, memoryview)):
        return {"$native_type": "binary", "base64": base64.b64encode(bytes(value)).decode("ascii")}
    if isinstance(value, (list, tuple)):
        return [_native(item) for item in value]
    if isinstance(value, dict):
        return {"$native_type": "map", "entries": [[_native(k), _native(v)]
                for k, v in value.items()]}
    as_dict = getattr(value, "asDict", None)
    if callable(as_dict):
        return {"$native_type": "struct", "fields": _native(as_dict(recursive=False))}
    raise TypeError("Unsupported collected Spark value type: " + type(value).__name__)


def _check_time(started):
    if time.monotonic() - started > MAX_BODY_SECONDS:
        raise TimeoutError("Read-only role capture exceeded 150-second body budget")


def _identifier(qualified_name):
    parts = qualified_name.split(".")
    if len(parts) != 3 or any(not part or "`" in part for part in parts):
        raise ValueError("Receipt table name is not a closed three-part identifier")
    return ".".join("`" + part + "`" for part in parts)


def capture(spark):
    started = time.monotonic()
    captured = {}
    for role in sorted(PINS):
        pin = PINS[role]
        table = _identifier(pin["name"])
        _check_time(started)
        detail_rows = spark.sql("DESCRIBE DETAIL " + table).limit(2).collect()
        _check_time(started)
        if len(detail_rows) != 1:
            raise ValueError("DESCRIBE DETAIL must return exactly one row for " + role)
        detail = detail_rows[0].asDict(recursive=True)
        if detail.get("id") != pin["uuid"]:
            raise ValueError("Current table UUID differs from producer receipt for " + role)
        # Version and UUID are bound to the producer's exact receipt; each role's
        # independent read is pinned to that version, never to the moving head.
        sql = "SELECT * FROM " + table + " VERSION AS OF " + str(pin["version"])
        _check_time(started)
        frame = spark.sql(sql)
        schema_json = frame.schema.jsonValue()
        schema_simple = frame.schema.simpleString()
        fields = list(frame.schema.fields)
        columns = [field.name for field in fields]
        if not columns or len(columns) > 128 or len(columns) != len(set(columns)):
            raise ValueError("Role schema has invalid/oversized/duplicate column names: " + role)
        rows = frame.limit(MAX_ROWS_PER_ROLE + 1).collect()
        _check_time(started)
        if len(rows) > MAX_ROWS_PER_ROLE:
            raise ValueError("Role row bound exceeded: " + role)
        encoded_rows = []
        for row in rows:
            values = tuple(row)
            if len(values) != len(columns):
                raise ValueError("Native row width differs from captured schema: " + role)
            encoded_rows.append({columns[i]: _native(values[i]) for i in range(len(columns))})
        captured[role] = {
            "table_name": pin["name"],
            "table_uuid": pin["uuid"],
            "version_as_of": pin["version"],
            "describe_detail_uuid": detail["id"],
            "select_sql": sql,
            "schema_simple_string": schema_simple,
            "schema_json_value": schema_json,
            "columns": columns,
            "row_count": len(encoded_rows),
            "rows": encoded_rows,
        }
        _check_time(started)
    result = {
        "profile": PROFILE,
        "source": "actual A0 producer result receipt installationInventory",
        "producer_receipt": {
            "local_path": RECEIPT_LOCAL_PATH,
            "sha256": RECEIPT_SHA256,
            "publication_id": PUBLICATION_ID,
        },
        "catalog": "client_dev",
        "schema": "ashlar_a1_release_20261103_serverless_r1",
        "roles": sorted(PINS),
        "read_only": True,
        "writes_acknowledgements_or_source_calls": False,
        "limits": {"rows_per_role": MAX_ROWS_PER_ROLE,
                   "body_seconds": MAX_BODY_SECONDS,
                   "output_bytes": MAX_OUTPUT_BYTES},
        "elapsed_seconds": time.monotonic() - started,
        "tables": captured,
    }
    output = json.dumps(result, sort_keys=True, separators=(",", ":"),
                        ensure_ascii=False, allow_nan=False)
    if len(output.encode("utf-8")) > MAX_OUTPUT_BYTES:
        raise ValueError("Role row capture exceeded 2 MiB output bound")
    _check_time(started)
    return output


dbutils.notebook.exit(capture(spark))
