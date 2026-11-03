# Databricks notebook source
"""Bounded read-only capture of the existing R19 six-role UC metadata."""
import datetime
import json
import time

PROFILE = "ashlar-managed-a1-r19-native-metadata-capture/0.1"
CATALOG = "client_dev"
SCHEMA = "ashlar_a1_release_20261103_serverless_r1"
ROLES = (
    "object_current", "edge_current", "tombstone", "whole_source_history",
    "publication_manifest", "publication_attempt_phase",
)
DATA_ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
MAX_ELAPSED_SECONDS = 150
MAX_ROWS_PER_QUERY = 129
MAX_OUTPUT_BYTES = 768 * 1024


def _json_value(value):
    if value is None or type(value) in (str, int, float, bool):
        return value
    if type(value) in (list, tuple):
        return [_json_value(item) for item in value]
    if type(value) is dict:
        if not all(type(key) is str for key in value):
            raise TypeError("Native metadata map keys must be strings")
        return {key: _json_value(item) for key, item in value.items()}
    if isinstance(value, (datetime.datetime, datetime.date)):
        return {"python_type": type(value).__name__, "isoformat": value.isoformat()}
    raise TypeError("Unsupported native metadata value: " + type(value).__name__)


def _collect(frame, *, maximum=MAX_ROWS_PER_QUERY):
    columns = list(frame.columns)
    if not columns or len(columns) > 96 or len(columns) != len(set(columns)):
        raise ValueError("Bounded unique native result columns required")
    schema = _schema(frame.schema)
    rows = frame.limit(maximum + 1).collect()
    if len(rows) > maximum:
        raise ValueError("Native metadata query exceeded its row bound")
    result = []
    for row in rows:
        values = row.asDict(recursive=True)
        if set(values) != set(columns):
            raise ValueError("Native row fields differ from captured result schema")
        result.append({key: _json_value(values[key]) for key in columns})
    return {"columns": columns, "schema": schema, "rows": result}


def _schema(value):
    fields = getattr(value, "fields", None)
    if type(fields) not in (list, tuple) or len(fields) > 96:
        raise ValueError("Bounded Spark schema fields required")
    result = []
    for field in fields:
        name = getattr(field, "name", None)
        data_type = getattr(field, "dataType", None)
        simple_string = getattr(data_type, "simpleString", None)
        nullable = getattr(field, "nullable", None)
        if (type(name) is not str or not name or not callable(simple_string)
                or type(nullable) is not bool):
            raise ValueError("Exact Spark schema field metadata required")
        result.append({"name": name, "simpleString": simple_string(), "nullable": nullable})
    return result


def capture(spark):
    started = time.monotonic()

    def query(label, sql, maximum=MAX_ROWS_PER_QUERY):
        if time.monotonic() - started >= MAX_ELAPSED_SECONDS:
            raise TimeoutError("Read-only metadata capture exceeded its task budget")
        result = _collect(spark.sql(sql), maximum=maximum)
        if time.monotonic() - started >= MAX_ELAPSED_SECONDS:
            raise TimeoutError("Read-only metadata capture exceeded its task budget")
        return result

    ns = "`" + CATALOG + "`.`" + SCHEMA + "`"
    tables = query(
        "tables",
        "SELECT table_name,table_type,table_owner "
        "FROM system.information_schema.tables "
        "WHERE table_catalog='" + CATALOG + "' AND table_schema='" + SCHEMA + "' "
        "ORDER BY table_name",
    )
    columns = query(
        "columns",
        "SELECT table_name,column_name,data_type,full_data_type,is_nullable,ordinal_position "
        "FROM system.information_schema.columns "
        "WHERE table_catalog='" + CATALOG + "' AND table_schema='" + SCHEMA + "' "
        "ORDER BY table_name,ordinal_position",
    )
    detail = {}
    history = {}
    extended = {}
    retention_properties = {}
    table_schemas = {}
    grants = {}
    grants["catalog"] = query(
        "grants:catalog", "SHOW GRANTS ON CATALOG `" + CATALOG + "`", maximum=256)
    grants["schema"] = query(
        "grants:schema", "SHOW GRANTS ON SCHEMA " + ns, maximum=256)
    for role in ROLES:
        table = ns + ".`" + role + "`"
        table_schemas[role] = _schema(spark.table(table).schema)
        grants[role] = query(
            "grants:" + role, "SHOW GRANTS ON TABLE " + table, maximum=256)
        detail[role] = query("detail:" + role, "DESCRIBE DETAIL " + table, maximum=2)
        history[role] = query(
            "history:" + role, "DESCRIBE HISTORY " + table + " LIMIT 8", maximum=8)
        extended[role] = query(
            "extended:" + role, "DESCRIBE TABLE EXTENDED " + table, maximum=96)
        if role in DATA_ROLES:
            retention_properties[role] = query(
                "properties:" + role, "SHOW TBLPROPERTIES " + table, maximum=128)

    runtime = query(
        "runtime", "SELECT current_user() AS principal, current_timezone() AS session_timezone, "
        "version() AS spark_build", maximum=2)
    elapsed = time.monotonic() - started
    result = {
        "profile": PROFILE,
        "scope": CATALOG + "." + SCHEMA,
        "roles": list(ROLES),
        "actions": ["information_schema.tables", "information_schema.columns",
                    "DESCRIBE DETAIL for six roles", "DESCRIBE HISTORY LIMIT 8 for six roles",
                    "DESCRIBE TABLE EXTENDED for six roles",
                    "SHOW TBLPROPERTIES for four data roles", "current_user/current_timezone"],
        "runtime": runtime,
        "tables": tables,
        "columns": columns,
        "detail": detail,
        "history": history,
        "extended": extended,
        "retention_properties": retention_properties,
        "table_schemas": table_schemas,
        "grants": grants,
        "elapsed_seconds": elapsed,
    }
    encoded = json.dumps(result, sort_keys=True, separators=(",", ":"),
                         ensure_ascii=False, allow_nan=False)
    if len(encoded.encode("utf-8")) > MAX_OUTPUT_BYTES:
        raise ValueError("Read-only metadata capture exceeded output bound")
    return encoded


dbutils.notebook.exit(capture(spark))
