"""Inspect original native multi-table COMMIT metadata; no SDK authority claim."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config

from ashlar_host.managed_connector import ManagedSQLConnectorFactory, execute_original


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output", type=Path, required=True)
    args = parser.parse_args()
    args.output.mkdir(parents=True, exist_ok=False)
    sources = args.output / "source"
    sources.mkdir()
    identities = []
    for source in (
        Path(__file__),
        Path(__file__).parents[1] / "src/ashlar_host/managed_connector.py",
        Path(__file__).with_name("managed_fixture_identity.py"),
    ):
        raw = source.read_bytes()
        (sources / source.name).write_bytes(raw)
        identities.append(
            {"path": str(source), "sha256": hashlib.sha256(raw).hexdigest()}
        )
    (args.output / "source-identity.json").write_text(
        json.dumps(identities, indent=2) + "\n"
    )
    admin = WorkspaceClient(
        config=Config(
            profile="aidev-cus", http_timeout_seconds=20, retry_timeout_seconds=20
        )
    )
    warehouse = "d4b8eb5b4d6e34b8"
    schema = "umf_sdk_commit_" + uuid4().hex[:12]
    qualified = "`client_dev`.`" + schema + "`"
    trace = []
    cleanup = []
    connection = None
    created = False
    status = "failed"

    def query(statement):
        cursor = connection.cursor()
        record = {"statement": statement}
        trace.append(record)
        try:
            execute_original(cursor, statement, None, deadline=monotonic() + 90)
            record["queryId"] = cursor.query_id
            record["columns"] = (
                [column[0] for column in cursor.description]
                if cursor.description
                else []
            )
            record["rows"] = (
                [tuple(row) for row in cursor.fetchall()] if cursor.description else []
            )
            return record
        except Exception as original:
            record["errorType"] = type(original).__name__
            record["error"] = str(original)
            raise
        finally:
            cursor.close()

    try:
        warehouse_original = admin.warehouses.get(warehouse)
        assert warehouse_original.name == "umf-typed-sdk-qualification-20261016"
        assert warehouse_original.cluster_size == "2X-Small"
        assert (
            warehouse_original.min_num_clusters
            == warehouse_original.max_num_clusters
            == 1
        )
        assert warehouse_original.auto_stop_mins == 5
        connection = ManagedSQLConnectorFactory(
            admin.config, warehouse_id=warehouse, catalog="client_dev", schema="default"
        )(deadline=monotonic() + 120)
        query("SELECT current_user(),current_catalog(),current_schema()")
        query("CREATE SCHEMA " + qualified)
        created = True
        for table in ("objects", "gate", "receipts"):
            query(
                "CREATE TABLE "
                + qualified
                + ".`"
                + table
                + "` (id BIGINT, value STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.isolationLevel'='Serializable')"
            )
            query("INSERT INTO " + qualified + ".`" + table + "` VALUES (1,'before')")
            query("DESCRIBE DETAIL " + qualified + ".`" + table + "`")
        query("SET AUTOCOMMIT = FALSE")
        for table in ("objects", "gate", "receipts"):
            query(
                "UPDATE "
                + qualified
                + ".`"
                + table
                + "` SET value='original-multi-table-commit' WHERE id=1"
            )
        query("COMMIT")
        query("SET AUTOCOMMIT = TRUE")
        for table in ("objects", "gate", "receipts"):
            query("DESCRIBE HISTORY " + qualified + ".`" + table + "` LIMIT 3")

        status = "observed-original-native-metadata"
    finally:
        if connection is not None:
            try:
                if created:
                    query("ROLLBACK")
                    query("SET AUTOCOMMIT = TRUE")
                    query("DROP SCHEMA " + qualified + " CASCADE")
                    created = False
            except Exception as original:  # noqa: BLE001 - retain cleanup failure while still attempting warehouse shutdown
                cleanup.append(type(original).__name__ + ": " + str(original))
            finally:
                connection.close()
        try:
            admin.warehouses.stop(warehouse)
            deadline = monotonic() + 300
            while admin.warehouses.get(warehouse).state.value != "STOPPED":
                if monotonic() >= deadline:
                    raise TimeoutError("dedicated warehouse stop deadline")
                sleep(2)
        except Exception as original:  # noqa: BLE001 - retain original cleanup error in final receipt
            cleanup.append(type(original).__name__ + ": " + str(original))
        (args.output / "native.json").write_text(
            json.dumps(
                {
                    "status": status,
                    "schema": schema,
                    "trace": trace,
                    "cleanupErrors": cleanup,
                    "temporal": "HELD",
                    "qualification": "metadata prerequisite only; no native lifecycle, human-role or SDK receipt qualification",
                },
                indent=2,
                default=str,
            )
            + "\n"
        )
        if cleanup:
            raise RuntimeError(
                "Owned managed cleanup failed; original evidence retained"
            )


if __name__ == "__main__":
    main()
