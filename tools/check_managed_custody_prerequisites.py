"""Owned managed native prerequisites, explicitly not SDK action qualification."""

from __future__ import annotations

import argparse
import hashlib
import json
from pathlib import Path
from time import monotonic, sleep
from uuid import uuid4

from databricks.sdk import WorkspaceClient
from databricks.sdk.core import Config
from databricks.sql.exc import ServerOperationError
from managed_fixture_identity import temporary_managed_identity

from ashlar_host.managed_connector import (
    ManagedSQLConnectorFactory,
    execute_original,
    qualified_table_name,
)


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
    schema = "umf_sdk_custody_" + uuid4().hex[:12]
    trace, checks, cleanup = [], [], []
    status = "failed"
    created = False
    human = None
    sequence = 0

    def query(connection, statement, parameters=None):
        nonlocal sequence
        cursor = connection.cursor()
        sequence += 1
        record = {
            "sequence": sequence,
            "statement": statement,
            "parameters": parameters,
        }
        trace.append(record)
        try:
            execute_original(cursor, statement, parameters, deadline=monotonic() + 60)
            record["queryId"] = cursor.query_id
            record["columns"] = (
                [column[0] for column in cursor.description]
                if cursor.description
                else []
            )
            rows = (
                [tuple(row) for row in cursor.fetchall()] if cursor.description else []
            )
            record["rows"] = rows
            return rows
        except Exception as original:
            record["errorType"] = type(original).__name__
            record["error"] = str(original)
            raise
        finally:
            cursor.close()

    def factory(config, schema_override=None):
        return ManagedSQLConnectorFactory(
            config,
            warehouse_id=warehouse,
            catalog="client_dev",
            schema=schema if schema_override is None else schema_override,
        )

    try:
        original = admin.warehouses.get(warehouse)
        assert original.name == "umf-typed-sdk-qualification-20261016"
        assert (
            original.cluster_size == "2X-Small"
            and original.min_num_clusters == original.max_num_clusters == 1
        )
        assert original.auto_stop_mins == 5
        human = admin.current_user.me().user_name
        assert human
        with temporary_managed_identity(
            admin,
            warehouse_id=warehouse,
            display_name="umf-typed-sdk-custody-" + schema.rsplit("_", 1)[1],
        ) as identity:
            connection = factory(admin.config, "default")(deadline=monotonic() + 120)
            try:
                assert query(connection, "SELECT current_user()") == [(human,)]
                query(
                    connection,
                    "CREATE SCHEMA "
                    + qualified_table_name(("client_dev", schema, "unused")).rsplit(
                        ".", 1
                    )[0],
                )
                created = True
                schema_name = "`client_dev`.`" + schema + "`"
                principal = "`" + identity.application_id.replace("`", "``") + "`"
                human_name = "`" + human.replace("`", "``") + "`"
                query(
                    connection,
                    "GRANT USE CATALOG ON CATALOG client_dev TO " + principal,
                )
                query(
                    connection, "ALTER SCHEMA " + schema_name + " OWNER TO " + principal
                )
                owner = factory(identity.config)(deadline=monotonic() + 120)
                try:
                    assert query(owner, "SELECT current_user()") == [
                        (identity.application_id,)
                    ]
                    for table in (
                        "objects",
                        "gate",
                        "receipts",
                        "alter_probe",
                        "drop_probe",
                    ):
                        query(
                            owner,
                            "CREATE TABLE "
                            + schema_name
                            + ".`"
                            + table
                            + "` (id BIGINT, value STRING) USING DELTA TBLPROPERTIES ('delta.feature.catalogManaged'='supported','delta.isolationLevel'='Serializable')",
                        )
                    query(
                        owner,
                        "GRANT USE SCHEMA ON SCHEMA "
                        + schema_name
                        + " TO "
                        + human_name,
                    )
                    for table in (
                        "objects",
                        "gate",
                        "receipts",
                        "alter_probe",
                        "drop_probe",
                    ):
                        query(
                            owner,
                            "GRANT SELECT,MODIFY ON TABLE "
                            + schema_name
                            + ".`"
                            + table
                            + "` TO "
                            + human_name,
                        ) if table != "gate" else query(
                            owner,
                            "GRANT SELECT ON TABLE "
                            + schema_name
                            + ".`gate` TO "
                            + human_name,
                        )
                    for table in ("objects", "gate", "receipts"):
                        query(
                            owner,
                            "INSERT INTO "
                            + schema_name
                            + ".`"
                            + table
                            + "` VALUES (1,'before')",
                        )
                    denials = []
                    for statement in (
                        "ALTER TABLE "
                        + schema_name
                        + ".`alter_probe` ADD COLUMN unauthorized STRING",
                        "DROP TABLE " + schema_name + ".`drop_probe`",
                        "UPDATE "
                        + schema_name
                        + ".`gate` SET value='unauthorized' WHERE id=-1",
                    ):
                        try:
                            query(connection, statement)
                        except ServerOperationError as original_error:
                            denials.append(
                                {
                                    "statement": statement,
                                    "errorType": type(original_error).__name__,
                                    "diagnostic": str(original_error),
                                }
                            )
                    checks.append(
                        {
                            "humanNativeLifecycleDenials": len(denials),
                            "required": 3,
                            "denials": denials,
                        }
                    )
                    # Separate native transaction prerequisite, not a human-role
                    # action: service identity remains explicitly a service.
                    query(owner, "SET AUTOCOMMIT = FALSE")
                    for table in ("objects", "gate", "receipts"):
                        query(
                            owner,
                            "UPDATE "
                            + schema_name
                            + ".`"
                            + table
                            + "` SET value='original-native-probe' WHERE id=1",
                        )
                    query(owner, "COMMIT")
                    query(owner, "SET AUTOCOMMIT = TRUE")
                    for table in ("objects", "gate", "receipts"):
                        query(
                            owner, "DESCRIBE DETAIL " + schema_name + ".`" + table + "`"
                        )
                        query(
                            owner,
                            "DESCRIBE HISTORY "
                            + schema_name
                            + ".`"
                            + table
                            + "` LIMIT 3",
                        )
                    checks.append(
                        {
                            "originalCommitMetadataRetained": True,
                            "qualification": "prerequisite only; exact transaction/version linkage must be reviewed",
                        }
                    )
                    status = "prerequisites-observed"
                finally:
                    try:
                        query(owner, "DROP SCHEMA " + schema_name + " CASCADE")
                        created = False
                    finally:
                        owner.close()
            finally:
                connection.close()
    finally:
        if created:
            cleanup.append("owned fixture cleanup incomplete: client_dev." + schema)
        try:
            admin.warehouses.stop(warehouse)
            deadline = monotonic() + 300
            while admin.warehouses.get(warehouse).state.value != "STOPPED":
                if monotonic() >= deadline:
                    raise TimeoutError("dedicated warehouse stop deadline")
                sleep(2)
        except Exception as error:  # noqa: BLE001 - retain cleanup failure after all owned cleanup attempts
            cleanup.append(type(error).__name__ + ": " + str(error))
        (args.output / "native.json").write_text(
            json.dumps(
                {
                    "status": status,
                    "schema": schema,
                    "checks": checks,
                    "trace": trace,
                    "cleanupErrors": cleanup,
                    "temporal": "HELD",
                    "qualification": "native managed custody prerequisites only; no SDK authority/receipt qualification",
                },
                indent=2,
                default=str,
            )
            + "\n"
        )
        if cleanup:
            raise RuntimeError("Owned managed cleanup failed; retained native.json")


if __name__ == "__main__":
    main()
