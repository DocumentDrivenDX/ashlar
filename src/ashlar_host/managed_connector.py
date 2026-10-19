# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit pinned SQL Connector factory and metadata-only native helpers.

Caller supplies an authenticated SDK Config; there is no profile discovery,
authority issuance, transaction retry or publication-target mutation API.
"""

from __future__ import annotations

import importlib.metadata
import re
from time import monotonic
from urllib.parse import urlparse

from umf_access.protocols import ActionRefused


class ManagedSQLConnectorFactory:
    def __init__(self, config, *, warehouse_id, catalog, schema):
        if importlib.metadata.version("databricks-sql-connector") != "4.6.0":
            raise ActionRefused("EXACT_SQL_CONNECTOR_4_6_0_REQUIRED")
        if (
            type(warehouse_id) is not str
            or re.fullmatch("[0-9a-f]{16}", warehouse_id) is None
        ):
            raise ActionRefused("EXPLICIT_ORIGINAL_WAREHOUSE_REQUIRED")
        parsed = urlparse(config.host)
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or parsed.path not in ("", "/")
        ):
            raise ActionRefused("ORIGINAL_AUTHENTICATED_HTTPS_WORKSPACE_REQUIRED")
        if any(
            type(value) is not str or not value or len(value) > 255 or "\x00" in value
            for value in (catalog, schema)
        ):
            raise ActionRefused("EXPLICIT_NATIVE_CATALOG_SCHEMA_REQUIRED")
        self.config, self.warehouse_id, self.catalog, self.schema = (
            config,
            warehouse_id,
            catalog,
            schema,
        )
        self.hostname = parsed.hostname

    def __call__(self, *, deadline):
        remaining = deadline - monotonic()
        if not 0 < remaining <= 120:
            raise ActionRefused("BOUNDED_ORIGINAL_MANAGED_DEADLINE_REQUIRED")
        from databricks import sql

        return sql.connect(
            server_hostname=self.hostname,
            http_path="/sql/1.0/warehouses/" + self.warehouse_id,
            credentials_provider=lambda: self.config.authenticate,
            catalog=self.catalog,
            schema=self.schema,
            autocommit=True,
            ignore_transactions=False,
            fetch_autocommit_from_server=True,
            use_inline_params=False,
            use_cloud_fetch=False,
            _use_arrow_native_complex_types=False,
            _socket_timeout=remaining,
            _retry_stop_after_attempts_count=1,
            enable_telemetry=False,
        )


def execute_original(cursor, statement, parameters, *, deadline):
    """One original native statement, bounded async polling; no retry or SQL rewrite."""
    from time import sleep

    if monotonic() >= deadline:
        raise TimeoutError(
            "original managed statement deadline expired before dispatch"
        )
    cursor.execute_async(statement, parameters=parameters or None)
    while cursor.is_query_pending():
        if monotonic() >= deadline:
            cursor.cancel()
            raise TimeoutError("original managed statement canceled at deadline")
        sleep(min(0.05, max(0.0, deadline - monotonic())))
    # The pinned public polling API raises the original ServerOperationError,
    # preserving its operation ID and diagnostic-info for native error states.
    cursor.get_async_execution_result()
    return cursor


def qualified_table_name(table):
    if type(table) is not tuple or len(table) != 3:
        raise ActionRefused("EXPLICIT_THREE_PART_NATIVE_TABLE_REQUIRED")
    for value in table:
        if type(value) is not str or not value or len(value) > 255 or "\x00" in value:
            raise ActionRefused("MANAGED_NATIVE_IDENTIFIER")
    return ".".join("`" + value.replace("`", "``") + "`" for value in table)


def describe_native_table(connection, table, *, deadline):
    """Original complete DESCRIBE DETAIL row, with native column names preserved."""
    cursor = connection.cursor()
    try:
        execute_original(
            cursor,
            "DESCRIBE DETAIL " + qualified_table_name(table),
            None,
            deadline=deadline,
        )
        names = [column[0] for column in cursor.description]
        rows = cursor.fetchmany(2)
        if len(rows) != 1 or len(names) != len(set(names)):
            raise ActionRefused("COMPLETE_ORIGINAL_NATIVE_TABLE_DETAIL_REQUIRED")
        return dict(zip(names, rows[0], strict=True))
    finally:
        cursor.close()
