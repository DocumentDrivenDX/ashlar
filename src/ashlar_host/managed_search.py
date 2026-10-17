"""Native UC search transport; explicit credentials and lifecycle custody only.

No publication writes or default lifecycle authority are provided. Qualification
requires the supplied native owner interval and independent principal witnesses.
"""

from __future__ import annotations

import re
from collections.abc import Callable, Mapping
from contextlib import AbstractContextManager
from time import monotonic
from typing import Any
from urllib.parse import quote, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from umf_access.native_program import program_json
from umf_access.unity_search import NativeHydration, UnitySearchError

from .managed_connector import (
    ManagedSQLConnectorFactory,
    describe_native_table,
    execute_original,
    qualified_table_name,
)


class ManagedUnitySearchTransport:
    def __init__(
        self,
        factory: ManagedSQLConnectorFactory,
        *,
        source_interval: Callable[[Mapping[str, Any], float], AbstractContextManager[Any]],
    ):
        if not callable(source_interval):
            raise UnitySearchError("qualified native lifecycle owner interval required")
        host = urlparse(factory.config.host)
        if host.username or host.password or host.query or host.fragment or host.port not in (None, 443):
            raise UnitySearchError("original workspace HTTPS origin required")
        self.factory, self.source_interval = factory, source_interval

    def _request(self, method, path, body, *, deadline):
        import json

        remaining = deadline - monotonic()
        if not 0 < remaining <= 120 or not path.startswith("/api/") or "?" in path or "#" in path:
            raise UnitySearchError("bounded original native API operation required")
        headers = dict(self.factory.config.authenticate())
        headers["Content-Type"] = "application/json"
        data = None if body is None else json.dumps(body, allow_nan=False, separators=(",", ":")).encode()
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise TimeoutError("native authentication exceeded whole-call budget")
        request = Request(self.factory.config.host.rstrip("/") + path, data=data, headers=headers, method=method)
        class NoRedirect(HTTPRedirectHandler):
            def redirect_request(self, req, fp, code, msg, headers, newurl):
                return None

        # Preserve native HTTP errors and never forward authenticated headers
        # to a redirect destination.
        with build_opener(NoRedirect()).open(request, timeout=remaining) as response:
            payload = response.read(4_194_305)
        if len(payload) > 4_194_304 or monotonic() >= deadline:
            raise TimeoutError("native search API exceeded whole-call budget")
        result = program_json(payload.decode("utf-8"))
        if not isinstance(result, dict):
            raise UnitySearchError("native search API object required")
        return result

    def principal(self, *, deadline):
        original = self._request("GET", "/api/2.0/preview/scim/v2/Me", None, deadline=deadline)
        subject = original.get("userName")
        if type(subject) is not str or not subject or original.get("active") is not True:
            raise UnitySearchError("original active authenticated workspace subject required")
        return subject

    def index_metadata(self, name, *, deadline):
        return self._request("GET", "/api/2.0/vector-search/indexes/" + quote(name, safe=""), None, deadline=deadline)

    def query_index(self, method, path, body, *, deadline):
        if method != "POST" or not path.startswith("/api/2.0/vector-search/indexes/") or not path.endswith("/query"):
            raise UnitySearchError("original closed vector query resource required")
        return self._request(method, path, body, deadline=deadline)

    def read_current(self, table, statement, parameters, *, deadline):
        native_name = (table["catalog"], table["schema"], table["table"])
        if native_name[:2] != (self.factory.catalog, self.factory.schema):
            raise UnitySearchError("original native catalog/schema differs from host configuration")
        with self.source_interval(table, deadline):
            connection = self.factory(deadline=deadline)
            try:
                before = self._admit(connection, table, native_name, deadline)
                cursor = connection.cursor()
                try:
                    execute_original(cursor, statement, parameters, deadline=deadline)
                    names = tuple(column[0] for column in cursor.description)
                    rows = tuple(tuple(row) for row in cursor.fetchmany(3))
                finally:
                    cursor.close()
                after = self._admit(connection, table, native_name, deadline)
                if before != after or monotonic() >= deadline:
                    raise UnitySearchError("native current-read admission changed before release")
                return NativeHydration(names, rows, before[0], after[0], before[1], after[1])
            finally:
                # This transport never enters a transaction. Do not call a
                # synchronous ROLLBACK during teardown of a failed warehouse.
                connection.close()

    def _admit(self, connection, table, native_name, deadline):
        detail = describe_native_table(connection, native_name, deadline=deadline)
        if detail.get("id") != table["id"] or str(detail.get("format")).lower() != "delta":
            raise UnitySearchError("original native Delta table UUID differs")
        cursor = connection.cursor()
        try:
            execute_original(cursor, "SELECT current_user()", None, deadline=deadline)
            subjects = cursor.fetchmany(2)
            if len(subjects) != 1 or len(subjects[0]) != 1 or type(subjects[0][0]) is not str:
                raise UnitySearchError("original native SQL principal required")
            information_schema = qualified_table_name((table["catalog"], "information_schema", "columns"))
            execute_original(cursor,
                "SELECT column_name,data_type,is_nullable,full_data_type FROM " + information_schema + " WHERE table_schema=:schema AND table_name=:table",
                {"schema": table["schema"], "table": table["table"]}, deadline=deadline)
            columns = cursor.fetchmany(2001)
            if len(columns) > 2000 or len({row[0] for row in columns}) != len(columns):
                raise UnitySearchError("bounded original native column inventory required")
            facts = {row[0]: row[1:] for row in columns}
            for column in table["columns"]:
                expected = {"string": "STRING", "boolean": "BOOLEAN", "vector": "ARRAY", "binary": "BINARY", "decimal": "DECIMAL"}.get(column["carrier"])
                if column["carrier"] == "integer":
                    expected = {"tinyint": "TINYINT", "smallint": "SMALLINT", "int": "INT", "bigint": "BIGINT"}.get(column.get("nativeType"))
                fact = facts.get(column["native"])
                if expected is None or fact is None or len(fact) != 3 or fact[0] != expected or fact[1] not in ("YES", "NO") or type(column.get("nullable")) is not bool or (fact[1] == "YES") is not column["nullable"]:
                    raise UnitySearchError("native current-read column differs from compiler carrier")
                if column["carrier"] == "vector" and (column.get("nativeType") != "array<float>" or type(fact[2]) is not str or fact[2].lower() != column["nativeType"]):
                    raise UnitySearchError("native vector component type differs from compiler binding")
                if column["carrier"] == "integer":
                    bits = {"tinyint": 8, "smallint": 16, "int": 32, "bigint": 64}.get(column.get("nativeType"))
                    if column.get("integerWidth") != {"bits": bits, "signed": True} or type(fact[2]) is not str or fact[2].lower() != column["nativeType"]:
                        raise UnitySearchError("native integer width differs from compiler binding")
                if column["carrier"] == "decimal":
                    shape = re.fullmatch(r"decimal\((\d+),\s*(\d+)\)", str(fact[2]).lower())
                    precision, scale = column.get("precision"), column.get("scale")
                    if type(precision) is not int or type(scale) is not int or not 0 <= scale <= precision <= 38 or precision == 0 or shape is None or tuple(map(int, shape.groups())) != (precision, scale) or column.get("nativeType") != f"decimal({precision},{scale})":
                        raise UnitySearchError("native decimal shape differs from compiler binding")
                if column["carrier"] == "binary" and (column.get("nativeType") != "binary" or type(fact[2]) is not str or fact[2].lower() != "binary"):
                    raise UnitySearchError("native binary type differs from compiler binding")
            if any(column["carrier"] == "string" for column in table["columns"]):
                execute_original(cursor, "DESCRIBE TABLE EXTENDED " + qualified_table_name(native_name) + " AS JSON", None, deadline=deadline)
                records = cursor.fetchmany(2)
                if len(records) != 1 or len(records[0]) != 1 or type(records[0][0]) is not str:
                    raise UnitySearchError("original structured native column metadata required")
                metadata = program_json(records[0][0])
                if not isinstance(metadata, dict) or (metadata.get("catalog_name"), metadata.get("schema_name"), metadata.get("table_name")) != native_name:
                    raise UnitySearchError("native column metadata names a different table")
                declarations = metadata.get("columns")
                if not isinstance(declarations, list) or len(declarations) > 2000 or any(not isinstance(value, dict) or type(value.get("name")) is not str for value in declarations):
                    raise UnitySearchError("bounded native column declarations required")
                types = {value["name"]: value.get("type") for value in declarations}
                if len(types) != len(declarations):
                    raise UnitySearchError("native column declarations repeat a name")
                for column in table["columns"]:
                    if column["carrier"] == "string":
                        native_type = types.get(column["native"])
                        if not isinstance(native_type, dict) or native_type.get("name") != "string" or native_type.get("collation") != "UTF8_BINARY":
                            raise UnitySearchError("qualified binary native string collation required")
            return detail["id"], subjects[0][0]
        finally:
            cursor.close()
