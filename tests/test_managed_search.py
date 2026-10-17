"""Native metadata fault controls; these doubles do not qualify managed UC."""

import copy
import json
import unittest
from contextlib import nullcontext
from unittest.mock import patch

from umf_access.unity_search import UnitySearchError

from ashlar_host import managed_search


class Cursor:
    def __init__(self, facts, collation):
        metadata = {"catalog_name": "fixture", "schema_name": "owned", "table_name": "rows",
                    "columns": [{"name": "id", "type": {"name": "string", "collation": collation}}]}
        self.results = iter([[("reader@example.test",)], facts, [(json.dumps(metadata),)]])

    def fetchmany(self, bound):
        return next(self.results)

    def close(self):
        pass


class Connection:
    def __init__(self, facts, collation):
        self.facts, self.collation = facts, collation

    def cursor(self):
        return Cursor(self.facts, self.collation)


class ManagedSearchAdmissionTests(unittest.TestCase):
    def setUp(self):
        self.table = {"id": "native-uuid", "catalog": "fixture", "schema": "owned", "table": "rows", "columns": [
            {"native": "id", "carrier": "string", "nullable": False},
            {"native": "flag", "carrier": "boolean", "nullable": True},
            {"native": "vector", "carrier": "vector", "nullable": False, "nativeType": "array<float>", "dimensions": 3},
            {"native": "small", "carrier": "integer", "nullable": False, "nativeType": "smallint", "integerWidth": {"bits": 16, "signed": True}},
            {"native": "amount", "carrier": "decimal", "nullable": False, "nativeType": "decimal(10,2)", "precision": 10, "scale": 2},
            {"native": "blob", "carrier": "binary", "nullable": False, "nativeType": "binary"},
        ]}
        self.facts = [["id", "STRING", "NO", "STRING"], ["flag", "BOOLEAN", "YES", "BOOLEAN"],
                      ["vector", "ARRAY", "NO", "ARRAY<FLOAT>"], ["small", "SMALLINT", "NO", "SMALLINT"],
                      ["amount", "DECIMAL", "NO", "DECIMAL(10, 2)"], ["blob", "BINARY", "NO", "BINARY"]]

    def admit(self, facts, *, detail=None, collation="UTF8_BINARY"):
        transport = object.__new__(managed_search.ManagedUnitySearchTransport)
        with patch.object(managed_search, "describe_native_table", return_value=detail or {"id": "native-uuid", "format": "delta"}), patch.object(managed_search, "execute_original"):
            return transport._admit(Connection(facts, collation), self.table, ("fixture", "owned", "rows"), 1.0)

    def test_exact_native_shapes_include_array_components_and_decimal_scale(self):
        self.assertEqual(self.admit(self.facts), ("native-uuid", "reader@example.test"))

    def test_native_shape_substitutions_refuse(self):
        changes = [(1, 2, "NO"), (2, 3, "ARRAY<DOUBLE>"), (3, 1, "BIGINT"),
                   (4, 3, "DECIMAL(10,3)"), (4, 3, "DECIMAL(11,2)"), (5, 3, "STRING")]
        for row, column, replacement in changes:
            facts = copy.deepcopy(self.facts)
            facts[row][column] = replacement
            with self.subTest(row=row, column=column), self.assertRaises(UnitySearchError):
                self.admit(facts)

    def test_same_name_replacement_uuid_refuses(self):
        with self.assertRaises(UnitySearchError):
            self.admit(self.facts, detail={"id": "replacement", "format": "delta"})

    def test_casefolding_or_unobserved_native_collation_refuses(self):
        for collation in ("UTF8_LCASE", "UTF8_BINARY_RTRIM", None):
            with self.subTest(collation=collation), self.assertRaises(UnitySearchError):
                self.admit(self.facts, collation=collation)

    def test_native_platform_observation_has_exact_versions_and_closes_owned_session(self):
        class VersionCursor:
            def __init__(self, rows):
                self.rows, self.closed = rows, False

            def fetchmany(self, bound):
                self.asserted_bound = bound
                return self.rows

            def close(self):
                self.closed = True

        class VersionConnection:
            def __init__(self, rows):
                self.raw, self.closed = VersionCursor(rows), False

            def cursor(self):
                return self.raw

            def close(self):
                self.closed = True

        for rows, valid in (([("2026.10", None, "4.0.1 native-build")], True),
                            ([(None, "17.3", "4.0.1 native-build")], True),
                            ([(None, None, "4.0.1")], False),
                            ([("2026.10", "17.3", "4.0.1")], False), ([], False)):
            connection = VersionConnection(rows)
            transport = object.__new__(managed_search.ManagedUnitySearchTransport)
            transport.factory = lambda connection=connection, **kwargs: connection
            with patch.object(managed_search, "execute_original") as execute, patch.object(managed_search.importlib.metadata, "version", return_value="4.6.0"):
                if valid:
                    observed = dict(transport.platform(deadline=managed_search.monotonic() + 5))
                    self.assertEqual(observed["spark"], "4.0.1")
                    self.assertEqual(observed["databricks-sql-connector"], "4.6.0")
                else:
                    with self.assertRaises(UnitySearchError):
                        transport.platform(deadline=managed_search.monotonic() + 5)
                self.assertIn("current_version().dbsql_version", execute.call_args.args[1])
            self.assertTrue(connection.closed)
            self.assertTrue(connection.raw.closed)

    def test_eager_native_table_verification_retains_owner_interval_and_cleanup(self):
        class Factory:
            catalog, schema = "fixture", "owned"

            def __call__(self, **kwargs):
                return self

            def close(self):
                self.closed = True

        factory = Factory()
        transport = object.__new__(managed_search.ManagedUnitySearchTransport)
        transport.factory = factory
        transport.source_interval = lambda *args: nullcontext()
        deadline = managed_search.monotonic() + 5
        with patch.object(transport, "_admit", return_value=("native-uuid", "reader@example.test")) as admit:
            result = transport.verify_current(self.table, deadline=deadline)
            self.assertEqual(result.table_id_before, "native-uuid")
            self.assertEqual(result.principal_after, "reader@example.test")
            self.assertEqual(admit.call_count, 2)
        self.assertTrue(factory.closed)
        factory.closed = False
        with patch.object(transport, "_admit", side_effect=[("native-uuid", "reader"), ("replacement", "reader")]), self.assertRaises(UnitySearchError):
            transport.verify_current(self.table, deadline=deadline)
        self.assertTrue(factory.closed)


if __name__ == "__main__":
    unittest.main()
