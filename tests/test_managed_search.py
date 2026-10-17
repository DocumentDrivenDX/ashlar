"""Native metadata fault controls; these doubles do not qualify managed UC."""

import copy
import json
import unittest
from unittest.mock import patch

from ashlar_host import managed_search
from umf_access.unity_search import UnitySearchError


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


if __name__ == "__main__":
    unittest.main()
