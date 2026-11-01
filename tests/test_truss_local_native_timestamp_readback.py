# SPDX-License-Identifier: MIT OR Apache-2.0
"""Timestamp readback remains exact across a non-UTC Python host timezone."""
import sys
import types
import unittest
from datetime import datetime
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from ashlar_host.truss_source_custody_selection import _local_native_rows


class _Expression:
    def __init__(self, name, operation="identity"):
        self.name = name
        self.operation = operation

    def alias(self, name):
        return _Expression(name, self.operation)


class _Frame:
    def __init__(self, rows):
        self.schema = SimpleNamespace(fields=[
            SimpleNamespace(name="published_at",
                dataType=SimpleNamespace(simpleString=lambda: "timestamp")),
            SimpleNamespace(name="label",
                dataType=SimpleNamespace(simpleString=lambda: "string")),
        ])
        self.rows = rows

    def select(self, *expressions):
        self.expressions = expressions
        projected = []
        for row in self.rows:
            values = {}
            for expression in expressions:
                value = row[expression.name]
                if expression.operation == "unix_micros":
                    # Spark evaluates against its timestamp instant. The
                    # naive value in the collected row represents host-local
                    # 09:47, while Spark's instant is 14:47 UTC.
                    value = row["_spark_unix_micros"]
                values[expression.name] = value
            projected.append(values)
        return _FrameResult(self.schema.fields, projected, expressions)


class _FrameResult:
    def __init__(self, fields, rows, expressions):
        self.schema = SimpleNamespace(fields=fields)
        self.rows = rows
        self.expressions = expressions

    def limit(self, _count):
        return self

    def collect(self):
        return [SimpleNamespace(asDict=lambda recursive=True, row=row: dict(row))
                for row in self.rows]


class _Reader:
    def format(self, _format):
        return self

    def option(self, _key, _value):
        return self

    def load(self, _path):
        # Aware UTC instant is deliberately returned as a naive host value by
        # the mock, mirroring PySpark's Python collection boundary.
        return _Frame([{"published_at": datetime(2026, 11, 1, 9, 47, 57, 944899),
                        "label": "seed", "_spark_unix_micros": 1793544477944899}])


class _Spark:
    read = _Reader()


class TimestampReadbackTests(unittest.TestCase):
    def test_timestamp_is_projected_to_epoch_micros_before_python_collection(self):
        functions = types.ModuleType("pyspark.sql.functions")
        functions.col = lambda name: _Expression(name)
        functions.unix_micros = lambda expression: _Expression(expression.name, "unix_micros")
        sql = types.ModuleType("pyspark.sql")
        sql.functions = functions
        pyspark = types.ModuleType("pyspark")
        pyspark.sql = sql
        with patch.dict(sys.modules, {"pyspark": pyspark, "pyspark.sql": sql,
                                      "pyspark.sql.functions": functions}):
            rows = _local_native_rows(_Spark(), Path("/unused"), 2)
        self.assertEqual(rows, [{"published_at": "1793544477944899", "label": "seed"}])


if __name__ == "__main__":
    unittest.main()
