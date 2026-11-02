# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused effect-free checks for the Truss .53 managed A/B source owner."""
import unittest

from ashlar_host.managed_current_truss053_genesis_owner import (
    _begin_public_catalog_transaction,
    _rollback_and_close,
)


class Truss053GenesisOwnerTests(unittest.TestCase):
    def test_catalog_owner_sets_read_write_in_first_single_statement(self):
        class Connection:
            def __init__(self):
                self.statements = []

            def execute(self, statement):
                self.statements.append(statement)

        connection = Connection()
        _begin_public_catalog_transaction(connection)
        self.assertEqual(connection.statements, [
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ WRITE",
        ])

    def test_close_is_attempted_even_when_rollback_fails(self):
        class Connection:
            closed = False

            def __init__(self):
                self.calls = []

            def rollback(self):
                self.calls.append("rollback")
                raise RuntimeError("rollback refused")

            def close(self):
                self.calls.append("close")

        connection = Connection()
        failures = _rollback_and_close(connection)
        self.assertEqual(connection.calls, ["rollback", "close"])
        self.assertEqual(len(failures), 1)
        self.assertEqual(str(failures[0]), "rollback refused")


if __name__ == "__main__":
    unittest.main()
