# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused effect-free checks for the Truss .53 managed A/B source owner."""
import unittest

from ashlar_host.managed_current_truss053_genesis_owner import (
    _begin_public_read_only_transaction,
)


class Truss053GenesisOwnerTests(unittest.TestCase):
    def test_public_read_owner_sets_access_mode_in_first_single_statement(self):
        class Connection:
            def __init__(self):
                self.statements = []

            def execute(self, statement):
                self.statements.append(statement)

        connection = Connection()
        _begin_public_read_only_transaction(connection)
        self.assertEqual(connection.statements, [
            "SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY",
        ])


if __name__ == "__main__":
    unittest.main()
