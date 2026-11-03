# SPDX-License-Identifier: MIT OR Apache-2.0
"""Focused effect-free checks for the Truss .53 managed A/B source owner."""
import gc
import unittest

from ashlar_host import managed_current_truss053_genesis_owner as owner_module
from ashlar_host.managed_current_truss053_genesis_owner import (
    ManagedCurrentTruss053GenesisError,
    ManagedCurrentTruss053SourceObservation,
    _issue_source_observation,
    _is_issued_source_observation,
    _begin_public_catalog_transaction,
    _rollback_and_close,
)


class Truss053GenesisOwnerTests(unittest.TestCase):
    def setUp(self):
        self.original_issued = dict(owner_module._ISSUED_SOURCE_OBSERVATIONS.items())
        owner_module._ISSUED_SOURCE_OBSERVATIONS.clear()

    def tearDown(self):
        owner_module._ISSUED_SOURCE_OBSERVATIONS.clear()
        owner_module._ISSUED_SOURCE_OBSERVATIONS.update(self.original_issued)

    @staticmethod
    def _observation(source):
        return ManagedCurrentTruss053SourceObservation(
            source, None, b"report", b"view", {}, {}, None, None, None)

    def test_issued_observation_provenance_tracks_only_live_bounded_objects(self):
        held = [_issue_source_observation(self._observation("A"))
                for _ in range(8)]
        self.assertEqual(len(owner_module._ISSUED_SOURCE_OBSERVATIONS), 8)
        self.assertTrue(all(_is_issued_source_observation(item) for item in held))
        with self.assertRaisesRegex(ManagedCurrentTruss053GenesisError,
                                    "Bounded exact fresh public Truss"):
            _issue_source_observation(self._observation("B"))

        forged = self._observation("A")
        self.assertFalse(_is_issued_source_observation(forged))
        del forged
        held.clear()
        gc.collect()
        self.assertEqual(len(owner_module._ISSUED_SOURCE_OBSERVATIONS), 0)

        # The same bounded live-proof registry supports multiple source
        # admissions in one worker as earlier selections become unreachable.
        for source in ("A", "B") * 6:
            current = _issue_source_observation(self._observation(source))
            self.assertTrue(_is_issued_source_observation(current))
            self.assertLessEqual(len(owner_module._ISSUED_SOURCE_OBSERVATIONS), 8)
            del current
            gc.collect()
        self.assertEqual(len(owner_module._ISSUED_SOURCE_OBSERVATIONS), 0)

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
