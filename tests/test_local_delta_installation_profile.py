# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert exact journal pinning for the native local `.4` installation profile."""
import json
from pathlib import Path
import sqlite3
import tempfile
import unittest

from ashlar_host.delta_custody import (
    DeltaTarget, LocalDeltaError, LocalDeltaTransport, encoded,
    local_journal_definitions,
)


class Tests(unittest.TestCase):
    def test_existing_journal_binds_exact_profile_and_refuses_mismatch(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory).resolve()
            root.chmod(0o700)
            targets = []
            for role in ("edge_current", "object_current", "tombstone", "whole_source_history",
                         "publication_attempt_phase", "publication_manifest"):
                path = root / role
                path.mkdir()
                targets.append(DeltaTarget("local.configured." + role, path, "uuid-" + role))
            profile = {
                "profile": "ashlar-local-current-installation-profile/0.1",
                "physical_profile": "ashlar-delta/0.4-xxhash64",
                "lookup_hash_profile": "spark-xxhash64-typed-key/0.1",
                "roles": {target.table: target.uuid for target in targets},
            }
            registry = {
                "installation": "gate3-local",
                "targets": [{"table": target.table, "path": str(target.path), "uuid": target.uuid}
                            for target in sorted(targets, key=lambda item: item.table)],
                "installation_profile": profile,
            }
            journal = root / "operations.sqlite"
            connection = sqlite3.connect(journal)
            try:
                for statement in local_journal_definitions().values():
                    connection.execute(statement)
                connection.execute("INSERT INTO local_installation VALUES(1, ?)", (encoded(registry),))
                connection.commit()
            finally:
                connection.close()
            journal.chmod(0o600)

            LocalDeltaTransport.require_existing_journal(
                journal, "gate3-local", tuple(targets), installation_profile=profile)
            changed = json.loads(encoded(profile))
            changed["lookup_hash_profile"] = "sha256-json-v1"
            with self.assertRaises(LocalDeltaError):
                LocalDeltaTransport.require_existing_journal(
                    journal, "gate3-local", tuple(targets), installation_profile=changed)


if __name__ == "__main__":
    unittest.main()
