"""Inert checks for the R05-owned-table cleanup candidate."""
from pathlib import Path
import importlib.util
import json
import os
import tempfile
import unittest

from ashlar_host.canonical_graphframes_materializer import (
    OWNERSHIP_RECEIPT, ROLES, _acquire_lock, _remove_owned_tree, _write_receipt,
    cleanup_canonical_discriminator,
)

R2 = (Path(__file__).resolve().parents[1] /
      "docs/helix/04-build/evidence/canonical-graphframes-holder-candidate-20261023/r05-native-action-r2")
SPEC = importlib.util.spec_from_file_location("r05_r2_supervisor", R2 / "bounded_r05_supervisor.py")
supervisor = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(supervisor)


UUID = "12345678-1234-1234-1234-123456789abc"


class _DetailRow:
    def asDict(self, recursive=True):
        return {"id": UUID}


class _DetailResult:
    def collect(self):
        return [_DetailRow()]


class _FakeSpark:
    def sql(self, query):
        return _DetailResult()


def _owned_fixture(root):
    os.chmod(root, 0o700)
    paths = {}
    for role in ROLES:
        path = root / role
        path.mkdir()
        (path / "part.delta").write_text("owned")
        info = path.lstat()
        paths[role] = {"deltaUuid": UUID, "path": {"device": info.st_dev, "inode": info.st_ino}}
    fd = _acquire_lock(root)
    try:
        receipt = {"profile": "ashlar-r05-owned-tables/0.1", "status": "materialized",
                   "root": {"device": root.stat().st_dev, "inode": root.stat().st_ino},
                   "lock": {"device": os.fstat(fd).st_dev, "inode": os.fstat(fd).st_ino},
                   "sourceSha256": "a" * 64, "tables": paths}
        _write_receipt(root, receipt)
    finally:
        os.close(fd)


class R05OwnedCleanupCandidateTests(unittest.TestCase):
    def test_supervisor_creates_and_observes_only_tmp_fixture(self):
        source = (R2 / "bounded_r05_supervisor.py").read_text()
        self.assertIn('attempt_root / "tmp" / "fixture"', source)
        self.assertNotIn('attempt_root / "fixture"', source)
        self.assertNotIn('("fixture", "spark-local"', source)
        self.assertIn("import re", source)

    def test_fake_spark_deletes_only_exact_owned_tables_and_retains_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "fixture"
            root.mkdir()
            _owned_fixture(root)
            result = cleanup_canonical_discriminator(_FakeSpark(), root)
            self.assertEqual(result["status"], "cleanup-complete")
            self.assertEqual(result["tablesRemoved"], list(ROLES))
            self.assertFalse(any((root / role).exists() for role in ROLES))
            self.assertEqual(set(path.name for path in root.iterdir()),
                             {".ashlar-graphframes-action.lock", OWNERSHIP_RECEIPT})
            receipt = json.loads((root / OWNERSHIP_RECEIPT).read_text())
            self.assertEqual(receipt["status"], "cleanup-complete")

    def test_wrong_uuid_or_path_identity_preserves_both_table_trees(self):
        for mutate in ("uuid", "path"):
            with self.subTest(mutate=mutate), tempfile.TemporaryDirectory() as directory:
                root = Path(directory) / "fixture"
                root.mkdir()
                _owned_fixture(root)
                receipt_path = root / OWNERSHIP_RECEIPT
                receipt = json.loads(receipt_path.read_text())
                if mutate == "uuid":
                    receipt["tables"]["edge_current"]["deltaUuid"] = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
                else:
                    receipt["tables"]["edge_current"]["path"]["inode"] += 1
                receipt_path.write_text(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n")
                with self.assertRaises(PermissionError):
                    cleanup_canonical_discriminator(_FakeSpark(), root)
                self.assertTrue((root / "object_current").is_dir())
                self.assertTrue((root / "edge_current").is_dir())

    def test_cleanup_refuses_symlinks_and_preserves_target(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "owned"
            root.mkdir()
            target = Path(directory) / "outside"
            target.write_text("preserve")
            (root / "link").symlink_to(target)
            with self.assertRaises(PermissionError):
                _remove_owned_tree(root)
            self.assertTrue(root.is_dir())
            self.assertEqual(target.read_text(), "preserve")

    def test_parent_observer_requires_no_tables_and_linked_receipt(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / "fixture"
            root.mkdir(mode=0o700)
            root.chmod(0o700)
            root = root.resolve(strict=True)
            lock = root / ".ashlar-graphframes-action.lock"
            lock.touch(mode=0o600)
            li, ri = lock.stat(), root.stat()
            receipt = {"profile": "ashlar-r05-owned-tables/0.1", "status": "cleanup-complete",
                       "tablesRemoved": list(ROLES), "root": {"device": ri.st_dev, "inode": ri.st_ino},
                       "lock": {"device": li.st_dev, "inode": li.st_ino},
                       "tables": {role: {"path": {"device": 1, "inode": 2}, "deltaUuid": UUID}
                                  for role in ROLES}}
            raw = (json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n").encode()
            receipt_path = root / OWNERSHIP_RECEIPT
            receipt_path.write_bytes(raw)
            receipt_path.chmod(0o600)
            claim = {"receiptSha256": __import__("hashlib").sha256(raw).hexdigest(),
                     "rootIdentity": receipt["root"], "lockIdentity": receipt["lock"]}
            observed = supervisor._observe_fixture_cleanup(root, claim)
            self.assertTrue(observed["tablesAbsent"])
            (root / "object_current").mkdir()
            with self.assertRaises(supervisor.NativeAttemptRefused):
                supervisor._observe_fixture_cleanup(root, claim)


if __name__ == "__main__":
    unittest.main()
