from __future__ import annotations

import importlib.util
import os
from pathlib import Path
import sys
import tempfile
import unittest

HERE = Path(__file__).parent
spec = importlib.util.spec_from_file_location("r05_runtime_accounting", HERE / "runtime_accounting.py")
accounting = importlib.util.module_from_spec(spec)
assert spec.loader
spec.loader.exec_module(accounting)
sys.path.insert(0, str(HERE))
supervisor_spec = importlib.util.spec_from_file_location("r05_supervisor", HERE / "bounded_r05_supervisor.py")
supervisor = importlib.util.module_from_spec(supervisor_spec)
assert supervisor_spec.loader
supervisor_spec.loader.exec_module(supervisor)


class RuntimeAccountingTests(unittest.TestCase):
    def test_deduplicates_overlapping_roots_and_hashes_files(self):
        with tempfile.TemporaryDirectory() as temporary:
            base = Path(temporary).resolve() / "runtime"
            nested = base / "lib"
            nested.mkdir(parents=True)
            (nested / "payload").write_bytes(b"abc")
            selected = str(nested / "payload")
            result = accounting.inventory_roots([base, nested], hash_paths={selected})
            self.assertEqual(result["uniqueBytes"], 3)
            self.assertEqual(result["uniqueFileCount"], 1)
            row = next(item for item in result["entries"] if item["path"] == selected)
            self.assertEqual(row["sha256"], "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad")

    def test_allows_only_exact_internal_symlink_and_does_not_follow_cycle(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "runtime"
            root.mkdir()
            (root / "payload").write_bytes(b"x")
            link = root / "alias"
            link.symlink_to(root / "payload")
            cycle = root / "cycle"
            cycle.symlink_to(root)
            links = {str(link): {"rawTarget": os.readlink(link), "target": str(root / "payload")},
                     str(cycle): {"rawTarget": os.readlink(cycle), "target": str(root)}}
            result = accounting.inventory_roots([root], allowed_links=links)
            self.assertEqual(result["uniqueBytes"], 1)
            with self.assertRaises(accounting.AccountingError):
                accounting.inventory_roots([root], allowed_links={str(link): {"rawTarget": "changed", "target": str(root / "payload")}})

    def test_refuses_symlink_with_unrecorded_target_relationship(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "runtime"
            root.mkdir()
            outside = Path(temporary).resolve() / "outside"
            outside.write_text("x")
            link = root / "escape"
            link.symlink_to(outside)
            with self.assertRaises(accounting.AccountingError):
                accounting.inventory_roots([root], allowed_links={str(link): {"rawTarget": os.readlink(link),
                    "target": str(outside) + ".changed"}})

    def test_external_target_requires_exact_known_dangling_link(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "runtime"
            root.mkdir()
            missing = Path(temporary).resolve() / "missing-target"
            link = root / "known-link"
            link.symlink_to(missing)
            with self.assertRaises(accounting.AccountingError):
                accounting.inventory_roots([root], allowed_links={str(link): {"rawTarget": os.readlink(link), "target": str(missing)}})
            result = accounting.inventory_roots([root], allowed_links={str(link): {"rawTarget": os.readlink(link), "target": str(missing)}},
                                                known_dangling_links={str(link)})
            self.assertEqual(result["knownDanglingLinks"], [str(link)])
            missing.write_text("target appeared")
            with self.assertRaises(accounting.AccountingError):
                accounting.inventory_roots([root], allowed_links={str(link): {"rawTarget": os.readlink(link), "target": str(missing)}},
                                           known_dangling_links={str(link)})

    def test_post_child_snapshot_refuses_runtime_metadata_drift(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "runtime"
            root.mkdir()
            payload = root / "library.bin"
            payload.write_bytes(b"before")
            snapshot = accounting.inventory_roots([root])
            supervisor._verify_runtime_tree_snapshot(snapshot, metadata_only=True)
            payload.write_bytes(b"after-and-longer")
            with self.assertRaises(supervisor.NativeAttemptRefused):
                supervisor._verify_runtime_tree_snapshot(snapshot, metadata_only=True)

    def test_deadline_callback_runs_through_final_metadata_verification(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary).resolve() / "runtime"
            root.mkdir()
            for index in range(300):
                (root / f"file-{index:03}.bin").write_bytes(b"x")
            checks = 0

            def check_deadline():
                nonlocal checks
                checks += 1

            accounting.inventory_roots([root], check=check_deadline)
            self.assertGreaterEqual(checks, 8)

    def test_python_pin_requires_exact_version_and_bytes(self):
        with tempfile.TemporaryDirectory() as temporary:
            executable = Path(temporary).resolve() / "python3.12"
            executable.write_bytes(b"pinned-python")
            executable.chmod(0o700)
            pin = {"pythonExecutable": str(executable),
                   "pythonExecutableSha256": supervisor.sha256_file(executable),
                   "pythonVersion": "3.12"}
            self.assertEqual(supervisor._validated_python_pin(pin), executable)
            pin["pythonVersion"] = "3.13"
            with self.assertRaises(supervisor.NativeAttemptRefused):
                supervisor._validated_python_pin(pin)

    def test_jdk_pin_requires_exact_home_build_and_executable_hash(self):
        with tempfile.TemporaryDirectory() as temporary:
            home = Path(temporary).resolve()
            java = home / "bin" / "java"
            java.parent.mkdir()
            java.write_bytes(b"pinned-java")
            java.chmod(0o700)
            (home / "release").write_text('JAVA_VERSION="21.0.13.0.2"\n')
            pin = {"javaHome": str(home), "javaExecutable": str(java),
                   "javaExecutableSha256": supervisor.sha256_file(java),
                   "jdkBuild": "21.0.13.0.2"}
            self.assertEqual(supervisor._validated_java_pin(pin), (home, java))
            pin["jdkBuild"] = "21.0.13"
            with self.assertRaises(supervisor.NativeAttemptRefused):
                supervisor._validated_java_pin(pin)


if __name__ == "__main__":
    unittest.main()
