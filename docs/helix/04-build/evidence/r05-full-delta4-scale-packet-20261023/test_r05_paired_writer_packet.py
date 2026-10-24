"""Effect-free checks for the held R05 paired writer packet."""
import unittest
import hashlib
import os
from pathlib import Path
import subprocess
import sys
import threading
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest import mock

from r05_paired_writer_packet import (
    EDGE_ROWS, NODE_ROWS, PacketRefused,
    _closed_approval, _execute_parallel_role, _source_select,
)
import r05_paired_writer_supervisor as supervisor
from full_delta4_trial import _parallel_roles


class PairedWriterPacketChecks(unittest.TestCase):
    def test_supervisor_bootstraps_under_isolated_python_without_pythonpath(self):
        script = Path(supervisor.__file__).resolve()
        env = {key: value for key, value in os.environ.items()
               if key != "PYTHONPATH"}
        result = subprocess.run(
            (sys.executable, "-I", "-B", str(script), "--help"),
            cwd=script.parent, env=env, stdin=subprocess.DEVNULL,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30, check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr.decode("utf-8", "replace"))
        self.assertIn(b"--print-review-pins", result.stdout)

    def approval(self, **changes):
        value = {
            "profile": "ashlar-r05-paired-writer-authorization/0.1",
            "status": "approved", "packetSha256": "a" * 64, "attempts": 1,
            "nodeRows": NODE_ROWS, "edgeRows": EDGE_ROWS,
            "totalSeconds": 900, "workerSeconds": 780,
            "rootBytes": 32 * 1024**3, "activeDeltaBytes": 16 * 1024**3,
            "minimumFreeBytes": 64 * 1024**3,
            "predictiveOptimizationDisabled": False,
            "sourcePinsSha256": "b" * 64,
            "runtimePinsSha256": "c" * 64,
            "ownerDecision": "reviewed bounded local paired discriminator",
        }
        value.update(changes)
        return value

    def test_exact_profile_requires_explicit_owner_and_fixed_scales(self):
        self.assertEqual("reviewed bounded local paired discriminator",
                         _closed_approval(self.approval(), "a" * 64)["ownerDecision"])
        for changes in ({"nodeRows": 1_000_000_000}, {"edgeRows": 5_000_000_000},
                        {"attempts": 2}, {"predictiveOptimizationDisabled": True},
                        {"ownerDecision": " "}, {"extra": "unreviewed"}):
            with self.subTest(changes=changes), self.assertRaises(PacketRefused):
                _closed_approval(self.approval(**changes), "a" * 64)

    def test_generator_is_bound_to_the_one_paired_sample_and_source_select(self):
        self.assertEqual((45_000_000, 225_000_000), (NODE_ROWS, EDGE_ROWS))
        for rows in (NODE_ROWS, EDGE_ROWS):
            role = SimpleNamespace(insert_sql=(
                f"INSERT INTO delta.`/tmp/r05-pair/output`\nSELECT id FROM range({rows})"))
            source = _source_select(role)
            self.assertTrue(source.startswith("SELECT"))
            self.assertIn(f"range({rows})", source)
        self.assertEqual(225_000_000, NODE_ROWS * 5)

    def test_non_generator_sql_is_refused(self):
        class Invalid:
            insert_sql = "INSERT INTO delta.`/tmp/x` SELECT * FROM arbitrary_table"
        with self.assertRaises(PacketRefused):
            _source_select(Invalid())

    def test_parallel_failure_clears_spark_42_job_group_properties_per_thread(self):
        barrier = threading.Barrier(2)
        lock = threading.Lock()
        properties = {}
        cancel_calls = []

        class FakeSparkContext:
            def _state(self):
                with lock:
                    return properties.setdefault(threading.get_ident(), {})

            def setJobGroup(self, group_id, description, interruptOnCancel=False):
                values = self._state()
                values.update({
                    "spark.job.description": description,
                    "spark.jobGroup.id": group_id,
                    "spark.job.interruptOnCancel": str(interruptOnCancel).lower(),
                })

            def setLocalProperty(self, key, value):
                values = self._state()
                if value is None:
                    values.pop(key, None)
                else:
                    values[key] = value

            def cancelJobGroup(self, _group_id):
                cancel_calls.append("cancelled")

        class FakeSpark:
            def __init__(self):
                self.sparkContext = FakeSparkContext()

            def sql(self, statement):
                barrier.wait(timeout=2)
                if statement == "fail":
                    raise RuntimeError("inert simulated writer failure")

        spark = FakeSpark()
        timings = {}
        group_id = "r05-paired-test"

        def execute(role):
            _execute_parallel_role(
                spark=spark, group_id=group_id, role=role,
                insert_sql="fail" if role == "node" else "succeed",
                role_seconds=timings)

        with self.assertRaisesRegex(RuntimeError, "simulated writer failure"):
            _parallel_roles(("node", "edge"), execute,
                            lambda: spark.sparkContext.cancelJobGroup(group_id))
        self.assertEqual(1, len(cancel_calls))
        self.assertEqual({"node", "edge"}, set(timings))
        self.assertEqual([{}, {}], sorted(properties.values(), key=len))

    def test_group_quiescence_wait_is_bounded_and_never_ignores_live_members(self):
        with mock.patch.object(
                supervisor.full, "_group_exists", side_effect=[True, True, False]):
            self.assertTrue(supervisor._wait_for_group_quiescence(123, 0.2))

        ticks = iter((0.0, 0.0, 0.1, 0.2, 0.3))
        with mock.patch.object(
                supervisor.full, "_group_exists", return_value=True), \
                mock.patch.object(supervisor.time, "monotonic",
                                  side_effect=lambda: next(ticks)), \
                mock.patch.object(supervisor.time, "sleep"):
            self.assertFalse(supervisor._wait_for_group_quiescence(123, 0.25))

    def test_cleanup_refuses_incomplete_receipts_and_preserves_attempt(self):
        from pathlib import Path
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / supervisor.WORKER_FILE).write_text('{"status":"failed"}')
            (root / supervisor.OWNERSHIP_FILE).write_text('{}')
            owned = root / "must-remain"
            owned.mkdir()
            with self.assertRaises(supervisor.PairedSupervisorRefused):
                supervisor._verify_and_cleanup(root, {})
            self.assertTrue(owned.is_dir())

    def test_authorization_requires_runtime_and_source_digests(self):
        approval = self.approval()
        del approval["runtimePinsSha256"]
        with self.assertRaises(PacketRefused):
            _closed_approval(approval, "a" * 64)

    def _fake_runtime(self, root):
        def digest(path):
            return hashlib.sha256(path.read_bytes()).hexdigest()

        bin_dir = root / "venv" / "bin"
        bin_dir.mkdir(parents=True)
        terminal = root / "python-real"
        terminal.write_bytes(b"python executable")
        terminal.chmod(0o700)
        python3 = bin_dir / "python3"
        python3.symlink_to(terminal)
        entry = bin_dir / "python"
        entry.symlink_to("python3")
        java = root / "java"
        java.write_bytes(b"java executable")
        java.chmod(0o700)
        java_path = java.resolve()
        jars_dir = root / "jars"
        jars_dir.mkdir()
        jar_names = (
            "delta-spark_4.2_2.13-4.4.1.jar", "delta-storage-4.4.1.jar",
            "graphframes-graphx-spark4_2.13-0.12.3.jar",
            "graphframes-spark4_2.13-0.12.3.jar",
            "datasketches-java-6.2.0.jar", "datasketches-memory-3.0.2.jar",
        )
        jars = {}
        for name in jar_names:
            path = jars_dir / name
            path.write_bytes(name.encode())
            jars[name] = str(path.resolve())
        closure = "d" * 64
        receipt = {
            "profile": "ashlar-r05-paired-current-runtime/0.1",
            "status": "candidate-from-reviewed-r14",
            "spark": "4.2.0", "delta": "4.4.1", "graphframes": "0.12.3",
            "scala": "2.13.18", "jdkBuild": "21.0.13.0.2",
            "python": {
                "entryPath": str(entry),
                "linkChain": [
                    {"path": str(entry), "target": "python3"},
                    {"path": str(python3), "target": str(terminal)},
                ],
                "terminalPath": str(terminal.resolve()), "terminalBytes": terminal.stat().st_size,
                "terminalSha256": digest(terminal), "version": "3.12.14",
                "packagesSha256": closure,
            },
            "java": {
                "path": str(java_path), "bytes": java.stat().st_size,
                "sha256": digest(java), "version": "21.0.13.0.2",
                "versionLine": 'openjdk version "21.0.13.0.2" test',
            },
            "jars": {
                name: {"path": path, "bytes": Path(path).stat().st_size,
                       "sha256": digest(Path(path))}
                for name, path in jars.items()
            },
        }
        document = {
            "profile": "ashlar-r05-reviewed-local-runtime/0.1",
            "python": str(entry), "java": str(java_path), "jars": jars,
            "pythonPackagesSha256": closure,
        }
        probes = {
            "package_probe": lambda _runtime: closure,
            "python_version_probe": lambda: "3.12.14",
            "java_version_probe": lambda: 'openjdk version "21.0.13.0.2" test',
        }
        return document, receipt, probes, entry, terminal, java, jars

    def test_paired_runtime_admits_exact_recorded_venv_chain_and_refuses_drift(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            document, receipt, probes, entry, terminal, java, jars = self._fake_runtime(root)
            identity = supervisor._paired_runtime_identity(
                document, receipt=receipt, **probes)
            self.assertEqual(str(entry), identity["pythonEntryPath"])
            self.assertEqual(str(terminal.resolve()), identity["pythonTerminalPath"])
            self.assertEqual(str(java.resolve()), identity["java"])

            with self.subTest("Python terminal binary drift"):
                terminal.write_bytes(b"changed Python")
                with self.assertRaises(supervisor.PairedSupervisorRefused):
                    supervisor._paired_runtime_identity(
                        document, receipt=receipt, **probes)
                terminal.write_bytes(b"python executable")
                terminal.chmod(0o700)

            with self.subTest("symlink drift"):
                entry.unlink()
                entry.symlink_to("unexpected-python")
                with self.assertRaises(supervisor.PairedSupervisorRefused):
                    supervisor._paired_runtime_identity(
                        document, receipt=receipt, **probes)
                entry.unlink()
                entry.symlink_to("python3")

            with self.subTest("Java binary drift"):
                java.write_bytes(b"changed Java")
                with self.assertRaises(supervisor.PairedSupervisorRefused):
                    supervisor._paired_runtime_identity(
                        document, receipt=receipt, **probes)
                java.write_bytes(b"java executable")
                java.chmod(0o700)

            with self.subTest("JAR drift"):
                Path(jars["delta-storage-4.4.1.jar"]).write_bytes(b"changed jar")
                with self.assertRaises(supervisor.PairedSupervisorRefused):
                    supervisor._paired_runtime_identity(
                        document, receipt=receipt, **probes)

    def test_paired_runtime_refuses_package_and_version_drift(self):
        with TemporaryDirectory() as temporary:
            root = Path(temporary)
            document, receipt, probes, *_ = self._fake_runtime(root)
            wrong_package = dict(probes, package_probe=lambda _runtime: "e" * 64)
            with self.assertRaises(supervisor.PairedSupervisorRefused):
                supervisor._paired_runtime_identity(
                    document, receipt=receipt, **wrong_package)
            wrong_java = dict(probes, java_version_probe=lambda: "OpenJDK changed")
            with self.assertRaises(supervisor.PairedSupervisorRefused):
                supervisor._paired_runtime_identity(
                    document, receipt=receipt, **wrong_java)
            wrong_python = dict(probes, python_version_probe=lambda: "3.13.0")
            with self.assertRaises(supervisor.PairedSupervisorRefused):
                supervisor._paired_runtime_identity(
                    document, receipt=receipt, **wrong_python)
            wrong_profile = dict(document, profile="ashlar-r05-full-delta4-runtime/0.1")
            with self.assertRaises(supervisor.PairedSupervisorRefused):
                supervisor._paired_runtime_identity(
                    wrong_profile, receipt=receipt, **probes)


if __name__ == "__main__":
    unittest.main()
