"""Focused offline checks for R12 GraphFrames Python and GraphX JAR admission."""

from __future__ import annotations

import copy
import hashlib
import json
import os
from pathlib import Path
import tempfile
import unittest
import zipfile
from unittest import mock


import r05_graphframes_worker
import bounded_r05_supervisor


PACKET = json.loads(Path("invocation-packet-proposal.json").read_text(encoding="utf-8"))
RUNTIME = PACKET["runtime"]


class GraphFramesPythonPreflightTests(unittest.TestCase):
    def test_selected_distribution_imports_and_content_hashes_match(self) -> None:
        graph_frame = r05_graphframes_worker._preflight_graphframes_python(RUNTIME)
        self.assertEqual(graph_frame.__module__, "graphframes.graphframe")
        self.assertEqual(RUNTIME["graphframesPython"]["version"], "0.12.3")
        self.assertIn(RUNTIME["graphframesPython"]["sitePackages"], os.environ["PYTHONPATH"].split(os.pathsep))

    def test_missing_selected_site_packages_refuses_before_import(self) -> None:
        changed = copy.deepcopy(RUNTIME)
        changed["graphframesPython"]["sitePackages"] = "/private/tmp/absent-graphframes-site-packages"
        with self.assertRaisesRegex(PermissionError, "site-packages must be selected"):
            r05_graphframes_worker._preflight_graphframes_python(changed)

    def test_changed_graphframes_module_hash_refuses(self) -> None:
        changed = copy.deepcopy(RUNTIME)
        target = next(path for path in changed["graphframesPython"]["contentSha256"]
                      if path.endswith("/graphframes/graphframe.py"))
        changed["graphframesPython"]["contentSha256"][target] = "0" * 64
        with self.assertRaisesRegex(PermissionError, "bytes differ from invocation packet"):
            r05_graphframes_worker._preflight_graphframes_python(changed)

    def test_worker_preflights_before_fixture_or_spark_creation(self) -> None:
        source = Path("r05_graphframes_worker.py").read_text(encoding="utf-8")
        self.assertLess(source.index("_preflight_graphframes_python(runtime)"),
                        source.index("fixture_root.mkdir(mode=0o700)"))
        self.assertLess(source.index("_preflight_graphframes_python(runtime)"),
                        source.index("SparkSession.builder"))

    def test_package_hashes_are_checked_before_graphframes_import(self) -> None:
        source = Path("r05_graphframes_worker.py").read_text(encoding="utf-8")
        self.assertLess(source.index("_verify_file(path, digest)"),
                        source.index("import graphframes"))
        self.assertLess(source.index("import graphframes"),
                        source.index("from graphframes import GraphFrame"))

    def test_open_package_hash_closure_refuses_at_static_admission(self) -> None:
        changed = copy.deepcopy(PACKET)
        changed["runtime"]["graphframesPython"]["contentSha256"].pop(
            next(iter(changed["runtime"]["graphframesPython"]["contentSha256"])))
        with self.assertRaisesRegex(bounded_r05_supervisor.NativeAttemptRefused,
                                    "content hash closure is incomplete"):
            bounded_r05_supervisor._verify_pinned_runtime_inventory(changed)

    def test_exact_graphx_companion_has_required_vertex_rdd_classes(self) -> None:
        bounded_r05_supervisor._verify_graphframes_graphx_jar(RUNTIME)

    def test_missing_graphx_companion_refuses_before_marker_creation(self) -> None:
        changed = copy.deepcopy(RUNTIME)
        changed["runtimeJars"].pop("graphframes-graphx-spark4_2.13-0.12.3.jar")
        changed["runtimeJarSha256"].pop("graphframes-graphx-spark4_2.13-0.12.3.jar")
        with self.assertRaisesRegex(bounded_r05_supervisor.NativeAttemptRefused,
                                    "Pinned GraphFrames GraphX companion JAR required"):
            bounded_r05_supervisor._verify_graphframes_graphx_jar(changed)
        source = Path("bounded_r05_supervisor.py").read_text(encoding="utf-8")
        static = source.index("def _admit_static_inputs(")
        marker = source.index("marker = parent /", source.index("def run_reviewed_native_attempt"))
        graphx_check = source.index("_verify_graphframes_graphx_jar(runtime)", static)
        self.assertLess(static, graphx_check)
        self.assertLess(graphx_check, marker)

    def test_graphx_companion_without_vertex_rdd_entries_refuses(self) -> None:
        with tempfile.TemporaryDirectory() as temporary:
            jar = Path(temporary) / "wrong-graphx.jar"
            with zipfile.ZipFile(jar, "w") as archive:
                archive.writestr("unrelated/Only.class", b"placeholder")
            digest = hashlib.sha256(jar.read_bytes()).hexdigest()
            runtime = {
                "runtimeJars": {"graphframes-graphx-spark4_2.13-0.12.3.jar": str(jar)},
                "runtimeJarSha256": {"graphframes-graphx-spark4_2.13-0.12.3.jar": digest},
            }
            with mock.patch.object(bounded_r05_supervisor,
                                   "GRAPHFRAMES_GRAPHX_SHA256", digest):
                with self.assertRaisesRegex(
                        bounded_r05_supervisor.NativeAttemptRefused,
                        "lacks required VertexRDD classes"):
                    bounded_r05_supervisor._verify_graphframes_graphx_jar(runtime)

    def test_worker_classpath_closure_includes_graphx_companion(self) -> None:
        source = Path("r05_graphframes_worker.py").read_text(encoding="utf-8")
        self.assertIn("graphframes-graphx-spark4_2.13-0.12.3.jar", source)
        self.assertLess(source.index("for name, expected_digest in expected_jar_digests.items()"),
                        source.index('.config("spark.jars", ",".join(classpath))'))


if __name__ == "__main__":
    unittest.main()
