"""R05-only native fixture worker; invoked only by its separately reviewed supervisor."""

from __future__ import annotations

import hashlib
import importlib
import json
import os
from pathlib import Path
import sys


def _read_json(path: Path) -> dict:
    if path.is_symlink() or not path.is_file():
        raise PermissionError("Canonical local evidence file required")
    return json.loads(path.read_text(encoding="utf-8"))


def _verify_file(path: Path, digest: str) -> bytes:
    if path.is_symlink() or not path.is_file():
        raise PermissionError("Canonical local evidence file required")
    raw = path.read_bytes()
    if hashlib.sha256(raw).hexdigest() != digest:
        raise PermissionError("Canonical local evidence bytes differ from invocation packet")
    return raw


def _preflight_graphframes_python(runtime: dict):
    """Verify the pinned GraphFrames Python package before any Spark or fixture effects."""
    selected = runtime.get("graphframesPython")
    if (type(selected) is not dict
            or selected.get("distributionName") != "graphframes-py"
            or selected.get("version") != "0.12.3"):
        raise PermissionError("Exact GraphFrames Python distribution pin required")
    site = Path(selected.get("sitePackages", ""))
    if (not site.is_absolute() or not site.exists() or site.is_symlink()
            or site.resolve(strict=True) != site or not site.is_dir()
            or str(site) not in sys.path):
        raise PermissionError("Pinned GraphFrames site-packages must be selected on PYTHONPATH")
    content_hashes = selected.get("contentSha256")
    if type(content_hashes) is not dict or not content_hashes:
        raise PermissionError("Closed GraphFrames Python package content pins required")
    package_roots = (site / "graphframes", site / "graphframes_py-0.12.3.dist-info")
    expected_paths = {
        str(path.resolve(strict=True))
        for package_root in package_roots
        for path in package_root.rglob("*")
        if path.is_file() and not path.is_symlink()
    }
    if set(content_hashes) != expected_paths:
        raise PermissionError("GraphFrames Python content pin does not close over package and distribution files")
    for path_text, digest in content_hashes.items():
        path = Path(path_text)
        if not any(path.parent == root or root in path.parents for root in package_roots):
            raise PermissionError("GraphFrames content pin escapes selected package roots")
        _verify_file(path, digest)
    # Do not import package code or read distribution metadata until every
    # selected package byte has passed its pinned digest check.
    import graphframes
    from graphframes import GraphFrame
    from importlib.metadata import version as distribution_version
    package_origin = Path(graphframes.__file__).resolve(strict=True)
    if package_origin.parent != site / "graphframes":
        raise PermissionError("GraphFrames import resolved outside its selected package root")
    if distribution_version("graphframes-py") != selected["version"]:
        raise PermissionError("Imported GraphFrames Python distribution version differs from its pin")
    if GraphFrame.__module__ != "graphframes.graphframe":
        raise PermissionError("Imported GraphFrame class differs from the selected GraphFrames package")
    return GraphFrame


def main(attempt_root: Path, invocation_packet: Path) -> int:
    packet = _read_json(invocation_packet)
    runtime = packet.get("runtime", {})
    expected_python = Path(runtime.get("pythonExecutable", "")).resolve(strict=True)
    selected_site = Path(runtime.get("pythonSitePackages", "")).resolve(strict=True)
    if (Path(sys.executable).resolve(strict=True) != expected_python
            or str(selected_site) not in sys.path
            or str(Path(runtime.get("pythonSourceRoot", "")).resolve(strict=True)) not in sys.path):
        raise PermissionError("Python executable, source path, or selected venv site-packages differ from the pin")
    if sys.version_info[:2] != (3, 12):
        raise PermissionError("Running Python version differs from the selected 3.12 pin")
    _preflight_graphframes_python(runtime)
    for module_name in ("pyspark", "delta"):
        module = importlib.import_module(module_name)
        origin = Path(module.__file__).resolve(strict=True)
        if selected_site not in origin.parents:
            raise PermissionError(f"{module_name} import resolved outside the selected venv site-packages")
    import pyspark
    if pyspark.__version__ != runtime.get("spark"):
        raise PermissionError("Imported PySpark version differs from the selected Spark pin")
    from importlib.metadata import version as distribution_version
    if distribution_version("delta-spark") != runtime.get("delta"):
        raise PermissionError("Selected Delta Python distribution version differs from its pin")
    action = packet["action"]
    action_module_path = Path(action["actionModulePath"])
    generator_path = Path(action["generatorPath"])
    manifest_path = Path(action["manifestPath"])
    anchor_path = Path(action["anchorPath"])
    source_path = Path(action["sourcePath"])
    oracle_path = Path(action["oraclePath"])
    action_module_bytes = _verify_file(action_module_path, action["actionModuleSha256"])
    generator_bytes = _verify_file(generator_path, action["generatorSha256"])
    manifest_file_bytes = _verify_file(manifest_path, action["manifestFileSha256"])
    manifest_object = json.loads(manifest_file_bytes.decode("utf-8"))
    manifest_bytes = json.dumps(manifest_object, ensure_ascii=False, sort_keys=True,
                                separators=(",", ":"), allow_nan=False).encode("utf-8")
    if hashlib.sha256(manifest_bytes).hexdigest() != action["manifestSha256"]:
        raise PermissionError("Canonical reviewed manifest digest mismatch")
    anchor_bytes = _verify_file(anchor_path, action["anchorSha256"])
    source_bytes = _verify_file(source_path, action["sourceFileSha256"])
    if not source_bytes.endswith(b"\n"):
        raise PermissionError("Reviewed R05 source file must retain its terminal LF")
    canonical_source_bytes = source_bytes[:-1]
    oracle_bytes = _verify_file(oracle_path, action["oracleSha256"])
    digest_path = Path(action["anchorDigestPath"])
    if digest_path.is_symlink() or not digest_path.is_file():
        raise PermissionError("Separately selected action anchor digest file required")
    anchor_digest_text = digest_path.read_text(encoding="ascii").strip()
    if (anchor_digest_text != action["anchorSha256"]
            or hashlib.sha256(anchor_bytes).hexdigest() != anchor_digest_text):
        raise PermissionError("Separately selected action anchor digest mismatch")

    # Import the exact module whose bytes were checked above through the child
    # PYTHONPATH selected by the invocation packet.
    import ashlar_host.pinned_delta_graphframes as action_module
    if Path(action_module.__file__).resolve(strict=True) != action_module_path:
        raise PermissionError("Imported action module path differs from selected bytes")
    if hashlib.sha256(action_module_bytes).hexdigest() != action["actionModuleSha256"]:
        raise PermissionError("Action module bytes differ from selected input")
    descriptor = action_module.ReviewedCanonicalActionDescriptor.from_reviewed_bytes(
        manifest_bytes, generator_bytes, canonical_source_bytes, oracle_bytes,
        review_anchor_path=anchor_path,
        selected_review_anchor_sha256=anchor_digest_text)

    expected_jar_digests = dict(action_module.CANONICAL_RUNTIME_ARTIFACTS)
    expected_jar_digests[
        "graphframes-graphx-spark4_2.13-0.12.3.jar"
    ] = "385ea9934bdeedff9cd4591df66ada45ce739148f15fe947a032886997626675"
    if (type(runtime) is not dict or runtime.get("master") != "local[2]"
            or {key: runtime.get(key) for key in ("spark", "delta", "graphframes", "jdk")}
               != {"spark": "4.2.0", "delta": "4.4.1",
                   "graphframes": "0.12.3", "jdk": "21.0.13"}):
        raise PermissionError("Exact reviewed local runtime selection required")
    jar_paths = runtime.get("runtimeJars")
    if type(jar_paths) is not dict or set(jar_paths) != set(expected_jar_digests):
        raise PermissionError("Exact locally selected runtime JAR paths required")
    classpath = []
    for name, expected_digest in expected_jar_digests.items():
        jar_path = Path(jar_paths[name])
        if jar_path.is_symlink() or not jar_path.is_file():
            raise PermissionError("Locally installed pinned runtime JAR required")
        if hashlib.sha256(jar_path.read_bytes()).hexdigest() != expected_digest:
            raise PermissionError("Locally installed runtime JAR bytes differ from pins")
        classpath.append(str(jar_path.resolve(strict=True)))

    scratch = attempt_root / "spark-local"
    fixture_root = attempt_root / "tmp" / "fixture"
    warehouse = attempt_root / "warehouse"
    fixture_root.mkdir(mode=0o700)
    for path in (scratch, fixture_root, warehouse):
        if path.is_symlink() or not path.is_dir() or path.stat().st_mode & 0o077:
            raise PermissionError("Private attempt directories required")
    if os.environ.get("SPARK_LOCAL_DIRS") != str(scratch):
        raise PermissionError("Spark local scratch must be inside accounted attempt root")

    from pyspark.sql import SparkSession
    spark = (SparkSession.builder.master("local[2]")
        .appName("ashlar-canonical-graphframes-one-hop")
        .config("spark.jars", ",".join(classpath))
        .config("spark.local.dir", str(scratch))
        .config("spark.sql.warehouse.dir", str(warehouse))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate())
    try:
        from ashlar_host.canonical_graphframes_materializer import (
            cleanup_canonical_discriminator, materialize_canonical_discriminator)
        paths = materialize_canonical_discriminator(spark, fixture_root)
        owner = action_module.PinnedCanonicalGraphFramesActionFixture(
            spark, descriptor, fixture_root, paths)
        receipt = action_module.run_canonical_graphframes_one_hop(
            spark, owner,
            native_fixture_verifier=action_module._verify_canonical_persisted_fixture)
        receipt["cleanup"] = cleanup_canonical_discriminator(spark, fixture_root)
        result_path = attempt_root / "worker-result.json"
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        fd = os.open(result_path, flags, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8") as stream:
            stream.write(json.dumps(receipt, sort_keys=True, separators=(",", ":")) + "\n")
            stream.flush()
            os.fsync(stream.fileno())
        return 0
    finally:
        spark.stop()


if __name__ == "__main__":
    if len(sys.argv) != 4:
        raise SystemExit("worker accepts only attempt-root, pinned packet and result path")
    root = Path(sys.argv[1]).resolve(strict=True)
    result_path = Path(sys.argv[3])
    if result_path != root / "worker-result.json":
        raise SystemExit("worker result path must be inside its accounted attempt root")
    raise SystemExit(main(root, Path(sys.argv[2]).resolve(strict=True)))
