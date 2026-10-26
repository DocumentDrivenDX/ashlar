#!/usr/bin/env python3
"""Private single-process Spark 4.2/Delta 4.4/GraphFrames compatibility smoke.

This is not a public Ashlar command. It accepts only a caller-created, mode-0600
private pin document; it never resolves Maven/PyPI dependencies itself.
"""
from __future__ import annotations

import hashlib
import base64
import inspect
import importlib.metadata
import json
import os
from pathlib import Path
import platform
import re
import stat
import sys
import uuid

PROFILE = "ashlar-private-graphframes-spark42-smoke/0.1"
PYSPARK_VERSION = "4.2.0"
GRAPHFRAMES_PY_VERSION = "0.12.3"
DIRECT_ARTIFACTS = {
    "io.delta:delta-spark_4.2_2.13:4.4.1",
    "io.delta:delta-storage:4.4.1",
    "io.graphframes:graphframes-spark4_2.13:0.12.3",
    "io.graphframes:graphframes-graphx-spark4_2.13:0.12.3",
    "org.apache.datasketches:datasketches-java:6.2.0",
    "org.apache.datasketches:datasketches-memory:3.0.2",
}
ARTIFACT_URLS = {
    "io.delta:delta-spark_4.2_2.13:4.4.1": (
        "https://repo.maven.apache.org/maven2/io/delta/delta-spark_4.2_2.13/4.4.1/"
        "delta-spark_4.2_2.13-4.4.1.jar"),
    "io.delta:delta-storage:4.4.1": (
        "https://repo.maven.apache.org/maven2/io/delta/delta-storage/4.4.1/"
        "delta-storage-4.4.1.jar"),
    "io.graphframes:graphframes-spark4_2.13:0.12.3": (
        "https://repo.maven.apache.org/maven2/io/graphframes/graphframes-spark4_2.13/0.12.3/"
        "graphframes-spark4_2.13-0.12.3.jar"),
    "io.graphframes:graphframes-graphx-spark4_2.13:0.12.3": (
        "https://repo.maven.apache.org/maven2/io/graphframes/graphframes-graphx-spark4_2.13/0.12.3/"
        "graphframes-graphx-spark4_2.13-0.12.3.jar"),
    "org.apache.datasketches:datasketches-java:6.2.0": (
        "https://repo.maven.apache.org/maven2/org/apache/datasketches/datasketches-java/6.2.0/"
        "datasketches-java-6.2.0.jar"),
    "org.apache.datasketches:datasketches-memory:3.0.2": (
        "https://repo.maven.apache.org/maven2/org/apache/datasketches/datasketches-memory/3.0.2/"
        "datasketches-memory-3.0.2.jar"),
}
KNOWN_ARTIFACT_SHA256 = {
    "io.delta:delta-spark_4.2_2.13:4.4.1": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
    "io.delta:delta-storage:4.4.1": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
    "io.graphframes:graphframes-spark4_2.13:0.12.3": "a78958f7ea3ac89ed1ea9dbbd1546f403d94151e969bb0de96e33697a5f49f50",
    "io.graphframes:graphframes-graphx-spark4_2.13:0.12.3": "385ea9934bdeedff9cd4591df66ada45ce739148f15fe947a032886997626675",
    "org.apache.datasketches:datasketches-java:6.2.0": "1b55103e1f7564150a0867eca4ce3bca13cd5935a32c199a5e738f8c5c24901a",
    "org.apache.datasketches:datasketches-memory:3.0.2": "a3dbdec4de16bf2b0a4c9b1b253bd4064d587675fc76063f8972cdfa104c66cb",
}
MAX_CONFIG_BYTES = 65536
MAX_ARTIFACTS = 64
MAX_TOTAL_JAR_BYTES = 1073741824


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False).encode("utf-8")


def _private_json(path):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError("Canonical absolute private pin document required")
    info = path.lstat()
    if (not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode)
            or info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) != 0o600
            or info.st_size <= 0 or info.st_size > MAX_CONFIG_BYTES):
        raise PermissionError("Owned mode-0600 bounded pin document required")
    raw = path.read_bytes()

    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("Duplicate private pin member")
            result[key] = value
        return result

    value = json.loads(raw, object_pairs_hook=pairs,
                       parse_float=lambda _: (_ for _ in ()).throw(ValueError("Float refused")),
                       parse_constant=lambda _: (_ for _ in ()).throw(ValueError("Constant refused")))
    if type(value) is not dict or _canonical(value) != raw:
        raise ValueError("Canonical private pin JSON required")
    return value


def _digest_file(path, maximum):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path:
        raise ValueError("Canonical pinned dependency path required")
    info = path.lstat()
    if not stat.S_ISREG(info.st_mode) or stat.S_ISLNK(info.st_mode) or not 0 < info.st_size <= maximum:
        raise ValueError("Bounded regular pinned dependency file required")
    digest = hashlib.sha256()
    size = 0
    with path.open("rb") as stream:
        while True:
            block = stream.read(min(65536, maximum + 1 - size))
            if not block:
                break
            size += len(block)
            if size > maximum:
                raise ValueError("Pinned dependency exceeds byte bound")
            digest.update(block)
    return size, digest.hexdigest()


def _owned_private_directory(path, *, require_private, require_owner=True):
    path = Path(path)
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        raise ValueError("Canonical nonsymlink private directory required")
    info = path.stat()
    if not stat.S_ISDIR(info.st_mode) or (require_owner and info.st_uid != os.getuid()):
        raise PermissionError("Expected caller-owned directory")
    if require_private and stat.S_IMODE(info.st_mode) & 0o077:
        raise PermissionError("Private directory permissions required")
    return path


def _select_pins(value):
    if type(value) is not dict or set(value) != {
            "profile", "pythonExecutable", "pythonBinary", "pythonExecutableSha256",
            "javaExecutable", "javaExecutableSha256",
            "ashlarWheelSha256", "ashlarModuleSha256", "scratchParent", "evidencePath", "artifacts"}:
        raise ValueError("Exact private GraphFrames smoke pins required")
    if value["profile"] != PROFILE:
        raise ValueError("Exact private GraphFrames smoke profile required")
    for key in ("ashlarWheelSha256", "ashlarModuleSha256"):
        if type(value[key]) is not str or re.fullmatch(r"[0-9a-f]{64}", value[key]) is None:
            raise ValueError("Exact installed Ashlar wheel and module digests required")
    if (type(value["pythonExecutableSha256"]) is not str
            or re.fullmatch(r"[0-9a-f]{64}", value["pythonExecutableSha256"]) is None):
        raise ValueError("Exact Python executable digest required")
    python_launcher = Path(value["pythonExecutable"])
    python_binary = Path(value["pythonBinary"])
    if (not python_launcher.is_absolute() or python_launcher != Path(sys.executable)
            or not python_binary.is_absolute() or python_binary.resolve() != python_binary
            or python_launcher.resolve(strict=True) != python_binary):
        raise PermissionError("Selected Python venv launcher and resolved interpreter required")
    python_size, python_sha = _digest_file(python_binary, 268435456)
    if python_sha != value["pythonExecutableSha256"]:
        raise PermissionError("Selected Python executable bytes changed")
    java = Path(value["javaExecutable"])
    if java.name != "java" or not java.is_absolute() or java.resolve() != java:
        raise ValueError("Canonical selected Java executable required")
    java_size, java_sha = _digest_file(java, 268435456)
    if java_sha != value["javaExecutableSha256"]:
        raise PermissionError("Selected Java executable bytes changed")
    scratch = Path(value["scratchParent"])
    _owned_private_directory(scratch, require_private=False, require_owner=False)
    output = Path(value["evidencePath"])
    if output.exists() or output.is_symlink():
        raise ValueError("Fresh canonical private evidence path required")
    _owned_private_directory(output.parent, require_private=True)
    artifacts = value["artifacts"]
    if type(artifacts) is not list or not len(DIRECT_ARTIFACTS) <= len(artifacts) <= MAX_ARTIFACTS:
        raise ValueError("Complete bounded Maven jar closure required")
    by_coordinate = {}
    total_bytes = 0
    records = []
    for item in artifacts:
        if type(item) is not dict or set(item) != {"coordinate", "path", "sha256"}:
            raise ValueError("Exact Maven coordinate/path/digest record required")
        coordinate, raw_path, expected_sha = item["coordinate"], item["path"], item["sha256"]
        if (type(coordinate) is not str or not re.fullmatch(r"[A-Za-z0-9_.:-]{3,256}", coordinate)
                or coordinate in by_coordinate or type(expected_sha) is not str
                or re.fullmatch(r"[0-9a-f]{64}", expected_sha) is None):
            raise ValueError("Unique exact Maven dependency pin required")
        path = Path(raw_path)
        if not path.name.endswith(".jar"):
            raise ValueError("Pinned JVM dependency jar required")
        size, sha = _digest_file(path, 536870912)
        if sha != expected_sha:
            raise PermissionError("Pinned JVM dependency bytes changed")
        if coordinate in KNOWN_ARTIFACT_SHA256 and sha != KNOWN_ARTIFACT_SHA256[coordinate]:
            raise PermissionError("Selected reviewed Delta 4.4.1 bytes required")
        total_bytes += size
        records.append({"coordinate": coordinate, "url": ARTIFACT_URLS.get(coordinate),
                        "sha256": sha, "bytes": size})
        by_coordinate[coordinate] = path
    if not DIRECT_ARTIFACTS <= set(by_coordinate) or total_bytes > MAX_TOTAL_JAR_BYTES:
        raise ValueError("Spark 4.2 Delta 4.4 and GraphFrames 0.12.3 jars required")
    if importlib.metadata.version("pyspark") != PYSPARK_VERSION:
        raise PermissionError("Selected PySpark 4.2.0 distribution required")
    if importlib.metadata.version("graphframes-py") != GRAPHFRAMES_PY_VERSION:
        raise PermissionError("Selected GraphFrames Python 0.12.3 distribution required")
    return {
        "ashlarWheelSha256": value["ashlarWheelSha256"],
        "ashlarModuleSha256": value["ashlarModuleSha256"],
        "java": java,
        "scratch": scratch,
        "output": output,
        "jars": tuple(by_coordinate.values()),
        "artifacts": sorted(records, key=lambda row: row["coordinate"]),
        "python": {"launcher": str(python_launcher), "binary": str(python_binary),
                   "sha256": python_sha, "bytes": python_size,
                   "version": platform.python_version()},
        "javaPin": {"sha256": java_sha, "bytes": java_size},
        "artifactPaths": dict(by_coordinate),
    }


def _distribution_module_origin(distribution, module_path):
    module_path = Path(module_path).resolve(strict=True)
    entries = distribution.files
    if entries is None:
        raise PermissionError("Installed GraphFrames distribution RECORD required")
    for entry in entries:
        candidate = Path(distribution.locate_file(entry)).resolve()
        if candidate != module_path:
            continue
        recorded_hash = entry.hash
        if recorded_hash is None or recorded_hash.mode != "sha256":
            raise PermissionError("GraphFrames module SHA-256 RECORD entry required")
        size, digest = _digest_file(candidate, 16777216)
        expected = base64.urlsafe_b64encode(bytes.fromhex(digest)).decode("ascii").rstrip("=")
        if expected != recorded_hash.value or (entry.size is not None and size != entry.size):
            raise PermissionError("Installed GraphFrames module differs from its distribution RECORD")
        return {"path": str(candidate), "sha256": digest, "bytes": size}
    raise PermissionError("Imported GraphFrames module is outside selected graphframes-py distribution")


def _graphframes_python_origins():
    import graphframes
    from graphframes.graphframe import GraphFrame
    from graphframes.classic.graphframe import GraphFrame as ClassicGraphFrame

    distribution = importlib.metadata.distribution("graphframes-py")
    if distribution.version != GRAPHFRAMES_PY_VERSION:
        raise PermissionError("Selected GraphFrames Python distribution required")
    return {
        "distribution": {"name": distribution.metadata["Name"], "version": distribution.version},
        "facade": _distribution_module_origin(distribution, inspect.getsourcefile(GraphFrame)),
        "classic": _distribution_module_origin(distribution, inspect.getsourcefile(ClassicGraphFrame)),
        "package": _distribution_module_origin(distribution, graphframes.__file__),
    }


def _ashlar_module_origin(expected_wheel_sha, expected_module_sha):
    distribution = importlib.metadata.distribution("ashlar-graph-toolkit")
    direct_url = distribution.read_text("direct_url.json")
    if direct_url is None:
        raise PermissionError("Installed Ashlar wheel origin record required")
    source = json.loads(direct_url)
    archive = source.get("archive_info", {})
    wheel_sha = archive.get("hashes", {}).get("sha256")
    if wheel_sha is None:
        legacy = archive.get("hash", "")
        if legacy.startswith("sha256="):
            wheel_sha = legacy.partition("=")[2]
    if wheel_sha != expected_wheel_sha:
        raise PermissionError("Installed Ashlar wheel differs from the selected reviewed wheel")
    origin = _distribution_module_origin(
        distribution, distribution.locate_file("ashlar_host/pinned_delta_graphframes.py"))
    if origin["sha256"] != expected_module_sha:
        raise PermissionError("Installed Ashlar smoke module differs from the reviewed module pin")
    return {"distribution": distribution.metadata["Name"],
            "version": distribution.version, "wheelSha256": wheel_sha,
            "module": origin}


def _jvm_class_origins(spark, pins):
    loader = spark._jvm.java.lang.Thread.currentThread().getContextClassLoader()
    targets = {
        "io.delta.sql.DeltaSparkSessionExtension": "io.delta:delta-spark_4.2_2.13:4.4.1",
        "io.delta.tables.DeltaTable": "io.delta:delta-spark_4.2_2.13:4.4.1",
        "org.graphframes.GraphFrame": "io.graphframes:graphframes-spark4_2.13:0.12.3",
    }
    result = {}
    for class_name, coordinate in targets.items():
        loaded = loader.loadClass(class_name)
        source = loaded.getProtectionDomain().getCodeSource()
        if source is None:
            raise PermissionError("Loaded runtime class has no code-source origin")
        uri = source.getLocation().toURI()
        actual_path = Path(str(uri.getPath())).resolve(strict=True)
        expected_path = Path(pins["artifactPaths"][coordinate]).resolve(strict=True)
        if actual_path != expected_path:
            raise PermissionError("Loaded runtime class did not come from its SHA-pinned jar")
        size, digest = _digest_file(actual_path, 536870912)
        expected_record = next(item for item in pins["artifacts"] if item["coordinate"] == coordinate)
        if digest != expected_record["sha256"] or size != expected_record["bytes"]:
            raise PermissionError("Loaded runtime class jar bytes differ from selected pin")
        result[class_name] = {
            "coordinate": coordinate,
            "path": str(actual_path),
            "sha256": digest,
            "bytes": size,
        }
    return result


def _write_private(path, raw):
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL
    if hasattr(os, "O_NOFOLLOW"):
        flags |= os.O_NOFOLLOW
    descriptor = os.open(path, flags, 0o600)
    with os.fdopen(descriptor, "wb") as stream:
        os.fchmod(stream.fileno(), 0o600)
        if stream.write(raw) != len(raw):
            raise OSError("Complete private smoke evidence write required")
        stream.flush()
        os.fsync(stream.fileno())


def run_private_smoke(configuration_path):
    """Run only the frozen tiny physical fixture and bounded one-hop query."""
    pins = _select_pins(_private_json(configuration_path))
    from pyspark import SparkConf, SparkContext
    from pyspark.sql import SparkSession
    from ashlar_host.pinned_delta_graphframes import (
        create_synthetic_delta_fixture, run_synthetic_graphframes_one_hop,
    )

    scratch = pins["scratch"] / ("ashlar-graphframes-smoke-" + uuid.uuid4().hex)
    scratch.mkdir(mode=0o700)
    if scratch.resolve() != scratch:
        raise ValueError("Canonical fresh private fixture path required")
    _owned_private_directory(scratch, require_private=True)
    paths = {"object_current": str(scratch / "object_current"),
             "edge_current": str(scratch / "edge_current")}
    conf = SparkConf().setMaster("local[2]").setAppName("Ashlar pinned Delta GraphFrames smoke")
    for key, setting in {
        "spark.driver.memory": "2g",
        "spark.ui.enabled": "false",
        "spark.sql.shuffle.partitions": "2",
        "spark.sql.session.timeZone": "UTC",
        "spark.sql.ansi.enabled": "true",
        "spark.sql.extensions": "io.delta.sql.DeltaSparkSessionExtension",
        "spark.sql.catalog.spark_catalog": "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        "spark.sql.warehouse.dir": str(scratch / "warehouse"),
        "spark.jars": ",".join(str(path) for path in pins["jars"]),
        "spark.pyspark.python": sys.executable,
        "spark.pyspark.driver.python": sys.executable,
    }.items():
        conf.set(key, setting)
    context = None
    primary = None
    report = None
    try:
        context = SparkContext(conf=conf)
        spark = SparkSession(context)
        if spark.version != "4.2.0":
            raise PermissionError("Actual Spark 4.2.0 runtime required")
        spark.sparkContext.setLogLevel("ERROR")
        opening_class_origins = _jvm_class_origins(spark, pins)
        opening_python_origins = _graphframes_python_origins()
        opening_ashlar_origin = _ashlar_module_origin(
            pins["ashlarWheelSha256"], pins["ashlarModuleSha256"])
        owner = create_synthetic_delta_fixture(spark, paths)
        report = run_synthetic_graphframes_one_hop(spark, owner)
        closing_class_origins = _jvm_class_origins(spark, pins)
        closing_python_origins = _graphframes_python_origins()
        closing_ashlar_origin = _ashlar_module_origin(
            pins["ashlarWheelSha256"], pins["ashlarModuleSha256"])
        if (opening_class_origins != closing_class_origins
                or opening_python_origins != closing_python_origins
                or opening_ashlar_origin != closing_ashlar_origin):
            raise PermissionError("Runtime class origins changed during smoke")
        report["runtimeClassProvenance"] = {
            "opening": opening_class_origins,
            "closing": closing_class_origins,
            "graphframesPython": {
                "opening": opening_python_origins,
                "closing": closing_python_origins,
            },
            "ashlarInstalledWheel": {
                "opening": opening_ashlar_origin,
                "closing": closing_ashlar_origin,
            },
        }
        report["runtime"].update({
            "python": pins["python"]["version"],
            "pyspark": importlib.metadata.version("pyspark"),
            "java": spark._jvm.java.lang.System.getProperty("java.version"),
            "scala": spark._jvm.scala.util.Properties.versionNumberString(),
        })
        report["dependencyPins"] = {
            "pythonExecutable": {
                "launcher": pins["python"]["launcher"],
                "resolvedBinary": pins["python"]["binary"],
                "sha256": pins["python"]["sha256"], "bytes": pins["python"]["bytes"],
            },
            "javaExecutable": pins["javaPin"],
            "jars": pins["artifacts"],
            "mavenClosureSha256": hashlib.sha256(_canonical(pins["artifacts"])).hexdigest(),
            "graphframesGraphx": {
                "coordinate": "io.graphframes:graphframes-graphx-spark4_2.13:0.12.3",
                "inputPinOnly": True,
                "executionClaim": "not-attested-by-the-one-hop-smoke",
            },
        }
        report["sourceAuthority"] = "synthetic-fixture-only"
        report["qualification"] = "one selected Spark 4.2.0 / Delta 4.4.1 / GraphFrames 0.12.3 struct-key and bounded one-hop compatibility smoke; no scale or production claim"
    except BaseException as error:
        primary = error
    finally:
        if context is not None:
            try:
                context.stop()
            except BaseException:
                if primary is None:
                    primary = RuntimeError("Selected Spark context did not close cleanly")
    if primary is not None:
        raise primary
    raw = _canonical(report) + b"\n"
    _write_private(pins["output"], raw)
    return report


if __name__ == "__main__":
    if len(sys.argv) != 2:
        raise SystemExit("private pin document path required")
    run_private_smoke(Path(sys.argv[1]))
