"""One-attempt process and byte-budget supervisor for the reviewed R05 discriminator.

This helper is inert until an independently reviewed invocation packet and a
separately selected one-run authorization are supplied. It never starts Spark
itself; the child worker is hash-checked and must configure Spark local mode.
"""

from __future__ import annotations

import hashlib
import json
import os
import re
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import tempfile
import time
from collections import Counter
from typing import Any

from runtime_accounting import inventory_roots

MAX_ATTEMPT_SECONDS = 300
MAX_AGGREGATE_BYTES = 2 << 30
MAX_ATTEMPT_ROOT_BYTES = 512 << 20
MAX_DIAGNOSTIC_BYTES = 128 << 20
MAX_RESULT_BYTES = 1 << 20
POLL_SECONDS = 0.025
TERMINATE_GRACE_SECONDS = 1.0


class NativeAttemptRefused(PermissionError):
    pass


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        while chunk := stream.read(1024 * 1024):
            digest.update(chunk)
    return digest.hexdigest()


def _canonical_regular_file(path: Path) -> Path:
    if path.is_symlink():
        raise NativeAttemptRefused("Selected verifier input must not be a symlink")
    resolved = path.resolve(strict=True)
    if resolved != path or not resolved.is_file():
        raise NativeAttemptRefused("Canonical verifier input file required")
    return resolved


def _tree_bytes(root: Path) -> int:
    """Account every regular file under root; refuse links and special files."""
    if root.is_symlink() or not root.is_dir():
        raise NativeAttemptRefused("Fresh private attempt root required")
    total = 0
    stack = [root]
    while stack:
        directory = stack.pop()
        with os.scandir(directory) as entries:
            for entry in entries:
                info = entry.stat(follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    raise NativeAttemptRefused("Symlinks are unsupported in accounted attempt root")
                if stat.S_ISDIR(info.st_mode):
                    stack.append(Path(entry.path))
                elif stat.S_ISREG(info.st_mode):
                    total += info.st_size
                else:
                    raise NativeAttemptRefused("Special files are unsupported in accounted attempt root")
    return total


def _check_deadline(started: float) -> None:
    if time.monotonic() - started >= MAX_ATTEMPT_SECONDS:
        raise NativeAttemptRefused("300-second total attempt deadline exceeded")


def _verify_runtime_tree_snapshot(inventory: dict[str, Any], *,
                                 total_started: float | None = None,
                                 metadata_only: bool = False) -> dict[str, Any]:
    """Re-inventory the pinned roots, checking the shared 300-second deadline."""
    try:
        selected_hashes = {item["path"] for item in inventory["entries"]
                           if type(item) is dict and "sha256" in item}
        live = inventory_roots(
            [Path(value) for value in inventory["roots"]],
            allowed_links={item["path"]: {"rawTarget": item["rawTarget"], "target": item["target"]}
                           for item in inventory["entries"] if item.get("type") == "symlink"},
            hash_paths=set() if metadata_only else selected_hashes,
            known_dangling_links=set(inventory.get("knownDanglingLinks", [])),
            check=(lambda: _check_deadline(total_started)) if total_started is not None else None)
    except (AccountingError, OSError, KeyError, TypeError) as error:
        raise NativeAttemptRefused(f"Retained runtime tree could not be re-inventoried: {error}") from error
    fields = ("roots", "entries", "knownDanglingLinks", "uniqueFileCount", "uniqueBytes")
    for field in fields:
        expected = inventory.get(field)
        observed = live.get(field)
        if metadata_only and field == "entries":
            expected = [{key: value for key, value in row.items() if key != "sha256"}
                        for row in expected]
            observed = [{key: value for key, value in row.items() if key != "sha256"}
                        for row in observed]
        if observed != expected:
            raise NativeAttemptRefused(f"Retained runtime tree {field} changed from its pinned inventory")
    return live


def _check_byte_accounting(root: Path, retained_bytes: int) -> int:
    attempt_bytes = _tree_bytes(root)
    if attempt_bytes > MAX_ATTEMPT_ROOT_BYTES:
        raise NativeAttemptRefused("Attempt root exceeded its 512 MiB limit")
    if retained_bytes < 0 or retained_bytes + attempt_bytes > MAX_AGGREGATE_BYTES:
        raise NativeAttemptRefused("Retained plus attempt bytes exceed the 2 GiB limit")
    return attempt_bytes


def _observe_fixture_cleanup(fixture_root: Path, worker_claim: dict[str, Any]) -> dict[str, Any]:
    """Independently verify the child left only its stable lock and cleanup receipt."""
    receipt_name = ".ashlar-r05-ownership.json"
    lock_name = ".ashlar-graphframes-action.lock"
    if (fixture_root.is_symlink() or not fixture_root.is_dir()
            or fixture_root.resolve(strict=True) != fixture_root
            or fixture_root.stat().st_mode & 0o077):
        raise NativeAttemptRefused("Post-child fixture root is not canonical and private")
    if {entry.name for entry in fixture_root.iterdir()} != {lock_name, receipt_name}:
        raise NativeAttemptRefused("Post-child fixture root contains tables or unexpected entries")
    lock_path = fixture_root / lock_name
    lock_info = lock_path.lstat()
    if (not stat.S_ISREG(lock_info.st_mode) or lock_info.st_nlink != 1
            or lock_info.st_mode & 0o077):
        raise NativeAttemptRefused("Post-child stable lock is not a private regular file")
    receipt_path = fixture_root / receipt_name
    before = receipt_path.lstat()
    if (not stat.S_ISREG(before.st_mode) or before.st_nlink != 1
            or before.st_size > 16384 or before.st_mode & 0o077):
        raise NativeAttemptRefused("Post-child ownership receipt is not bounded and regular")
    fd = os.open(receipt_path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        opened = os.fstat(fd)
        if ((opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
                or not stat.S_ISREG(opened.st_mode) or opened.st_size > 16384):
            raise NativeAttemptRefused("Post-child ownership receipt changed during open")
        with os.fdopen(os.dup(fd), "rb") as stream:
            raw = stream.read(16385)
    finally:
        os.close(fd)
    if len(raw) > 16384:
        raise NativeAttemptRefused("Post-child ownership receipt exceeded its bound")
    try:
        receipt = json.loads(raw)
    except (UnicodeError, json.JSONDecodeError) as error:
        raise NativeAttemptRefused("Post-child ownership receipt is malformed") from error
    root_info = fixture_root.stat()
    expected_root = {"device": root_info.st_dev, "inode": root_info.st_ino}
    expected_lock = {"device": lock_info.st_dev, "inode": lock_info.st_ino}
    digest = hashlib.sha256(raw).hexdigest()
    tables = receipt.get("tables") if type(receipt) is dict else None
    if (type(receipt) is not dict or receipt.get("profile") != "ashlar-r05-owned-tables/0.1"
            or receipt.get("status") != "cleanup-complete"
            or receipt.get("tablesRemoved") != ["object_current", "edge_current"]
            or receipt.get("root") != expected_root or receipt.get("lock") != expected_lock
            or type(tables) is not dict or set(tables) != {"object_current", "edge_current"}
            or any(type(tables[role]) is not dict
                   or type(tables[role].get("path")) is not dict
                   or set(tables[role]["path"]) != {"device", "inode"}
                   or type(tables[role].get("deltaUuid")) is not str
                   or re.fullmatch(r"[0-9a-fA-F-]{36}", tables[role]["deltaUuid"]) is None
                   for role in ("object_current", "edge_current"))
            or not (type(worker_claim) is dict
                    and worker_claim.get("receiptSha256") == digest
                    and worker_claim.get("rootIdentity") == expected_root
                    and worker_claim.get("lockIdentity") == expected_lock)):
        raise NativeAttemptRefused("Parent-observed cleanup receipt differs from worker claim or fixture identities")
    return {"receiptSha256": digest, "rootIdentity": expected_root,
            "lockIdentity": expected_lock, "tablesAbsent": True}


def _validated_java_pin(runtime: dict[str, Any]) -> tuple[Path, Path]:
    home_text = runtime.get("javaHome")
    executable_text = runtime.get("javaExecutable")
    digest = runtime.get("javaExecutableSha256")
    if (type(home_text) is not str or type(executable_text) is not str
            or type(digest) is not str or not Path(home_text).is_absolute()
            or not Path(executable_text).is_absolute()):
        raise NativeAttemptRefused("Exact JAVA_HOME and Java executable pin required")
    home = Path(home_text)
    if home.is_symlink() or home.resolve(strict=True) != home or not home.is_dir():
        raise NativeAttemptRefused("JAVA_HOME must be a canonical directory")
    executable = _canonical_regular_file(Path(executable_text))
    release = home / "release"
    release_info = _canonical_regular_file(release).read_text(encoding="utf-8")
    if (executable != home / "bin/java"
            or not executable.stat().st_mode & stat.S_IXUSR
            or sha256_file(executable) != digest
            or runtime.get("jdkBuild") != "21.0.13.0.2"
            or 'JAVA_VERSION="21.0.13.0.2"' not in release_info):
        raise NativeAttemptRefused("Java executable path or SHA-256 differs from the selected JAVA_HOME pin")
    return home, executable


def _validated_python_pin(runtime: dict[str, Any]) -> Path:
    executable_text = runtime.get("pythonExecutable")
    digest = runtime.get("pythonExecutableSha256")
    if (type(executable_text) is not str or type(digest) is not str
            or not Path(executable_text).is_absolute()):
        raise NativeAttemptRefused("Exact Python executable path and SHA-256 pin required")
    executable = _canonical_regular_file(Path(executable_text))
    if (not executable.stat().st_mode & stat.S_IXUSR
            or sha256_file(executable) != digest
            or runtime.get("pythonVersion") != "3.12"):
        raise NativeAttemptRefused("Python executable path or SHA-256 differs from the selected interpreter pin")
    return executable


def _validated_spark_home(runtime: dict[str, Any]) -> tuple[Path, Path]:
    home_text = runtime.get("sparkHome")
    submit_text = runtime.get("sparkSubmitExecutable")
    submit_digest = runtime.get("sparkSubmitSha256")
    if (type(home_text) is not str or type(submit_text) is not str
            or type(submit_digest) is not str or not Path(home_text).is_absolute()
            or not Path(submit_text).is_absolute()):
        raise NativeAttemptRefused("Exact Spark home and spark-submit SHA-256 pin required")
    home = Path(home_text)
    if home.is_symlink() or home.resolve(strict=True) != home or not home.is_dir():
        raise NativeAttemptRefused("SPARK_HOME must be a canonical directory")
    submit = _canonical_regular_file(Path(submit_text))
    if (submit != home / "bin/spark-submit" or not submit.stat().st_mode & stat.S_IXUSR
            or sha256_file(submit) != submit_digest or not (home / "jars").is_dir()):
        raise NativeAttemptRefused("spark-submit path, digest, or Spark jars directory differs from the selected Spark home pin")
    return home, submit


def _child_environment(runtime: dict[str, Any], attempt_root: Path) -> dict[str, str]:
    java_home, _java_executable = _validated_java_pin(runtime)
    python_executable = _validated_python_pin(runtime)
    spark_home, _spark_submit = _validated_spark_home(runtime)
    scratch = attempt_root / "spark-local"
    return {
        "PATH": os.pathsep.join((str(java_home / "bin"), "/usr/bin", "/bin")),
        "JAVA_HOME": str(java_home),
        "SPARK_HOME": str(spark_home),
        "HOME": str(attempt_root / "home"),
        "TMPDIR": str(attempt_root / "tmp"), "TMP": str(attempt_root / "tmp"),
        "TEMP": str(attempt_root / "tmp"),
        "SPARK_LOCAL_DIRS": str(scratch),
        "JAVA_TOOL_OPTIONS": "-Djava.io.tmpdir=" + str(attempt_root / "tmp"),
        "PYTHONPATH": runtime["pythonPath"], "PYTHONDONTWRITEBYTECODE": "1",
        "PYSPARK_PYTHON": str(python_executable),
        "PYSPARK_DRIVER_PYTHON": str(python_executable),
    }


def _require_bound_attempt_parent(parent: Path, authorized_parent: str) -> None:
    if str(parent) != authorized_parent:
        raise NativeAttemptRefused("Attempt parent differs from separately authorized immutable parent")


def _discard_success_receipts(attempt_root: Path) -> None:
    for success_path in (attempt_root / "supervisor-receipt.json",
                         attempt_root / ".supervisor-receipt.pending"):
        try:
            success_path.unlink(missing_ok=True)
        except OSError:
            pass


def _kill_process_group(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=TERMINATE_GRACE_SECONDS)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait()
    # The process-group leader is reaped above. Signal any remaining members
    # so local Spark children cannot outlive the attempt leader.
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass


def _ensure_process_group_stopped(process: subprocess.Popen[bytes]) -> None:
    try:
        os.killpg(process.pid, 0)
    except ProcessLookupError:
        pass
    else:
        _kill_process_group(process)
    if process.poll() is None:
        _kill_process_group(process)
        process.wait()


def _collect_child(argv: list[str], *, cwd: Path, env: dict[str, str],
                   root: Path, retained_bytes: int, timeout_seconds: int,
                   diagnostics_path: Path, result_path: Path,
                   total_started: float | None = None) -> dict[str, Any]:
    started = time.monotonic()
    total_started = started if total_started is None else total_started
    if not 0 < timeout_seconds <= MAX_ATTEMPT_SECONDS:
        raise NativeAttemptRefused("The R05 verifier has one fixed 300-second deadline")
    _check_byte_accounting(root, retained_bytes)

    process = subprocess.Popen(
        argv, cwd=cwd, env=env, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        start_new_session=True, close_fds=True)
    try:
        pgid = os.getpgid(process.pid)
        if pgid != process.pid:
            raise NativeAttemptRefused("Verifier must own one fresh process group")
    except BaseException:
        _ensure_process_group_stopped(process)
        raise

    try:
        selector = selectors.DefaultSelector()
    except BaseException:
        _ensure_process_group_stopped(process)
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        raise
    assert process.stdout is not None and process.stderr is not None
    try:
        selector.register(process.stdout, selectors.EVENT_READ, "diagnostics")
        selector.register(process.stderr, selectors.EVENT_READ, "diagnostics")
    except BaseException:
        _ensure_process_group_stopped(process)
        selector.close()
        process.stdout.close()
        process.stderr.close()
        raise
    diagnostic_bytes = 0
    refusal = None
    try:
        with diagnostics_path.open("xb") as diagnostic_file:
            while selector.get_map() or process.poll() is None:
                now = time.monotonic()
                if now - total_started >= timeout_seconds:
                    refusal = "300-second total attempt deadline exceeded"
                try:
                    _check_byte_accounting(root, retained_bytes)
                except (NativeAttemptRefused, OSError) as error:
                    refusal = str(error)
                if refusal is not None:
                    _kill_process_group(process)
                    break

                for key, _mask in selector.select(POLL_SECONDS):
                    chunk = os.read(key.fileobj.fileno(), 64 * 1024)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        key.fileobj.close()
                        continue
                    remaining = MAX_DIAGNOSTIC_BYTES - diagnostic_bytes
                    if len(chunk) > remaining:
                        if remaining:
                            diagnostic_file.write(chunk[:remaining])
                            diagnostic_bytes += remaining
                        refusal = "Verifier diagnostics exceeded 128 MiB"
                        break
                    diagnostic_file.write(chunk)
                    diagnostic_bytes += len(chunk)
                diagnostic_file.flush()
                if refusal is not None:
                    _kill_process_group(process)
                    break

            return_code = process.wait()
            if selector.get_map():
                for key in list(selector.get_map().values()):
                    selector.unregister(key.fileobj)
                    key.fileobj.close()
    except BaseException:
        _ensure_process_group_stopped(process)
        selector.close()
        for pipe in (process.stdout, process.stderr):
            if pipe is not None:
                pipe.close()
        raise

    selector.close()
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        group_remains = False
    else:
        group_remains = True
    if refusal is None and group_remains:
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        refusal = "Verifier process group outlived its worker leader"
    duration = time.monotonic() - total_started
    if refusal is None:
        try:
            _check_byte_accounting(root, retained_bytes)
        except (NativeAttemptRefused, OSError) as error:
            refusal = str(error)
    if refusal is None and return_code != 0:
        refusal = f"Verifier exited with status {return_code}"
    if refusal is None:
        try:
            if (result_path.is_symlink() or not result_path.is_file()
                    or result_path.stat().st_size > MAX_RESULT_BYTES):
                raise NativeAttemptRefused("Verifier result file missing or exceeded its 1 MiB bound")
            result = json.loads(result_path.read_text(encoding="utf-8"))
        except (OSError, UnicodeError, json.JSONDecodeError, NativeAttemptRefused) as error:
            refusal = "Verifier did not return one bounded JSON receipt"
            result = None
        if (refusal is None and (type(result) is not dict
                or result.get("status") != "bounded-one-hop-passed"
                or result.get("canonicalFixtureQualification")
                   != "persisted-carrier-and-one-hop-verified"
                or result.get("sourceProgress") != "absent"
                or result.get("protectedAck") != "absent"
                or result.get("publication") != "absent"
                or result.get("unityCatalog") != "absent"
                or result.get("productionAuthority") != "absent")):
            refusal = "Native result lacks exact action pass and absent-authority fields"
    if refusal is not None:
        raise NativeAttemptRefused(refusal)
    return {
        "status": "bounded-one-hop-passed",
        "receipt": result,
        "executionAttestation": {
            "kind": "supervisor-observed-local-child-completion",
            "pid": process.pid, "processGroupId": pgid,
            "returnCode": return_code,
            "durationSeconds": duration,
            "resultSha256": sha256_file(result_path),
            "diagnosticsBytes": diagnostic_bytes,
            "diagnosticsSha256": sha256_file(diagnostics_path),
            "attemptBytesHighWaterMethod": "periodic-full-tree-stat",
            "retainedPlusAttemptByteLimit": MAX_AGGREGATE_BYTES,
            "attemptRootByteLimit": MAX_ATTEMPT_ROOT_BYTES,
            "diagnosticsLimit": MAX_DIAGNOSTIC_BYTES,
        },
    }


def _verify_packet_and_authorization(packet_path: Path, selected_packet_sha256: str,
                                     authorization_path: Path,
                                     selected_authorization_sha256: str) -> tuple[
                                         dict[str, Any], Path, Path, Path, dict[str, Any]]:
    packet_file = _canonical_regular_file(packet_path)
    if sha256_file(packet_file) != selected_packet_sha256:
        raise NativeAttemptRefused("Selected supervisor invocation packet digest changed")
    packet = json.loads(packet_file.read_text(encoding="utf-8"))
    if (type(packet) is not dict
            or packet.get("reviewStatus") != "independently-reviewed"
            or packet.get("executionStatus") != "authorized-for-one-attempt"):
        raise NativeAttemptRefused("Reviewed wrapper packet and one-run authorization required")
    if (packet.get("runtime", {}).get("master") != "local[2]"
            or packet.get("hardBounds") != {
                "attempts": 1, "totalSeconds": MAX_ATTEMPT_SECONDS,
                "retainedPlusAttemptBytes": MAX_AGGREGATE_BYTES,
                "attemptRootBytes": MAX_ATTEMPT_ROOT_BYTES,
                "diagnosticsBytes": MAX_DIAGNOSTIC_BYTES,
                "freshRootMode": "0700",
                "processGroup": "one child session; terminate/kill group and reap leader",
                "sparkScratch": "entirely under the fresh accounted attempt root"}):
        raise NativeAttemptRefused("Exact local one-attempt hard bounds required")
    if (packet.get("profile") != "ashlar-r05-canonical-graphframes-one-attempt/0.2"
            or packet.get("cleanupStatus") != "reviewed-owned-cleanup-qualified"
            or packet.get("outputRootPolicy") != "attempt-root/tmp/fixture; private mode 0700"
            or packet.get("executionStatus") != "authorized-for-one-attempt"):
        raise NativeAttemptRefused("R05 cleanup qualification and one-attempt review are required")
    authorization_file = _canonical_regular_file(authorization_path)
    if sha256_file(authorization_file) != selected_authorization_sha256:
        raise NativeAttemptRefused("Separately selected execution authorization digest changed")
    authorization = json.loads(authorization_file.read_text(encoding="utf-8"))
    expected_authorization = {
        "profile": "ashlar-canonical-graphframes-execution-authorization/0.1",
        "status": "authorized-for-one-attempt",
        "invocationPacketSha256": selected_packet_sha256,
        "attempts": 1,
        "maximumSeconds": MAX_ATTEMPT_SECONDS,
        "maximumAggregateBytes": MAX_AGGREGATE_BYTES,
        "maximumAttemptRootBytes": MAX_ATTEMPT_ROOT_BYTES,
        "maximumDiagnosticsBytes": MAX_DIAGNOSTIC_BYTES,
        "master": "local[2]",
    }
    expected_authorization_keys = set(expected_authorization) | {"attemptParent"}
    if (type(authorization) is not dict
            or set(authorization) != expected_authorization_keys
            or any(authorization.get(key) != value
                   for key, value in expected_authorization.items())
            or type(authorization.get("attemptParent")) is not str
            or not Path(authorization["attemptParent"]).is_absolute()
            or str(Path(authorization["attemptParent"])) != authorization["attemptParent"]):
        raise NativeAttemptRefused("Execution authorization does not bind this exact one-run packet")
    wrapper_path = _canonical_regular_file(Path(__file__))
    wrapper = packet.get("wrapper")
    if (type(wrapper) is not dict or wrapper.get("path") != wrapper_path.name
            or wrapper.get("sha256") != sha256_file(wrapper_path)):
        raise NativeAttemptRefused("Supervisor wrapper bytes differ from reviewed packet")
    worker = packet.get("worker")
    if type(worker) is not dict:
        raise NativeAttemptRefused("One pinned native verifier worker required")
    worker_path = _canonical_regular_file(wrapper_path.parent / worker.get("path", ""))
    if worker_path.parent != wrapper_path.parent or sha256_file(worker_path) != worker.get("sha256"):
        raise NativeAttemptRefused("Native verifier worker bytes differ from reviewed packet")
    _java_home, java_executable = _validated_java_pin(packet.get("runtime", {}))
    python_executable = _validated_python_pin(packet.get("runtime", {}))
    _spark_home, spark_submit = _validated_spark_home(packet.get("runtime", {}))
    retained_inputs = packet.get("retainedInputs")
    retained_hashes = packet.get("retainedInputSha256")
    java_path_text = str(java_executable)
    if (type(retained_inputs) is not list or type(retained_hashes) is not dict
            or java_path_text not in retained_inputs
            or retained_hashes.get(java_path_text)
               != packet["runtime"]["javaExecutableSha256"]
            or str(spark_submit) not in retained_inputs
            or retained_hashes.get(str(spark_submit))
               != packet["runtime"]["sparkSubmitSha256"]
            or str(python_executable) not in retained_inputs
            or retained_hashes.get(str(python_executable))
               != packet["runtime"]["pythonExecutableSha256"]):
        raise NativeAttemptRefused("Pinned Python, Java, and spark-submit executables and digests must be mandatory retained inputs")
    return packet, worker_path, packet_file, authorization_file, authorization


def _validate_native_receipt(receipt: dict[str, Any], packet: dict[str, Any]) -> None:
    action = packet["action"]
    cleanup = receipt.get("cleanup")
    if (type(cleanup) is not dict or cleanup.get("status") != "cleanup-complete"
            or cleanup.get("tablesRemoved") != ["object_current", "edge_current"]
            or cleanup.get("ownershipReceipt") != ".ashlar-r05-ownership.json"
            or type(cleanup.get("receiptSha256")) is not str):
        raise NativeAttemptRefused("Native receipt lacks verified R05 owned-table cleanup")
    expected_digests = {"manifest": action["manifestSha256"],
                        "reviewAnchor": action["anchorSha256"],
                        "generator": action["generatorSha256"],
                        "source": action["sourceContentSha256"],
                        "oracle": action["oracleSha256"]}
    expected_seed = ["ashlar-r05-source-a", 1, 101]
    expected_schema = ["src_source", "src_type", "src_id", "edge_source",
                       "edge_type", "edge_id", "dst_source", "dst_type", "dst_id"]
    if (receipt.get("descriptorSha256") != action["descriptorSha256"]
            or receipt.get("inputDigests") != expected_digests
            or receipt.get("expectedRowCounts") != [5, 4]
            or receipt.get("cap") != 3
            or receipt.get("selectedSeed") != expected_seed
            or receipt.get("outputSchema") != expected_schema):
        raise NativeAttemptRefused("Native receipt does not bind the reviewed R05 descriptor")
    persisted = receipt.get("persistedFixtureEvidence")
    if (type(persisted) is not dict or persisted.get("status") != "verified"
            or persisted.get("carrierRowsCollected") != 0
            or set(persisted.get("tables", {})) != {"object_current", "edge_current"}
            or persisted["tables"]["object_current"].get("rowCount") != 5
            or persisted["tables"]["edge_current"].get("rowCount") != 4):
        raise NativeAttemptRefused("Native receipt lacks exact persisted R05 fixture witnesses")
    oracle = json.loads(_canonical_regular_file(Path(action["oraclePath"])).read_text(encoding="utf-8"))
    expected_rows = Counter(tuple(row) for row in oracle["rows"])
    actual_rows = Counter(tuple(row) for row in receipt.get("result", []))
    if (expected_rows != actual_rows or receipt.get("resultRows") != 3
            or receipt.get("resultRows") > receipt.get("cap")):
        raise NativeAttemptRefused("Native R05 occurrence bag differs from the independent oracle")
    if not (type(receipt.get("lockWindow")) is dict
            and type(receipt["lockWindow"].get("closedMonotonicNs")) is int):
        raise NativeAttemptRefused("Native receipt lacks close-time custody attestation")
    runtime = receipt.get("runtime")
    expected_runtime = {"spark": "4.2.0", "delta": "4.4.1",
                        "graphframes": "0.12.3", "jdk": "21.0.13"}
    if (type(runtime) is not dict
            or {key: runtime.get(key) for key in expected_runtime} != expected_runtime
            or runtime.get("jdkBuild") != "21.0.13.0.2"
            or {name: value.get("sha256") for name, value in runtime.get("jars", {}).items()}
               != packet["runtime"]["runtimeJarSha256"]):
        raise NativeAttemptRefused("Native R05 runtime/JAR attestation differs from packet pins")


def run_reviewed_native_attempt(*, packet_path: Path, selected_packet_sha256: str,
                                 authorization_path: Path,
                                 selected_authorization_sha256: str,
                                 attempt_parent: Path) -> tuple[Path, dict[str, Any]]:
    """Run exactly one separately authorized local attempt; never retries."""
    started = time.monotonic()
    packet, worker_path, packet_file, authorization_file, authorization = _verify_packet_and_authorization(
        packet_path, selected_packet_sha256, authorization_path,
        selected_authorization_sha256)
    if attempt_parent.is_symlink():
        raise NativeAttemptRefused("Attempt parent must not be a symlink")
    parent = attempt_parent.resolve(strict=True)
    if (not parent.is_dir() or stat.S_IMODE(parent.stat().st_mode) != 0o700
            or (hasattr(os, "getuid") and parent.stat().st_uid != os.getuid())):
        raise NativeAttemptRefused("Private mode-0700 attempt parent required")
    if any(parent.iterdir()):
        raise NativeAttemptRefused("A fresh empty attempt parent is required")
    _require_bound_attempt_parent(parent, authorization["attemptParent"])

    marker = parent / ".ashlar-canonical-graphframes-attempt-consumed"
    try:
        marker_fd = os.open(marker, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                            | getattr(os, "O_NOFOLLOW", 0), 0o600)
    except FileExistsError as error:
        raise NativeAttemptRefused("The single R05 GraphFrames attempt is already consumed") from error
    with os.fdopen(marker_fd, "w", encoding="ascii") as stream:
        stream.write(json.dumps({
            "invocationPacketSha256": selected_packet_sha256,
            "authorizationSha256": selected_authorization_sha256,
            "attemptParent": str(parent),
        }, sort_keys=True, separators=(",", ":")) + "\n")
        stream.flush()
        os.fsync(stream.fileno())

    attempt_root = Path(tempfile.mkdtemp(prefix="ashlar-graphframes-attempt-", dir=parent))
    os.chmod(attempt_root, 0o700)
    for name in ("spark-local", "tmp", "home", "warehouse"):
        (attempt_root / name).mkdir(mode=0o700)
    scratch = attempt_root / "spark-local"
    runtime = packet["runtime"]
    env = _child_environment(runtime, attempt_root)
    argv = [os.fspath(runtime["pythonExecutable"]), os.fspath(worker_path),
            os.fspath(attempt_root), os.fspath(packet_path.resolve(strict=True)),
            os.fspath(attempt_root / "worker-result.json")]
    retained_inputs = packet.get("retainedInputs")
    retained_hashes = packet.get("retainedInputSha256")
    if (type(retained_inputs) is not list or type(retained_hashes) is not dict
            or len(retained_inputs) != len(set(retained_inputs))
            or set(retained_hashes) != set(retained_inputs)):
        raise NativeAttemptRefused("Closed retained-input inventory and digests required")
    selected_hashes = dict(retained_hashes)
    selected_hashes[str(packet_file)] = selected_packet_sha256
    selected_hashes[str(authorization_file)] = selected_authorization_sha256
    selected_hashes[str(marker)] = sha256_file(marker)
    action = packet["action"]
    anchor_digest_file = _canonical_regular_file(Path(action["anchorDigestPath"]))
    mandatory = {
        str(_canonical_regular_file(Path(__file__))): packet["wrapper"]["sha256"],
        str(_canonical_regular_file(Path(packet["accountingModule"]["path"]))): packet["accountingModule"]["sha256"],
        str(worker_path): packet["worker"]["sha256"],
        str(_canonical_regular_file(Path(action["actionModulePath"]))): action["actionModuleSha256"],
        str(_canonical_regular_file(Path(action["generatorPath"]))): action["generatorSha256"],
        str(_canonical_regular_file(Path(action["manifestPath"]))): action["manifestFileSha256"],
        str(_canonical_regular_file(Path(action["anchorPath"]))): action["anchorSha256"],
        str(_canonical_regular_file(Path(action["sourcePath"]))): action["sourceFileSha256"],
        str(_canonical_regular_file(Path(action["oraclePath"]))): action["oracleSha256"],
        str(_canonical_regular_file(Path(action["carrierPath"]))): action["carrierSha256"],
        str(_canonical_regular_file(Path(action["materializerPath"]))): action["materializerSha256"],
        str(anchor_digest_file): sha256_file(anchor_digest_file),
    }
    python_path = _validated_python_pin(runtime)
    mandatory[str(python_path)] = runtime["pythonExecutableSha256"]
    _java_home, java_executable = _validated_java_pin(runtime)
    mandatory[str(java_executable)] = runtime["javaExecutableSha256"]
    _spark_home, spark_submit = _validated_spark_home(runtime)
    mandatory[str(spark_submit)] = runtime["sparkSubmitSha256"]
    for jar_name, jar_path in runtime.get("runtimeJars", {}).items():
        if jar_path is None:
            raise NativeAttemptRefused("Exact local runtime JAR paths are not selected")
        mandatory[str(_canonical_regular_file(Path(jar_path)))] = runtime[
            "runtimeJarSha256"][jar_name]
    for path_text, digest in mandatory.items():
        if selected_hashes.get(path_text) != digest:
            raise NativeAttemptRefused("Retained-input packet omits or changes a required pinned artifact")
    if not runtime.get("pythonExecutable") or not runtime.get("pythonExecutableSha256"):
        raise NativeAttemptRefused("Exact selected local Python executable and digest required")
    all_retained = dict(selected_hashes)
    for path_text, digest in mandatory.items():
        prior = all_retained.setdefault(path_text, digest)
        if prior != digest:
            raise NativeAttemptRefused("Conflicting retained input digest")
    inventory_path = _canonical_regular_file(Path(runtime.get("runtimeTreeInventoryPath", "")))
    if sha256_file(inventory_path) != runtime.get("runtimeTreeInventorySha256"):
        raise NativeAttemptRefused("Retained runtime-tree inventory digest differs from packet pin")
    inventory = json.loads(inventory_path.read_text(encoding="utf-8"))
    if (type(inventory) is not dict or inventory.get("profile") != "ashlar-r05-retained-runtime-tree-inventory/0.2"
            or type(inventory.get("roots")) is not list or type(inventory.get("entries")) is not list):
        raise NativeAttemptRefused("Closed retained runtime-tree inventory required")
    live_tree = _verify_runtime_tree_snapshot(inventory, total_started=started)
    tree_file_by_path = {item["path"]: item for item in live_tree["entries"]
                         if item["type"] == "file"}
    for path_text, tree_item in tree_file_by_path.items():
        if "sha256" in tree_item:
            prior = all_retained.setdefault(path_text, tree_item["sha256"])
            if prior != tree_item["sha256"]:
                raise NativeAttemptRefused("Conflicting selected runtime-tree content hash")
    accounted_identities = {(item["device"], item["inode"]) for item in live_tree["entries"]
                            if item["type"] == "file"}
    retained_bytes = live_tree["uniqueBytes"]
    for path_text, expected in all_retained.items():
        selected = _canonical_regular_file(Path(path_text))
        tree_item = tree_file_by_path.get(str(selected))
        actual_digest = tree_item["sha256"] if tree_item else sha256_file(selected)
        if actual_digest != expected:
            raise NativeAttemptRefused("Retained runtime or action input hash mismatch")
        identity = (selected.stat().st_dev, selected.stat().st_ino)
        if identity not in accounted_identities:
            retained_bytes += selected.stat().st_size
            accounted_identities.add(identity)
    if time.monotonic() - started >= MAX_ATTEMPT_SECONDS:
        raise NativeAttemptRefused("300-second total attempt deadline exceeded before launch")
    try:
        result = _collect_child(
            argv, cwd=attempt_root, env=env, root=attempt_root,
            retained_bytes=retained_bytes, timeout_seconds=MAX_ATTEMPT_SECONDS,
            diagnostics_path=attempt_root / "diagnostics.log",
            result_path=attempt_root / "worker-result.json", total_started=started)
        result["parentObservedCleanup"] = _observe_fixture_cleanup(
            attempt_root / "tmp" / "fixture", result["receipt"].get("cleanup"))
        _validate_native_receipt(result["receipt"], packet)
        _check_deadline(started)
        for path_text, expected in all_retained.items():
            _check_deadline(started)
            selected = _canonical_regular_file(Path(path_text))
            if sha256_file(selected) != expected:
                raise NativeAttemptRefused("Retained runtime or action input changed during the attempt")
        _check_deadline(started)
        _verify_runtime_tree_snapshot(inventory, total_started=started, metadata_only=True)
        _check_deadline(started)
        result["executionAttestation"].update({
            "invocationPacketSha256": selected_packet_sha256,
            "wrapperSha256": packet["wrapper"]["sha256"],
            "workerSha256": packet["worker"]["sha256"],
            "actionAnchorSha256": packet["action"]["anchorSha256"],
            "actionModuleSha256": packet["action"]["actionModuleSha256"],
            "attemptRoot": str(attempt_root),
            "accountedRetainedBytes": retained_bytes,
        })
        receipt_path = attempt_root / "supervisor-receipt.json"
        staged_receipt_path = attempt_root / ".supervisor-receipt.pending"
        receipt_bytes = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if len(receipt_bytes) > MAX_RESULT_BYTES:
            raise NativeAttemptRefused("Supervisor receipt exceeded its 1 MiB bound")
        _check_deadline(started)
        attempt_bytes = _tree_bytes(attempt_root)
        if (attempt_bytes + len(receipt_bytes) > MAX_ATTEMPT_ROOT_BYTES
                or attempt_bytes + retained_bytes + len(receipt_bytes) > MAX_AGGREGATE_BYTES):
            raise NativeAttemptRefused("Attempt root or retained-plus-attempt bytes exceed close-time limits")
        fd = os.open(staged_receipt_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                     | getattr(os, "O_NOFOLLOW", 0), 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(receipt_bytes)
            stream.flush()
            os.fsync(stream.fileno())
        _check_deadline(started)
        if (_tree_bytes(attempt_root) > MAX_ATTEMPT_ROOT_BYTES
                or _tree_bytes(attempt_root) + retained_bytes > MAX_AGGREGATE_BYTES
                or staged_receipt_path.stat().st_size != len(receipt_bytes)):
            raise NativeAttemptRefused("Attempt root or retained-plus-attempt bytes exceed close-time accounting")
        _check_deadline(started)
        staged_receipt_path.replace(receipt_path)
        if (_tree_bytes(attempt_root) > MAX_ATTEMPT_ROOT_BYTES
                or _tree_bytes(attempt_root) + retained_bytes > MAX_AGGREGATE_BYTES
                or receipt_path.stat().st_size != len(receipt_bytes)):
            raise NativeAttemptRefused("Renamed receipt failed final byte accounting")
        _check_deadline(started)
    except BaseException as error:
        _discard_success_receipts(attempt_root)
        failure = {
            "status": "refused-or-incomplete",
            "reason": str(error)[:1024], "attemptRoot": str(attempt_root),
            "diagnosticsBytes": ((attempt_root / "diagnostics.log").stat().st_size
                                 if (attempt_root / "diagnostics.log").is_file() else 0),
            "sourceProgress": "absent", "protectedAck": "absent",
            "publication": "absent", "unityCatalog": "absent",
            "productionAuthority": "absent",
        }
        refusal_path = attempt_root / "supervisor-refusal.json"
        refusal_bytes = (json.dumps(failure, sort_keys=True, separators=(",", ":")) + "\n").encode()
        if len(refusal_bytes) > MAX_RESULT_BYTES:
            raise NativeAttemptRefused("Refusal receipt exceeded its 1 MiB bound") from error
        try:
            _check_deadline(started)
            attempt_bytes = _tree_bytes(attempt_root)
            if (attempt_bytes + len(refusal_bytes) > MAX_ATTEMPT_ROOT_BYTES
                    or attempt_bytes + retained_bytes + len(refusal_bytes) > MAX_AGGREGATE_BYTES):
                raise NativeAttemptRefused("Refusal receipt would exceed byte accounting") from error
            fd = os.open(refusal_path, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                         | getattr(os, "O_NOFOLLOW", 0), 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(refusal_bytes)
                stream.flush()
                os.fsync(stream.fileno())
            if (_tree_bytes(attempt_root) > MAX_ATTEMPT_ROOT_BYTES
                    or _tree_bytes(attempt_root) + retained_bytes > MAX_AGGREGATE_BYTES
                    or refusal_path.stat().st_size != len(refusal_bytes)):
                raise NativeAttemptRefused("Refusal receipt exceeded byte accounting") from error
        except OSError as write_error:
            raise NativeAttemptRefused("Could not write a bounded refusal receipt") from write_error
        raise
    return attempt_root, result
