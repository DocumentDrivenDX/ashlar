#!/usr/bin/env python3
"""One-shot bounded supervisor for the retained R3 manifest-only worker."""
from __future__ import annotations

import argparse
import datetime
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import signal
import shutil
import stat
import subprocess
import sys
import tempfile
import time
import uuid

PACKET = Path(__file__).resolve().parent
_WORKER_SPEC = importlib.util.spec_from_file_location(
    "_ashlar_r3_continuation_worker", PACKET / "continuation_worker.py")
if _WORKER_SPEC is None or _WORKER_SPEC.loader is None:
    raise RuntimeError("Bound continuation worker module is unavailable")
_WORKER_MODULE = importlib.util.module_from_spec(_WORKER_SPEC)
_WORKER_SPEC.loader.exec_module(_WORKER_MODULE)
REPO = _WORKER_MODULE.REPO
read_invocation = _WORKER_MODULE.read_invocation
canonical = _WORKER_MODULE.canonical


PYTHON = Path("/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/"
              "venv/bin/python3.12")
WORKER = PACKET / "continuation_worker.py"
PREFLIGHT = PACKET / "inspect_r3_continuation.py"
INVOCATION = PACKET / "invocation.json"
MAX_LOG_BYTES = 1_048_576
POLL_SECONDS = 1.0


def require(value: bool, message: str) -> None:
    if not value:
        raise RuntimeError(message)


def private_directory(path: Path) -> bool:
    try:
        info = path.lstat()
    except OSError:
        return False
    return (stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700)


def atomic_receipt(path: Path, value: dict) -> str:
    raw = canonical(value) + b"\n"
    require(len(raw) <= 1_048_576, "Bounded supervisor receipt required")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, raw) == len(raw), "Short supervisor receipt write")
    finally:
        os.close(fd)
    return hashlib.sha256(raw).hexdigest()


def process_table() -> list[tuple[int, int, int, str]]:
    result = subprocess.run(["/bin/ps", "-axo", "pid=,pgid=,rss=,command="],
                            check=True, capture_output=True, text=True, timeout=5)
    return parse_process_table(result.stdout)


def parse_process_table(raw: str) -> list[tuple[int, int, int, str]]:
    rows = []
    for line in raw.splitlines():
        parts = line.strip().split(None, 3)
        if len(parts) != 4:
            continue
        try:
            rows.append((int(parts[0]), int(parts[1]), int(parts[2]) * 1024, parts[3]))
        except ValueError:
            raise RuntimeError("Could not parse process RSS inventory") from None
    return rows


def group_rss(pgid: int) -> tuple[int, int]:
    selected = [row for row in process_table() if row[1] == pgid]
    return sum(row[2] for row in selected), len(selected)


def occupied_java_slot() -> bool:
    for _pid, _pgid, _rss, command in process_table():
        lowered = command.lower()
        if "/java" in lowered or "org.apache.spark.deploy" in lowered:
            return True
    return False


def write_status(attempt: Path, *, classification: str, process_empty: bool,
                 effects_possible: bool, detail: str | None = None,
                 elapsed_ms: int | None = None, result_sha: str | None = None) -> None:
    status = {
        "profile": "ashlar-g03-r3-manifest-continuation-status/0.1",
        "classification": classification,
        "attemptRoot": str(attempt),
        "processGroupEmpty": process_empty,
        "effectsPossible": effects_possible,
        "noRetry": True,
        "protectedAck": False,
        "cloud": False,
        "resultSha256": result_sha,
    }
    if detail is not None:
        status["failureClass"] = detail
    if elapsed_ms is not None:
        status["elapsedMs"] = elapsed_ms
    atomic_receipt(attempt / "execution-status.json", status)


def terminate_group(process: subprocess.Popen, pgid: int) -> bool:
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pass
        try:
            _rss, count = group_rss(pgid)
        except BaseException:
            count = 1
        if count == 0:
            return True
        time.sleep(0.2)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        if process.poll() is not None:
            pass
        try:
            _rss, count = group_rss(pgid)
        except BaseException:
            count = 1
        if count == 0:
            return True
        time.sleep(0.2)
    return False


def _run_preflight(invocation: dict) -> dict:
    environment = {
        "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "HOME": "/nonexistent", "PYTHONDONTWRITEBYTECODE": "1",
        "TMPDIR": tempfile.gettempdir(),
    }
    completed = subprocess.run([str(PYTHON), "-I", "-B", str(PREFLIGHT)],
                               cwd=REPO, env=environment, capture_output=True,
                               timeout=180, check=True)
    require(len(completed.stdout) <= 65536 and len(completed.stderr) == 0,
            "Effect-free preflight must produce one bounded clean receipt")
    report = json.loads(completed.stdout)
    require(report.get("reachedSparkBoundary") is True
            and report.get("sparkStarted") is False
            and report.get("nativeDispatch") is False
            and report.get("retainedInstallationBytesUnchanged") is True,
            "Exact effect-free Spark-boundary preflight required")
    invocation_sha = hashlib.sha256(INVOCATION.read_bytes()).hexdigest()
    private_tmp = Path(tempfile.gettempdir()).resolve(strict=True)
    require(private_directory(private_tmp), "Private disposable preflight root required")
    disposable = Path(tempfile.mkdtemp(prefix="ashlar-r4-worker-preflight-",
                                       dir=private_tmp))
    require(private_directory(disposable), "Private disposable worker attempt required")
    try:
        invocation_copy = disposable / "invocation.json"
        fd = os.open(invocation_copy, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                     | os.O_NOFOLLOW, 0o600)
        try:
            raw = INVOCATION.read_bytes()
            require(os.write(fd, raw) == len(raw),
                    "Exact invocation could not be copied for worker preflight")
        finally:
            os.close(fd)
        worker_check = subprocess.run(
            [str(PYTHON), "-I", "-B", str(WORKER), "--invocation", str(invocation_copy),
             "--attempt", str(disposable), "--invocation-sha256", invocation_sha,
             "--preflight"], cwd=REPO, env=environment, capture_output=True,
            timeout=90, check=True)
        require(len(worker_check.stdout) <= 65536 and worker_check.stderr == b"",
                "Actual worker preflight must produce one bounded clean receipt")
        worker_report = json.loads(worker_check.stdout)
        require(worker_report.get("profile") == "ashlar-g03-r5-worker-preflight/0.1"
                and worker_report.get("nativeCall") is False
                and worker_report.get("outputCreated") is False
                and worker_report.get("receiptProjectionExact") is True
                and worker_report.get("deltaHeads") is not None
                and worker_report.get("invocationSha256") == invocation_sha,
                "Actual worker complete pre-call preflight required")
    finally:
        shutil.rmtree(disposable)
    require(not disposable.exists(), "Disposable preflight directory cleanup failed")
    return {"profile": "ashlar-g03-r5-complete-effect-free-preflight/0.1",
            "invocationSha256": invocation_sha,
            "retainedRuntimePreSparkCheck": report,
            "actualWorkerPreCallCheck": worker_report,
            "dispatchStarted": False,
            "plannedAttemptRemainsAbsent":
                not Path(invocation["bounds"]["attemptParent"]).exists()}


def _prepare_attempt(attempt: Path, invocation_raw: bytes, invocation_sha: str) -> None:
    parent = attempt.parent
    if not parent.exists():
        parent.mkdir(mode=0o700)
    require(private_directory(parent), "Private one-shot attempt parent required")
    require(not attempt.exists() and not attempt.is_symlink(),
            "Continuation namespace is one-shot and must be absent")
    attempt.mkdir(mode=0o700)
    require(private_directory(attempt), "Private fresh attempt directory required")
    (attempt / "tmp").mkdir(mode=0o700)
    require(private_directory(attempt / "tmp"), "Private worker temp directory required")
    fd = os.open(attempt / "invocation.json",
                 os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, invocation_raw) == len(invocation_raw),
                "Bound invocation copy could not be retained")
    finally:
        os.close(fd)
    prepared = {
        "profile": "ashlar-g03-r3-manifest-continuation-prepared/0.1",
        "invocationSha256": invocation_sha,
        "sourceCommit": "0693f1a42a72930e33976c212de76c37b2fd1d78",
        "r3CompletionSha256": "68a4b40cb6ff45ffe6da92c31e2fba419a93728262f7ee14e277df1547abb157",
        "initialGraphOps": 3,
        "initialMetadataOps": 2,
        "initialOutputReceiptSha256": "eef82376bff8eafc281442aa8f7da2e424d0439ab942b5ccb0c5464c07f37b2c",
        "bounds": {"wallSeconds": 900, "groupRssBytes": 2 * 1024**3},
    }
    atomic_receipt(attempt / "prepared.json", prepared)


def supervise() -> dict:
    invocation, digest = read_invocation(INVOCATION)
    limits = invocation["bounds"]
    attempt = Path(limits["attemptParent"])
    require(Path(limits["freshOutput"]) == attempt / "output"
            and limits["attemptParentMustBeAbsentAtDispatch"] is True,
            "Exact fresh attempt/output binding required")
    invocation_raw = INVOCATION.read_bytes()

    # All admission and byte binding runs before consuming the one-shot namespace.
    _run_preflight(invocation)
    require(not occupied_java_slot(), "An existing Java process occupies the local slot")
    _prepare_attempt(attempt, invocation_raw, digest)

    worker_out = attempt / "worker.stdout.log"
    worker_err = attempt / "worker.stderr.log"
    out_fd = os.open(worker_out, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    err_fd = os.open(worker_err, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    worker = None
    pgid = None
    started = time.monotonic()
    dispatch = {
        "profile": "ashlar-g03-r3-manifest-continuation-dispatch/0.1",
        "attemptRoot": str(attempt),
        "dispatchedAtUtc": datetime.datetime.now(datetime.timezone.utc).isoformat(),
        "invocationSha256": digest,
        "effectsPossible": True,
        "noRetry": True,
        "phase": "manifest-only-continuation",
    }
    try:
        private_tmp = Path(tempfile.gettempdir()).resolve(strict=True)
        info = private_tmp.lstat()
        require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
                and not info.st_mode & 0o022,
                "Owned private process TMPDIR required")
        environment = {
            "PATH": "/usr/bin:/bin", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
            "HOME": "/nonexistent", "JAVA_HOME": invocation["runtime"]["javaHome"],
            "PYSPARK_PYTHON": str(PYTHON), "PYSPARK_DRIVER_PYTHON": str(PYTHON),
            "PYTHONDONTWRITEBYTECODE": "1", "SPARK_LOCAL_IP": "127.0.0.1",
            "TMPDIR": str(attempt / "tmp"),
        }
        # Reject ambient Spark remoting, connect and gateway configuration.
        for name in ("SPARK_REMOTE", "MASTER", "SPARK_CONNECT_MODE_ENABLED",
                     "PYSPARK_SUBMIT_ARGS", "PYSPARK_GATEWAY_PORT",
                     "PYSPARK_GATEWAY_SECRET"):
            require(name not in environment, "Remote Spark environment refused")
        def child_limits():
            import resource
            resource.setrlimit(resource.RLIMIT_CPU, (900, 900))
        atomic_receipt(attempt / "dispatch.json", dispatch)
        worker = subprocess.Popen(
            [str(PYTHON), "-I", "-B", str(WORKER), "--invocation",
             str(attempt / "invocation.json"), "--attempt", str(attempt),
             "--invocation-sha256", digest],
            cwd=REPO, env=environment, stdin=subprocess.DEVNULL,
            stdout=out_fd, stderr=err_fd, start_new_session=True,
            preexec_fn=child_limits)
        pgid = worker.pid
        deadline = started + limits["wallSeconds"]
        failure = None
        max_rss = 0
        while True:
            rss, count = group_rss(pgid)
            max_rss = max(max_rss, rss)
            if worker_out.stat().st_size > MAX_LOG_BYTES or worker_err.stat().st_size > MAX_LOG_BYTES:
                failure = "worker-output-limit"
                break
            if rss > limits["rssBytes"]:
                failure = "process-group-rss-limit"
                break
            if time.monotonic() >= deadline:
                failure = "wall-time-limit"
                break
            code = worker.poll()
            if code is not None and count == 0:
                if code != 0:
                    failure = "worker-exit-nonzero"
                break
            time.sleep(POLL_SECONDS)
        process_empty = True
        if failure is not None:
            process_empty = terminate_group(worker, pgid)
        elif worker is not None:
            worker.wait(timeout=5)
            _rss, count = group_rss(pgid)
            if count != 0:
                process_empty = terminate_group(worker, pgid)
                failure = "worker-left-process-group-member"

        elapsed_ms = int((time.monotonic() - started) * 1000)
        if failure is not None or not process_empty:
            write_status(attempt, classification="uncertain-after-dispatch",
                         process_empty=process_empty, effects_possible=True,
                         detail=failure or "process-group-not-empty", elapsed_ms=elapsed_ms)
            raise RuntimeError("Continuation failed after dispatch; preserve attempt and do not retry")

        result_file = attempt / "continuation-result.json"
        postcheck_file = attempt / "postcheck.json"
        require(result_file.is_file() and postcheck_file.is_file(),
                "Bound worker result and postcheck required")
        result_raw = result_file.read_bytes()
        postcheck = json.loads(postcheck_file.read_bytes())
        require(hashlib.sha256(result_raw).hexdigest() ==
                postcheck["continuationResultSha256"]
                and postcheck.get("graphOperationsReplayed") == 0
                and postcheck.get("metadataOperationsAdded") == 0
                and postcheck.get("ack") is False,
                "Exact manifest-only postcheck required")
        result_digest = hashlib.sha256(result_raw).hexdigest()
        completion = {
            "profile": "ashlar-g03-r3-manifest-continuation-completion/0.1",
            "invocationSha256": digest,
            "continuationResultSha256": result_digest,
            "postcheckSha256": hashlib.sha256(postcheck_file.read_bytes()).hexdigest(),
            "elapsedMs": elapsed_ms,
            "maximumObservedProcessGroupRssBytes": max_rss,
            "processGroupEmpty": True,
            "manifestOnly": True,
            "graphOperationsReplayed": 0,
            "ack": False,
            "cloud": False,
        }
        atomic_receipt(attempt / "completion.json", completion)
        write_status(attempt, classification="completed-verified", process_empty=True,
                     effects_possible=True, elapsed_ms=elapsed_ms, result_sha=result_digest)
        return completion
    except BaseException as error:
        if worker is not None and pgid is not None:
            empty = terminate_group(worker, pgid)
            status_path = attempt / "execution-status.json"
            if not status_path.exists():
                write_status(attempt, classification="uncertain-after-dispatch",
                             process_empty=empty, effects_possible=True,
                             detail=type(error).__name__,
                             elapsed_ms=int((time.monotonic() - started) * 1000))
        elif not (attempt / "execution-status.json").exists():
            write_status(attempt, classification="refused-before-worker",
                         process_empty=True, effects_possible=False,
                         detail=type(error).__name__)
        raise
    finally:
        os.close(out_fd)
        os.close(err_fd)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--check", action="store_true",
                        help="validate exact invocation and process-limit parser only")
    parser.add_argument("--preflight", action="store_true",
                        help="run the effect-free real pre-Spark checker only")
    args = parser.parse_args()
    invocation, digest = read_invocation(INVOCATION)
    if args.check:
        print(json.dumps({"profile": "ashlar-g03-r3-supervisor-check/0.1",
                          "invocationSha256": digest,
                          "wallSeconds": invocation["bounds"]["wallSeconds"],
                          "rssBytes": invocation["bounds"]["rssBytes"],
                          "dispatchStarted": False}, sort_keys=True, separators=(",", ":")))
        return
    if args.preflight:
        print(json.dumps(_run_preflight(invocation), sort_keys=True,
                         separators=(",", ":")))
        return
    result = supervise()
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))


if __name__ == "__main__":
    main()
