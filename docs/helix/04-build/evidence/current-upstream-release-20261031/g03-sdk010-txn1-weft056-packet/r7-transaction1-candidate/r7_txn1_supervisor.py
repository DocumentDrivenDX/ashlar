#!/usr/bin/env python3
"""Effect-free supervisor for the reviewed SDK010 R7 transaction-one packet."""
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
from pathlib import Path
import selectors
import signal
import stat
import subprocess
import time
import uuid

PACKET = Path(__file__).resolve().parent
import r7_txn1_gate as gate


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


def write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, raw) == len(raw), "short bounded receipt write")
        os.fsync(fd)
    finally:
        os.close(fd)


def private_dir(path: Path) -> None:
    info = path.lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700,
            "attempt directory must be private and user-owned")


def process_slot() -> dict:
    raw = subprocess.run(["/bin/ps", "-Ao", "pid=,ppid=,pgid=,comm="], check=True,
                         capture_output=True, text=True, timeout=5).stdout
    java = []
    for line in raw.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and Path(parts[3]).name.lower() in ("java", "java_home"):
            java.append({"pid": int(parts[0]), "ppid": int(parts[1]),
                         "pgid": int(parts[2]), "command": Path(parts[3]).name})
    return {"profile": "ashlar-g03-r7-txn1-process-slot/0.1",
            "capturedUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "inventorySha256": gate.sha_bytes(raw.encode()), "javaProcesses": java,
            "empty": not java}


def prepare() -> Path:
    inspection = gate.inspect()
    selected = gate.manifest()
    attempt_parent = Path(selected["attemptParent"])
    if not attempt_parent.exists():
        attempt_parent.mkdir(mode=0o700, parents=True)
    private_dir(attempt_parent)
    require(not any(attempt_parent.glob("ashlar-g03-sdk010-r7-txn1-*")),
            "one-shot packet already has an attempt; do not create another")
    slot = process_slot()
    require(slot["empty"], "Spark process slot is occupied; no attempt prepared")
    attempt = attempt_parent / ("ashlar-g03-sdk010-r7-txn1-" + uuid.uuid4().hex)
    attempt.mkdir(mode=0o700)
    private_dir(attempt)
    inputs = attempt / "inputs"
    inputs.mkdir(mode=0o700)
    for name, digest in selected["inputs"].items():
        source = PACKET / "inputs" / name
        require(gate.sha_file(source) == digest, "frozen input changed: " + name)
        write_new(inputs / name, source.read_bytes())
    (attempt / "tmp").mkdir(mode=0o700)
    (attempt / "spark-local").mkdir(mode=0o700)
    write_new(attempt / "prepare-process-slot.json", gate.canonical(slot))
    prepared = {"profile": "ashlar-g03-sdk010-r7-txn1-prepared/0.1",
                "attemptId": attempt.name, "attemptRoot": str(attempt),
                "packetTreeSha256": inspection["packetTreeSha256"],
                "sourceCommit": selected["sourceCommit"],
                "reconciliationSha256": inspection["r7Initialization"]["receiptSha256"],
                "roleHeads": inspection["r7Initialization"]["roleHeads"],
                "limits": selected["limits"], "status": "prepared-no-native-effects"}
    write_new(attempt / "prepared.json", gate.canonical(prepared))
    return attempt


def classify_outcome(dispatch_written: bool, completion_verified: bool) -> str:
    if not dispatch_written:
        return "refused-before-dispatch"
    if completion_verified:
        return "completed-verified"
    return "uncertain-after-dispatch"


def attempt_size(root: Path) -> int:
    total = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(base) / name
            info = path.lstat()
            require(not stat.S_ISLNK(info.st_mode), "attempt symlink refused")
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def group_rss_bytes(pgid: int) -> int:
    raw = subprocess.run(["/bin/ps", "-Ao", "pgid=,rss="], check=True,
                         capture_output=True, text=True, timeout=3).stdout
    total = 0
    for line in raw.splitlines():
        fields = line.split()
        if len(fields) == 2 and int(fields[0]) == pgid:
            total += int(fields[1]) * 1024
    return total


def process_group_exists(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
        return True
    except ProcessLookupError:
        return False


def stop_and_reap(proc: subprocess.Popen) -> None:
    pgid = proc.pid
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return
    raise ValueError("worker process group closure could not be verified")


def dispatch(attempt: Path, permit_path: Path) -> None:
    selected = gate.manifest()
    inspection = gate.inspect(complete_source_admission=True)
    require(inspection["packetDispatchGatesPassed"] is True
            and inspection["nativeTransactionEffectsEnabled"] is True
            and inspection["publication"] is False and inspection["ack"] is False
            and selected.get("dispatchEnabled") is True,
            "transaction-one dispatch disabled pending independent Sol review")
    attempt = attempt.resolve(strict=True)
    private_dir(attempt)
    prepared_path = attempt / "prepared.json"
    prepared_raw = prepared_path.read_bytes()
    prepared = json.loads(prepared_raw)
    packet_sha = inspection["packetTreeSha256"]
    require(prepared.get("profile") == "ashlar-g03-sdk010-r7-txn1-prepared/0.1"
            and prepared.get("attemptRoot") == str(attempt)
            and prepared.get("packetTreeSha256") == packet_sha
            and prepared.get("reconciliationSha256") ==
                inspection["r7Initialization"]["receiptSha256"],
            "prepared attempt identity differs")
    require(not (attempt / "dispatch.json").exists()
            and not (attempt / "execution-permit-consumed.json").exists()
            and not (attempt / "transaction1-stage-result.json").exists(),
            "attempt already dispatched or has a result; no retry")
    raw = permit_path.read_bytes()
    permit = json.loads(raw)
    require(len(raw) <= 4096
            and set(permit) == {"profile", "packetTreeSha256", "preparedSha256",
                                "reconciliationSha256", "solReviewReceiptSha256", "execute"}
            and permit.get("profile") == "ashlar-g03-sdk010-r7-txn1-execution-permit/0.1"
            and permit.get("packetTreeSha256") == packet_sha
            and permit.get("preparedSha256") == gate.sha_bytes(prepared_raw)
            and permit.get("reconciliationSha256") ==
                inspection["r7Initialization"]["receiptSha256"]
            and permit.get("solReviewReceiptSha256") == selected["solReviewReceiptSha256"]
            and permit.get("execute") is True,
            "exact one-shot permit bound to Sol review, packet and R7 receipt required")
    slot = process_slot()
    require(slot["empty"], "fresh Spark process slot is occupied; refused before dispatch")
    inspection["transaction1DispatchAllowed"] = True
    require(inspection["transaction1DispatchAllowed"] is True,
            "transaction-one dispatch authorization gates did not all pass")
    write_new(attempt / "dispatch-process-slot.json", gate.canonical(slot))
    require(attempt_size(attempt) <= selected["limits"]["attemptBytes"],
            "attempt directory exceeds 512 MiB")
    write_new(attempt / "execution-permit-consumed.json", raw)
    dispatch_receipt = {"profile": "ashlar-g03-sdk010-r7-txn1-dispatch/0.1",
                        "packetTreeSha256": packet_sha,
                        "preparedSha256": gate.sha_bytes(prepared_raw),
                        "permitSha256": gate.sha_bytes(raw),
                        "reconciliationSha256": inspection["r7Initialization"]["receiptSha256"],
                        "processSlotSha256": gate.sha_file(attempt / "dispatch-process-slot.json"),
                        "attemptId": prepared["attemptId"], "noRetry": True,
                        "dispatchedAtUtc": dt.datetime.now(dt.timezone.utc).isoformat()}
    write_new(attempt / "dispatch.json", gate.canonical(dispatch_receipt))
    limits, runtime = selected["limits"], selected["runtime"]
    env = {"HOME": str(Path.home()), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
           "PATH": "/usr/bin:/bin", "JAVA_HOME": runtime["javaHome"],
           "PYSPARK_PYTHON": runtime["python"], "PYSPARK_DRIVER_PYTHON": runtime["python"],
           "PYTHONDONTWRITEBYTECODE": "1", "SPARK_LOCAL_IP": "127.0.0.1",
           "TMPDIR": str(attempt / "tmp"), "SPARK_LOCAL_DIRS": str(attempt / "spark-local")}
    command = [runtime["python"], "-I", "-B", "-u", str(PACKET / "r7_txn1_worker.py"),
               "--attempt", str(attempt), "--permit", str(attempt / "execution-permit-consumed.json")]
    deadline = time.monotonic() + limits["wallSeconds"]
    out_path, err_path = attempt / "worker.stdout.log", attempt / "worker.stderr.log"
    proc = None
    selector = selectors.DefaultSelector()
    captured = 0
    failure = None
    try:
        proc = subprocess.Popen(command, cwd=attempt, env=env, stdin=subprocess.DEVNULL,
                                stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                start_new_session=True, bufsize=0)
        with out_path.open("xb") as out, err_path.open("xb") as err:
            selector.register(proc.stdout, selectors.EVENT_READ, out)
            selector.register(proc.stderr, selectors.EVENT_READ, err)
            while selector.get_map() or proc.poll() is None:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "300-second transaction-one deadline reached")
                for key, _ in selector.select(min(0.25, remaining)):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    captured += len(chunk)
                    require(captured <= limits["combinedLogBytes"],
                            "combined worker logs exceeded 1 MiB")
                    key.data.write(chunk)
                    key.data.flush()
                require(group_rss_bytes(proc.pid) <= limits["processGroupRssBytes"],
                        "worker process group exceeded 2 GiB RSS")
                require(attempt_size(attempt) <= limits["attemptBytes"],
                        "attempt directory exceeded 512 MiB")
        status = proc.wait()
        if process_group_exists(proc.pid):
            stop_and_reap(proc)
            raise ValueError("worker process group remained after child exit")
        require(status == 0, "transaction-one worker child failed")
        result_path = attempt / "transaction1-stage-result.json"
        result_raw = result_path.read_bytes()
        result = json.loads(result_raw)
        require(result.get("profile") == "ashlar-g03-sdk010-r7-txn1-stage-result/0.1"
                and result.get("publication") is False and result.get("manifest") is False
                and result.get("ack") is False,
                "transaction-one completion receipt did not verify")
        status_receipt = {"profile": "ashlar-g03-sdk010-r7-txn1-execution-status/0.1",
                          "classification": "completed", "effectsPossible": True,
                          "resultSha256": gate.sha_bytes(result_raw),
                          "processGroupEmpty": True}
        write_new(attempt / "execution-status.json", gate.canonical(status_receipt))
    except BaseException as exc:
        failure = exc
        group_empty = proc is None
        if proc is not None:
            try:
                stop_and_reap(proc)
                group_empty = not process_group_exists(proc.pid)
            except BaseException:
                pass
        status_receipt = {"profile": "ashlar-g03-sdk010-r7-txn1-execution-status/0.1",
                          "classification": "uncertain-after-dispatch", "effectsPossible": True,
                          "errorType": type(exc).__name__, "processGroupEmpty": group_empty}
        try:
            write_new(attempt / "execution-status.json", gate.canonical(status_receipt))
        except BaseException:
            pass
    finally:
        selector.close()
        for pipe in (() if proc is None else (proc.stdout, proc.stderr)):
            if pipe is not None:
                pipe.close()
    if failure is not None:
        raise ValueError("uncertain-after-dispatch; this attempt is closed and cannot be retried") from failure


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("inspect", help="run effect-free packet and R7 admission checks")
    sub.add_parser("prepare", help="create the single fresh, effect-free attempt directory")
    run = sub.add_parser("dispatch", help="closed while this candidate awaits review")
    run.add_argument("--attempt", type=Path, required=True)
    run.add_argument("--permit", type=Path, required=True)
    args = parser.parse_args()
    if args.command == "inspect":
        print(gate.canonical(gate.inspect()).decode(), end="")
    elif args.command == "prepare":
        print(str(prepare()))
    else:
        dispatch(args.attempt, args.permit)


if __name__ == "__main__":
    main()
