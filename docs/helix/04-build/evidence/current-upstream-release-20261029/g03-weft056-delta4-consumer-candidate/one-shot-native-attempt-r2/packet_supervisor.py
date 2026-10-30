#!/usr/bin/env python3
"""Small one-shot supervisor for a fresh local G03 `.4` attempt.

`prepare` is effect-free. `execute` consumes a packet-bound permit before its
first child starts; every failure leaves its private attempt directory intact.
"""
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
import sys
import time
import uuid

PACKET = Path(__file__).resolve().parent
sys.path.insert(0, str(PACKET))
from packet_common import (canonical, manifest, packet_digest, sha_bytes,
                           sha_file, verify_packet, write_new)  # noqa: E402


def require(ok, message):
    if not ok:
        raise ValueError(message)


def private_dir(path):
    info = Path(path).lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700,
            "attempt path must be a private user-owned directory")


def copy_inputs(inputs, selected):
    inputs.mkdir(mode=0o700)
    for name, digest in selected["inputs"].items():
        source = PACKET / "inputs" / name
        require(sha_file(source) == digest, "packet input changed: " + name)
        write_new(inputs / name, source.read_bytes())


def invocation(attempt, selected):
    runtime, producer, limits = selected["runtime"], selected["umfProducer"], selected["limits"]
    suffix = attempt.name.rsplit("-", 1)[-1]
    return {
        "profile": "ashlar-configured-local/0.3", "operation": "initialize", "ordinal": 0,
        "python": runtime["python"], "installation": str(attempt / "installation"),
        "installation_id": "g03-delta4-" + suffix, "installation_sha256": None,
        "output": str(attempt / "initialize-output"), "jars": selected["runtimePaths"]["jars"],
        "jar_inventory": runtime["jars"], "java_home": runtime["javaHome"],
        "source_configuration": str(attempt / "inputs/configured-typed-graph-source.json"),
        "producer": {"source": producer["source"], "bun": producer["bun"],
                     "git": producer["git"], "timeout_seconds": producer["timeoutSeconds"],
                     "maximum_output_bytes": producer["maximumOutputBytes"],
                     "maximum_receipt_bytes": producer["maximumReceiptBytes"]},
        "stream": "g03-typed-graph-1-" + suffix,
        "materialization_clock_us": str(int(time.time() * 1_000_000)),
        "operation_capacity": None, "carrier_profile": "ashlar-delta/0.4-xxhash64",
        "bounds": {"whole_seconds": limits["wallSeconds"],
                   "stream_bytes": limits["combinedStdoutStderrBytes"],
                   "receipt_bytes": limits["resultBytes"],
                   "maximum_admission_receipt_bytes": limits["admissionReceiptBytes"]},
    }


def prepare(args):
    selected = manifest()
    receipt = verify_packet(runtime=True)
    digest = receipt["packetTreeSha256"]
    require(digest == args.packet_sha256, "reviewed packet digest differs")
    parent = Path(args.attempt_parent)
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    private_dir(parent)
    attempt = parent / ("ashlar-g03-txn1-" + uuid.uuid4().hex)
    attempt.mkdir(mode=0o700)
    private_dir(attempt)
    copy_inputs(attempt / "inputs", selected)
    (attempt / "tmp").mkdir(mode=0o700)
    (attempt / "spark-local").mkdir(mode=0o700)
    raw_invocation = canonical(invocation(attempt, selected))
    require(len(raw_invocation) <= selected["limits"]["invocationBytes"],
            "runtime invocation exceeds bound")
    write_new(attempt / "initialize-invocation.json", raw_invocation)
    prepared = {"profile": "ashlar-g03-delta4-txn1-prepared/0.2",
                "attemptId": attempt.name, "attemptRoot": str(attempt),
                "packetTreeSha256": digest, "sourceCommit": selected["sourceCommit"],
                "currentCommit": receipt["currentCommit"],
                "sourceOracleSha256": selected["inputs"]["source-oracle.json"],
                "initializeInvocationSha256": sha_bytes(raw_invocation),
                "limits": selected["limits"], "status": "prepared-no-effects"}
    write_new(attempt / "prepared.json", canonical(prepared))
    print(str(attempt))


def attempt_size(root):
    total = 0
    for base, dirs, files in os.walk(root, followlinks=False):
        for name in dirs + files:
            path = Path(base) / name
            info = path.lstat()
            require(not stat.S_ISLNK(info.st_mode), "attempt symlink refused")
            if stat.S_ISREG(info.st_mode):
                total += info.st_size
    return total


def descendants(pid):
    result = subprocess.run(["/bin/ps", "-axo", "pid=,ppid=,pgid=,rss="],
                            capture_output=True, text=True, timeout=3, check=True)
    parents = {}
    rss = {}
    groups = {}
    for line in result.stdout.splitlines():
        parts = line.split()
        if len(parts) == 4:
            child, parent, group, kb = map(int, parts)
            parents[child], rss[child] = parent, kb * 1024
            groups[child] = group
    selected = {pid}
    while True:
        new = {child for child, parent in parents.items() if parent in selected}
        if new <= selected:
            break
        selected |= new
    # Spark's JVM is in the worker's process group even if it reparents.
    group_members = {child for child, group in groups.items() if group == pid}
    selected |= group_members
    return selected, sum(rss.get(item, 0) for item in selected)


def _process_group_exists(pgid):
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    return True


def _stop_and_reap_group(proc):
    """Stop every worker/JVM process and reap the child before another stage."""
    pgid = proc.pid
    if _process_group_exists(pgid):
        try:
            os.killpg(pgid, signal.SIGTERM)
        except ProcessLookupError:
            pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        pass
    if _process_group_exists(pgid):
        try:
            os.killpg(pgid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        os.killpg(pgid, signal.SIGKILL)
        proc.wait()
    deadline = time.monotonic() + 5
    while _process_group_exists(pgid) and time.monotonic() < deadline:
        time.sleep(0.05)
    require(not _process_group_exists(pgid),
            "worker process group could not be stopped; no second Spark session")


def run_child(attempt, stage, prepared_sha, packet_sha, deadline, selected, env):
    python = selected["runtime"]["python"]
    worker = PACKET / "packet_worker.py"
    command = [python, "-I", "-B", "-u", str(worker), "--attempt", str(attempt),
               "--stage", stage, "--packet-sha256", packet_sha,
               "--prepared-sha256", prepared_sha]
    out_path, err_path = attempt / (stage + ".stdout.log"), attempt / (stage + ".stderr.log")
    proc = subprocess.Popen(command, cwd=attempt, env=env, stdin=subprocess.DEVNULL,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                            start_new_session=True, bufsize=0)
    selector = None
    try:
        cap = selected["limits"]["combinedStdoutStderrBytes"]
        captured = sum(path.stat().st_size for path in attempt.glob("*.stdout.log"))
        captured += sum(path.stat().st_size for path in attempt.glob("*.stderr.log"))
        selector = selectors.DefaultSelector()
        with out_path.open("xb") as out, err_path.open("xb") as err:
            selector.register(proc.stdout, selectors.EVENT_READ, out)
            selector.register(proc.stderr, selectors.EVENT_READ, err)
            while selector.get_map() or proc.poll() is None:
                remaining = deadline - time.monotonic()
                require(remaining > 0, "450-second attempt deadline reached")
                for key, _ in selector.select(min(0.25, remaining)):
                    chunk = os.read(key.fileobj.fileno(), 65536)
                    if not chunk:
                        selector.unregister(key.fileobj)
                        continue
                    captured += len(chunk)
                    require(captured <= cap, "combined child output exceeded 1 MiB")
                    key.data.write(chunk)
                    key.data.flush()
                _, rss_bytes = descendants(proc.pid)
                require(rss_bytes <= selected["limits"]["processTreeRssBytes"],
                        "child process tree exceeded 2 GiB RSS")
                require(attempt_size(attempt) <= selected["limits"]["attemptBytes"],
                        "attempt directory exceeded 512 MiB")
                if proc.poll() is not None and not selector.get_map():
                    break
        status = proc.wait()
        require(status == 0, stage + " child failed; evidence retained")
        require(not _process_group_exists(proc.pid),
                "worker process group remained after stage; no second Spark session")
    except BaseException:
        _stop_and_reap_group(proc)
        raise
    finally:
        if selector is not None:
            selector.close()
        for pipe in (proc.stdout, proc.stderr):
            if pipe is not None:
                pipe.close()
    require(attempt_size(attempt) <= selected["limits"]["attemptBytes"],
            "attempt directory exceeded 512 MiB")


def execute(args):
    selected = manifest()
    deadline = time.monotonic() + selected["limits"]["wallSeconds"]
    packet_sha = packet_digest()
    require(packet_sha == args.packet_sha256, "packet changed since Sol review")
    receipt = verify_packet(runtime=True)
    require(receipt["packetTreeSha256"] == packet_sha, "source/runtime preflight changed")
    prepared_arg = Path(args.prepared)
    require(not prepared_arg.is_symlink(), "prepared path symlink refused")
    prepared_arg = prepared_arg.resolve(strict=True)
    attempt = prepared_arg.parent
    private_dir(attempt)
    prepared_path = attempt / "prepared.json"
    require(prepared_arg == prepared_path, "prepared path is not attempt-owned")
    prepared_raw = prepared_path.read_bytes()
    prepared = json.loads(prepared_raw)
    prepared_sha = sha_bytes(prepared_raw)
    require(prepared.get("profile") == "ashlar-g03-delta4-txn1-prepared/0.2"
            and prepared_sha == args.prepared_sha256
            and prepared.get("packetTreeSha256") == packet_sha
            and prepared.get("sourceCommit") == selected["sourceCommit"]
            and prepared.get("currentCommit") == receipt["currentCommit"]
            and prepared.get("attemptRoot") == str(attempt),
            "prepared attempt identity differs")
    permit_path = Path(args.permit)
    require(not permit_path.is_symlink(), "execution permit symlink refused")
    permit_raw = permit_path.read_bytes()
    permit = json.loads(permit_raw)
    require(set(permit) == {"profile", "packetTreeSha256", "sourceCommit", "currentCommit",
                            "preparedSha256", "execute"}
            and permit["profile"] == "ashlar-g03-delta4-txn1-execution-permit/0.2"
            and permit["packetTreeSha256"] == packet_sha
            and permit["sourceCommit"] == selected["sourceCommit"]
            and permit.get("currentCommit") == prepared.get("currentCommit")
            and permit["preparedSha256"] == prepared_sha
            and permit["execute"] is True,
            "explicit permit for this packet and fresh attempt required")
    require(len(permit_raw) <= 4096, "execution permit exceeds bound")
    require(not (attempt / "dispatch.json").exists(), "attempt already dispatched; no retry")
    require(not (attempt / "initialize-stage-result.json").exists()
            and not (attempt / "transaction1-stage-result.json").exists(),
            "attempt already has stage output; no retry")
    require(sha_file(attempt / "initialize-invocation.json")
            == prepared["initializeInvocationSha256"], "initialization invocation changed")
    verify_packet(attempt / "inputs", runtime=True)
    require(time.monotonic() < deadline, "450-second attempt deadline reached before dispatch")
    require(attempt_size(attempt) <= selected["limits"]["attemptBytes"],
            "attempt directory exceeded 512 MiB before dispatch")
    write_new(attempt / "execution-permit-consumed.json", permit_raw)
    dispatch = {"profile": "ashlar-g03-txn1-dispatch/0.2", "packetTreeSha256": packet_sha,
                "preparedSha256": prepared_sha, "permitSha256": sha_bytes(permit_raw),
                "attemptId": prepared["attemptId"], "dispatchedAtUtc":
                dt.datetime.now(dt.timezone.utc).isoformat(), "noRetry": True}
    write_new(attempt / "dispatch.json", canonical(dispatch))
    env = {"HOME": str(Path.home()), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
           "PATH": "/usr/bin:/bin", "JAVA_HOME": selected["runtime"]["javaHome"],
           "PYSPARK_PYTHON": selected["runtime"]["python"],
           "PYSPARK_DRIVER_PYTHON": selected["runtime"]["python"],
           "PYTHONDONTWRITEBYTECODE": "1", "SPARK_LOCAL_IP": "127.0.0.1",
           "TMPDIR": str(attempt / "tmp"), "SPARK_LOCAL_DIRS": str(attempt / "spark-local")}
    run_child(attempt, "initialize", prepared_sha, packet_sha, deadline, selected, env)
    # The first worker must fully exit before a second local Spark session opens.
    run_child(attempt, "transaction1", prepared_sha, packet_sha, deadline, selected, env)
    summary = {"profile": "ashlar-g03-txn1-supervisor-result/0.2",
               "packetTreeSha256": packet_sha, "preparedSha256": prepared_sha,
               "dispatchSha256": sha_file(attempt / "dispatch.json"),
               "initializeStageSha256": sha_file(attempt / "initialize-stage-result.json"),
               "transaction1StageSha256": sha_file(attempt / "transaction1-stage-result.json"),
               "result": "effects-validated", "publication": False, "manifest": False,
               "ack": False, "consumer": False}
    raw = canonical(summary)
    require(len(raw) <= selected["limits"]["resultBytes"], "result receipt exceeds bound")
    write_new(attempt / "supervisor-result.json", raw)
    print(str(attempt / "supervisor-result.json"))


def main():
    parser = argparse.ArgumentParser()
    sub = parser.add_subparsers(dest="command", required=True)
    p = sub.add_parser("prepare")
    p.add_argument("--attempt-parent", required=True)
    p.add_argument("--packet-sha256", required=True)
    e = sub.add_parser("execute")
    e.add_argument("--prepared", required=True)
    e.add_argument("--prepared-sha256", required=True)
    e.add_argument("--permit", required=True)
    e.add_argument("--packet-sha256", required=True)
    args = parser.parse_args()
    if args.command == "prepare":
        prepare(args)
    else:
        execute(args)


if __name__ == "__main__":
    main()
