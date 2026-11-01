"""Bounded prepare / process-slot / one-shot dispatch for SDK010 initialization."""
from __future__ import annotations
import datetime as dt
import hashlib
import argparse
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
import uuid

PACKET = Path(__file__).resolve().parent
PARENT = PACKET.parent
REPO = PACKET.parents[6]
MANIFEST_PATH = PACKET / "packet-manifest.json"


def require(ok, message):
    if not ok:
        raise ValueError(message)


def canonical(value):
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False,
                      allow_nan=False) + "\n").encode()


def sha_bytes(raw): return hashlib.sha256(raw).hexdigest()
def sha_file(path): return sha_bytes(Path(path).read_bytes())


def manifest():
    value = json.loads(MANIFEST_PATH.read_bytes())
    require(value.get("profile") == "ashlar-g03-sdk010-fresh-delta4-initialize-r7/0.1",
            "R7 SDK010 initialization manifest required")
    return value


def packet_digest():
    entries = []
    for path in sorted(PACKET.rglob("*")):
        if not path.is_file() or path.name == "packet-manifest.json" or "__pycache__" in path.parts:
            continue
        rel = path.relative_to(PACKET).as_posix()
        entries.append({"path": rel, "bytes": path.stat().st_size, "sha256": sha_file(path)})
    # The timing helper is shared with the transaction candidate and is part of this packet.
    helper = PARENT / "r4_timing.py"
    entries.append({"path": "../r4_timing.py", "bytes": helper.stat().st_size,
                    "sha256": sha_file(helper)})
    return sha_bytes(canonical(entries))


def sdk_config(selected):
    from ashlar_host.typed_graph_sdk010_admission import TypedGraphSdk010AdmissionConfig
    pin = selected["sdk010"]
    return TypedGraphSdk010AdmissionConfig(Path(pin["node"]), Path(pin["sdkPackageRoot"]),
                                           Path(pin["sdkArchive"]))


def verify_sparse_umf_source(selected):
    """Require a clean, exact-commit sparse view of the admitted UMF producer."""
    pin = selected["producer"]
    source = Path(pin["source"])
    git = pin["git"]
    environment = {"LANG": "C.UTF-8", "LC_ALL": "C.UTF-8", "HOME": "/nonexistent",
        "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
        "GIT_TERMINAL_PROMPT": "0", "GIT_OPTIONAL_LOCKS": "0"}
    def git_read(args):
        return subprocess.run([git, "-C", str(source), *args], check=True,
            capture_output=True, text=True, timeout=10, env=environment).stdout.strip()
    commit = git_read(["rev-parse", "HEAD"])
    tree = git_read(["rev-parse", "HEAD^{tree}"])
    require(commit == pin["sourceCommit"] and tree == pin["sourceTree"],
            "sparse UMF source commit/tree differs")
    sparse = git_read(["sparse-checkout", "list"]).splitlines()
    require(sparse == pin["sparseCheckout"], "sparse UMF path set differs")
    sparse_index = git_read(["config", "--get", "index.sparse"])
    require(sparse_index == "true", "sparse index is required for bounded custody")
    dependency_link = source / "node_modules"
    require(dependency_link.is_symlink()
            and str(dependency_link.resolve(strict=True)) == pin["nodeModulesTarget"],
            "pinned UMF dependency link differs")
    status = subprocess.run([git, "-c", "core.fsmonitor=false", "-c", "core.hooksPath=/dev/null",
                             "-C", str(source), "status", "--porcelain",
                             "--untracked-files=normal"], check=True, capture_output=True,
                            text=True, timeout=10, env=environment).stdout
    require(not status, "sparse UMF producer working tree is not clean")
    return {"commit": commit, "tree": tree, "sparseCheckout": sparse,
            "sparseIndex": True, "clean": True,
            "nodeModulesTarget": str(dependency_link.resolve(strict=True))}


def verify_admitted_source_tree(selected):
    git = selected["producer"]["git"]
    current = subprocess.run([git, "-C", str(REPO), "rev-parse", "HEAD"], check=True, capture_output=True,
                             text=True, timeout=10).stdout.strip()
    subprocess.run([git, "-C", str(REPO),
                    "merge-base", "--is-ancestor", selected["sourceCommit"], current],
                   check=True, timeout=10)
    tree_object = subprocess.run([git, "-C", str(REPO), "rev-parse",
                                  selected["sourceCommit"] + ":src"], check=True,
                                 capture_output=True, text=True, timeout=10).stdout.strip()
    require(tree_object == selected["sourceTreeObject"], "admitted src tree object differs")
    require(subprocess.run([git, "-C", str(REPO), "diff", "--quiet",
                            selected["sourceCommit"], "HEAD", "--", "src"],
                           timeout=10).returncode == 0, "working src differs from admitted tree")
    src_status = subprocess.run([git, "-C", str(REPO), "status", "--porcelain",
                                 "--untracked-files=all", "--", "src"], check=True,
                                capture_output=True, text=True, timeout=10).stdout
    require(not src_status.strip(), "src has staged, unstaged, or untracked changes")
    return current, tree_object


def verify_effect_free():
    selected = manifest()
    # Verify committed and working source identity before importing any repository code.
    current, tree_object = verify_admitted_source_tree(selected)
    producer_custody = verify_sparse_umf_source(selected)
    sys.path.insert(0, str(REPO / "src"))
    from ashlar.typed_graph_source import build_typed_graph_request
    from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
    from ashlar_host.typed_graph_sdk010_admission import (
        admit_typed_graph_sdk010, SDK010_PRODUCER_REVISION)
    from ashlar_host.local_current_delta4_umf_sdk010 import pinned_sdk010_resources
    from ashlar_host.schema_rows import LOCAL_CURRENT_ROLE_TO_UMF

    inputs = PACKET / "inputs"
    for name, digest in selected["inputs"].items():
        require(sha_file(inputs / name) == digest, "frozen input digest differs: " + name)
    config_path = inputs / "configured-typed-graph-source.json"
    config = load_typed_graph_jsonl_configuration(config_path)
    model = config.model_source
    request = build_typed_graph_request(config)
    receipt = admit_typed_graph_sdk010(sdk_config(selected), config, model, request)
    require(sha_file(selected["sdk010"]["sdkArchive"]) == selected["sdk010"]["archiveSha256"]
            and sha_file(Path(selected["sdk010"]["sdkPackageRoot"]) / "dist/umf.js")
            == selected["sdk010"]["publicEntrySha256"], "SDK010 archive or public entry changed")
    bridge_manifest, _bridge, _sources, _semantics = pinned_sdk010_resources()
    require(bridge_manifest["profile"] ==
            "ashlar-local-current-delta4-umf-source-sdk010/0.1"
            and set(bridge_manifest["roles"]) == set(LOCAL_CURRENT_ROLE_TO_UMF.values()), "SDK010 six-role bridge mapping differs")

    runtime = selected["runtime"]
    python = Path(runtime["python"])
    require(python.is_file() and os.access(python, os.X_OK)
            and sha_file(python.resolve()) == runtime["pythonSha256"],
            "pinned Python executable differs")
    for item in runtime["jars"].values():
        require(sha_file(item["path"]) == item["sha256"], "pinned Delta jar differs")
    java = Path(runtime["javaHome"]) / "bin/java"
    require(java.is_file() and sha_file(java.resolve()) == runtime["javaExecutableSha256"],
            "pinned Java executable differs")
    runtime_probe = "import json,pyspark; from importlib.metadata import version; import py_mini_racer; print(json.dumps({'python':__import__('sys').version.split()[0],'pyspark':pyspark.__version__,'deltaSpark':version('delta-spark'),'miniRacer':version('mini-racer')}))"
    versions = json.loads(subprocess.run([str(python), "-c", runtime_probe], check=True,
                                         capture_output=True, text=True, timeout=15).stdout)
    require(versions == {"python": runtime["pythonVersion"], "pyspark": runtime["pyspark"],
                         "deltaSpark": runtime["deltaSpark"], "miniRacer": runtime["miniRacer"]},
            "pinned runtime versions differ")
    return {"sourceCommit": selected["sourceCommit"], "sourceTreeObject": tree_object,
            "currentCommit": current,
            "configurationSha256": sha_file(config_path),
            "modelSha256": sha_bytes(model), "requestSha256": sha_bytes(request),
            "sourceAdmissionReceiptSha256": sha_bytes(receipt),
            "producerRevision": SDK010_PRODUCER_REVISION,
            "producerCustody": producer_custody,
            "bridgeProfile": bridge_manifest["profile"],
            "runtimeVersions": versions, "sparkStarted": False,
            "packetTreeSha256": packet_digest()}


def capture_process_slot():
    # Full inventory digest supports review; only a Java process occupies Spark's slot.
    raw = subprocess.run(["/bin/ps", "-Ao", "pid=,ppid=,pgid=,comm="], check=True,
                         capture_output=True, text=True, timeout=5).stdout
    java = []
    for line in raw.splitlines():
        parts = line.split(None, 3)
        if len(parts) == 4 and Path(parts[3]).name.lower() in ("java", "java_home"):
            java.append({"pid": int(parts[0]), "ppid": int(parts[1]),
                         "pgid": int(parts[2]), "command": Path(parts[3]).name})
    return {"profile": "ashlar-g03-r7-process-slot/0.1",
            "capturedUtc": dt.datetime.now(dt.timezone.utc).isoformat(),
            "inventorySha256": sha_bytes(raw.encode()), "javaProcesses": java,
            "empty": not java}


def attempt_size(root):
    total = 0
    for path in Path(root).rglob("*"):
        if path.is_file() and not path.is_symlink():
            total += path.stat().st_size
    return total


def process_group_inventory(pgid):
    raw = subprocess.run(["/bin/ps", "-Ao", "pid=,ppid=,pgid=,rss=,comm="], check=True,
                         capture_output=True, text=True, timeout=5).stdout
    rows = []
    for line in raw.splitlines():
        parts = line.split(None, 4)
        if len(parts) == 5 and int(parts[2]) == int(pgid):
            rows.append({"pid": int(parts[0]), "ppid": int(parts[1]),
                         "pgid": int(parts[2]), "rssKiB": int(parts[3]),
                         "command": Path(parts[4]).name})
    return rows


def group_rss_bytes(rows):
    return sum(row["rssKiB"] for row in rows) * 1024


def stop_and_verify_group(proc):
    """Terminate any Spark JVM left in the owned session and verify no group member remains."""
    errors = []
    try:
        rows = process_group_inventory(proc.pid)
        initial_verified = True
    except Exception as exc:
        rows = []
        initial_verified = False
        errors.append(type(exc).__name__)
    # Signal if members remain or inventory failure means they cannot be ruled out.
    if rows or not initial_verified:
        try: os.killpg(proc.pid, signal.SIGTERM)
        except ProcessLookupError: pass
        except Exception as exc: errors.append("SIGTERM-" + type(exc).__name__)
    deadline = time.monotonic() + 5
    while time.monotonic() < deadline:
        try:
            observed = process_group_inventory(proc.pid)
        except Exception as exc:
            errors.append(type(exc).__name__)
            break
        if not observed:
            break
        time.sleep(0.1)
    try:
        survivors = process_group_inventory(proc.pid)
    except Exception as exc:
        survivors = []
        errors.append(type(exc).__name__)
    if survivors or errors:
        try: os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        except Exception as exc: errors.append("SIGKILL-" + type(exc).__name__)
    try:
        remaining = process_group_inventory(proc.pid)
        inventory_verified = True
    except Exception as exc:
        remaining = []
        inventory_verified = False
        errors.append(type(exc).__name__)
    proc.poll()
    return {"initialMembers": rows, "terminatedSurvivors": survivors,
            "remaining": remaining, "inventoryVerified": inventory_verified,
            "inventoryErrors": errors,
            "empty": inventory_verified and not remaining}


def write_new(path, raw):
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        require(os.write(fd, raw) == len(raw), "short packet write")
        os.fsync(fd)
    finally:
        os.close(fd)


def run_source_admission_preflight(attempt, *, receipt_name="source-admission-preflight.json",
                                  expected_receipt_sha256=None):
    """Exercise exact configured-source open/close in an isolated child, before dispatch."""
    selected = manifest()
    invocation_path = Path(attempt) / "initialize-invocation.json"
    invocation_raw = invocation_path.read_bytes()
    require(sha_bytes(invocation_raw) == sha_file(invocation_path),
            "initializer invocation changed while reading")
    env = {"HOME": str(Path.home()), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin", "JAVA_HOME": selected["runtime"]["javaHome"],
        "PYSPARK_PYTHON": selected["runtime"]["python"],
        "PYSPARK_DRIVER_PYTHON": selected["runtime"]["python"],
        "PYTHONDONTWRITEBYTECODE": "1", "TMPDIR": str(Path(attempt) / "tmp"),
        "SPARK_LOCAL_DIRS": str(Path(attempt) / "spark-local"),
        "PYTHONPATH": str(REPO / "src") + os.pathsep + str(PARENT)}
    started = time.monotonic()
    proc = subprocess.Popen([selected["runtime"]["python"], "-I", "-B", "-u",
        str(PACKET / "r7_init_worker.py"), "--preflight-source-admission",
        str(invocation_path)], cwd=attempt, env=env, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, start_new_session=True)
    try:
        stdout, stderr = proc.communicate(timeout=selected["limits"]["sourcePreflightWallSeconds"])
    except BaseException:
        try: os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError: pass
        proc.wait()
        cleanup = stop_and_verify_group(proc)
        write_new(Path(attempt) / (receipt_name + ".cleanup.json"), canonical(cleanup))
        raise
    cleanup = stop_and_verify_group(proc)
    require(cleanup["empty"], "source admission preflight child group did not close")
    require(proc.returncode == 0, "configured-source admission preflight refused")
    require(len(stdout) <= selected["limits"]["sourcePreflightOutputBytes"]
            and len(stderr) <= selected["limits"]["sourcePreflightOutputBytes"],
            "source admission preflight output limit exceeded")
    result = json.loads(stdout)
    require(result.get("profile") == "ashlar-g03-sdk010-r7-source-admission-preflight/0.1"
            and result.get("invocationSha256") == sha_bytes(invocation_raw)
            and result.get("sparkStarted") is False,
            "isolated source admission preflight receipt differs")
    require(result.get("configurationSha256") == selected["inputs"]["configured-typed-graph-source.json"]
            and result.get("sourceSha256") == selected["inputs"]["typed-graph-source.jsonl"]
            and result.get("modelSha256") == selected["inputs"]["typed-graph-model.umf.json"],
            "isolated source admission input hashes differ")
    if expected_receipt_sha256 is not None:
        require(result.get("sourceAdmissionReceiptSha256") == expected_receipt_sha256,
                "isolated SDK010 receipt differs from frozen admission")
    stable = dict(result)
    stable.pop("elapsedSeconds", None)
    receipt = {"profile": "ashlar-g03-sdk010-r7-source-admission-supervisor-receipt/0.1",
        "invocationSha256": sha_bytes(invocation_raw), "childReturnCode": proc.returncode,
        "childStdoutSha256": sha_bytes(stdout), "childStderrSha256": sha_bytes(stderr),
        "childElapsedSeconds": round(time.monotonic() - started, 3),
        "sourceAdmission": stable, "processGroupCleanup": cleanup,
        "sparkStarted": False, "nativeEffects": False}
    raw = canonical(receipt)
    write_new(Path(attempt) / receipt_name, raw)
    return {"sha256": sha_bytes(raw), "stable": stable, "receipt": receipt}


def private_dir(path):
    info = Path(path).lstat()
    require(stat.S_ISDIR(info.st_mode) and info.st_uid == os.getuid()
            and stat.S_IMODE(info.st_mode) == 0o700, "private user-owned directory required")


def invocation(attempt, selected):
    source = selected["producer"]
    runtime = selected["runtime"]
    return {"profile": "ashlar-configured-local/0.3", "operation": "initialize", "ordinal": 0,
        "python": runtime["python"], "installation": str(attempt / "installation"),
        "installation_id": "g03-sdk010-r7-" + attempt.name[-12:],
        "installation_sha256": None, "output": str(attempt / "initialize-output"),
        "jars": str(Path(next(iter(runtime["jars"].values()))["path"]).parent),
        "jar_inventory": {name: item["sha256"] for name, item in runtime["jars"].items()},
        "java_home": runtime["javaHome"],
        "source_configuration": str(attempt / "inputs/configured-typed-graph-source.json"),
        "producer": {"source": source["source"], "bun": source["bun"],
            "git": source["git"], "timeout_seconds": source["timeoutSeconds"],
            "maximum_output_bytes": source["maximumOutputBytes"],
            "maximum_receipt_bytes": source["maximumReceiptBytes"]},
        "stream": "g03-sdk010-r7-" + attempt.name[-12:],
        "materialization_clock_us": str(int(time.time() * 1_000_000)),
        "operation_capacity": None, "carrier_profile": selected["carrierProfile"],
        "bounds": {"whole_seconds": selected["limits"]["wallSeconds"],
            "stream_bytes": selected["limits"]["combinedLogBytes"],
            "receipt_bytes": selected["limits"]["receiptBytes"],
            "maximum_admission_receipt_bytes": 1048576}}


def prepare():
    selected = manifest()
    admission = verify_effect_free()
    parent = Path(selected["attemptParent"])
    require(not parent.exists() and not parent.is_symlink(),
            "R7 fresh attempt parent must be absent; never reuse R3 or prior attempts")
    parent.mkdir(mode=0o700, parents=True)
    private_dir(parent)
    attempt = parent / ("ashlar-g03-sdk010-init-" + uuid.uuid4().hex)
    attempt.mkdir(mode=0o700)
    private_dir(attempt)
    inputs = attempt / "inputs"
    inputs.mkdir(mode=0o700)
    for name in selected["inputs"]:
        write_new(inputs / name, (PACKET / "inputs" / name).read_bytes())
    (attempt / "tmp").mkdir(mode=0o700)
    (attempt / "spark-local").mkdir(mode=0o700)
    raw = canonical(invocation(attempt, selected))
    require(len(raw) <= 262144, "initializer invocation exceeds bound")
    write_new(attempt / "initialize-invocation.json", raw)
    source_preflight = run_source_admission_preflight(attempt,
        expected_receipt_sha256=admission["sourceAdmissionReceiptSha256"])
    prepared = {"profile": "ashlar-g03-sdk010-r7-init-prepared/0.1",
        "attemptRoot": str(attempt), "attemptId": attempt.name,
        "packetTreeSha256": admission["packetTreeSha256"],
        "manifestSha256": sha_file(MANIFEST_PATH), "currentCommit": admission["currentCommit"],
        "sourceTreeObject": admission["sourceTreeObject"],
        "sourceAdmission": admission,
        "sourceAdmissionPreflightSha256": source_preflight["sha256"],
        "configuredSourcePreflight": source_preflight["stable"],
        "invocationSha256": sha_bytes(raw), "status": "prepared-no-native-effects"}
    write_new(attempt / "prepared.json", canonical(prepared))
    print(attempt / "prepared.json")


def record_slot(attempt_path):
    attempt = Path(attempt_path).resolve(strict=True)
    require((attempt / "prepared.json").is_file(), "prepared attempt required")
    private_dir(attempt)
    require(not (attempt / "process-slot.json").exists(), "process slot already recorded")
    slot = capture_process_slot()
    require(slot["empty"], "Spark Java process slot is occupied")
    raw = canonical(slot)
    write_new(attempt / "process-slot.json", raw)
    print(sha_bytes(raw))


def _kill_group(proc):
    return stop_and_verify_group(proc)


def execute(attempt_path, permit_path):
    selected = manifest()
    attempt = Path(attempt_path).resolve(strict=True)
    private_dir(attempt)
    prepared_raw = (attempt / "prepared.json").read_bytes()
    prepared = json.loads(prepared_raw)
    require(prepared["profile"] == "ashlar-g03-sdk010-r7-init-prepared/0.1"
            and prepared["attemptRoot"] == str(attempt)
            and prepared["packetTreeSha256"] == packet_digest()
            and prepared["manifestSha256"] == sha_file(MANIFEST_PATH),
            "prepared packet identity differs")
    initial_preflight_path = attempt / "source-admission-preflight.json"
    require(initial_preflight_path.is_file()
            and sha_file(initial_preflight_path) == prepared.get("sourceAdmissionPreflightSha256"),
            "prepared full configured-source preflight receipt missing or changed")
    slot_raw = (attempt / "process-slot.json").read_bytes()
    slot = json.loads(slot_raw)
    require(slot.get("profile") == "ashlar-g03-r7-process-slot/0.1" and slot.get("empty") is True,
            "reviewed empty process-slot receipt required")
    permit_raw = Path(permit_path).read_bytes()
    permit = json.loads(permit_raw)
    expected = {"profile": "ashlar-g03-sdk010-r7-init-permit/0.1",
        "packetTreeSha256": prepared["packetTreeSha256"],
        "manifestSha256": prepared["manifestSha256"],
        "preparedSha256": sha_bytes(prepared_raw), "processSlotSha256": sha_bytes(slot_raw),
        "sourceAdmissionPreflightSha256": prepared["sourceAdmissionPreflightSha256"],
        "currentCommit": prepared["currentCommit"], "execute": True}
    require(permit == expected, "exact reviewed R7 init permit required")
    require(not (attempt / "dispatch.json").exists()
            and not (attempt / "init-result.json").exists(), "attempt is one-shot; no retry")
    # The full configured-source admission ran once during prepare. At dispatch,
    # recheck source/runtime/packet custody and the owned process slot; the worker
    # performs the real configured-source open before Spark startup.
    preflight = verify_effect_free()
    require(preflight["packetTreeSha256"] == prepared["packetTreeSha256"],
            "source, request, runtime, or packet changed before dispatch")
    # Recheck the owned Spark slot immediately after source admission, before
    # the permit and dispatch receipts are consumed.
    current_slot = capture_process_slot()
    require(current_slot["empty"], "Spark process slot changed before dispatch")
    require(not (attempt / "dispatch-process-slot.json").exists(),
            "immediate process-slot receipt already exists")
    current_slot_raw = canonical(current_slot)
    write_new(attempt / "dispatch-process-slot.json", current_slot_raw)
    write_new(attempt / "execution-permit-consumed.json", permit_raw)
    write_new(attempt / "dispatch.json", canonical({"profile": "ashlar-g03-sdk010-r7-init-dispatch/0.1",
        "permitSha256": sha_bytes(permit_raw), "packetTreeSha256": prepared["packetTreeSha256"],
        "dispatchedUtc": dt.datetime.now(dt.timezone.utc).isoformat(), "noRetry": True,
        "immediateProcessSlotSha256": sha_bytes(current_slot_raw)}))
    env = {"HOME": str(Path.home()), "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
        "PATH": "/usr/bin:/bin", "JAVA_HOME": selected["runtime"]["javaHome"],
        "PYSPARK_PYTHON": selected["runtime"]["python"],
        "PYSPARK_DRIVER_PYTHON": selected["runtime"]["python"], "PYTHONDONTWRITEBYTECODE": "1",
        "SPARK_LOCAL_IP": "127.0.0.1", "TMPDIR": str(attempt / "tmp"),
        "SPARK_LOCAL_DIRS": str(attempt / "spark-local"),
        "PYTHONPATH": str(REPO / "src") + os.pathsep + str(PARENT)}
    stdout = (attempt / "initialize.stdout.log").open("xb")
    stderr = (attempt / "initialize.stderr.log").open("xb")
    proc = subprocess.Popen([selected["runtime"]["python"], "-I", "-B", "-u",
        str(PACKET / "r7_init_worker.py"), "--attempt", str(attempt),
        "--prepared-sha256", sha_bytes(prepared_raw)], cwd=attempt, env=env,
        stdin=subprocess.DEVNULL, stdout=stdout, stderr=stderr, start_new_session=True)
    deadline = time.monotonic() + selected["limits"]["wallSeconds"]
    group_cleanup = None
    try:
        while proc.poll() is None:
            if time.monotonic() >= deadline:
                raise TimeoutError("R7 initialization deadline reached; attempt retained")
            if sum(path.stat().st_size for path in
                   (attempt / "initialize.stdout.log", attempt / "initialize.stderr.log")) > selected["limits"]["combinedLogBytes"]:
                raise ValueError("R7 combined worker output limit exceeded")
            if attempt_size(attempt) > selected["limits"]["attemptBytes"]:
                raise ValueError("R7 attempt storage limit exceeded")
            rows = process_group_inventory(proc.pid)
            if group_rss_bytes(rows) > selected["limits"]["processGroupRssBytes"]:
                raise ValueError("R7 process-group RSS limit exceeded")
            time.sleep(0.1)
        group_cleanup = _kill_group(proc)
        require(group_cleanup["empty"], "R7 child exited with surviving process-group members")
        require(proc.returncode == 0, "R7 initialization worker failed; attempt retained")
        require(sum(path.stat().st_size for path in
                   (attempt / "initialize.stdout.log", attempt / "initialize.stderr.log")) <= selected["limits"]["combinedLogBytes"],
                "R7 combined worker output limit exceeded")
    except BaseException:
        group_cleanup = _kill_group(proc)
        write_new(attempt / "process-group-cleanup.json", canonical(group_cleanup))
        raise
    finally:
        stdout.close(); stderr.close()
    result = json.loads((attempt / "init-result.json").read_bytes())
    require(result.get("profile") == "ashlar-g03-sdk010-six-role-init-result/0.1"
            and result.get("sourcePolicyRevision") == selected["sourcePolicyRevision"],
            "R7 initialization receipt profile differs")
    write_new(attempt / "process-group-cleanup.json", canonical(group_cleanup))
    write_new(attempt / "execution-status.json", canonical({
        "profile": "ashlar-g03-sdk010-init-execution-status/0.1",
        "classification": "completed", "effectsPossible": True,
        "processGroupEmpty": True, "resultSha256": sha_file(attempt / "init-result.json")}))
    print(attempt / "init-result.json")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("prepare", help="effect-free verify and create a fresh attempt")
    slot = sub.add_parser("slot", help="record a fresh empty Java process inventory")
    slot.add_argument("attempt")
    run = sub.add_parser("execute", help="consume exact permit and run one init")
    run.add_argument("attempt")
    run.add_argument("permit")
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            prepare()
        elif args.command == "slot":
            record_slot(args.attempt)
        else:
            execute(args.attempt, args.permit)
    except BaseException as exc:
        if args.command == "execute":
            attempt = Path(args.attempt)
            if attempt.is_dir() and not (attempt / "execution-status.json").exists():
                dispatched = (attempt / "dispatch.json").exists()
                status = {"profile": "ashlar-g03-sdk010-init-execution-status/0.1",
                    "classification": "uncertain-after-dispatch" if dispatched else "refused-before-dispatch",
                    "effectsPossible": dispatched, "errorType": type(exc).__name__}
                try: write_new(attempt / "execution-status.json", canonical(status))
                except FileExistsError: pass
        raise


if __name__ == "__main__":
    main()
