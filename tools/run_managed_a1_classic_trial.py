#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Bounded owner-managed classic Jobs trial wrapper.

This wrapper owns exactly one single-node classic cluster.  Its global 900s
clock starts before the create request, includes the existing managed A1
runner and reserves the final 60s for cluster termination/permanent deletion.
Without --launch it performs local-only preflight and never creates compute.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import selectors
import shutil
import signal
import subprocess
import sys
import time
import uuid
from urllib.parse import urlsplit

REPO = Path(__file__).resolve().parents[1]
PROFILE = "aidev-cus"
CLI_VERSION = "Databricks CLI v1.21.0"
HOST = "https://adb-7405607548213398.18.azuredatabricks.net"
CREATE_SPEC = Path("/private/tmp/ashlar-a1-classic-config-20261103/cluster-create.json")
CREATE_SPEC_SHA256 = "4c35ce0c880a3d3f902b75094cf30b4dcf1b1b1bcbff0df698f61756f1716bd1"
TRIAL_ID = "a1-release-20261103-c17"
CLUSTER_NAME = "ashlar-release-classic-20261103-r17-df9e30c858"
CLUSTER_NONCE = "df9e30c858d846e0a2401132a3202688"
CLUSTER_OWNER_TAG = "ashlar-release"
POLICY_ID = "001654FA4AA08D1A"
NODE_TYPE = "Standard_D4ds_v5"
SPARK_VERSION = "19.x-scala2.13"
CLASSIC_REQUIREMENTS_LOCAL = Path(
    "/private/tmp/ashlar-release-owned-wheel-cache-20261103/requirements-a1-serverless.txt")
CLASSIC_REQUIREMENTS_REMOTE = (
    "/Workspace/Users/erik.labianca@synaptiq.ai/"
    ".ashlar-release-20261103/requirements-a1-serverless.txt")
CLASSIC_REQUIREMENTS_SHA256 = (
    "44204ac45c523a92975e2bcdda52e34cc2b9bbb89339f0bf43d1edb6dd93d6cf")
GLOBAL_WALL_SECONDS = 900
CLUSTER_CLEANUP_RESERVE_SECONDS = 60
RUNNER_CLEANUP_RESERVE_SECONDS = 60
MAX_COMMAND_OUTPUT = 16 * 1024 * 1024
MAX_LIST_OUTPUT = 8 * 1024 * 1024
MAX_POLL_SECONDS = 5


class Refusal(RuntimeError):
    pass


class CreateCliPreApiRefusal(Refusal):
    """The Databricks CLI rejected CREATE arguments before API submission."""


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _require(condition: bool, message: str) -> None:
    if not condition:
        raise Refusal(message)


def _validate_dependency_mappings(items: list[str]) -> None:
    _require(type(items) is list and len(items) > 0,
             "Exact dependency wheel/constraints mappings required")
    requirements_seen = 0
    for item in items:
        _require(type(item) is str and "=" in item,
                 "Dependency mapping must be LOCAL_FILE=REMOTE_PATH")
        local, remote = item.split("=", 1)
        p = Path(local).resolve(strict=True)
        _require(p.is_file() and p.suffix in (".whl", ".txt"),
                 "Dependency must be a local wheel or constraints file")
        # Jobs uses a requirements library value prefixed by `-r `. Validate
        # the path after that marker, while preserving the original mapping
        # unchanged for the config generator and launch command.
        remote_path = remote[3:] if remote.startswith("-r ") else remote
        _require(remote_path.startswith(("/Workspace/", "/Volumes/")),
                 "Dependency must bind to a Workspace or Volume path")
        if remote.startswith("-r ") or p.name == CLASSIC_REQUIREMENTS_LOCAL.name:
            requirements_seen += 1
            _require(remote == "-r " + CLASSIC_REQUIREMENTS_REMOTE and
                     p == CLASSIC_REQUIREMENTS_LOCAL and
                     _sha(p.read_bytes()) == CLASSIC_REQUIREMENTS_SHA256,
                     "Requirements dependency differs from the exact locked artifact")
    _require(requirements_seen == 1,
             "Exactly one locked non-Spark requirements artifact is required")


def _read_json(path: Path) -> dict:
    if not path.is_absolute() or path.is_symlink() or not path.is_file():
        raise Refusal("Exact regular JSON input file required")
    raw = path.read_bytes()
    _require(len(raw) <= 4 * 1024 * 1024, "JSON input exceeds local bound")
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Refusal("Input must be strict UTF-8 JSON") from error
    _require(type(value) is dict, "JSON object required")
    return value


def _write_private(path: Path, raw: bytes) -> None:
    _require(type(raw) is bytes and len(raw) <= MAX_COMMAND_OUTPUT,
             "Bounded private output bytes required")
    _require(path.parent.is_dir() and path.parent.stat().st_uid == os.getuid()
             and path.parent.stat().st_mode & 0o077 == 0,
             "Owner-private output directory required")
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        view = memoryview(raw)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise OSError("Private output write made no progress")
            view = view[count:]
        os.fsync(fd)
    finally:
        os.close(fd)
    dirfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dirfd)
    finally:
        os.close(dirfd)


def _write_json_new(path: Path, value: dict) -> None:
    _write_private(path, json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n")


def _atomic_json(path: Path, value: dict) -> None:
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"
    tmp = path.with_name(path.name + ".tmp-" + uuid.uuid4().hex)
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(tmp, path)
        dirfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    finally:
        try:
            tmp.unlink()
        except FileNotFoundError:
            pass


def _verify_create_spec(path: Path) -> dict:
    _require(path == CREATE_SPEC and path.is_absolute(),
             "Only the reviewed owner-supplied classic CREATE specification is admitted")
    raw = path.read_bytes()
    _require(_sha(raw) == CREATE_SPEC_SHA256,
             "Classic CREATE specification SHA-256 differs from reviewed bytes")
    spec = _read_json(path)
    required = {
        "apply_policy_default_values", "autotermination_minutes", "azure_attributes",
        "cluster_name", "custom_tags", "data_security_mode", "driver_node_type_id",
        "enable_elastic_disk", "node_type_id", "num_workers", "policy_id",
        "runtime_engine", "single_user_name", "spark_conf", "spark_version",
    }
    _require(set(spec) == required, "Closed reviewed cluster create schema required")
    _require(spec == {
        "apply_policy_default_values": True,
        "autotermination_minutes": 10,
        "azure_attributes": {"availability": "ON_DEMAND_AZURE", "first_on_demand": 1},
        "cluster_name": CLUSTER_NAME,
        "custom_tags": {"ResourceClass": "SingleNode", "ashlar_nonce": CLUSTER_NONCE,
                        "ashlar_owner": CLUSTER_OWNER_TAG,
                        "ashlar_trial": "a1_release_20261103_c17"},
        "data_security_mode": "SINGLE_USER",
        "driver_node_type_id": NODE_TYPE,
        "enable_elastic_disk": True,
        "node_type_id": NODE_TYPE,
        "num_workers": 0,
        "policy_id": POLICY_ID,
        "runtime_engine": "STANDARD",
        "single_user_name": "erik.labianca@synaptiq.ai",
        "spark_conf": {"spark.databricks.cluster.profile": "singleNode",
                       "spark.master": "local[*]",
                       "spark.sql.session.timeZone": "UTC"},
        "spark_version": SPARK_VERSION,
    }, "Exact reviewed single-node D4ds_v5 owner configuration required")
    return spec


def _cli_path() -> Path:
    found = shutil.which("databricks")
    _require(found is not None, "Databricks CLI is required on PATH")
    path = Path(found).resolve(strict=True)
    _require(path.is_file() and os.access(path, os.X_OK),
             "Executable Databricks CLI required")
    return path


def _closed_cli_env() -> dict:
    env = dict(os.environ)
    for name in ("DATABRICKS_HOST", "DATABRICKS_TOKEN", "DATABRICKS_CLIENT_ID",
                 "DATABRICKS_CLIENT_SECRET", "DATABRICKS_ACCOUNT_ID",
                 "DATABRICKS_USERNAME", "DATABRICKS_PASSWORD"):
        env.pop(name, None)
    env["DATABRICKS_CONFIG_PROFILE"] = PROFILE
    return env


def _run_process(argv: list[str], *, deadline: float,
                 output_cap: int = MAX_COMMAND_OUTPUT,
                 interrupt_first: bool = False) -> tuple[int, bytes]:
    _require(time.monotonic() < deadline, "Global classic trial deadline expired")
    process = subprocess.Popen(argv, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.PIPE, close_fds=True,
        start_new_session=True, env=_closed_cli_env())
    assert process.stdout is not None and process.stderr is not None
    selector = selectors.DefaultSelector()
    buffers = {"stdout": bytearray(), "stderr": bytearray()}
    streams = {"stdout": process.stdout, "stderr": process.stderr}
    for name, stream in streams.items():
        os.set_blocking(stream.fileno(), False)
        selector.register(stream.fileno(), selectors.EVENT_READ, data=name)
    try:
        while selector.get_map() or process.poll() is None:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                _stop_owned_process(process, interrupt_first=interrupt_first)
                raise TimeoutError("Bounded classic trial child exceeded its deadline")
            for key, _ in selector.select(min(0.25, remaining)):
                name = key.data
                try:
                    block = os.read(key.fd, 65536)
                except BlockingIOError:
                    continue
                if not block:
                    try:
                        selector.unregister(key.fd)
                    except Exception:
                        pass
                    continue
                buffers[name].extend(block)
                if sum(map(len, buffers.values())) > output_cap:
                    _stop_owned_process(process, interrupt_first=interrupt_first)
                    raise Refusal("Combined child stdout/stderr exceeded its closed byte bound")
        code = process.wait()
        stdout = bytes(buffers["stdout"])
        stderr = bytes(buffers["stderr"])
        if code == 0:
            if stderr:
                binary = getattr(sys.stderr, "buffer", None)
                if binary is not None:
                    binary.write(stderr)
                    binary.flush()
                else:
                    sys.stderr.write(stderr.decode("utf-8", "replace"))
                    sys.stderr.flush()
            return code, stdout
        diagnostic = stdout
        if stderr:
            diagnostic += (b"\n[child stderr]\n" if diagnostic else b"") + stderr
        return code, diagnostic
    except BaseException:
        if process.poll() is None:
            _stop_owned_process(process, interrupt_first=interrupt_first)
        raise
    finally:
        selector.close()
        process.stdout.close()
        process.stderr.close()
        if process.poll() is None:
            _stop_owned_process(process, interrupt_first=interrupt_first)


def _stop_owned_process(process: subprocess.Popen, *, interrupt_first: bool = False) -> None:
    if process.poll() is not None:
        return
    if interrupt_first:
        try:
            os.killpg(process.pid, signal.SIGINT)
        except ProcessLookupError:
            pass
        try:
            process.wait(timeout=10)
            return
        except subprocess.TimeoutExpired:
            pass
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        pass
    try:
        process.wait(timeout=5)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        process.wait(timeout=5)


def _databricks(*args: str, deadline: float,
                output_cap: int = MAX_COMMAND_OUTPUT) -> bytes:
    cli = _cli_path()
    argv = [str(cli), "--profile", PROFILE, *args, "--output", "json"]
    code, output = _run_process(argv, deadline=deadline, output_cap=output_cap)
    if code != 0:
        raise RuntimeError("Databricks CLI returned nonzero: " +
            output[-1500:].decode("utf-8", "replace"))
    return output


def _parse_json_bytes(raw: bytes, label: str) -> dict:
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Refusal(label + " did not return bounded JSON") from error
    _require(type(value) is dict, label + " must return one JSON object")
    return value


def _verify_cluster_identity(cluster: dict, spec: dict, *, require_running: bool) -> None:
    _verify_cluster_owner(cluster, spec)
    _require(cluster.get("policy_id") == POLICY_ID
             and cluster.get("node_type_id") == NODE_TYPE
             and cluster.get("driver_node_type_id") == NODE_TYPE
             and cluster.get("spark_version") == SPARK_VERSION
             and cluster.get("num_workers") == 0
             and cluster.get("data_security_mode") == "SINGLE_USER"
             and cluster.get("runtime_engine") == "STANDARD"
             and cluster.get("autotermination_minutes") == 10
             and cluster.get("azure_attributes") == spec["azure_attributes"],
             "Cluster runtime, policy, ownership, or cost envelope differs")
    _require(cluster.get("spark_conf") == spec["spark_conf"],
             "Cluster Spark single-node/UTC configuration differs")
    _require(cluster.get("autoscale") in (None, {}),
             "Autoscaling is not admitted for this single-node trial")
    _require(cluster.get("state") in ({"RUNNING"} if require_running else
             {"PENDING", "RESTARTING", "RESIZING", "RUNNING", "TERMINATING",
              "TERMINATED", "ERROR", "UNKNOWN"}),
             "Cluster state is outside the closed lifecycle vocabulary")


def _verify_cluster_owner(cluster: dict, spec: dict) -> None:
    tags = cluster.get("custom_tags")
    _require(type(cluster.get("cluster_id")) is str and cluster["cluster_id"],
             "Cluster ID missing from exact GET readback")
    _require(cluster.get("cluster_name") == spec["cluster_name"]
             and type(tags) is dict
             and tags.get("ashlar_nonce") == CLUSTER_NONCE
             and tags.get("ashlar_owner") == CLUSTER_OWNER_TAG
             and tags.get("ashlar_trial") == spec["custom_tags"]["ashlar_trial"],
             "Cluster response does not bind the exact unique owner nonce")
    _require(cluster.get("creator_user_name") == spec["single_user_name"]
             and cluster.get("single_user_name") == spec["single_user_name"],
             "Cluster creator/single-user identity differs from owner lock")
    _require(cluster.get("state") in {"PENDING", "RESTARTING", "RESIZING",
             "RUNNING", "TERMINATING", "TERMINATED", "ERROR", "UNKNOWN"},
             "Cluster state is outside the closed lifecycle vocabulary")


def _admit_profile_host(value: dict) -> str:
    _require(type(value) is dict and value.get("status") == "success",
             "Databricks CLI profile authentication did not report success")
    details = value.get("details")
    if "details" in value:
        _require(type(details) is dict,
                 "Databricks auth describe details must be an object")
    hosts = []
    for host in ((details.get("host") if type(details) is dict else None), value.get("host")):
        if host is None:
            continue
        _require(type(host) is str and host == host.strip(),
                 "Databricks auth profile host is malformed")
        parsed = urlsplit(host)
        _require(parsed.scheme == "https" and parsed.netloc and parsed.hostname
                 and parsed.username is None and parsed.password is None
                 and parsed.path in ("", "/") and not parsed.query and not parsed.fragment,
                 "Databricks auth profile host is malformed")
        hosts.append(host.rstrip("/"))
    _require(bool(hosts) and all(host == hosts[0] for host in hosts),
             "Databricks auth profile host sources are absent or contradictory")
    _require(hosts[0] == HOST.rstrip("/"),
             "Selected CLI profile does not target the admitted workspace")
    return hosts[0]


def _list_clusters(deadline: float) -> list[dict]:
    raw = _databricks("clusters", "list", "--limit", "10000", "--page-size", "1000",
                      deadline=deadline, output_cap=MAX_LIST_OUTPUT)
    try:
        value = json.loads(raw)
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise Refusal("clusters list did not return bounded JSON") from error
    if type(value) is list:
        clusters = value
        # CLI 1.21 returns the fully iterated inventory as a flat JSON list.
        _require(len(clusters) < 10000,
                 "Cluster list reached the requested cap; uniqueness is unproven")
    else:
        _require(type(value) is dict, "Cluster inventory must be a list or API object")
        clusters = value.get("clusters")
        _require(type(clusters) is list and len(clusters) < 10000,
                 "Bounded cluster inventory response required")
        _require(value.get("next_page_token") in (None, ""),
                 "Incomplete cluster inventory cannot support unique nonce ownership")
    _require(all(type(item) is dict for item in clusters),
             "Cluster inventory must contain objects")
    return clusters


def _find_unique_owned_cluster(spec: dict, deadline: float) -> dict | None:
    matches = []
    for cluster in _list_clusters(deadline):
        tags = cluster.get("custom_tags")
        if (type(tags) is dict and tags.get("ashlar_nonce") == CLUSTER_NONCE
                and tags.get("ashlar_owner") == CLUSTER_OWNER_TAG):
            matches.append(cluster)
    _require(len(matches) <= 1,
             "More than one cluster has the exact trial nonce/owner; refuse to choose")
    if not matches:
        return None
    candidate = matches[0]
    cluster_id = candidate.get("cluster_id")
    _require(type(cluster_id) is str and cluster_id,
             "Unique exact-nonce cluster omitted its ID")
    get = _parse_json_bytes(_databricks("clusters", "get", cluster_id,
        deadline=deadline), "clusters get after exact nonce match")
    _verify_cluster_owner(get, spec)
    return get


def _get_owned_cluster(cluster_id: str, spec: dict, deadline: float) -> dict:
    cluster = _parse_json_bytes(_databricks("clusters", "get", cluster_id,
        deadline=deadline), "clusters get")
    _verify_cluster_owner(cluster, spec)
    _require(cluster["cluster_id"] == cluster_id,
             "GET returned a different cluster ID")
    return cluster


def _wait_running(cluster_id: str, spec: dict, deadline: float,
                  state_record: dict) -> dict:
    last = None
    while time.monotonic() < deadline:
        cluster = _get_owned_cluster(cluster_id, spec, deadline)
        state = cluster.get("state")
        state_record.update({"cluster_id": cluster_id, "cluster_state": state,
                             "last_state_check_epoch_ms": int(time.time() * 1000)})
        _atomic_json(Path(state_record["state_path"]), state_record)
        if state == "RUNNING":
            _verify_cluster_identity(cluster, spec, require_running=True)
            return cluster
        if state in ("TERMINATED", "ERROR"):
            last = cluster.get("state_message")
            raise RuntimeError("Owned cluster failed before RUNNING: " + str(last)[:1000])
        time.sleep(min(MAX_POLL_SECONDS, max(0.1, deadline - time.monotonic())))
    raise TimeoutError("Owned cluster did not reach RUNNING before work cutoff")


def _cleanup_owned_cluster(cluster_id: str | None, spec: dict, deadline: float) -> dict:
    """Terminate then permanently delete only the exact nonce-bound cluster."""
    record = {"cluster_id": cluster_id, "terminate_requested": False,
              "terminated_confirmed": False, "permanent_delete_requested": False,
              "permanent_delete_confirmed": False, "errors": []}
    if cluster_id is None:
        try:
            found = _find_unique_owned_cluster(spec, deadline)
            if found is None:
                record.update({"status": "no_matching_owned_cluster"})
                return record
            cluster_id = found["cluster_id"]
            record["cluster_id"] = cluster_id
        except Exception as error:
            record["status"] = "ownership_unresolved"
            record["errors"].append(type(error).__name__ + ": " + str(error)[:1200])
            return record
    try:
        cluster = _get_owned_cluster(cluster_id, spec, deadline)
        if cluster["state"] not in ("TERMINATED", "ERROR"):
            if time.monotonic() >= deadline:
                raise TimeoutError("Cleanup deadline expired before termination request")
            _databricks("clusters", "delete", cluster_id, "--no-wait", deadline=deadline)
            record["terminate_requested"] = True
        while time.monotonic() < deadline:
            cluster = _get_owned_cluster(cluster_id, spec, deadline)
            if cluster["state"] in ("TERMINATED", "ERROR"):
                record["terminated_confirmed"] = True
                break
            time.sleep(min(2.0, max(0.1, deadline - time.monotonic())))
        if not record["terminated_confirmed"]:
            raise TimeoutError("Owned cluster termination not confirmed within cleanup reserve")
        if time.monotonic() >= deadline:
            raise TimeoutError("Cleanup deadline expired before permanent deletion")
        # Re-read identity immediately before the irreversible permanent delete.
        cluster = _get_owned_cluster(cluster_id, spec, deadline)
        _require(cluster["state"] in ("TERMINATED", "ERROR"),
                 "Permanent deletion requires a terminated exact-owned cluster")
        _databricks("clusters", "permanent-delete", cluster_id, deadline=deadline)
        record["permanent_delete_requested"] = True
        if time.monotonic() < deadline:
            remaining = max(1, int(deadline - time.monotonic()))
            matches = _find_unique_owned_cluster(spec,
                min(deadline, time.monotonic() + min(15, remaining)))
            if matches is None:
                record["permanent_delete_confirmed"] = True
        if not record["permanent_delete_confirmed"]:
            raise TimeoutError("Permanent deletion was requested but not confirmed")
        record["status"] = "deleted"
    except Exception as error:
        record["status"] = "cleanup_incomplete"
        record["errors"].append(type(error).__name__ + ": " + str(error)[:1200])
    return record


def _spawn_watchdog(state_path: Path, root: Path) -> subprocess.Popen:
    log_path = root / "watchdog.log"
    log_file = open(log_path, "xb", buffering=0)
    os.chmod(log_path, 0o600)
    child = subprocess.Popen([sys.executable, str(Path(__file__).resolve()),
        "--watchdog-state", str(state_path)], stdin=subprocess.DEVNULL,
        stdout=log_file, stderr=subprocess.STDOUT, close_fds=True,
        start_new_session=True, env={"PATH": os.environ.get("PATH", ""),
                                     "HOME": os.environ.get("HOME", "")})
    log_file.close()
    return child


def _pid_alive(pid: int) -> bool:
    if type(pid) is not int or pid <= 1:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def _watchdog(state_path: Path) -> int:
    state = _read_json(state_path)
    spec = _verify_create_spec(Path(state["create_spec_path"]))
    root = Path(state["output_root"])
    stop_path = root / "watchdog.stop"
    hard_deadline = float(state["hard_deadline_mono"])
    work_cutoff = float(state["work_cutoff_mono"])
    owner_pid = state["owner_pid"]
    while not stop_path.exists():
        state = _read_json(state_path)
        owner_gone = not _pid_alive(owner_pid)
        too_late = time.monotonic() >= work_cutoff
        if owner_gone or too_late:
            if not state.get("create_attempted"):
                _atomic_json(root / "watchdog-receipt.json", {
                    "profile": "ashlar-managed-classic-cluster-watchdog/0.1",
                    "trigger": "owner_process_gone" if owner_gone else "work_cutoff_reached",
                    "result": {"status": "no_create_attempt", "cluster_cleanup": "not_applicable"},
                    "created_epoch_ms": int(time.time() * 1000)})
                return 0
            if state.get("create_api_submitted") is False:
                _atomic_json(root / "watchdog-receipt.json", {
                    "profile": "ashlar-managed-classic-cluster-watchdog/0.1",
                    "trigger": "owner_process_gone" if owner_gone else "work_cutoff_reached",
                    "result": {"status": "create_not_submitted",
                               "create_outcome": state.get("create_outcome")},
                    "created_epoch_ms": int(time.time() * 1000)})
                return 0
            cluster_id = state.get("cluster_id")
            if cluster_id is None and not state.get("create_response_received"):
                # The CREATE response can be lost while the API request is
                # committed asynchronously.  Keep resolving only this unique
                # nonce through the bounded cleanup reserve; never retry CREATE.
                while time.monotonic() < hard_deadline:
                    found = _find_unique_owned_cluster(spec,
                        min(hard_deadline, time.monotonic() + 10))
                    if found is not None:
                        cluster_id = found["cluster_id"]
                        break
                    time.sleep(min(2.0, max(0.1, hard_deadline - time.monotonic())))
                if cluster_id is None:
                    _atomic_json(root / "watchdog-receipt.json", {
                        "profile": "ashlar-managed-classic-cluster-watchdog/0.1",
                        "trigger": "create_response_unknown",
                        "result": {"status": "create_outcome_unknown_no_cluster_visible"},
                        "created_epoch_ms": int(time.time() * 1000)})
                    return 2
            cleanup_deadline = hard_deadline
            result = _cleanup_owned_cluster(cluster_id, spec, cleanup_deadline)
            _atomic_json(root / "watchdog-receipt.json", {
                "profile": "ashlar-managed-classic-cluster-watchdog/0.1",
                "trigger": "owner_process_gone" if owner_gone else "work_cutoff_reached",
                "result": result, "created_epoch_ms": int(time.time() * 1000)})
            return 0 if result.get("status") in ("deleted", "no_matching_owned_cluster") else 2
        time.sleep(2)
    return 0


def _local_preflight(args) -> tuple[dict, dict, Path, Path]:
    _require(args.profile == PROFILE,
             "Only the owner-admitted aidev-cus profile is supported")
    spec = _verify_create_spec(args.cluster_create_spec)
    cli = _cli_path()
    code, raw = _run_process([str(cli), "version"], deadline=time.monotonic() + 10,
                             output_cap=4096)
    _require(code == 0 and raw.decode("utf-8", "replace").strip() == CLI_VERSION,
             "Unexpected Databricks CLI version")
    python = Path(args.python).expanduser()
    _require(python.is_absolute() and python.is_file() and os.access(python, os.X_OK)
             and python.resolve(strict=True) == Path(sys.executable).resolve(strict=True),
             "Selected runner Python must be the executable for this exact environment")
    bun = Path(args.bun).resolve(strict=True)
    _require(bun.is_file() and os.access(bun, os.X_OK),
             "Selected Bun executable must be executable")
    receipt = Path(args.local_receipt).resolve(strict=True)
    _require(receipt.is_file() and receipt.is_relative_to(REPO),
             "Local A1 qualification receipt must be a repository file")
    _require(args.trial_id == TRIAL_ID,
             "Trial ID differs from the exact owner-admitted cluster specification")
    _require(args.notebook_root.startswith("/Workspace/") and
             not args.notebook_root.endswith("/"), "Workspace notebook root required")
    _validate_dependency_mappings(args.dependency)
    root = Path(args.output_root)
    _require(root.is_absolute() and root.parent == Path("/private/tmp")
             and not root.exists() and not root.is_symlink(),
             "Fresh direct child of /private/tmp required for owner-private trial output")
    return spec, {"cli": str(cli), "cli_version": CLI_VERSION,
                  "python": str(python), "bun": str(bun),
                  "local_receipt": str(receipt)}, root, python


def _create_or_find(spec: dict, create_spec_path: Path, deadline: float,
                    state: dict, state_path: Path) -> dict:
    # A pre-create inventory prevents accidental adoption/reuse of an old trial.
    existing = _find_unique_owned_cluster(spec, deadline)
    _require(existing is None,
             "A cluster already exists with this exact nonce/owner; no create/reuse allowed")
    state["create_attempted"] = True
    state["create_api_submitted"] = False
    state["create_attempt_epoch_ms"] = int(time.time() * 1000)
    _atomic_json(state_path, state)
    cli = _cli_path()
    # Persist intent immediately before invoking the CLI. If the owner exits
    # before this point, the watchdog knows no CREATE request was submitted.
    state["create_api_submitted"] = True
    _atomic_json(state_path, state)
    try:
        output = _databricks("clusters", "create",
            "--json", "@" + str(create_spec_path), "--no-wait", deadline=deadline)
        create_result = _parse_json_bytes(output, "clusters create")
        cluster_id = create_result.get("cluster_id")
        _require(type(cluster_id) is str and cluster_id,
                 "Create response omitted the new cluster ID")
        state.update({"cluster_id": cluster_id, "create_response_sha256": _sha(output),
                      "create_response_received": True})
        _atomic_json(state_path, state)
    except BaseException as error:
        message = str(error)
        if ("when --json flag is specified, no positional arguments are allowed" in message
                and "Provide 'spark_version' in your JSON input" in message):
            # This exact CLI parser diagnostic occurs before the API request.
            # Preserve it and avoid classifying the outcome as ambiguous.
            state.update({"create_api_submitted": False,
                "create_outcome": "pre_api_cli_argument_refusal",
                "create_response_received": False,
                "create_error": type(error).__name__ + ": " + message[:1200]})
            _atomic_json(state_path, state)
            raise CreateCliPreApiRefusal(message) from error
        # The API can commit while its response is lost. Never repeat CREATE;
        # resolve only the exact unique owner/nonce cluster before cleanup.
        state["create_response_received"] = False
        state["create_error"] = type(error).__name__ + ": " + str(error)[:1200]
        _atomic_json(state_path, state)
        found = _find_unique_owned_cluster(spec, deadline)
        if found is None:
            raise RuntimeError("CREATE outcome is unknown and no exact nonce-owned cluster is visible") from error
        cluster_id = found["cluster_id"]
        state.update({"cluster_id": cluster_id, "create_response_recovered_by_exact_nonce": True})
        _atomic_json(state_path, state)
    cluster = _get_owned_cluster(cluster_id, spec, deadline)
    state["cluster_state"] = cluster.get("state")
    _atomic_json(state_path, state)
    return cluster


def _run_generator(args, cluster_readback_path: Path, config_path: Path,
                   remaining_seconds: int, deadline: float, runtime: dict) -> dict:
    generator = REPO / "tools/create_managed_a1_trial_config.py"
    _require(generator.is_file() and not generator.is_symlink(),
             "Reviewed classic config generator is absent")
    # The generator validates that its interpreter is the exact Python selected
    # for every managed task/source-holder component.
    argv = [str(Path(args.python).expanduser()), str(generator), "--execution-profile",
        "classic_existing_cluster", "--classic-cluster-readback", str(cluster_readback_path),
        "--trial-id", args.trial_id, "--notebook-root", args.notebook_root,
        "--local-receipt", str(Path(args.local_receipt).resolve(strict=True)),
        "--bun", str(Path(args.bun).resolve(strict=True)),
        "--python", str(Path(args.python).expanduser()),
        "--remaining-wall-seconds", str(remaining_seconds),
        "--evidence-dir", args.evidence_dir, "--output", str(config_path)]
    for dependency in args.dependency:
        argv.extend(["--dependency", dependency])
    code, output = _run_process(argv, deadline=deadline, output_cap=65536)
    _require(code == 0, "Classic config generator failed: " + output[-2048:].decode("utf-8", "replace"))
    printed = output.decode("utf-8", "strict").strip()
    _require(printed == str(config_path),
             "Classic config generator did not identify the exact output config")
    config = _read_json(config_path)
    _require(config.get("execution_profile") == "classic_existing_cluster"
             and config.get("maximum_wall_seconds") == remaining_seconds
             and config.get("expected_runtime", {}).get("clusterId") == runtime["cluster_id"]
             and config.get("classic_cluster", {}).get("cluster_id") == runtime["cluster_id"]
             and config.get("classic_cluster", {}).get("policy_id") == POLICY_ID
             and config.get("classic_cluster", {}).get("owner") == runtime["single_user_name"]
             and config.get("cleanup_reserve_seconds") == RUNNER_CLEANUP_RESERVE_SECONDS,
             "Generated runner config does not carry exact live cluster binding/deadline")
    return config


def _run_managed_runner(args, config_path: Path, deadline: float, root: Path) -> dict:
    runner = REPO / "tools/run_managed_a1_serverless.py"
    _require(runner.is_file() and not runner.is_symlink(),
             "Existing managed A1 runner is absent")
    argv = [str(Path(args.python).expanduser()), str(runner), "--config", str(config_path),
            "--profile", PROFILE, "--launch"]
    code, output = _run_process(argv, deadline=deadline,
                                output_cap=MAX_COMMAND_OUTPUT, interrupt_first=True)
    _write_private(root / "managed-runner.stdout.json", output)
    result = _parse_json_bytes(output, "managed A1 runner result")
    return {"returncode": code, "result": result, "stdout_sha256": _sha(output),
            "stdout_bytes": len(output), "status": result.get("status")}


def _runner_wall_seconds(global_deadline: float, now: float) -> int:
    remaining = int(global_deadline - now)
    _require(remaining > CLUSTER_CLEANUP_RESERVE_SECONDS + RUNNER_CLEANUP_RESERVE_SECONDS,
             "Insufficient global wall time remains for inner and cluster cleanup reserves")
    return remaining - CLUSTER_CLEANUP_RESERVE_SECONDS


def _create_outcome_unknown(state: dict, cluster_id: str | None,
                            cleanup: dict) -> bool:
    return (state.get("create_attempted") is True and cluster_id is None
            and state.get("create_response_received") is not True
            and cleanup.get("status") == "no_matching_owned_cluster")


def _write_stop(root: Path) -> None:
    path = root / "watchdog.stop"
    try:
        fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                     getattr(os, "O_NOFOLLOW", 0), 0o600)
        os.write(fd, b"stop\n")
        os.fsync(fd)
        os.close(fd)
        dirfd = os.open(root, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
        try:
            os.fsync(dirfd)
        finally:
            os.close(dirfd)
    except FileExistsError:
        pass


def _execute(args) -> int:
    spec, local, root, python = _local_preflight(args)
    root.mkdir(mode=0o700)
    _require(root.stat().st_uid == os.getuid() and root.stat().st_mode & 0o077 == 0,
             "Owner-private output root required")
    state_path = root / "state.json"
    receipt_path = root / "trial-receipt.json"
    create_spec_path = args.cluster_create_spec
    start_mono = time.monotonic()
    start_wall = time.time()
    deadline = start_mono + GLOBAL_WALL_SECONDS
    cleanup_cutoff = deadline - CLUSTER_CLEANUP_RESERVE_SECONDS
    spec_hash = _sha(create_spec_path.read_bytes())
    state = {"profile": "ashlar-managed-classic-a1-trial-state/0.1",
        "status": "running", "trial_id": args.trial_id,
        "create_spec_path": str(create_spec_path), "create_spec_sha256": spec_hash,
        "output_root": str(root), "owner_pid": os.getpid(),
        "started_epoch_ms": int(start_wall * 1000),
        "hard_deadline_mono": deadline,
        "work_cutoff_mono": cleanup_cutoff,
        "create_attempted": False, "create_api_submitted": False,
        "state_path": str(state_path), "cluster_id": None}
    _write_json_new(state_path, state)
    _write_json_new(receipt_path, {"profile": "ashlar-managed-classic-a1-trial/0.1",
        "status": "running", "trial_id": args.trial_id,
        "create_spec_sha256": spec_hash, "cli": local["cli"],
        "cli_version": local["cli_version"], "python": local["python"],
        "bun": local["bun"], "local_receipt": local["local_receipt"],
        "limits": {"global_wall_seconds": GLOBAL_WALL_SECONDS,
                   "outer_cluster_cleanup_reserve_seconds": CLUSTER_CLEANUP_RESERVE_SECONDS,
                   "runner_cleanup_reserve_seconds": RUNNER_CLEANUP_RESERVE_SECONDS}})
    watchdog = None
    keep_watchdog_for_unknown_create = False
    cluster_id = None
    result = {"status": "failed", "cluster_cleanup": None}
    old_handler = signal.getsignal(signal.SIGALRM)
    old_timer = signal.getitimer(signal.ITIMER_REAL)
    def expired(_signum, _frame):
        raise TimeoutError("Global 900-second classic A1 deadline expired")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, GLOBAL_WALL_SECONDS)
    try:
        watchdog = _spawn_watchdog(state_path, root)
        state["watchdog_pid"] = watchdog.pid
        _atomic_json(state_path, state)
        auth = _parse_json_bytes(_databricks("auth", "describe", deadline=cleanup_cutoff),
                                 "Databricks CLI auth describe")
        result["workspace_profile_host"] = _admit_profile_host(auth)
        cluster = _create_or_find(spec, create_spec_path, cleanup_cutoff, state, state_path)
        cluster_id = cluster["cluster_id"]
        _require(cluster.get("state") not in ("ERROR", "TERMINATED"),
                 "Owned cluster failed while CREATE was submitted")
        cluster = _wait_running(cluster_id, spec, cleanup_cutoff, state)
        readback_path = root / "classic-cluster-get.json"
        raw_readback = _canonical(cluster) + b"\n"
        _write_private(readback_path, raw_readback)
        # The config's own 60s cleanup reserve is inside runner_wall.  Only
        # subtract the distinct outer cluster cleanup reserve from the global
        # clock, and never reset the absolute cutoff after local generation.
        runner_wall = _runner_wall_seconds(deadline, time.monotonic())
        # The generated runner's wall limit is measured from its own start;
        # this outer absolute deadline also charges local config generation.
        runner_deadline = time.monotonic() + runner_wall
        config_path = root / "managed-a1-classic-config.json"
        config = _run_generator(args, readback_path, config_path, runner_wall,
                                cleanup_cutoff, cluster)
        result["generated_config_sha256"] = _sha(config_path.read_bytes())
        result["cluster_readback_sha256"] = _sha(raw_readback)
        result["cluster_id"] = cluster_id
        result["runner_wall_seconds"] = config["maximum_wall_seconds"]
        result["runner"] = _run_managed_runner(args, config_path, runner_deadline, root)
        runner_result = result["runner"]["result"]
        if (result["runner"]["returncode"] != 0
                or result["runner"]["status"] != "acknowledged_and_cleaned"
                or runner_result.get("schema_absent_after_cleanup") is not True
                or runner_result.get("source_holder_closed") is not True):
            raise RuntimeError("Managed A1 runner did not report a successful exact trial")
        result["status"] = "passed"
    except BaseException as error:
        result["status"] = "failed"
        result["error"] = {"type": type(error).__name__, "message": str(error)[:1500]}
    finally:
        cleanup_deadline = deadline
        if state.get("create_attempted") and state.get("create_api_submitted") is not False:
            cleanup = _cleanup_owned_cluster(cluster_id, spec, cleanup_deadline)
            result["cluster_cleanup"] = cleanup
            if cleanup.get("status") not in ("deleted", "no_matching_owned_cluster"):
                result["status"] = "failed_cleanup_incomplete"
            if _create_outcome_unknown(state, cluster_id, cleanup):
                result["cluster_cleanup"] = {"status": "create_outcome_unknown_no_cluster_visible",
                    "last_reconciliation": cleanup}
                result["status"] = "failed_cleanup_incomplete"
                keep_watchdog_for_unknown_create = True
        elif state.get("create_outcome") == "pre_api_cli_argument_refusal":
            result["cluster_cleanup"] = {"status": "create_not_submitted",
                "reason": "databricks_cli_argument_parser_refusal"}
        if watchdog is not None:
            if not keep_watchdog_for_unknown_create:
                _write_stop(root)
                try:
                    watchdog.wait(timeout=max(1, min(10, deadline - time.monotonic())))
                except subprocess.TimeoutExpired:
                    # The child is this run's exact owned watchdog process group.
                    _stop_owned_process(watchdog)
                    result["watchdog_cleanup"] = "watchdog_did_not_exit_after_stop_signal"
        result["elapsed_seconds"] = round(time.monotonic() - start_mono, 3)
        result["hard_deadline_seconds"] = GLOBAL_WALL_SECONDS
        if time.monotonic() > deadline:
            result["status"] = "failed_hard_deadline_exceeded"
        try:
            _atomic_json(receipt_path, {**_read_json(receipt_path), **result})
        except Exception:
            result["receipt_write_error"] = "atomic final receipt update failed"
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, old_handler)
        if old_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, old_timer[0], old_timer[1])
    print(json.dumps(result, sort_keys=True, indent=2))
    return 0 if result.get("status") == "passed" else 2


def _canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def main(argv: list[str] | None = None) -> int:
    argv = list(sys.argv[1:] if argv is None else argv)
    # The watchdog is launched as a deliberately small child command.  Dispatch
    # it before parsing the launch-only arguments, which it must not receive.
    if len(argv) == 2 and argv[0] == "--watchdog-state":
        return _watchdog(Path(argv[1]))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--cluster-create-spec", type=Path, default=CREATE_SPEC)
    parser.add_argument("--profile", default=PROFILE)
    parser.add_argument("--trial-id", default=TRIAL_ID)
    parser.add_argument("--notebook-root", required=True)
    parser.add_argument("--local-receipt", type=Path, required=True)
    parser.add_argument("--dependency", action="append", default=[])
    parser.add_argument("--evidence-dir", default="docs/helix/04-build/evidence/current-upstream-release-20261102")
    parser.add_argument("--bun", type=Path, required=True)
    parser.add_argument("--python", type=Path, default=Path(sys.executable))
    parser.add_argument("--output-root", type=Path, required=True)
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--preflight-only", action="store_true")
    group.add_argument("--launch", action="store_true",
                       help="Create the exact owned cluster and run one bounded trial")
    args = parser.parse_args(argv)
    try:
        spec, local, root, _python = _local_preflight(args)
        if not args.launch:
            print(json.dumps({"status": "preflight_passed_no_resource_effects",
                "create_spec_sha256": _sha(args.cluster_create_spec.read_bytes()),
                "trial_id": args.trial_id, "profile": PROFILE,
                "cli_version": local["cli_version"], "create_spec": spec,
                "selected_python": local["python"],
                "output_root_absent": not root.exists()}, sort_keys=True, indent=2))
            return 0
        return _execute(args)
    except Exception as error:
        print(json.dumps({"status": "refused", "type": type(error).__name__,
                          "message": str(error)[:1500]}, sort_keys=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
