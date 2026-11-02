#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Submit, monitor, hand off, and clean up one bounded managed A1 trial.

The JSON input is an explicit lock for the two notebook artifacts, their
serverless environments and task parameters. Validation is inert. Network
mutations require --launch; this program never selects a workspace, runtime,
wheel, source page, or cost estimate on the caller's behalf.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import re
import socket
import struct
import subprocess
import sys
import tempfile
import time
import signal
from pathlib import Path


HOST = "https://adb-7405607548213398.18.azuredatabricks.net"
PROFILE = "aidev-cus"
MAX_SECONDS = 900
HARD_USD = 100.0
PREFERRED_USD = 10.0
IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")
MAX_HOLDER_FRAME = 48 * 1024 * 1024


def require(ok: bool, message: str) -> None:
    if not ok:
        raise ValueError(message)


def _workspace_file(path: object, suffix: str) -> bool:
    return (type(path) is str and "\0" not in path
            and (path.startswith("/Workspace/") or path.startswith("/Volumes/"))
            and path.endswith(suffix))


def _validate_task(task: object, key: str) -> dict:
    require(type(task) is dict and set(task) == {
        "notebook_path", "notebook_sha256", "environment_version",
        "dependencies", "base_parameters", "timeout_seconds",
    }, f"Closed {key} task artifact lock required")
    require(_workspace_file(task["notebook_path"], "")
            and task["notebook_path"].startswith("/Workspace/"),
            f"{key} notebook must be an absolute Workspace path")
    require(type(task["notebook_sha256"]) is str and SHA.fullmatch(task["notebook_sha256"]),
            f"{key} notebook SHA-256 required")
    require(type(task["environment_version"]) is int and task["environment_version"] > 0,
            f"Read-back-confirmed {key} serverless environment version required")
    deps = task["dependencies"]
    require(type(deps) is list and len(deps) > 0,
            f"Exact {key} wheel/environment artifacts required")
    paths = []
    for item in deps:
        require(type(item) is dict and set(item) == {"path", "sha256"}
                and type(item["path"]) is str and "\0" not in item["path"]
                and (item["path"].startswith("/Workspace/")
                     or item["path"].startswith("/Volumes/")
                     or item["path"].startswith("-r /Workspace/")
                     or item["path"].startswith("-r /Volumes/"))
                and type(item["sha256"]) is str and SHA.fullmatch(item["sha256"]),
                f"Each {key} dependency needs an immutable path and SHA-256")
        paths.append(item["path"])
        filename = item["path"].removeprefix("-r ").rsplit("/", 1)[-1]
        require(filename.endswith((".whl", ".txt")),
                f"{key} dependency must be a wheel or immutable requirements file")
    require(len(paths) == len(set(paths)), f"Duplicate {key} dependency path")
    params = task["base_parameters"]
    require(type(params) is dict and params and all(
        type(k) is str and k and type(v) is str and "\0" not in v
        for k, v in params.items()), f"Explicit string-only {key} task parameters required")
    require(type(task["timeout_seconds"]) is int and 1 <= task["timeout_seconds"] <= MAX_SECONDS,
            f"{key} timeout must be finite and at most 900 seconds")
    return {"task_key": key, "depends_on": [],
            "timeout_seconds": task["timeout_seconds"], "max_retries": 0,
            "retry_on_timeout": False, "disable_auto_optimization": True,
            "notebook_task": {"notebook_path": task["notebook_path"],
                              "base_parameters": params},
            "environment_key": key + "_env"}


def build_payload(config: object) -> tuple[dict, dict]:
    require(type(config) is dict and set(config) == {
        "host", "trial_id", "catalog", "schema", "expected_principal",
        "expected_runtime", "maximum_wall_seconds", "cleanup_reserve_seconds", "cost", "publisher",
        "initializer", "verifier", "holder_command", "volume_name", "publisher_result_parameter",
        "local_qualification", "evidence_dir",
    }, "Closed managed A1 runner input required")
    require(config["host"] == HOST,
            "A1 runner is pinned to the approved dbw-aidev-cus workspace")
    require(type(config["trial_id"]) is str
            and re.fullmatch(r"a1-[a-z0-9][a-z0-9-]{0,49}", config["trial_id"]),
            "Unique bounded A1 trial ID required")
    require(config["catalog"] == "client_dev" and type(config["schema"]) is str
            and IDENT.fullmatch(config["schema"])
            and config["schema"].startswith("ashlar_a1_"),
            "Use one newly owned ashlar_a1_* schema under client_dev")
    require(type(config["expected_principal"]) is str and config["expected_principal"].strip(),
            "Exact admitted Jobs principal required")
    runtime = config["expected_runtime"]
    runtime_keys = {"runtimeName", "sparkVersion", "sparkBuildText", "runtimeTag",
                    "principal", "catalog", "schema", "protocol"}
    require(type(runtime) is dict and set(runtime) == runtime_keys
            and runtime.get("runtimeName") == "databricks-serverless"
            and runtime.get("principal") == config["expected_principal"]
            and runtime.get("catalog") == "client_dev"
            and runtime.get("schema") == config["schema"]
            and type(runtime.get("sparkVersion")) is str and runtime["sparkVersion"]
            and (type(runtime.get("sparkBuildText")) is str and runtime["sparkBuildText"]
                 or type(runtime.get("runtimeTag")) is str and runtime["runtimeTag"])
            and type(runtime.get("protocol")) is str and runtime["protocol"],
            "Exact serverless runtime/build, principal, protocol and destination pins required")
    wall = config["maximum_wall_seconds"]
    require(type(wall) is int and 1 <= wall <= MAX_SECONDS,
            "Whole-run deadline, including startup, handoff and cleanup, must be <=900s")
    cleanup_seconds = config["cleanup_reserve_seconds"]
    require(type(cleanup_seconds) is int and 30 <= cleanup_seconds < wall,
            "At least 30 seconds must be reserved within the whole-run deadline for cancel/cleanup")
    cost = config["cost"]
    require(type(cost) is dict and set(cost) == {
        "estimated_total_dbu", "cleanup_reserve_dbu", "authorized_total_dbu",
        "estimated_total_usd", "cleanup_reserve_usd", "preferred_budget_exception",
        "includes_startup_driver_verifier_handoff_and_cleanup",
    }, "Complete whole-trial cost estimate and cleanup reserve required")
    nums = ("estimated_total_dbu", "cleanup_reserve_dbu", "authorized_total_dbu",
            "estimated_total_usd", "cleanup_reserve_usd")
    require(all(type(cost[k]) in (int, float) and 0 <= cost[k] < float("inf")
                for k in nums), "Finite nonnegative DBU and USD estimates required")
    require(cost["estimated_total_dbu"] > 0 and cost["authorized_total_dbu"] > 0
            and cost["estimated_total_dbu"] + cost["cleanup_reserve_dbu"]
                <= cost["authorized_total_dbu"]
            and cost["includes_startup_driver_verifier_handoff_and_cleanup"] is True,
            "DBU bound must include startup, both tasks, holder handoff, and cleanup")
    total_usd = cost["estimated_total_usd"] + cost["cleanup_reserve_usd"]
    require(cost["estimated_total_usd"] > 0 and cost["cleanup_reserve_usd"] >= 0
            and total_usd <= HARD_USD,
            "Estimated whole-trial spend plus cleanup reserve exceeds $100")
    if total_usd > PREFERRED_USD:
        require(type(cost["preferred_budget_exception"]) is str
                and cost["preferred_budget_exception"].strip(),
                "Estimate above $10 needs an owner rationale")
    else:
        require(cost["preferred_budget_exception"] is None,
                "Omit preferred-budget exception when total estimate is <=$10")
    holder = config["holder_command"]
    require(type(holder) is list and holder and all(
        type(x) is str and x and "\0" not in x for x in holder),
        "Local source-holder worker command required for inherited Unix IPC")
    require(type(config["volume_name"]) is str and IDENT.fullmatch(config["volume_name"])
            and config["volume_name"].startswith("ashlar_a1_"),
            "Unique owned managed-volume name required inside the per-run schema")
    require(type(config["publisher_result_parameter"]) is str
            and re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]{0,127}", config["publisher_result_parameter"])
            and config["publisher_result_parameter"] in config["verifier"]["base_parameters"],
            "Verifier notebook must declare its publisher-result JSON parameter")
    gate = config["local_qualification"]
    require(type(gate) is dict and set(gate) == {"path", "sha256"}
            and type(gate["path"]) is str and type(gate["sha256"]) is str
            and SHA.fullmatch(gate["sha256"])
            and not os.path.isabs(gate["path"])
            and ".." not in Path(gate["path"]).parts,
            "Exact local A1 qualification receipt path and SHA-256 required before launch")
    evidence_dir = config["evidence_dir"]
    require(type(evidence_dir) is str and evidence_dir
            and not os.path.isabs(evidence_dir) and ".." not in Path(evidence_dir).parts,
            "Evidence directory must be a repository-relative owned path")

    stage_payloads = {}
    for key in ("initializer", "publisher", "verifier"):
        task_payload = _validate_task(config[key], key)
        task = config[key]
        require(task["timeout_seconds"] + 15 <= wall - cleanup_seconds,
                f"{key} task budget must fit with startup/holder/cleanup reserve")
        env = {"environment_key": key + "_env", "spec": {
            "environment_version": str(task["environment_version"]),
            "dependencies": [item["path"] for item in task["dependencies"]],
        }}
        stage_payloads[key] = {"task": task_payload, "environment": env}
    require(sum(config[k]["timeout_seconds"] for k in ("initializer", "publisher", "verifier")) + 45
            <= wall - cleanup_seconds,
            "All three bounded runs plus holder handoffs must fit before cleanup reserve")
    return stage_payloads, config


class DatabricksCLI:
    def __init__(self, profile: str):
        self.profile = profile

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        argv = ["databricks", "--profile", self.profile, "--output", "json", "api", method, path]
        if body is not None:
            argv += ["--json", json.dumps(body, sort_keys=True, separators=(",", ":"))]
        cp = subprocess.run(argv, capture_output=True, text=True, check=False)
        if cp.returncode:
            raise RuntimeError(f"Databricks API {method.upper()} {path} failed ({cp.returncode}): "
                               f"{cp.stderr.strip()[:1200]}")
        if not cp.stdout.strip():
            return {}
        result = json.loads(cp.stdout)
        require(type(result) is dict, f"Databricks API {path} returned a non-object response")
        return result

    def copy(self, source: str, target: str) -> None:
        cp = subprocess.run(["databricks", "--profile", self.profile,
                             "fs", "cp", source, target],
                            capture_output=True, text=True, check=False)
        if cp.returncode:
            raise RuntimeError("Databricks Files copy failed: " + cp.stderr.strip()[:1200])

    def export_workspace(self, path: str, target: str, *, notebook: bool) -> None:
        fmt = "SOURCE" if notebook else "RAW"
        cp = subprocess.run(["databricks", "--profile", self.profile, "workspace", "export",
                             path, "--format", fmt, "--file", target],
                            capture_output=True, text=True, check=False)
        if cp.returncode:
            raise RuntimeError("Databricks Workspace export failed: " + cp.stderr.strip()[:1200])

    def auth_readback(self) -> dict:
        cp = subprocess.run(["databricks", "--profile", self.profile,
                             "--output", "json", "auth", "describe"],
                            capture_output=True, text=True, check=False)
        if cp.returncode:
            raise RuntimeError("Databricks CLI profile authentication refused: "
                               + cp.stderr.strip()[:1200])
        value = json.loads(cp.stdout)
        require(type(value) is dict, "Databricks auth describe returned no profile identity")
        return value


class SourceHolderIPC:
    """Framed inherited-FD parent for the source-owned long-lived holder."""

    def __init__(self, process: subprocess.Popen, connection: socket.socket):
        self.process = process
        self.connection = connection

    @classmethod
    def start(cls, command: list[str], deadline: float) -> tuple["SourceHolderIPC", bytes]:
        parent, child = socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)
        child_fd = child.fileno()
        preexec = None if child_fd == 3 else (lambda: os.dup2(child_fd, 3))
        try:
            process = subprocess.Popen(command + ["--fd", "3"], pass_fds=(child_fd,),
                preexec_fn=preexec, start_new_session=True, stdin=subprocess.DEVNULL,
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL, close_fds=True)
        except BaseException:
            parent.close(); child.close()
            raise
        child.close()
        ipc = cls(process, parent)
        try:
            genesis = ipc.receive(deadline)
            require(type(genesis) is dict and set(genesis) == {
                "type", "genesis_bytes_base64"} and genesis["type"] == "genesis"
                and type(genesis["genesis_bytes_base64"]) is str,
                "Source holder did not provide its closed A/B genesis package")
            package = base64.b64decode(genesis["genesis_bytes_base64"], validate=True)
            require(base64.b64encode(package).decode("ascii") == genesis["genesis_bytes_base64"]
                    and package and len(package) <= 36 * 1024 * 1024,
                    "Source-holder A/B genesis package exceeds its canonical frame bound")
            return ipc, package
        except BaseException:
            try: ipc.close(deadline, orderly=True)
            except Exception: pass
            raise

    def open_a1(self, request_bytes: bytes, publication_id: str,
                destination_id: str, deadline: float) -> tuple[bytes, dict]:
        response = self.rpc({"type": "open",
            "request_bytes_base64": base64.b64encode(request_bytes).decode("ascii"),
            "publication_id": publication_id, "destination_id": destination_id},
            "ready", deadline)
        require(set(response) == {"type", "snapshot_bytes_base64", "custody_digests"}
                and type(response["snapshot_bytes_base64"]) is str
                and type(response["custody_digests"]) is dict,
                "Source-holder ready frame omitted exact opened custody anchors")
        raw = base64.b64decode(response["snapshot_bytes_base64"], validate=True)
        require(raw and base64.b64encode(raw).decode("ascii") == response["snapshot_bytes_base64"]
                and len(raw) <= 36 * 1024 * 1024,
                "Source-holder A1 page snapshot exceeds its canonical frame bound")
        return raw, response["custody_digests"]

    def send(self, message: dict, deadline: float) -> None:
        payload = json.dumps(message, sort_keys=True, separators=(",", ":"),
                             ensure_ascii=False, allow_nan=False).encode("utf-8")
        require(len(payload) <= 24 * 1024 * 1024,
                "Source-holder IPC command exceeds 24 MiB")
        remaining = deadline - time.monotonic()
        require(remaining > 0, "Whole-trial deadline expired before source-holder IPC")
        self.connection.settimeout(remaining)
        self.connection.sendall(struct.pack(">I", len(payload)) + payload)

    def receive(self, deadline: float) -> dict:
        def exact(count: int) -> bytes:
            parts = []
            remaining = count
            while remaining:
                timeout = deadline - time.monotonic()
                require(timeout > 0, "Whole-trial deadline expired waiting for source holder")
                self.connection.settimeout(timeout)
                chunk = self.connection.recv(remaining)
                require(chunk, "Source-holder IPC closed before a complete frame")
                parts.append(chunk); remaining -= len(chunk)
            return b"".join(parts)
        frame_size = struct.unpack(">I", exact(4))[0]
        require(0 < frame_size <= MAX_HOLDER_FRAME,
                "Source-holder IPC frame exceeds its 48 MiB bound")
        raw = exact(frame_size)
        def unique(items):
            result = {}
            for key, value in items:
                require(key not in result, "Duplicate member in source-holder IPC frame")
                result[key] = value
            return result
        message = json.loads(raw.decode("utf-8"), object_pairs_hook=unique,
                             parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite")))
        require(type(message) is dict and type(message.get("type")) is str,
                "Source-holder IPC frame must be a typed JSON object")
        return message

    def rpc(self, message: dict, expected_type: str, deadline: float) -> dict:
        self.send(message, deadline)
        response = self.receive(deadline)
        require(response.get("type") == expected_type,
                f"Expected source-holder {expected_type} event after {message.get('type')}")
        return response

    def close(self, deadline: float, *, orderly: bool) -> dict | None:
        response = None
        try:
            if orderly and self.process.poll() is None:
                response = self.rpc({"type": "close"}, "closed", deadline)
        finally:
            self.connection.close()
            if self.process.poll() is None:
                try:
                    self.process.wait(timeout=max(0.1, deadline - time.monotonic()))
                except subprocess.TimeoutExpired:
                    try: os.killpg(self.process.pid, signal.SIGTERM)
                    except ProcessLookupError: pass
                    try:
                        self.process.wait(timeout=2)
                    except subprocess.TimeoutExpired:
                        try: os.killpg(self.process.pid, signal.SIGKILL)
                        except ProcessLookupError: pass
                        self.process.wait(timeout=2)
        return response


def _terminal(state: dict) -> bool:
    life = state.get("life_cycle_state")
    return life in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _job_settings(config: dict, stage: str, task_payload: dict) -> dict:
    return {
        "name": "ashlar-a1-" + config["trial_id"] + "-" + stage,
        "run_as": {"user_name": config["expected_principal"]},
        "tags": {"project": "ashlar", "purpose": "managed-current-a1",
                 "ashlar_trial_id": config["trial_id"]},
        "max_concurrent_runs": 1,
        "max_retries": 0,
        "retry_on_timeout": False,
        "timeout_seconds": config[stage]["timeout_seconds"],
        "tasks": [task_payload["task"]],
        "environments": [task_payload["environment"]],
    }


def _admit_job_readback(job: dict, expected: dict, trial_id: str) -> None:
    settings = job.get("settings")
    require(type(settings) is dict, "Jobs GET omitted settings")
    for key in ("name", "run_as", "max_concurrent_runs", "max_retries",
                "retry_on_timeout", "timeout_seconds"):
        require(settings.get(key) == expected[key],
                "Jobs GET differs from submitted one-shot contract at " + key)
    require(settings.get("tags", {}).get("ashlar_trial_id") == trial_id,
            "Jobs GET omitted the unique A1 trial tag")
    require(not any(key in settings for key in ("schedule", "trigger", "triggers",
                                                  "job_clusters", "compute")),
            "A1 task must be unscheduled and serverless without classic compute")
    tasks = settings.get("tasks")
    expected_tasks = {item["task_key"]: item for item in expected["tasks"]}
    require(type(tasks) is list and len(tasks) == len(expected_tasks) == 1,
            "Jobs GET task count differs from one-stage A1 workflow")
    for actual in tasks:
        key = actual.get("task_key") if type(actual) is dict else None
        require(key in expected_tasks, "Jobs GET returned an unselected A1 task")
        wanted = expected_tasks[key]
        require(actual.get("notebook_task") == wanted["notebook_task"]
                and actual.get("environment_key") == wanted["environment_key"]
                and actual.get("timeout_seconds") == wanted["timeout_seconds"]
                and actual.get("max_retries", 0) == 0
                and actual.get("retry_on_timeout", False) is False
                and actual.get("disable_auto_optimization") is True
                and actual.get("depends_on", []) == wanted["depends_on"]
                and not any(field in actual for field in
                            ("existing_cluster_id", "job_cluster_key", "new_cluster", "sql_warehouse_id")),
                "Jobs GET task differs from its pinned serverless artifact lock: " + str(key))
    envs = settings.get("environments")
    require(type(envs) is list and {item.get("environment_key"): item for item in envs}
            == {item["environment_key"]: item for item in expected["environments"]},
            "Jobs GET environment versions/dependencies differ from exact artifact locks")
    require(job.get("run_as_user_name") == expected["run_as"]["user_name"],
            "Effective Jobs run-as principal differs from exact admission")


def _list_schema(api: DatabricksCLI, catalog: str, schema_name: str,
                 deadline: float) -> dict | None:
    token = None
    while True:
        require(time.monotonic() < deadline, "Deadline expired while checking schema inventory")
        query = f"catalog_name={catalog}&max_results=100"
        if token:
            from urllib.parse import quote
            query += "&page_token=" + quote(token, safe="")
        page = api.call("get", "/api/2.1/unity-catalog/schemas?" + query)
        for item in page.get("schemas", []):
            if item.get("name") == schema_name:
                return item
        token = page.get("next_page_token")
        if not token:
            return None


def _list_owned_tables(api: DatabricksCLI, config: dict, deadline: float) -> list[dict]:
    from urllib.parse import quote
    query = "catalog_name=" + quote(config["catalog"], safe="") + "&schema_name=" + quote(config["schema"], safe="") + "&max_results=100"
    token = None
    rows = []
    while True:
        require(time.monotonic() < deadline, "Deadline expired while checking owned table inventory")
        path = "/api/2.1/unity-catalog/tables?" + query
        if token:
            path += "&page_token=" + quote(token, safe="")
        page = api.call("get", path)
        rows.extend(page.get("tables", []))
        token = page.get("next_page_token")
        if not token:
            return rows


def _task_output(api: DatabricksCLI, run_id: int) -> dict:
    output = api.call("get", f"/api/2.2/jobs/runs/get-output?run_id={run_id}")
    notebook = output.get("notebook_output")
    result = notebook.get("result") if type(notebook) is dict else None
    require(type(result) is str and result,
            "Terminal task omitted its bounded notebook result")
    value = json.loads(result)
    require(type(value) is dict, "A1 task result must be a JSON object")
    return value


def _check_artifact_lock(api: DatabricksCLI, config: dict) -> None:
    """Hash current remote bytes before creating the schema or any Jobs object."""
    for name in ("initializer", "publisher", "verifier"):
        task = config[name]
        fd, local = tempfile.mkstemp(prefix="ashlar-a1-artifact-")
        os.close(fd)
        try:
            api.export_workspace(task["notebook_path"], local, notebook=True)
            actual = hashlib.sha256(Path(local).read_bytes()).hexdigest()
            require(actual == task["notebook_sha256"],
                    f"{name} notebook exported bytes differ from supplied SHA-256")
        finally:
            try: os.unlink(local)
            except FileNotFoundError: pass
        for dep in task["dependencies"]:
            raw_path = dep["path"][3:] if dep["path"].startswith("-r ") else dep["path"]
            fd, local = tempfile.mkstemp(prefix="ashlar-a1-dependency-")
            os.close(fd)
            try:
                if raw_path.startswith("/Workspace/"):
                    api.export_workspace(raw_path, local, notebook=False)
                else:
                    api.copy("dbfs:" + raw_path, local)
                actual = hashlib.sha256(Path(local).read_bytes()).hexdigest()
                require(actual == dep["sha256"],
                        f"{name} dependency bytes differ from supplied SHA-256: {dep['path']}")
            finally:
                try: os.unlink(local)
                except FileNotFoundError: pass


def _validate_task_runtime(value: dict, expected: dict, stage: str) -> None:
    identity = value.get("runtimeIdentity")
    require(type(identity) is dict and type(identity.get("opening")) is dict,
            f"{stage} output omitted its observed serverless runtime identity")
    opening = identity["opening"]
    for key in ("runtimeName", "principal", "catalog", "schema", "protocol"):
        require(opening.get(key) == expected[key],
                f"{stage} observed runtime differs at {key}")
    for key in ("sparkVersion", "sparkBuildText", "runtimeTag"):
        if expected[key] is not None:
            require(opening.get(key) == expected[key],
                    f"{stage} observed runtime differs at {key}")


def _run_stage(api: DatabricksCLI, config: dict, stage: str, stage_payload: dict,
               parameters: dict, deadline: float) -> tuple[int, int, dict, dict]:
    task = dict(stage_payload["task"])
    notebook_task = dict(task["notebook_task"])
    notebook_task["base_parameters"] = parameters
    task["notebook_task"] = notebook_task
    job_settings = _job_settings(config, stage,
                                 {"task": task, "environment": stage_payload["environment"]})
    created = api.call("post", "/api/2.2/jobs/create", job_settings)
    job_id = created.get("job_id")
    require(type(job_id) is int and job_id > 0, f"{stage} Jobs create omitted job ID")
    run_id = None
    run_attempted = False
    snapshot = {}
    try:
        readback = api.call("get", f"/api/2.2/jobs/get?job_id={job_id}")
        _admit_job_readback(readback, job_settings, config["trial_id"])
        require(time.monotonic() < deadline, f"Deadline expired before {stage} run-now")
        run_attempted = True
        token = hashlib.sha256((config["trial_id"] + ":" + stage).encode()).hexdigest()
        started = api.call("post", "/api/2.2/jobs/run-now", {
            "job_id": job_id, "idempotency_token": token})
        run_id = started.get("run_id")
        require(type(run_id) is int and run_id > 0,
                f"{stage} run-now response omitted run ID")
        while True:
            require(time.monotonic() < deadline,
                    f"{stage} work deadline reached; cancel requested")
            snapshot = api.call("get", f"/api/2.2/jobs/runs/get?run_id={run_id}")
            if _terminal(snapshot.get("state", {})):
                break
            time.sleep(min(3, max(0.2, deadline - time.monotonic())))
        require(snapshot.get("state", {}).get("result_state") == "SUCCESS",
                f"{stage} task did not terminate successfully")
        task_rows = snapshot.get("tasks", [])
        task_run_id = run_id
        if task_rows:
            require(type(task_rows) is list and len(task_rows) == 1
                    and task_rows[0].get("task_key") == stage
                    and type(task_rows[0].get("run_id")) is int,
                    f"{stage} terminal run omitted its single exact task identity")
            task_run_id = task_rows[0]["run_id"]
        output = _task_output(api, task_run_id)
        api.call("delete", "/api/2.2/jobs/delete", {"job_id": job_id})
        return run_id, task_run_id, snapshot, output
    except BaseException:
        if run_id is not None:
            try:
                snapshot = api.call("get", f"/api/2.2/jobs/runs/get?run_id={run_id}")
                if not _terminal(snapshot.get("state", {})):
                    api.call("post", "/api/2.2/jobs/runs/cancel", {"run_id": run_id})
                    while time.monotonic() < deadline:
                        snapshot = api.call("get", f"/api/2.2/jobs/runs/get?run_id={run_id}")
                        if _terminal(snapshot.get("state", {})):
                            break
                        time.sleep(min(2, max(0.2, deadline - time.monotonic())))
            except Exception:
                pass
        if not run_attempted or (run_id is not None and _terminal(snapshot.get("state", {}))):
            try:
                api.call("delete", "/api/2.2/jobs/delete", {"job_id": job_id})
            except Exception:
                pass
        raise


def _publisher_bundle(result: dict, authority: str) -> tuple[dict, bytes, bytes, bytes, dict, str]:
    require(set(result) == {"profile", "runtimeIdentity", "predictiveOptimization",
                            "requestBytesBase64", "manifestBytesBase64",
                            "destinationReceiptBytesBase64", "receipt"}
            and result.get("profile") == "ashlar-managed-a1-publisher-result/0.1",
            "Publisher output must use the closed A1 publisher result profile")
    require(result.get("predictiveOptimization") == "ENABLE",
            "Publisher did not observe Unity Catalog predictive optimization ENABLE")
    try:
        request_bytes = base64.b64decode(result["requestBytesBase64"], validate=True)
        manifest_bytes = base64.b64decode(result["manifestBytesBase64"], validate=True)
        receipt_bytes = base64.b64decode(result["destinationReceiptBytesBase64"], validate=True)
    except (ValueError, TypeError, KeyError) as error:
        raise ValueError("Publisher output exact request, manifest and source receipt bytes required") from error
    require(all(raw and len(raw) <= 4 * 1024 * 1024
                for raw in (request_bytes, manifest_bytes, receipt_bytes)),
            "Publisher exact request/manifest/receipt bytes exceed their bound")
    task_receipt = result["receipt"]
    require(type(task_receipt) is dict,
            "Publisher output must carry the existing managed destination receipt")
    receipt = task_receipt
    manifest = receipt.get("manifest")
    vector = receipt.get("dataRoleVector")
    require(receipt.get("profile") == "ashlar-managed-current-destination-receipt/0.1"
            and type(manifest) is dict and type(vector) is dict
            and type(receipt.get("manifestSha256")) is str
            and type(receipt.get("requestDigest")) is str,
            "Publisher result omitted exact manifest, four-role vector and receipt anchors")
    require(json.loads(manifest_bytes.decode("utf-8")) == manifest,
            "Publisher exact manifest bytes differ from the destination receipt")
    request = json.loads(request_bytes.decode("utf-8"))
    require(type(request) is dict and request.get("request_digest") == receipt["requestDigest"],
            "Publisher exact request bytes differ from the destination receipt anchor")
    require(hashlib.sha256(manifest_bytes).hexdigest() == receipt["manifestSha256"],
            "Publisher receipt manifest digest does not match its exact bytes")
    require(set(vector) == {item["name"] for role, item in receipt.get(
                "installationInventory", {}).items() if role in {
                    "object_current", "edge_current", "tombstone", "whole_source_history"}}
            and len(vector) == 4,
            "Publisher receipt does not bind the complete four-role data vector")
    # Use Ashlar's manifest API to derive the exact manifest-scoped native pin
    # vector from the publisher's independent installation inventory.
    src = str(Path(__file__).resolve().parents[1] / "src")
    if src not in sys.path:
        sys.path.insert(0, src)
    from ashlar.manifest import manifest_pin_vector
    roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
    inventory = receipt["installationInventory"]
    uuids = {inventory[role]["name"]: inventory[role]["uuid"] for role in roles}
    vector_obj = manifest_pin_vector(manifest, uuids, authority=authority)
    expected_targets = {name: [uuid, version]
                        for name, (uuid, version) in vector_obj.targets.items()}
    actual_targets = {name: [item["uuid"], item["version"]]
                      for name, item in vector.items()}
    require(actual_targets == expected_targets,
            "Publisher four-role UUID/version vector differs from the manifest-derived PinVector")
    from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
    expected_receipt_bytes = encode_truss_destination_receipt(request_bytes, manifest_bytes, vector_obj)
    require(receipt_bytes == expected_receipt_bytes,
            "Publisher source receipt bytes differ from exact request/manifest/PinVector")
    vector_wire = {"authority": vector_obj.authority,
                   "scope_kind": vector_obj.scope_kind,
                   "scope_id": vector_obj.scope_id,
                   "custody_digest": vector_obj.custody_digest,
                   "targets": {table: list(target) for table, target in vector_obj.targets.items()}}
    vector_raw = json.dumps(vector_wire, ensure_ascii=False, sort_keys=True,
                            separators=(",", ":"), allow_nan=False).encode("utf-8")
    return (receipt, request_bytes, manifest_bytes, receipt_bytes, vector_wire,
            hashlib.sha256(vector_raw).hexdigest())


def _verify_terminal_proof(output: dict, run_id: int, task_run_id: int,
                           request_bytes: bytes, manifest_bytes: bytes,
                           receipt_bytes: bytes, vector_sha256: str) -> dict:
    require(set(output) == {"profile", "runtimeIdentity", "verifierTerminalSuccess",
                            "predictiveOptimization"}
            and output.get("profile") == "ashlar-managed-a1-verifier-result/0.1",
            "Verifier output must use the closed independent A1 verifier result profile")
    require(output.get("predictiveOptimization") == "ENABLE",
            "Verifier did not observe Unity Catalog predictive optimization ENABLE")
    proof = output["verifierTerminalSuccess"]
    require(type(proof) is dict and proof.get("profile") ==
            "ashlar-managed-a1-verifier-terminal-success/0.1"
            and proof.get("run_id") == str(run_id)
            and proof.get("task_id") == str(task_run_id)
            and proof.get("terminal") is True and proof.get("status") == "SUCCEEDED"
            and proof.get("outcome") == "durably_verified"
            and proof.get("request_sha256") == hashlib.sha256(request_bytes).hexdigest()
            and proof.get("manifest_sha256") == hashlib.sha256(manifest_bytes).hexdigest()
            and proof.get("destination_receipt_sha256") == hashlib.sha256(receipt_bytes).hexdigest()
            and proof.get("vector_sha256") == vector_sha256
            and type(proof.get("result_sha256")) is str
            and SHA.fullmatch(proof["result_sha256"]),
            "Independent verifier terminal proof differs from exact publisher request/manifest/vector/receipt")
    return proof


def run(config: dict, payload: dict, profile: str, *, launch: bool) -> dict:
    # Static mode deliberately does not even resolve CLI credentials.
    if not launch:
        return {"status": "validated_only", "profile": profile,
                "workspace": config["host"], "payload": payload}
    require(profile == PROFILE, "Use the explicitly selected aidev-cus CLI profile")
    repo_root = Path(__file__).resolve().parents[1]
    gate_path = repo_root / config["local_qualification"]["path"]
    gate_bytes = gate_path.read_bytes()
    require(hashlib.sha256(gate_bytes).hexdigest() == config["local_qualification"]["sha256"],
            "Local A1 qualification receipt differs from its supplied digest")
    gate_result = json.loads(gate_bytes)
    require(type(gate_result) is dict and gate_result.get("status") == "passed",
            "Managed A1 launch requires a successful exact local qualification receipt")
    api = DatabricksCLI(profile)
    auth = api.auth_readback()
    require(auth.get("host", "").rstrip("/") == config["host"].rstrip("/"),
            "Selected Databricks CLI profile is not the admitted dbw-aidev-cus host")
    _check_artifact_lock(api, config)
    start = time.monotonic()
    deadline = start + config["maximum_wall_seconds"]
    work_deadline = deadline - config["cleanup_reserve_seconds"]
    full_schema = f"{config['catalog']}.{config['schema']}"
    from urllib.parse import quote
    schema_path = "/api/2.1/unity-catalog/schemas/" + quote(full_schema, safe="")
    volume_full = full_schema + "." + config["volume_name"]
    volume_path = f"/Volumes/{config['catalog']}/{config['schema']}/{config['volume_name']}"
    volume_uri = volume_path + "/source-snapshot.bin"
    volume_dbfs_uri = "dbfs:" + volume_uri
    volume_path_api = "/api/2.1/unity-catalog/volumes/" + quote(volume_full, safe="")
    existing = _list_schema(api, config["catalog"], config["schema"], deadline)
    require(existing is None, "The selected per-run schema already exists; refusing to reuse it")
    # Catalog existence and permission are checked before any create or compute action.
    api.call("get", "/api/2.1/unity-catalog/catalogs/" + config["catalog"])
    schema = api.call("post", "/api/2.1/unity-catalog/schemas", {
        "name": config["schema"], "catalog_name": config["catalog"],
        "comment": "Ashlar bounded managed A1 trial " + config["trial_id"],
        "properties": {"ashlar.trial_id": config["trial_id"]},
    })
    require(schema.get("name") == config["schema"]
            and schema.get("catalog_name") == config["catalog"],
            "Created schema readback differs from the exact owned trial namespace")
    volume = api.call("post", "/api/2.1/unity-catalog/volumes", {
        "name": config["volume_name"], "catalog_name": config["catalog"],
        "schema_name": config["schema"], "volume_type": "MANAGED",
        "comment": "Private A1 snapshot transport for " + config["trial_id"],
    })
    require(volume.get("name") == config["volume_name"]
            and volume.get("volume_type") == "MANAGED"
            and volume.get("catalog_name") == config["catalog"]
            and volume.get("schema_name") == config["schema"],
            "Created UC Volume readback differs from the selected private snapshot target")
    require(type(schema.get("schema_id")) is str and schema["schema_id"],
            "Fresh schema create response omitted its native schema identity")
    result = {"status": "submitted", "trial_id": config["trial_id"],
              "schema": full_schema, "schema_id": schema.get("schema_id"),
              "volume": volume_full, "volume_id": volume.get("volume_id"),
              "artifact_lock": {
                  name: {"notebook_path": config[name]["notebook_path"],
                         "notebook_sha256": config[name]["notebook_sha256"],
                         "environment_version": config[name]["environment_version"],
                         "dependencies": config[name]["dependencies"]}
                  for name in ("initializer", "publisher", "verifier")},
              "expected_runtime": config["expected_runtime"], "cost": config["cost"]}
    holder = None
    snapshot_file = None
    run_ids = {}
    verified_ack = False
    ack_attempted = False
    try:
        # Hold original A custody open before publication. The worker is a
        # source-owner child process; the runner never receives its credentials.
        holder, snapshot_bytes = SourceHolderIPC.start(config["holder_command"], work_deadline)
        fd, snapshot_file = tempfile.mkstemp(prefix="ashlar-a1-snapshot-")
        try:
            os.fchmod(fd, 0o600)
            with os.fdopen(fd, "wb") as stream:
                stream.write(snapshot_bytes)
                stream.flush()
                os.fsync(stream.fileno())
        except BaseException:
            try: os.close(fd)
            except OSError: pass
            raise
        snapshot_sha256 = hashlib.sha256(snapshot_bytes).hexdigest()
        api.copy(snapshot_file, volume_dbfs_uri)
        fd, roundtrip_file = tempfile.mkstemp(prefix="ashlar-a1-snapshot-check-")
        os.close(fd)
        try:
            api.copy(volume_dbfs_uri, roundtrip_file)
            roundtrip = Path(roundtrip_file).read_bytes()
            require(hashlib.sha256(roundtrip).hexdigest() == snapshot_sha256
                    and roundtrip == snapshot_bytes,
                    "Uploaded private snapshot failed exact byte/digest readback")
        finally:
            try: os.unlink(roundtrip_file)
            except FileNotFoundError: pass
        os.unlink(snapshot_file); snapshot_file = None
        del snapshot_bytes
        result["snapshot"] = {"volume_uri": volume_uri, "sha256": snapshot_sha256,
                              "bytes": len(roundtrip)}

        publisher_parameters = dict(config["publisher"]["base_parameters"])
        publisher_parameters.update({"trial_id": config["trial_id"], "catalog": config["catalog"],
            "schema": config["schema"], "snapshot_volume_uri": volume_uri,
            "snapshot_sha256": snapshot_sha256,
            "expected_runtime_json": json.dumps(config["expected_runtime"], sort_keys=True,
                                                  separators=(",", ":")),
            "predictive_optimization_disabled": "false"})
        holder.rpc({"type": "renew"}, "renewed", work_deadline)
        publisher_id, publisher_task_id, publisher_run, publisher_result = _run_stage(
            api, config, "publisher", payload["publisher"], publisher_parameters, work_deadline)
        run_ids["publisher"] = publisher_id
        _validate_task_runtime(publisher_result, config["expected_runtime"], "publisher")
        receipt, request_bytes, manifest_bytes, receipt_bytes, vector, vector_sha256 = (
            _publisher_bundle(publisher_result, config["expected_principal"]))
        result["publisher"] = {"run_id": publisher_id, "task_run_id": publisher_task_id,
            "publication_id": receipt.get("publicationId"),
            "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "destination_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "vector_sha256": vector_sha256,
            "runtime_identity": publisher_result["runtimeIdentity"]}

        verifier_parameters = dict(config["verifier"]["base_parameters"])
        verifier_parameters.update({"trial_id": config["trial_id"], "catalog": config["catalog"],
            "schema": config["schema"], "snapshot_volume_uri": volume_uri,
            "snapshot_sha256": snapshot_sha256,
            "expected_runtime_json": json.dumps(config["expected_runtime"], sort_keys=True,
                                                  separators=(",", ":")),
            config["publisher_result_parameter"]: json.dumps(
                publisher_result, sort_keys=True, separators=(",", ":"), ensure_ascii=False)})
        require(len(verifier_parameters[config["publisher_result_parameter"]].encode("utf-8"))
                <= 1024 * 1024, "Publisher receipt exceeds the bounded verifier parameter")
        holder.rpc({"type": "renew"}, "renewed", work_deadline)
        verifier_id, verifier_task_id, verifier_run, verifier_result = _run_stage(
            api, config, "verifier", payload["verifier"], verifier_parameters, work_deadline)
        run_ids["verifier"] = verifier_id
        _validate_task_runtime(verifier_result, config["expected_runtime"], "verifier")
        holder.rpc({"type": "renew"}, "renewed", work_deadline)
        proof = _verify_terminal_proof(verifier_result, verifier_id, verifier_task_id,
            request_bytes, manifest_bytes, receipt_bytes, vector_sha256)
        result["verifier_terminal_proof"] = proof
        result["verifier"] = {"run_id": verifier_id, "task_run_id": verifier_task_id,
            "result_sha256": proof["result_sha256"],
            "runtime_identity": verifier_result["runtimeIdentity"],
            "predictive_optimization": verifier_result["predictiveOptimization"]}

        # Holder validates its selected native PinVector against this exact
        # publication before allowing the durable prepared ACK attempt.
        bound = holder.rpc({"type": "bind",
            "request_bytes_base64": base64.b64encode(request_bytes).decode("ascii"),
            "manifest_bytes_base64": base64.b64encode(manifest_bytes).decode("ascii"),
            "vector": vector,
            "receipt_bytes_base64": base64.b64encode(receipt_bytes).decode("ascii"),
            "verifier_terminal_success": proof},
            "bound", work_deadline)
        attempt_directory = bound.get("attempt_directory")
        generation = bound.get("generation")
        require(type(attempt_directory) is str and attempt_directory
                and type(generation) is int and generation >= 0,
                "Source-holder bind omitted durable ACK attempt custody")
        ack_attempted = True
        settlement = holder.rpc({"type": "ack", "attempt_directory": attempt_directory,
                                 "generation": generation}, "settlement", deadline)
        require(settlement.get("profile") == "ashlar-managed-a1-release-bridge/0.1"
                and settlement.get("outcome") == "acknowledged_exactly_reconciled"
                and settlement.get("publication_id") == receipt.get("publicationId")
                and settlement.get("request_sha256") == hashlib.sha256(request_bytes).hexdigest()
                and settlement.get("manifest_sha256") == hashlib.sha256(manifest_bytes).hexdigest()
                and settlement.get("destination_receipt_sha256") == hashlib.sha256(receipt_bytes).hexdigest(),
                "Protected A1 ACK did not reconcile as acknowledged; schema retained")
        result["source_holder_settlement"] = settlement
        verified_ack = True
        holder.close(deadline, orderly=False)
        require(holder.process.returncode == 0,
                "Source-holder did not close cleanly after reconciled ACK")
        result["source_holder_closed"] = True

        trial_result = repo_root / config["evidence_dir"] / (config["trial_id"] + "-managed-run.json")
        trial_result.parent.mkdir(parents=True, exist_ok=True)
        trial_result.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        current = _list_schema(api, config["catalog"], config["schema"], deadline)
        require(current is not None and current.get("schema_id") == schema.get("schema_id"),
                "Schema identity changed; refusing cleanup")
        current_volume = api.call("get", volume_path_api)
        require(current_volume.get("volume_id") == volume.get("volume_id")
                and current_volume.get("owner") == config["expected_principal"]
                and current_volume.get("volume_type") == "MANAGED",
                "Trial volume ownership changed; refusing cleanup")
        directory = api.call("get", "/api/2.0/fs/directories/"
                              + quote(volume_path, safe="/"))
        contents = directory.get("contents")
        require(type(contents) is list and len(contents) == 1
                and contents[0].get("path", "").rstrip("/") == volume_uri
                and contents[0].get("is_directory") is False,
                "Trial Volume contains unknown files; refusing cleanup")
        api.call("delete", volume_path_api)
        volume_list = api.call("get", "/api/2.1/unity-catalog/volumes?catalog_name="
                               + quote(config["catalog"], safe="") + "&schema_name="
                               + quote(config["schema"], safe=""))
        require(volume_list.get("volumes", []) == [],
                "Trial schema still contains a volume after volume cleanup")
        current = _list_schema(api, config["catalog"], config["schema"], deadline)
        require(current is not None and current.get("schema_id") == schema.get("schema_id")
                and current.get("properties", {}).get("ashlar.trial_id") == config["trial_id"],
                "Schema ownership marker changed; refusing cleanup")
        owned_tables = _list_owned_tables(api, config, deadline)
        required_roles = {"object_current", "edge_current", "tombstone",
                          "whole_source_history", "publication_manifest",
                          "publication_attempt_phase"}
        require({row.get("name") for row in owned_tables} == required_roles
                and all(row.get("table_type") == "MANAGED"
                        and row.get("owner") == config["expected_principal"]
                        and type(row.get("table_id")) is str and row["table_id"]
                        for row in owned_tables),
                "Cleanup requires exactly the six owned managed-current tables under the admitted principal")
        expected_uuids = {receipt["installationInventory"][role]["name"]:
                          receipt["installationInventory"][role]["uuid"]
                          for role in ("object_current", "edge_current", "tombstone",
                                       "whole_source_history", "publication_manifest",
                                       "publication_attempt_phase")}
        require({row["name"]: row["table_id"] for row in owned_tables} == expected_uuids,
                "Cleanup table UUID inventory differs from the retained installation receipt")
        result["cleanup_table_inventory"] = sorted(
            [{"name": row["name"], "table_id": row["table_id"], "owner": row["owner"]}
             for row in owned_tables], key=lambda item: item["name"])
        api.call("delete", schema_path + "?force=true")
        require(_list_schema(api, config["catalog"], config["schema"], deadline) is None,
                "Owned trial schema remains after cleanup")
        result["status"] = "acknowledged_and_cleaned"
        result["schema_absent_after_cleanup"] = True
        trial_result.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        return result
    except BaseException:
        if holder is not None:
            try:
                if ack_attempted and holder.process.poll() is None:
                    settlement = holder.receive(deadline)
                    result["source_holder_settlement_after_error"] = settlement
                    holder.close(deadline, orderly=False)
                else:
                    holder.close(deadline, orderly=True)
            except Exception:
                pass
        if snapshot_file:
            try: os.unlink(snapshot_file)
            except FileNotFoundError: pass
        # Keep schema and volume if either run or source ACK has an unclear result.
        result["status"] = "recovery_required" if not verified_ack else "cleanup_incomplete"
        raise


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="Closed JSON input manifest")
    parser.add_argument("--profile", default=PROFILE)
    parser.add_argument("--launch", action="store_true",
                        help="Create the owned schema and submit exactly one Jobs run")
    parser.add_argument("--print-payload", action="store_true")
    args = parser.parse_args(argv)
    try:
        config = json.loads(Path(args.config).read_text(encoding="utf-8"))
        payload, config = build_payload(config)
        if args.print_payload:
            print(json.dumps(payload, sort_keys=True, indent=2))
            return 0
        output = run(config, payload, args.profile, launch=args.launch)
        print(json.dumps(output, sort_keys=True, indent=2))
        return 0
    except Exception as exc:
        print(f"managed A1 runner refused: {type(exc).__name__}: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
