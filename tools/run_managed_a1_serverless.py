#!/usr/bin/env python3
# SPDX-License-Identifier: MIT OR Apache-2.0
"""Submit, monitor, hand off, and clean up one bounded managed A1 trial.

The JSON input is an explicit lock for three sequential notebook artifacts, their
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
import subprocess
import sys
import tempfile
import threading
import time
import signal
from pathlib import Path


HOST = "https://adb-7405607548213398.18.azuredatabricks.net"
PROFILE = "aidev-cus"
MAX_SECONDS = 900
HARD_USD = 100.0
PREFERRED_USD = 10.0
SERVERLESS_USD_PER_DBUH = 0.47
WORST_SERVERLESS_DBUH = 960.0
MAX_PAID_TASK_SECONDS = 540
MAX_SOURCE_BOOTSTRAP_SECONDS = 240
MIN_DRIVER_AND_CLEANUP_RESERVE_USD = 20.0
ACK_REAP_RESERVE_SECONDS = 30
POST_ACK_CLEANUP_RESERVE_SECONDS = 30


class AmbiguousRunSubmissionError(RuntimeError):
    """A Jobs run may exist but could not be safely found and cancelled."""


def _write_private_json(path: Path, value: dict) -> None:
    path = Path(path)
    if (not path.is_absolute() or path.is_symlink() or not path.parent.is_dir()
            or path.parent.stat().st_uid != os.getuid()
            or path.parent.stat().st_mode & 0o077):
        raise RuntimeError("Owner-private recovery receipt directory required")
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8")
    require(len(raw) <= 4 * 1024 * 1024, "Private recovery receipt exceeds 4 MiB")
    temporary = path.with_name(path.name + ".tmp-" + os.urandom(8).hex())
    fd = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    try:
        view = memoryview(raw)
        while view:
            count = os.write(fd, view)
            if count <= 0:
                raise OSError("Recovery receipt write made no progress")
            view = view[count:]
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(temporary, path)
    directory_fd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try: os.fsync(directory_fd)
    finally: os.close(directory_fd)


def _validate_holder_bootstrap_config(path: Path) -> dict:
    raw = path.read_bytes()
    from ashlar_host.managed_a1_source_bootstrap import (
        MODEL_RELATIVE_PATH, MODEL_SHA256, candidate_seed_group,
        validate_private_bootstrap_config,
    )
    value = validate_private_bootstrap_config(raw)
    root = Path(value["root"])
    require(root.is_absolute() and root.is_dir() and not root.is_symlink()
            and root.resolve(strict=True) == root and root.stat().st_uid == os.getuid()
            and root.stat().st_mode & 0o077 == 0,
            "Owner-private canonical source-supervisor root required")
    model = Path(value["model_path"])
    expected_model = Path(__file__).resolve().parents[1] / MODEL_RELATIVE_PATH
    require(model == expected_model and model.is_file() and not model.is_symlink()
            and value["model_sha256"] == MODEL_SHA256
            and hashlib.sha256(model.read_bytes()).hexdigest() == MODEL_SHA256,
            "Exact frozen R28 model artifact path and digest required")
    for key, executable in (("bun_path", True), ("pg18_initdb", False)):
        path_value = Path(value[key])
        require(path_value.is_absolute() and path_value.is_file()
                and not path_value.is_symlink()
                and (not executable or os.access(path_value, os.X_OK)),
                "Exact source bootstrap runtime artifact required: " + key)
    from importlib.resources import files
    expected_initdb = Path(str(files("pgserver"))) / "pginstall" / "bin" / "initdb"
    require(Path(value["pg18_initdb"]) == expected_initdb
            and expected_initdb.is_file() and os.access(expected_initdb, os.X_OK),
            "Exact bundled PostgreSQL 18 initdb artifact required")
    require(value["a1_group"] == candidate_seed_group("A"),
            "Exact frozen R28 source A1 four-record/three-edge group required")
    return value
IDENT = re.compile(r"[A-Za-z_][A-Za-z0-9_]{0,127}\Z")
SHA = re.compile(r"[0-9a-f]{64}\Z")


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
        "initializer", "verifier", "holder_bootstrap", "trial", "protocol_profile",
        "volume_name", "local_qualification", "evidence_dir",
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
    require(type(cleanup_seconds) is int
            and ACK_REAP_RESERVE_SECONDS + POST_ACK_CLEANUP_RESERVE_SECONDS
                <= cleanup_seconds < wall,
            "Reserve 30 seconds for ACK/reap and 30 more for cleanup within the whole-run deadline")
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
            "DBU bound must include the five sequential runs, holder handoffs, and cleanup")
    expected_usd = cost["estimated_total_usd"]
    worst_case_usd = cost["authorized_total_dbu"] * SERVERLESS_USD_PER_DBUH \
        + cost["cleanup_reserve_usd"]
    require(cost["estimated_total_usd"] > 0 and cost["cleanup_reserve_usd"] >= 0
            and cost["estimated_total_usd"] + 1e-9 >=
                cost["estimated_total_dbu"] * SERVERLESS_USD_PER_DBUH
            and cost["cleanup_reserve_usd"] >= MIN_DRIVER_AND_CLEANUP_RESERVE_USD
            and worst_case_usd <= HARD_USD,
            "Worst-case 2XL DBU exposure plus driver/cleanup reserve exceeds $100")
    if expected_usd > PREFERRED_USD:
        require(type(cost["preferred_budget_exception"]) is str
                and cost["preferred_budget_exception"].strip(),
                "Estimate above $10 needs an owner rationale")
    else:
        require(cost["preferred_budget_exception"] is None,
                "Omit preferred-budget exception when total estimate is <=$10")
    holder = config["holder_bootstrap"]
    require(type(holder) is dict and set(holder) == {"bootstrap", "bun_path", "python"}
            and holder["bootstrap"] ==
                "ashlar_host.managed_a1_source_bootstrap:bootstrap_from_private_config"
            and type(holder["bun_path"]) is str and Path(holder["bun_path"]).is_absolute()
            and type(holder["python"]) is str and Path(holder["python"]).is_absolute(),
            "Exact source bootstrap module and Bun/Python runtime locks required")
    trial = config["trial"]
    require(type(trial) is dict and set(trial) == {"catalog", "schema", "expected_runtime",
            "stream", "stage_id", "materialized_at", "destination_id", "cost",
            "publisher_timeout_seconds", "verifier_timeout_seconds"}
            and trial.get("catalog") == config["catalog"]
            and trial.get("schema") == config["schema"]
            and trial.get("expected_runtime") == runtime
            and trial.get("cost") == cost and trial.get("publisher_timeout_seconds") ==
                config["publisher"]["timeout_seconds"]
            and trial.get("verifier_timeout_seconds") == config["verifier"]["timeout_seconds"]
            and trial.get("destination_id") == "client_dev." + config["schema"]
            and all(type(trial[k]) is str and trial[k] and "\0" not in trial[k]
                    for k in ("stream", "stage_id", "materialized_at"))
            and re.fullmatch(r"0|[1-9][0-9]{0,18}", trial["materialized_at"])
            and int(trial["materialized_at"]) < 2**63,
            "Closed A1 task contract must bind destination, runtime, cost and task deadline")
    require(type(config["protocol_profile"]) is dict and set(config["protocol_profile"]) == {
        "identity", "reader_versions", "writer_versions", "features"},
        "Exact reader protocol profile required")
    require(type(config["volume_name"]) is str and IDENT.fullmatch(config["volume_name"])
            and config["volume_name"].startswith("ashlar_a1_"),
            "Unique owned managed-volume name required inside the per-run schema")
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
        require(task["base_parameters"] == {"task_parameters_json": "{}"},
                f"{key} notebook must accept only the closed JSON task payload widget")
        require(task["timeout_seconds"] + 15 <= wall - cleanup_seconds,
                f"{key} task budget must fit with startup/holder/cleanup reserve")
        env = {"environment_key": key + "_env", "spec": {
            "environment_version": str(task["environment_version"]),
            "dependencies": [item["path"] for item in task["dependencies"]],
        }}
        stage_payloads[key] = {"task": task_payload, "environment": env}
    paid_seconds = (config["initializer"]["timeout_seconds"]
                    + 2 * (config["publisher"]["timeout_seconds"]
                           + config["verifier"]["timeout_seconds"]))
    require(paid_seconds <= MAX_PAID_TASK_SECONDS
            and cost["authorized_total_dbu"] >= paid_seconds * WORST_SERVERLESS_DBUH / 3600
            and paid_seconds + MAX_SOURCE_BOOTSTRAP_SECONDS + 45
            <= wall - cleanup_seconds,
            "Two publication cycles, <=540s paid tasks, 240s source bootstrap, and handoffs must fit before cleanup reserve")
    return stage_payloads, config


class DatabricksCLI:
    def __init__(self, profile: str):
        self.profile = profile
        self.deadline = None

    def _timeout(self) -> float:
        if self.deadline is None:
            return 30.0
        remaining = self.deadline - time.monotonic()
        require(remaining > 0, "Whole-trial deadline expired before Databricks CLI operation")
        return min(60.0, remaining)

    def call(self, method: str, path: str, body: dict | None = None) -> dict:
        argv = ["databricks", "--profile", self.profile, "--output", "json", "api", method, path]
        if body is not None:
            argv += ["--json", json.dumps(body, sort_keys=True, separators=(",", ":"))]
        cp = subprocess.run(argv, capture_output=True, text=True, check=False,
                            timeout=self._timeout())
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
                            capture_output=True, text=True, check=False,
                            timeout=self._timeout())
        if cp.returncode:
            raise RuntimeError("Databricks Files copy failed: " + cp.stderr.strip()[:1200])

    def remove_file(self, path: str) -> None:
        cp = subprocess.run(["databricks", "--profile", self.profile,
                             "fs", "rm", path],
                            capture_output=True, text=True, check=False,
                            timeout=self._timeout())
        if cp.returncode:
            raise RuntimeError("Databricks Files removal failed: " + cp.stderr.strip()[:1200])

    def export_workspace(self, path: str, target: str, *, notebook: bool) -> None:
        fmt = "SOURCE" if notebook else "RAW"
        cp = subprocess.run(["databricks", "--profile", self.profile, "workspace", "export",
                             path, "--format", fmt, "--file", target],
                            capture_output=True, text=True, check=False,
                            timeout=self._timeout())
        if cp.returncode:
            raise RuntimeError("Databricks Workspace export failed: " + cp.stderr.strip()[:1200])

    def auth_readback(self) -> dict:
        cp = subprocess.run(["databricks", "--profile", self.profile,
                             "--output", "json", "auth", "describe"],
                            capture_output=True, text=True, check=False,
                            timeout=self._timeout())
        if cp.returncode:
            raise RuntimeError("Databricks CLI profile authentication refused: "
                               + cp.stderr.strip()[:1200])
        value = json.loads(cp.stdout)
        require(type(value) is dict, "Databricks auth describe returned no profile identity")
        return value


class SourceHolderIPC:
    """Thin parent adapter over the source owner's inherited-FD process API."""

    def __init__(self, process):
        self.process = process.process
        self.owner = process

    @classmethod
    def start(cls, spec: dict, deadline: float):
        require(type(spec) is dict and set(spec) == {"bootstrap", "config_path", "python"}
                and type(spec["bootstrap"]) is str and ":" in spec["bootstrap"]
                and type(spec["config_path"]) is str and Path(spec["config_path"]).is_absolute()
                and (spec["python"] is None or type(spec["python"]) is str),
                "Closed holder bootstrap/config lock required")
        src = str(Path(__file__).resolve().parents[1] / "src")
        if src not in sys.path:
            sys.path.insert(0, src)
        from ashlar_host.a1_source_holder_worker import A1Truss053SourceHolderProcess
        available = deadline - time.monotonic()
        require(available > POST_ACK_CLEANUP_RESERVE_SECONDS + 1,
                "Whole-trial deadline lacks holder startup and cleanup reserve")
        timeout = max(1, min(MAX_SOURCE_BOOTSTRAP_SECONDS,
            int(available - POST_ACK_CLEANUP_RESERVE_SECONDS)))
        owner = A1Truss053SourceHolderProcess.spawn(bootstrap=spec["bootstrap"],
            config_path=spec["config_path"], python=spec["python"],
            operation_timeout_seconds=timeout)
        ipc = cls(owner)
        try:
            require(time.monotonic() < deadline, "Trial deadline expired before holder genesis")
            genesis = owner.wait_genesis()
            owner.operation_timeout_seconds = min(30, owner.operation_timeout_seconds)
            owner.channel.settimeout(owner.operation_timeout_seconds)
            return ipc, genesis
        except BaseException as startup_error:
            # Bootstrap may not have completed far enough to service a close
            # command. Kill/reap the dedicated process group with a short bound.
            remaining = max(1, int(deadline - time.monotonic()))
            owner.operation_timeout_seconds = min(5, remaining)
            try:
                owner.terminate()
                _wait_source_group_absent(ipc, deadline)
                bootstrap = json.loads(Path(spec["config_path"]).read_text())
                from ashlar_host.managed_a1_postgres_cleanup import inspect_pg_ctl
                pg_ctl_lock = inspect_pg_ctl(Path(bootstrap["pg18_initdb"]).with_name("pg_ctl"))
                _cleanup_source_pgdata(bootstrap, pg_ctl_lock, deadline)
            except BaseException as cleanup_error:
                add_note = getattr(startup_error, "add_note", None)
                if callable(add_note):
                    add_note("Escaped A/B PostgreSQL cleanup could not be proven: "
                             + f"{type(cleanup_error).__name__}: {cleanup_error}")
            raise

    def renew(self, deadline):
        remaining = deadline - time.monotonic()
        require(remaining > POST_ACK_CLEANUP_RESERVE_SECONDS,
                "Deadline lacks source-renewal and cleanup reserve")
        self.owner.channel.settimeout(min(self.owner.operation_timeout_seconds,
            remaining - POST_ACK_CLEANUP_RESERVE_SECONDS))
        return self.owner.renew()

    def open_a1(self, request_bytes, publication_id, destination_id, deadline):
        remaining = deadline - time.monotonic()
        require(remaining > POST_ACK_CLEANUP_RESERVE_SECONDS,
                "Deadline lacks prepublication hold and cleanup reserve")
        self.owner.channel.settimeout(min(self.owner.operation_timeout_seconds,
            remaining - POST_ACK_CLEANUP_RESERVE_SECONDS))
        raw = self.owner.open_prepublication(request_bytes=request_bytes,
            destination_id=destination_id, publication_id=publication_id)
        self.owner.operation_timeout_seconds = min(30, self.owner.operation_timeout_seconds)
        self.owner.channel.settimeout(self.owner.operation_timeout_seconds)
        return raw, self.owner.custody_digests

    def bind(self, *, manifest_bytes, vector, receipt_bytes, deadline):
        remaining = deadline - time.monotonic()
        require(remaining > POST_ACK_CLEANUP_RESERVE_SECONDS,
                "Deadline lacks source bind and cleanup reserve")
        self.owner.channel.settimeout(min(self.owner.operation_timeout_seconds,
            remaining - POST_ACK_CLEANUP_RESERVE_SECONDS))
        self.owner.bind_publication(manifest_bytes=manifest_bytes,
            vector_payload=vector, receipt_bytes=receipt_bytes)

    def acknowledge_once(self, store, *, attempt_directory, deadline):
        remaining = deadline - time.monotonic()
        require(remaining >= ACK_REAP_RESERVE_SECONDS + POST_ACK_CLEANUP_RESERVE_SECONDS,
                "ACK must not dispatch without bounded ACK, reap and cleanup reserve")
        operation_timeout = max(1, min(15,
            int((remaining - POST_ACK_CLEANUP_RESERVE_SECONDS) / 2)))
        self.owner.operation_timeout_seconds = operation_timeout
        self.owner.channel.settimeout(operation_timeout)
        return self.owner.acknowledge_once(store, attempt_directory=attempt_directory)

    def advance_after_a0_ack(self, settlement, deadline):
        remaining = deadline - time.monotonic()
        require(remaining > POST_ACK_CLEANUP_RESERVE_SECONDS,
                "Deadline lacks A0 reconciliation, A1 source transition and cleanup reserve")
        self.owner.channel.settimeout(min(self.owner.operation_timeout_seconds,
            remaining - POST_ACK_CLEANUP_RESERVE_SECONDS))
        return self.owner.advance_after_a0_ack(settlement, deadline=deadline)

    def close(self, deadline, *, orderly):
        del deadline
        if orderly:
            self.owner.close_before_ack()
        else:
            self.owner.terminate()
        return None


def _process_group_absent(process_group_id: int) -> bool:
    try:
        os.killpg(process_group_id, 0)
        return False
    except ProcessLookupError:
        return True
    except PermissionError as error:
        raise RuntimeError("Cannot prove source-holder worker process group is absent") from error


def _wait_source_group_absent(holder, deadline: float) -> None:
    while time.monotonic() < deadline:
        if _process_group_absent(holder.owner.process_group_id):
            return
        time.sleep(min(0.05, max(0, deadline - time.monotonic())))
    raise TimeoutError("Source-holder worker process group remains after bounded reap")


def _cleanup_source_pgdata(holder_config: dict, pg_ctl_lock: dict,
                           deadline: float) -> list[dict]:
    from ashlar_host.managed_a1_postgres_cleanup import cleanup_owned_postgres
    initdb = Path(holder_config["pg18_initdb"])
    pg_bin = initdb.parent
    results = []
    for source in ("A", "B"):
        pgdata = Path(holder_config["root"]) / f"pgdata-{source}"
        if not pgdata.exists() and not pgdata.is_symlink():
            results.append({"source": source, "state": "not_created"})
            continue
        result = cleanup_owned_postgres(pgdata, pg_ctl_lock,
            deadline=deadline, pg_controldata=pg_bin / "pg_controldata")
        result["source"] = source
        results.append(result)
    return results


def _cleanup_pin_pgdata(pin_owner, pin_pg_ctl_lock: dict, deadline: float) -> dict:
    if pin_owner is None:
        return {"state": "not_started"}
    pgdata = pin_owner.data_directory
    if not pgdata.exists() and not pgdata.is_symlink():
        return {"state": "not_created"}
    from ashlar_host.managed_a1_postgres_cleanup import cleanup_owned_postgres
    return cleanup_owned_postgres(pgdata, pin_pg_ctl_lock, deadline=deadline,
        pg_controldata=Path(pin_pg_ctl_lock["path"]).with_name("pg_controldata"))


def _terminal(state: dict) -> bool:
    life = state.get("life_cycle_state")
    return life in {"TERMINATED", "SKIPPED", "INTERNAL_ERROR"}


def _job_settings(config: dict, stage: str, task_payload: dict, *, config_key=None) -> dict:
    config_key = stage if config_key is None else config_key
    return {
        "name": "ashlar-a1-" + config["trial_id"] + "-" + stage,
        "run_as": {"user_name": config["expected_principal"]},
        "tags": {"project": "ashlar", "purpose": "managed-current-a1",
                 "ashlar_trial_id": config["trial_id"]},
        "max_concurrent_runs": 1,
        "max_retries": 0,
        "retry_on_timeout": False,
        "timeout_seconds": config[config_key]["timeout_seconds"],
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
    require(len(result.encode("utf-8")) <= 4 * 1024 * 1024,
            "Terminal task result exceeds the 4 MiB evidence bound")
    def unique(items):
        value = {}
        for key, item in items:
            require(key not in value, "Duplicate member in terminal task result")
            value[key] = item
        return value
    value = json.loads(result, object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")))
    require(type(value) is dict, "A1 task result must be a JSON object")
    return value


def _job_runs(api: DatabricksCLI, job_id: int) -> list[dict]:
    response = api.call("get", f"/api/2.2/jobs/runs/list?job_id={job_id}&limit=100")
    rows = response.get("runs")
    require(type(rows) is list and len(rows) <= 100
            and response.get("next_page_token") in (None, ""),
            "Bounded complete fresh-job run inventory required")
    require(all(type(row) is dict and type(row.get("run_id")) is int for row in rows),
            "Jobs run inventory omitted a native run ID")
    return rows


def _recover_run_after_lost_response(api: DatabricksCLI, job_id: int, token: str,
        request_started_ms: int, deadline: float, baseline_ids: set[int], persist) -> int | None:
    """Discover only the run added to this unique fresh job; never resubmit."""
    last_rows = []
    while time.monotonic() < deadline:
        rows = _job_runs(api, job_id)
        last_rows = rows
        candidates = []
        for row in rows:
            if row["run_id"] in baseline_ids:
                continue
            row_job_id = row.get("job_id")
            start_ms = row.get("start_time")
            token_match = row.get("idempotency_token") == token
            time_match = type(start_ms) is int and start_ms >= request_started_ms - 5000
            if row_job_id in (None, job_id) and (token_match or time_match):
                candidates.append(row)
        if len(candidates) == 1:
            run_id = candidates[0]["run_id"]
            persist({"submission_state": "run_id_recovered_after_lost_response",
                     "run_id": run_id, "recovered_by":
                         "idempotency_token" if candidates[0].get("idempotency_token") == token
                         else "unique_fresh_job_run"})
            return run_id
        if len(candidates) > 1:
            for row in candidates:
                try: api.call("post", "/api/2.2/jobs/runs/cancel", {"run_id": row["run_id"]})
                except Exception: pass
            persist({"submission_state": "multiple_runs_unknown_cancel_requested",
                     "run_ids": [row["run_id"] for row in candidates]})
            return None
        time.sleep(min(1.0, max(0.1, deadline - time.monotonic())))
    persist({"submission_state": "unknown_submission_exposure",
             "run_ids_seen": [row["run_id"] for row in last_rows]})
    return None


def _put_volume_bytes(api: DatabricksCLI, volume_uri: str, raw: bytes) -> dict:
    require(type(raw) is bytes and 0 < len(raw) <= 4 * 1024 * 1024,
            "Private task artifact must be nonempty and at most 4 MiB")
    fd, local = tempfile.mkstemp(prefix="ashlar-a1-volume-")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(raw); stream.flush(); os.fsync(stream.fileno())
        api.copy(local, "dbfs:" + volume_uri)
        fd, check = tempfile.mkstemp(prefix="ashlar-a1-volume-readback-")
        os.close(fd)
        try:
            api.copy("dbfs:" + volume_uri, check)
            observed = Path(check).read_bytes()
            require(observed == raw and hashlib.sha256(observed).digest()
                    == hashlib.sha256(raw).digest(),
                    "Private UC Volume artifact failed exact digest/readback")
        finally:
            try: os.unlink(check)
            except FileNotFoundError: pass
    finally:
        try: os.unlink(local)
        except FileNotFoundError: pass
    return {"uri": volume_uri, "sha256": hashlib.sha256(raw).hexdigest(), "bytes": len(raw)}


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
               parameters: dict, deadline: float, *, cleanup_deadline=None,
               on_poll=None, recovery_state=None, recovery_path=None,
               config_key=None) -> tuple[int, int, dict, dict]:
    cleanup_deadline = deadline if cleanup_deadline is None else cleanup_deadline
    original_api_deadline = api.deadline
    require(time.monotonic() < deadline,
            f"Work deadline expired before {stage} Jobs creation")
    require(type(parameters) is dict and all(type(key) is str and type(value) is str
            and len(value.encode("utf-8")) <= 9000 for key, value in parameters.items()),
            "Serverless task parameters exceed the closed 9,000-byte widget bound")
    task = dict(stage_payload["task"])
    notebook_task = dict(task["notebook_task"])
    notebook_task["base_parameters"] = parameters
    task["notebook_task"] = notebook_task
    job_settings = _job_settings(config, stage,
        {"task": task, "environment": stage_payload["environment"]},
        config_key=config_key)
    job_id = None
    run_id = None
    run_attempted = False
    submission_recovery_attempted = False
    snapshot = {}
    stage_record = {"stage": stage, "job_name": job_settings["name"],
        "job_create_state": "intent", "run_now_state": "not_submitted"}
    if recovery_state is not None:
        recovery_state.setdefault("jobs", {})[stage] = stage_record

    def persist(update=None):
        if update:
            stage_record.update(update)
            if (recovery_state is not None and type(update.get("run_id")) is int):
                recovery_state.setdefault("known_run_ids", {})[stage] = update["run_id"]
        if recovery_path is not None:
            _write_private_json(recovery_path, recovery_state)

    try:
        api.deadline = deadline
        persist()
        created = api.call("post", "/api/2.2/jobs/create", job_settings)
        job_id = created.get("job_id")
        require(type(job_id) is int and job_id > 0, f"{stage} Jobs create omitted job ID")
        persist({"job_create_state": "created", "job_id": job_id})
        readback = api.call("get", f"/api/2.2/jobs/get?job_id={job_id}")
        _admit_job_readback(readback, job_settings, config["trial_id"])
        require(time.monotonic() < deadline, f"Deadline expired before {stage} run-now")
        baseline = _job_runs(api, job_id)
        require(not baseline,
                "Fresh unique one-run Jobs object unexpectedly has prior runs")
        request_started_ms = int(time.time() * 1000)
        token = hashlib.sha256((config["trial_id"] + ":" + stage).encode()).hexdigest()
        run_attempted = True
        persist({"run_now_state": "submission_intent", "idempotency_token": token,
                 "request_started_ms": request_started_ms, "baseline_run_ids": []})
        try:
            started = api.call("post", "/api/2.2/jobs/run-now", {
                "job_id": job_id, "idempotency_token": token})
            run_id = started.get("run_id")
            if type(run_id) is not int or run_id <= 0:
                raise RuntimeError("Jobs run-now response omitted native run ID")
            persist({"run_now_state": "submitted", "run_id": run_id})
        except BaseException as submit_error:
            api.deadline = cleanup_deadline
            submission_recovery_attempted = True
            run_id = _recover_run_after_lost_response(api, job_id, token,
                request_started_ms, cleanup_deadline, set(), persist)
            if run_id is None:
                raise AmbiguousRunSubmissionError(
                    f"{stage} run-now outcome unknown; do not resubmit; recovery receipt retained") from submit_error
        while True:
            require(time.monotonic() < deadline,
                    f"{stage} work deadline reached; cancel requested")
            api.deadline = deadline
            snapshot = api.call("get", f"/api/2.2/jobs/runs/get?run_id={run_id}")
            persist({"run_state": snapshot.get("state"), "run_id": run_id})
            if _terminal(snapshot.get("state", {})):
                break
            if callable(on_poll):
                on_poll()
            time.sleep(min(3, max(0.2, deadline - time.monotonic())))
        require(snapshot.get("state", {}).get("result_state") == "SUCCESS",
                f"{stage} task did not terminate successfully")
        task_rows = snapshot.get("tasks", [])
        task_run_id = run_id
        if task_rows:
            require(type(task_rows) is list and len(task_rows) == 1
                    and task_rows[0].get("task_key") == task["task_key"]
                    and type(task_rows[0].get("run_id")) is int,
                    f"{stage} terminal run omitted its single exact task identity")
            task_run_id = task_rows[0]["run_id"]
        output = _task_output(api, task_run_id)
        persist({"run_now_state": "terminal_success", "run_id": run_id,
                 "task_run_id": task_run_id, "result_state": snapshot.get("state", {}).get("result_state")})
        api.deadline = cleanup_deadline
        api.call("delete", "/api/2.2/jobs/delete", {"job_id": job_id})
        persist({"job_create_state": "deleted"})
        return run_id, task_run_id, snapshot, output
    except BaseException:
        api.deadline = cleanup_deadline
        if (run_attempted and run_id is None and job_id is not None
                and not submission_recovery_attempted):
            try:
                submission_recovery_attempted = True
                run_id = _recover_run_after_lost_response(api, job_id,
                    stage_record.get("idempotency_token", ""),
                    stage_record.get("request_started_ms", int(time.time() * 1000)),
                    cleanup_deadline, set(), persist)
            except Exception as recovery_error:
                persist({"run_now_state": "unknown_submission_exposure",
                         "recovery_error": type(recovery_error).__name__})
        if run_attempted and run_id is None:
            persist({"run_now_state": "unknown_submission_exposure"})
        if run_id is not None:
            try:
                snapshot = api.call("get", f"/api/2.2/jobs/runs/get?run_id={run_id}")
                persist({"run_id": run_id, "run_state": snapshot.get("state")})
                if not _terminal(snapshot.get("state", {})):
                    api.call("post", "/api/2.2/jobs/runs/cancel", {"run_id": run_id})
                    persist({"cancel_requested": True})
                    while time.monotonic() < cleanup_deadline:
                        snapshot = api.call("get", f"/api/2.2/jobs/runs/get?run_id={run_id}")
                        persist({"run_state": snapshot.get("state")})
                        if _terminal(snapshot.get("state", {})):
                            break
                        time.sleep(min(2, max(0.2, cleanup_deadline - time.monotonic())))
            except Exception:
                pass
        if (job_id is not None and
                (not run_attempted or (run_id is not None and _terminal(snapshot.get("state", {}))))):
            try:
                api.call("delete", "/api/2.2/jobs/delete", {"job_id": job_id})
                persist({"job_create_state": "deleted"})
            except Exception:
                pass
        persist({"stage_error": "stage_failed"})
        raise
    finally:
        api.deadline = original_api_deadline


def _publisher_bundle(result: dict, authority: str, *, phase: str) -> tuple[dict, bytes, bytes, bytes, dict, str]:
    require(set(result) == {"profile", "runtimeIdentity", "predictiveOptimization",
                            "requestBytesBase64", "manifestBytesBase64",
                            "destinationReceiptBytesBase64", "receipt", "phase"}
            and result.get("profile") == "ashlar-managed-a1-publisher-result/0.1"
            and result.get("phase") == phase,
            "Publisher output must use the closed phase-bound publisher result profile")
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


def _retention_bundle(*, trial, protocol_profile, plan, request_bytes,
                      manifest_bytes, destination_receipt,
                      task_receipt, prefix_bytes, prefix_sha, selected_prefix,
                      source_snapshot, custody_digests):
    from ashlar_host.managed_current_a1_retention_builder import (
        build_managed_current_a1_retention_bundle,
    )
    from ashlar_host.managed_current_spark_receipt_read import (
        CARRIER_PROFILE_XXHASH64, ManagedCurrentSparkReceiptConfig,
    )
    from ashlar_host.managed_current_trial import ManagedCurrentTrialConfig
    reader = ManagedCurrentSparkReceiptConfig(catalog=trial["catalog"],
        schema=trial["schema"], expected_principal=trial["expected_runtime"]["principal"],
        expected_table_owner=trial["expected_runtime"]["principal"],
        source_truss_version="0.0.53", protocol_profile=protocol_profile,
        expected_runtime=trial["expected_runtime"], maximum_elapsed_seconds=300,
        maximum_statements=128, maximum_rows=1024, maximum_result_bytes=4 * 1024 * 1024,
        carrier_profile=CARRIER_PROFILE_XXHASH64)
    cost = trial["cost"]
    core = ManagedCurrentTrialConfig(catalog=trial["catalog"], schema=trial["schema"],
        stream=trial["stream"], predecessor=plan.predecessor.manifest["publication_id"],
        schedule_sha256=plan.selection.schedule_sha256, schedule_step=1,
        stage_id=trial["stage_id"], schema_revisions_json=json.dumps(
            plan.selection.schema_revisions, sort_keys=True, separators=(",", ":")),
        materialized_at=trial["materialized_at"], expected_environment=trial["expected_runtime"],
        maximum_task_seconds=trial["verifier_timeout_seconds"],
        estimated_total_dbu=cost["estimated_total_dbu"],
        authorized_total_dbu=cost["authorized_total_dbu"],
        estimated_total_cost_usd=cost["estimated_total_usd"],
        reserve_dbu_for_cleanup=cost["cleanup_reserve_dbu"],
        reserve_usd_for_cleanup=cost["cleanup_reserve_usd"],
        cost_estimate_includes_startup_driver_and_cleanup=True, create_schema=False,
        preferred_budget_exception=cost["preferred_budget_exception"],
        carrier_profile=CARRIER_PROFILE_XXHASH64).core_config()
    return build_managed_current_a1_retention_bundle(plan=plan,
        reader_config=reader, request_bytes=request_bytes, manifest_bytes=manifest_bytes,
        task_receipt=task_receipt, destination_receipt_bytes=destination_receipt,
        source_prefix_bytes=prefix_bytes, source_prefix_sha256=prefix_sha,
        selected_prefix=selected_prefix, source_snapshot_bytes=source_snapshot,
        source_custody_digests=custody_digests, source_core_config=core)


class _LocalPinStages:
    """Hold the native source-bound pin across the verifier's three RPC stages."""
    def __init__(self, owner, *, lifetime_seconds):
        self.owner = owner
        self.lifetime_seconds = lifetime_seconds
        require(type(lifetime_seconds) is int and 31 <= lifetime_seconds <= 900
                and owner.native_timeout_seconds <= lifetime_seconds,
                "Parent A1 pin timeout must fit the bounded trial lifetime")
        self.handle = None
        self.stages = []

    def __call__(self, stage):
        require(stage in ("open", "pre_ack", "close"), "Closed pin stage required")
        if stage == "open":
            require(self.handle is None and not self.stages,
                    "Native A1 pin may open only once")
            self.handle = self.owner.open_native_pin()
        elif stage == "pre_ack":
            require(self.handle is not None and self.stages == ["open"],
                    "Native A1 pin pre-ACK renewal must follow its opening")
            self.handle.renew()
        else:
            require(self.handle is not None and self.stages == ["open", "pre_ack"],
                    "Native A1 pin close must follow open and pre-ACK")
            self.handle.close()
            self.handle = None
        self.stages.append(stage)

    def close_on_error(self):
        if self.handle is not None:
            try: self.handle.close()
            finally: self.handle = None


def _verify_terminal_proof(output: dict, run_id: int, task_run_id: int,
                           request_bytes: bytes, manifest_bytes: bytes,
                           receipt_bytes: bytes, vector_sha256: str,
                           vector_wire: dict, *, phase: str) -> dict:
    require(set(output) == {"profile", "runtimeIdentity", "predictiveOptimization",
                            "managedObservation", "managedObservationSha256",
                            "requestSha256", "manifestSha256", "phase",
                            "destinationReceiptSha256", "vector"}
            and output.get("profile") == "ashlar-managed-a1-verifier-result/0.1"
            and output.get("phase") == phase,
            "Verifier output must use the closed independent phase-bound result profile")
    require(output.get("predictiveOptimization") == "ENABLE",
            "Verifier did not observe Unity Catalog predictive optimization ENABLE")
    request_sha = hashlib.sha256(request_bytes).hexdigest()
    manifest_sha = hashlib.sha256(manifest_bytes).hexdigest()
    receipt_sha = hashlib.sha256(receipt_bytes).hexdigest()
    observation = output["managedObservation"]
    observation_raw = json.dumps(observation, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8")
    require(output["requestSha256"] == request_sha
            and output["manifestSha256"] == manifest_sha
            and output["destinationReceiptSha256"] == receipt_sha
            and output["vector"] == vector_wire
            and output["managedObservationSha256"] == hashlib.sha256(observation_raw).hexdigest()
            and type(observation) is dict
            and observation.get("profile") == "ashlar-managed-operational-retention/0.1"
            and observation.get("outcome") == "observed_available_at_checks"
            and observation.get("authority") == "read_observation_only_no_source_ack"
            and type(observation.get("observations")) is list
            and [item.get("stage") for item in observation["observations"]]
                == ["open", "pre_ack", "close"]
            and all(item.get("predictive_optimization") == "ENABLE"
                    and item.get("vector_targets") == {
                        table: {"uuid": target[0], "version": target[1]}
                        for table, target in vector_wire["targets"].items()}
                    for item in observation["observations"]),
            "Independent verifier managed observations differ from exact publication vector or PO state")
    return {"profile": "ashlar-managed-a1-verifier-terminal-success/0.1",
        "run_id": str(run_id), "task_id": str(task_run_id), "terminal": True,
        "status": "SUCCEEDED", "outcome": "durably_verified",
        "request_sha256": request_sha, "manifest_sha256": manifest_sha,
        "destination_receipt_sha256": receipt_sha, "vector_sha256": vector_sha256,
        "result_sha256": hashlib.sha256(observation_raw).hexdigest()}


def _validate_ack_settlement(settlement, *, request_bytes: bytes,
                             manifest_bytes: bytes, snapshot_bytes: bytes,
                             next_checkpoint_bytes: bytes,
                             receipt_bytes: bytes) -> None:
    """Accept only the holder's exact matched readback for this prepared attempt."""
    from ashlar_host.a1_ack_attempt import A1AckSettlement
    require(type(settlement) is A1AckSettlement
            and settlement.outcome == "matched"
            and settlement.request_sha256 == hashlib.sha256(request_bytes).hexdigest()
            and settlement.manifest_sha256 == hashlib.sha256(manifest_bytes).hexdigest()
            and settlement.selection_sha256 == hashlib.sha256(snapshot_bytes).hexdigest()
            and settlement.checkpoint_bytes == next_checkpoint_bytes
            and settlement.receipt_bytes == receipt_bytes,
            "Protected A1 ACK did not reconcile against durable exact attempt; schema retained")


def _service_source_observer_rpc(api: DatabricksCLI, holder: SourceHolderIPC,
                                 volume_rpc_uri: str, deadline: float,
                                 *, expected_vector_sha256: str,
                                 expected_custody_digests: dict, pin_stage=None) -> None:
    """Renew original same-session Truss custody for a live verifier request."""
    from urllib.parse import quote
    directory = api.call("get", "/api/2.0/fs/directories/" + quote(volume_rpc_uri, safe="/"))
    contents = directory.get("contents")
    require(type(contents) is list and len(contents) <= 16,
            "Bounded managed A1 source-observer RPC directory required")
    requests = []
    for item in contents:
        path = item.get("path", "") if type(item) is dict else ""
        name = path.rsplit("/", 1)[-1]
        if name.startswith("request-") and name.endswith(".json"):
            require(item.get("is_directory") is False,
                    "Source-observer request must be a regular file")
            requests.append((path, name))
    require(len(requests) <= 1,
            "Source observer RPC must be sequential with at most one outstanding request")
    if not requests:
        return
    require(time.monotonic() < deadline,
            "Trial deadline expired while servicing source-observer RPC")
    request_path, name = requests[0]
    token = name[len("request-"):-len(".json")]
    require(re.fullmatch(r"[0-9a-f]{32}", token),
            "Source-observer request filename nonce invalid")
    fd, local_request = tempfile.mkstemp(prefix="ashlar-a1-source-rpc-")
    os.close(fd)
    try:
        api.copy("dbfs:" + request_path, local_request)
        request_raw = Path(local_request).read_bytes()
    finally:
        try: os.unlink(local_request)
        except FileNotFoundError: pass
    require(len(request_raw) <= 16384, "Source/pin RPC request exceeds its closed frame bound")
    def unique(items):
        out = {}
        for key, value in items:
            require(key not in out, "Duplicate member in source/pin RPC request")
            out[key] = value
        return out
    request = json.loads(request_raw.decode("utf-8"), object_pairs_hook=unique,
        parse_constant=lambda _: (_ for _ in ()).throw(ValueError("non-finite JSON")))
    require(type(request) is dict and set(request) == {
        "profile", "kind", "stage", "nonce", "expected", "vectorSha256"}
        and request["profile"] == "ashlar-managed-a1-source-observer-rpc/0.1"
        and request["nonce"] == token and request["stage"] in ("open", "pre_ack", "close")
        and request["kind"] in ("source", "pin")
        and (request["kind"] != "pin" or request["stage"] in ("open", "pre_ack"))
        and type(request["expected"]) is dict
        and request["expected"] == expected_custody_digests
        and request["vectorSha256"] == expected_vector_sha256,
        "Closed source-observer renewal request required")
    if request["kind"] == "pin":
        require(pin_stage is not None, "Parent native pin observer owner required")
        holder.renew(deadline)
        require(holder.owner.custody_digests == expected_custody_digests,
                "Source custody drift before managed verifier read")
        pin_stage(request["stage"])
        custody = expected_custody_digests
    else:
        holder.renew(deadline)
        custody = holder.owner.custody_digests
        require(type(custody) is dict and custody == expected_custody_digests,
                "Same-session Truss renewal differs from exact source custody selected by verifier")
        if request["stage"] == "close" and pin_stage is not None:
            pin_stage("close")
    response_value = {"profile": "ashlar-managed-a1-source-observer-rpc/0.1",
        "kind": request["kind"], "nonce": token,
        "stage": request["stage"], "custodyDigests": custody}
    fd, local_response = tempfile.mkstemp(prefix="ashlar-a1-source-rpc-response-")
    try:
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "wb") as stream:
            stream.write(json.dumps(response_value, sort_keys=True, separators=(",", ":")).encode())
            stream.flush(); os.fsync(stream.fileno())
        response_uri = "dbfs:" + volume_rpc_uri + "/response-" + token + ".json"
        api.copy(local_response, response_uri)
        api.remove_file("dbfs:" + request_path)
    finally:
        try: os.unlink(local_response)
        except FileNotFoundError: pass


def _launch_trial(config: dict, payload: dict, profile: str, *, recovery_root: Path,
                  recovery_state: dict, start: float) -> dict:
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
    deadline = start + config["maximum_wall_seconds"]
    api.deadline = deadline
    auth = api.auth_readback()
    require(auth.get("host", "").rstrip("/") == config["host"].rstrip("/"),
            "Selected Databricks CLI profile is not the admitted dbw-aidev-cus host")
    _check_artifact_lock(api, config)
    python = config["holder_bootstrap"]["python"]
    python_path = Path(python)
    require(python_path.is_absolute() and python_path.is_file()
            and os.access(python_path, os.X_OK)
            and python_path.resolve(strict=True) == Path(sys.executable).resolve(strict=True),
            "Runner and source-holder must use the same exact executable Python runtime")
    bun_path = Path(config["holder_bootstrap"]["bun_path"])
    require(bun_path.is_file() and not bun_path.is_symlink() and os.access(bun_path, os.X_OK),
            "Exact executable local Bun runtime required before trial mutations")
    # Generate the source bootstrap config from its checked-in owner factory;
    # no credential-bearing callback or prebuilt caller JSON is accepted.
    bootstrap_root = recovery_root / "truss-source"
    bootstrap_root.mkdir(mode=0o700)
    from ashlar_host.managed_a1_source_bootstrap import example_private_config
    holder_config_value = example_private_config(root=bootstrap_root, bun_path=bun_path)
    holder_config_path = recovery_root / "source-bootstrap.json"
    _write_private_json(holder_config_path, holder_config_value)
    holder_config = _validate_holder_bootstrap_config(holder_config_path)
    from ashlar_host.managed_a1_postgres_cleanup import inspect_pg_ctl
    source_pg_ctl_lock = inspect_pg_ctl(Path(holder_config["pg18_initdb"]).with_name("pg_ctl"))
    recovery_state["source_bootstrap_config_path"] = str(holder_config_path)
    recovery_state["source_supervisor_root"] = str(bootstrap_root)
    recovery_state["source_pg_ctl_lock"] = source_pg_ctl_lock
    _write_private_json(recovery_path, recovery_state)
    holder_spec = {"bootstrap": config["holder_bootstrap"]["bootstrap"],
                   "config_path": str(holder_config_path), "python": python}
    work_deadline = deadline - config["cleanup_reserve_seconds"]
    recovery_path = recovery_root / "recovery.json"
    recovery_state.update({"phase": "pre_mutation", "workspace": config["host"],
        "trial_id": config["trial_id"], "schema": config["catalog"] + "." + config["schema"],
        "volume": config["volume_name"],
        "cleanup_reserve_seconds": config["cleanup_reserve_seconds"]})
    _write_private_json(recovery_path, recovery_state)
    full_schema = f"{config['catalog']}.{config['schema']}"
    from urllib.parse import quote
    schema_path = "/api/2.1/unity-catalog/schemas/" + quote(full_schema, safe="")
    volume_full = full_schema + "." + config["volume_name"]
    volume_path = f"/Volumes/{config['catalog']}/{config['schema']}/{config['volume_name']}"
    volume_path_api = "/api/2.1/unity-catalog/volumes/" + quote(volume_full, safe="")
    existing = _list_schema(api, config["catalog"], config["schema"], deadline)
    require(existing is None, "The selected per-run schema already exists; refusing to reuse it")
    require(time.monotonic() < work_deadline,
            "Work deadline expired before creating the owned schema")
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
    recovery_state.update({"phase": "schema_created", "schema_id": schema.get("schema_id")})
    _write_private_json(recovery_path, recovery_state)
    require(time.monotonic() < work_deadline,
            "Work deadline expired before creating the owned private Volume")
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
    recovery_state.update({"phase": "volume_created", "volume_id": volume.get("volume_id")})
    _write_private_json(recovery_path, recovery_state)
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
    pin_owner = None
    pin_stages = None
    pin_pg_ctl_lock = None
    pin_root = None
    run_ids = {}
    verified_ack = False
    ack_attempted = False
    recovery_state["phase"] = "trial_started"
    _write_private_json(recovery_path, recovery_state)
    try:
        # The source child is started before the genesis initializer and retains
        # the original source's issuers/credentials. Only detached exact bytes
        # cross the process boundary.
        holder, genesis_bytes = SourceHolderIPC.start(holder_spec, work_deadline)
        phase = holder.owner.phase
        require(phase == "a0", "Source holder must begin at the explicit A0 metadata phase")
        recovery_state["source_bootstrap"] = {"worker_pid": holder.process.pid,
            "genesis_sha256": hashlib.sha256(genesis_bytes).hexdigest(),
            "genesis_bytes": len(genesis_bytes), "state": "source_A1_committed_and_genesis_selected"}
        _write_private_json(recovery_path, recovery_state)
        genesis_uri = volume_path + "/genesis.bin"
        genesis_artifact = _put_volume_bytes(api, genesis_uri, genesis_bytes)
        trial = dict(config["trial"])
        trial["expected_runtime"] = config["expected_runtime"]
        trial["cost"] = config["cost"]
        init_params = {"trial": trial, "genesis_volume_uri": genesis_uri,
            "genesis_sha256": genesis_artifact["sha256"],
            "protocol_profile": config["protocol_profile"]}
        init_json = json.dumps(init_params, sort_keys=True, separators=(",", ":"),
                               ensure_ascii=False)
        init_id, init_task_id, _, init_result = _run_stage(api, config, "initializer",
            payload["initializer"], {"task_parameters_json": init_json}, work_deadline,
            cleanup_deadline=deadline, recovery_state=recovery_state,
            recovery_path=recovery_path)
        run_ids["initializer"] = init_id
        _validate_task_runtime(init_result, config["expected_runtime"], "initializer")
        require(set(init_result) == {"profile", "runtimeIdentity", "predictiveOptimization",
                    "genesisManifestBytesBase64", "manifestSha256", "installationInventory"}
                and init_result["profile"] == "ashlar-managed-a1-initializer-result/0.1"
                and init_result["predictiveOptimization"] == "ENABLE",
                "Initializer must return the exact empty six-role genesis result")
        initial_manifest = base64.b64decode(init_result["genesisManifestBytesBase64"], validate=True)
        require(hashlib.sha256(initial_manifest).hexdigest() == init_result["manifestSha256"]
                and len(initial_manifest) <= 4 * 1024 * 1024,
                "Initializer exact genesis manifest digest/bound failed")
        recovery_state["uc_genesis"] = {"manifest_sha256": hashlib.sha256(initial_manifest).hexdigest(),
            "installation_inventory": init_result["installationInventory"]}
        _write_private_json(recovery_path, recovery_state)
        from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
        context = object()
        plan = admit_detached_a1_plan(genesis_snapshot_bytes=genesis_bytes,
            genesis_manifest=json.loads(initial_manifest), stream=trial["stream"],
            materialized_at=trial["materialized_at"], context=context)
        request_bytes = json.dumps(plan.request, sort_keys=True, separators=(",", ":"),
                                   ensure_ascii=False, allow_nan=False).encode("utf-8")
        publication_id = plan.request["publication_id"]
        recovery_state["frozen_a1_request"] = {"request_sha256": hashlib.sha256(request_bytes).hexdigest(),
            "publication_id": publication_id}
        _write_private_json(recovery_path, recovery_state)
        destination_id = trial.get("destination_id", "client_dev." + trial["schema"])
        snapshot_bytes, custody_digests = holder.open_a1(
            request_bytes, publication_id, destination_id, work_deadline)
        recovery_state["source_hold"] = {"selection_sha256": hashlib.sha256(snapshot_bytes).hexdigest(),
            "selection_bytes": len(snapshot_bytes), "custody_digests": custody_digests,
            "state": "same_session_prepublication_hold_open"}
        _write_private_json(recovery_path, recovery_state)
        from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        detached = decode_detached_source_snapshot(snapshot_bytes)
        require(len(snapshot_bytes) <= 4 * 1024 * 1024,
                "Held exact A1 source selection exceeds the durable 4 MiB bound")
        holder.owner.start_heartbeat()
        snapshot_uri = volume_path + "/a1-source-snapshot.bin"
        snapshot_artifact = _put_volume_bytes(api, snapshot_uri, snapshot_bytes)
        initial_uri = volume_path + "/genesis-manifest.json"
        initial_artifact = _put_volume_bytes(api, initial_uri, initial_manifest)
        task_trial = dict(trial)
        task_trial["source_custody_digests"] = custody_digests
        task_trial["destination_id"] = destination_id
        common = {"trial": task_trial, "protocol_profile": config["protocol_profile"],
            "phase": phase,
            "genesis_volume_uri": genesis_uri,
            "genesis_sha256": genesis_artifact["sha256"],
            "genesis_manifest_volume_uri": initial_uri,
            "genesis_manifest_sha256": initial_artifact["sha256"],
            "a1_snapshot_volume_uri": snapshot_uri,
            "a1_snapshot_sha256": snapshot_artifact["sha256"]}
        publisher_params = dict(common)
        publisher_id, publisher_task_id, _, publisher_result = _run_stage(api, config,
            "a0-publisher", payload["publisher"], {"task_parameters_json": json.dumps(
                publisher_params, sort_keys=True, separators=(",", ":"), ensure_ascii=False)},
            work_deadline, cleanup_deadline=deadline, recovery_state=recovery_state,
            recovery_path=recovery_path, config_key="publisher")
        run_ids["publisher"] = publisher_id
        _validate_task_runtime(publisher_result, config["expected_runtime"], "publisher")
        receipt, request_bytes, manifest_bytes, receipt_bytes, vector, vector_sha256 = (
            _publisher_bundle(publisher_result, "managed-current-spark:" + config["expected_principal"],
                              phase=phase))
        require(request_bytes == json.dumps(plan.request, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8"),
            "Publisher exact request differs from parent detached A/B admission")
        result["publisher"] = {"run_id": publisher_id, "task_run_id": publisher_task_id,
            "publication_id": receipt.get("publicationId"),
            "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "destination_receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "vector_sha256": vector_sha256, "runtime_identity": publisher_result["runtimeIdentity"]}
        publisher_result_raw = json.dumps(publisher_result, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        publisher_result_uri = volume_path + "/a0-publisher-result.json"
        publisher_result_artifact = _put_volume_bytes(api, publisher_result_uri,
                                                       publisher_result_raw)
        recovery_state["publisher_result"] = {"volume_uri": publisher_result_uri,
            "sha256": publisher_result_artifact["sha256"], "bytes": publisher_result_artifact["bytes"]}
        _write_private_json(recovery_path, recovery_state)
        from ashlar_host.managed_current_a1_prefix_builder import build_managed_current_a1_prefix_evidence
        prefix_bytes, prefix_sha, selected_prefix = build_managed_current_a1_prefix_evidence(
            genesis_bytes=genesis_bytes, source_snapshot_bytes=snapshot_bytes,
            initial_manifest_bytes=initial_manifest, request_bytes=request_bytes,
            publication_manifest_bytes=manifest_bytes,
            destination={"catalog": config["catalog"], "schema": config["schema"]},
            carrier_profile="ashlar-delta/0.4-xxhash64",
            materialization_clock_us=trial["materialized_at"], phase=phase)
        prefix_uri = volume_path + "/source-prefix.json"
        prefix_artifact = _put_volume_bytes(api, prefix_uri, prefix_bytes)
        selected_prefix_raw = json.dumps(selected_prefix, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        selected_prefix_uri = volume_path + "/selected-prefix.json"
        selected_prefix_artifact = _put_volume_bytes(api, selected_prefix_uri,
                                                       selected_prefix_raw)
        recovery_state["source_prefix"] = {"sha256": prefix_sha,
            "volume_uri": prefix_uri, "selected_sha256": selected_prefix_artifact["sha256"]}
        _write_private_json(recovery_path, recovery_state)
        bundle = _retention_bundle(trial=task_trial | {
                "verifier_timeout_seconds": config["verifier"]["timeout_seconds"]},
            protocol_profile=config["protocol_profile"], plan=plan, request_bytes=request_bytes,
            manifest_bytes=manifest_bytes, destination_receipt=receipt_bytes,
            task_receipt=receipt, prefix_bytes=prefix_bytes, prefix_sha=prefix_sha,
            selected_prefix=selected_prefix, source_snapshot=snapshot_bytes,
            custody_digests=custody_digests)
        bundle_wire = {"authority": bundle.vector.authority,
            "scope_kind": bundle.vector.scope_kind, "scope_id": bundle.vector.scope_id,
            "custody_digest": bundle.vector.custody_digest,
            "targets": {key: list(value) for key, value in bundle.vector.targets.items()}}
        require(bundle_wire == vector,
                "Parent retention builder and publisher vector differ")
        pin_root = recovery_root / "pin-owner"
        pin_root.mkdir(mode=0o700)
        os.chmod(pin_root, 0o700)
        pin_native_timeout = min(900, config["maximum_wall_seconds"],
            int(deadline - time.monotonic() - POST_ACK_CLEANUP_RESERVE_SECONDS))
        require(pin_native_timeout >= 31,
                "Insufficient outer-deadline coverage for a bounded native A1 pin")
        from ashlar_host.managed_a1_local_pin_owner import ManagedA1LocalPinOwner
        from importlib.resources import files
        pin_pg_bin = Path(str(files("pgserver"))) / "pginstall" / "bin"
        pin_pg_ctl_lock = inspect_pg_ctl(pin_pg_bin / "pg_ctl")
        recovery_state["pin_pg_ctl_lock"] = pin_pg_ctl_lock
        _write_private_json(recovery_path, recovery_state)
        pin_owner = ManagedA1LocalPinOwner(pin_root / "pgdata", bundle.selection,
            bundle.vector, native_timeout_seconds=pin_native_timeout,
            deadline_monotonic=work_deadline - config["verifier"]["timeout_seconds"])
        pin_owner.__enter__()
        pin_registration = pin_owner.register()
        pin_stages = _LocalPinStages(pin_owner,
            lifetime_seconds=pin_native_timeout)
        result["local_pin_owner"] = pin_registration
        rpc_dir = volume_path
        verify_params = dict(common)
        verify_params.update({"publisher_result_volume_uri": publisher_result_uri,
            "publisher_result_sha256": publisher_result_artifact["sha256"],
            "prefix_volume_uri": prefix_uri, "prefix_sha256": prefix_artifact["sha256"],
            "selected_prefix_volume_uri": selected_prefix_uri,
            "selected_prefix_sha256": selected_prefix_artifact["sha256"],
            "source_rpc_directory": rpc_dir})
        verifier_id, verifier_task_id, _, verifier_result = _run_stage(api, config,
            "a0-verifier", payload["verifier"], {"task_parameters_json": json.dumps(
                verify_params, sort_keys=True, separators=(",", ":"), ensure_ascii=False)},
            work_deadline, on_poll=lambda: _service_source_observer_rpc(api, holder, rpc_dir,
                work_deadline, expected_vector_sha256=vector_sha256,
                expected_custody_digests=custody_digests, pin_stage=pin_stages),
            cleanup_deadline=deadline, recovery_state=recovery_state,
            recovery_path=recovery_path, config_key="verifier")
        run_ids["verifier"] = verifier_id
        _validate_task_runtime(verifier_result, config["expected_runtime"], "verifier")
        holder.renew(work_deadline)
        require(pin_stages.stages == ["open", "pre_ack", "close"]
                and pin_stages.handle is None,
                "Local native pin interval did not complete all three managed observer stages")
        pin_owner.__exit__(None, None, None)
        recovery_state["pin_postgres_cleanup"] = _cleanup_pin_pgdata(
            pin_owner, pin_pg_ctl_lock, deadline)
        result["pin_postgres_cleanup"] = recovery_state["pin_postgres_cleanup"]
        pin_owner = None
        proof = _verify_terminal_proof(verifier_result, verifier_id, verifier_task_id,
            request_bytes, manifest_bytes, receipt_bytes, vector_sha256, vector,
            phase=phase)
        result["verifier_terminal_proof"] = proof
        result["verifier"] = {"run_id": verifier_id, "task_run_id": verifier_task_id,
            "result_sha256": proof["result_sha256"],
            "runtime_identity": verifier_result["runtimeIdentity"],
            "predictive_optimization": verifier_result["predictiveOptimization"]}

        require(deadline - time.monotonic() >= config["cleanup_reserve_seconds"],
                "Do not bind or ACK without the full remaining ACK/reap/cleanup reserve")
        recovery_state["phase"] = "verified_prepared_for_ack"
        recovery_state["source"] = {"publication_id": publication_id,
            "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "selection_sha256": hashlib.sha256(snapshot_bytes).hexdigest(),
            "prior_checkpoint_sha256": hashlib.sha256(detached.source_page.prior_bytes).hexdigest(),
            "next_checkpoint_sha256": hashlib.sha256(detached.source_page.next_bytes).hexdigest(),
            "page_sha256": detached.source_page.page_fields["sha256"]}
        _write_private_json(recovery_path, recovery_state)
        holder.bind(manifest_bytes=manifest_bytes, vector=vector,
            receipt_bytes=receipt_bytes, deadline=deadline)
        from ashlar_host.a1_ack_attempt import DurableA1AckAttemptStore
        attempt_dir = pin_root / "attempts"
        attempt_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        os.chmod(attempt_dir, 0o700)
        store = DurableA1AckAttemptStore(attempt_dir)
        page = detached.source_page
        store.begin(selection_bytes=snapshot_bytes, request_bytes=request_bytes,
            manifest_bytes=manifest_bytes, receipt_bytes=receipt_bytes,
            prior_checkpoint_bytes=page.prior_bytes, next_checkpoint_bytes=page.next_bytes,
            page_sha256=page.page_fields["sha256"], destination_id=destination_id,
            publication_id=publication_id)
        prepared = store.load()
        recovery_state["ack_attempt_directory"] = str(store.directory)
        result["ack_attempt"] = {"selection_sha256": prepared.selection_sha256,
            "request_sha256": prepared.request_sha256,
            "manifest_sha256": prepared.manifest_sha256,
            "receipt_sha256": prepared.receipt_sha256,
            "publication_id": prepared.publication_id}
        attempt_raw = store.path.read_bytes()
        recovery_state["ack"] = {"state": "durable_attempt_prepared",
            "attempt_directory": str(store.directory),
            "attempt_sha256": hashlib.sha256(attempt_raw).hexdigest(),
            **result["ack_attempt"]}
        _write_private_json(recovery_path, recovery_state)
        require(deadline - time.monotonic() >= config["cleanup_reserve_seconds"],
                "Do not dispatch ACK without the full reserved ACK/reap/cleanup window")
        ack_attempted = True
        recovery_state["ack"]["state"] = "dispatch_intent"
        _write_private_json(recovery_path, recovery_state)
        settlement = holder.acknowledge_once(store, attempt_directory=store.directory,
                                             deadline=deadline)
        _validate_ack_settlement(settlement, request_bytes=request_bytes,
            manifest_bytes=manifest_bytes, snapshot_bytes=snapshot_bytes,
            next_checkpoint_bytes=detached.source_page.next_bytes,
            receipt_bytes=receipt_bytes)
        result["source_holder_settlement"] = {"outcome": settlement.outcome,
            "request_sha256": settlement.request_sha256,
            "manifest_sha256": settlement.manifest_sha256,
            "selection_sha256": settlement.selection_sha256}
        recovery_state["ack"]["state"] = "acknowledged_exactly_reconciled"
        recovery_state["a0_ack"] = dict(recovery_state["ack"])
        recovery_state["phase"] = "a0_acknowledged_advancing_to_a1"
        _write_private_json(recovery_path, recovery_state)
        result["a0"] = {"publication_id": publication_id,
            "request_sha256": hashlib.sha256(request_bytes).hexdigest(),
            "manifest_sha256": hashlib.sha256(manifest_bytes).hexdigest(),
            "receipt_sha256": hashlib.sha256(receipt_bytes).hexdigest(),
            "vector_sha256": vector_sha256,
            "publisher_run_id": publisher_id, "verifier_run_id": verifier_id,
            "settlement": {"outcome": settlement.outcome,
                "selection_sha256": settlement.selection_sha256}}

        # The holder applies the frozen semantic A1 group only after A0's exact
        # source ACK has reconciled. A1 then receives its own immutable request,
        # publisher/verifier jobs, pin interval, durable ACK attempt and ACK.
        a0_manifest_bytes = manifest_bytes
        a1_snapshot_bundle_bytes = holder.advance_after_a0_ack(settlement,
            deadline=work_deadline)
        a1_genesis_uri = genesis_uri
        a1_manifest_uri = volume_path + "/a0-manifest.json"
        a1_manifest_artifact = _put_volume_bytes(api, a1_manifest_uri, a0_manifest_bytes)
        a1_genesis_artifact = genesis_artifact
        from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
        a1_plan = admit_detached_a1_plan(genesis_snapshot_bytes=a1_snapshot_bundle_bytes,
            genesis_manifest=json.loads(a0_manifest_bytes), stream=trial["stream"],
            materialized_at=trial["materialized_at"], context=object(), continuation=True)
        a1_request_bytes = json.dumps(a1_plan.request, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8")
        a1_publication_id = a1_plan.request["publication_id"]
        a1_snapshot_bytes, a1_custody_digests = holder.open_a1(
            a1_request_bytes, a1_publication_id, destination_id, work_deadline)
        require(len(a1_snapshot_bytes) <= 4 * 1024 * 1024,
                "Held A1 continuation selection exceeds the durable 4 MiB bound")
        from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot
        a1_detached = decode_detached_source_snapshot(a1_snapshot_bytes)
        holder.owner.start_heartbeat()
        a1_snapshot_uri = volume_path + "/a1-source-snapshot.bin"
        a1_snapshot_artifact = _put_volume_bytes(api, a1_snapshot_uri, a1_snapshot_bytes)
        task_trial["source_custody_digests"] = a1_custody_digests
        a1_common = {"trial": task_trial, "protocol_profile": config["protocol_profile"],
            "phase": "a1", "genesis_volume_uri": a1_genesis_uri,
            "genesis_sha256": a1_genesis_artifact["sha256"],
            "genesis_manifest_volume_uri": a1_manifest_uri,
            "genesis_manifest_sha256": a1_manifest_artifact["sha256"],
            "a1_snapshot_volume_uri": a1_snapshot_uri,
            "a1_snapshot_sha256": a1_snapshot_artifact["sha256"]}
        a1_pub_id, a1_pub_task_id, _, a1_pub_result = _run_stage(api, config,
            "a1-publisher", payload["publisher"], {"task_parameters_json": json.dumps(
                a1_common, sort_keys=True, separators=(",", ":"), ensure_ascii=False)},
            work_deadline, cleanup_deadline=deadline, recovery_state=recovery_state,
            recovery_path=recovery_path, config_key="publisher")
        run_ids["a1-publisher"] = a1_pub_id
        _validate_task_runtime(a1_pub_result, config["expected_runtime"], "publisher")
        a1_receipt, a1_request_bytes, a1_manifest_bytes, a1_receipt_bytes, a1_vector, a1_vector_sha = (
            _publisher_bundle(a1_pub_result,
                "managed-current-spark:" + config["expected_principal"], phase="a1"))
        require(a1_request_bytes == json.dumps(a1_plan.request, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8"),
            "A1 publisher request differs from the exact post-A0 continuation plan")
        a1_result_raw = json.dumps(a1_pub_result, sort_keys=True, separators=(",", ":"),
            ensure_ascii=False, allow_nan=False).encode("utf-8")
        a1_result_uri = volume_path + "/a1-publisher-result.json"
        a1_result_artifact = _put_volume_bytes(api, a1_result_uri, a1_result_raw)
        from ashlar_host.managed_current_a1_prefix_builder import build_managed_current_a1_prefix_evidence
        a1_prefix, a1_prefix_sha, a1_selected_prefix = build_managed_current_a1_prefix_evidence(
            genesis_bytes=genesis_bytes, source_snapshot_bytes=a1_snapshot_bytes,
            initial_manifest_bytes=a0_manifest_bytes, request_bytes=a1_request_bytes,
            publication_manifest_bytes=a1_manifest_bytes,
            destination={"catalog": config["catalog"], "schema": config["schema"]},
            carrier_profile="ashlar-delta/0.4-xxhash64",
            materialization_clock_us=trial["materialized_at"])
        a1_prefix_uri = volume_path + "/a1-source-prefix.json"
        a1_prefix_artifact = _put_volume_bytes(api, a1_prefix_uri, a1_prefix)
        a1_selected_raw = json.dumps(a1_selected_prefix, sort_keys=True,
            separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
        a1_selected_uri = volume_path + "/a1-selected-prefix.json"
        a1_selected_artifact = _put_volume_bytes(api, a1_selected_uri, a1_selected_raw)
        a1_bundle = _retention_bundle(trial=task_trial | {
                "verifier_timeout_seconds": config["verifier"]["timeout_seconds"]},
            protocol_profile=config["protocol_profile"], plan=a1_plan,
            request_bytes=a1_request_bytes, manifest_bytes=a1_manifest_bytes,
            destination_receipt=a1_receipt_bytes, task_receipt=a1_receipt,
            prefix_bytes=a1_prefix, prefix_sha=a1_prefix_sha,
            selected_prefix=a1_selected_prefix, source_snapshot=a1_snapshot_bytes,
            custody_digests=a1_custody_digests)
        a1_vector_wire = {"authority": a1_bundle.vector.authority,
            "scope_kind": a1_bundle.vector.scope_kind, "scope_id": a1_bundle.vector.scope_id,
            "custody_digest": a1_bundle.vector.custody_digest,
            "targets": {key: list(value) for key, value in a1_bundle.vector.targets.items()}}
        require(a1_vector_wire == a1_vector,
                "A1 retention builder and publisher vector differ")
        a1_pin_root = pin_root / "a1"
        a1_pin_root.mkdir(mode=0o700)
        a1_pin_timeout = min(900, int(deadline - time.monotonic()
            - POST_ACK_CLEANUP_RESERVE_SECONDS))
        require(a1_pin_timeout >= 31,
                "Insufficient remaining wall deadline for the A1 native pin interval")
        pin_owner = ManagedA1LocalPinOwner(a1_pin_root / "pgdata", a1_bundle.selection,
            a1_bundle.vector, native_timeout_seconds=a1_pin_timeout,
            deadline_monotonic=work_deadline - config["verifier"]["timeout_seconds"])
        pin_owner.__enter__()
        result["a1_local_pin_owner"] = pin_owner.register()
        pin_stages = _LocalPinStages(pin_owner, lifetime_seconds=a1_pin_timeout)
        a1_verify_params = dict(a1_common)
        a1_verify_params.update({"publisher_result_volume_uri": a1_result_uri,
            "publisher_result_sha256": a1_result_artifact["sha256"],
            "prefix_volume_uri": a1_prefix_uri, "prefix_sha256": a1_prefix_artifact["sha256"],
            "selected_prefix_volume_uri": a1_selected_uri,
            "selected_prefix_sha256": a1_selected_artifact["sha256"],
            "source_rpc_directory": rpc_dir})
        a1_ver_id, a1_ver_task_id, _, a1_ver_result = _run_stage(api, config,
            "a1-verifier", payload["verifier"], {"task_parameters_json": json.dumps(
                a1_verify_params, sort_keys=True, separators=(",", ":"), ensure_ascii=False)},
            work_deadline, on_poll=lambda: _service_source_observer_rpc(api, holder, rpc_dir,
                work_deadline, expected_vector_sha256=a1_vector_sha,
                expected_custody_digests=a1_custody_digests, pin_stage=pin_stages),
            cleanup_deadline=deadline, recovery_state=recovery_state,
            recovery_path=recovery_path, config_key="verifier")
        run_ids["a1-verifier"] = a1_ver_id
        _validate_task_runtime(a1_ver_result, config["expected_runtime"], "verifier")
        holder.renew(work_deadline)
        require(pin_stages.stages == ["open", "pre_ack", "close"] and pin_stages.handle is None,
                "A1 local native pin interval did not complete all observer stages")
        pin_owner.__exit__(None, None, None)
        recovery_state["a1_pin_postgres_cleanup"] = _cleanup_pin_pgdata(
            pin_owner, pin_pg_ctl_lock, deadline)
        pin_owner = None
        a1_proof = _verify_terminal_proof(a1_ver_result, a1_ver_id, a1_ver_task_id,
            a1_request_bytes, a1_manifest_bytes, a1_receipt_bytes, a1_vector_sha,
            a1_vector, phase="a1")
        result["a1"] = {"publication_id": a1_publication_id,
            "request_sha256": hashlib.sha256(a1_request_bytes).hexdigest(),
            "manifest_sha256": hashlib.sha256(a1_manifest_bytes).hexdigest(),
            "receipt_sha256": hashlib.sha256(a1_receipt_bytes).hexdigest(),
            "vector_sha256": a1_vector_sha, "publisher_run_id": a1_pub_id,
            "verifier_run_id": a1_ver_id, "terminal_proof": a1_proof}
        require(deadline - time.monotonic() >= config["cleanup_reserve_seconds"],
                "Do not bind or ACK A1 without the full remaining ACK/reap/cleanup reserve")
        holder.bind(manifest_bytes=a1_manifest_bytes, vector=a1_vector,
            receipt_bytes=a1_receipt_bytes, deadline=deadline)
        a1_attempt_dir = a1_pin_root / "attempts"
        a1_attempt_dir.mkdir(parents=True, exist_ok=False, mode=0o700)
        os.chmod(a1_attempt_dir, 0o700)
        a1_store = DurableA1AckAttemptStore(a1_attempt_dir)
        a1_page = a1_detached.source_page
        recovery_state["a1_source"] = {"phase": "a1",
            "publication_id": a1_publication_id,
            "request_sha256": hashlib.sha256(a1_request_bytes).hexdigest(),
            "manifest_sha256": hashlib.sha256(a1_manifest_bytes).hexdigest(),
            "receipt_sha256": hashlib.sha256(a1_receipt_bytes).hexdigest(),
            "selection_sha256": hashlib.sha256(a1_snapshot_bytes).hexdigest(),
            "prior_checkpoint_sha256": hashlib.sha256(a1_page.prior_bytes).hexdigest(),
            "next_checkpoint_sha256": hashlib.sha256(a1_page.next_bytes).hexdigest(),
            "page_sha256": a1_page.page_fields["sha256"],
            "custody_digests": a1_custody_digests}
        recovery_state["phase"] = "a1_verified_prepared_for_ack"
        _write_private_json(recovery_path, recovery_state)
        a1_store.begin(selection_bytes=a1_snapshot_bytes, request_bytes=a1_request_bytes,
            manifest_bytes=a1_manifest_bytes, receipt_bytes=a1_receipt_bytes,
            prior_checkpoint_bytes=a1_page.prior_bytes, next_checkpoint_bytes=a1_page.next_bytes,
            page_sha256=a1_page.page_fields["sha256"], destination_id=destination_id,
            publication_id=a1_publication_id)
        a1_prepared = a1_store.load()
        recovery_state["a1_ack_attempt_directory"] = str(a1_store.directory)
        recovery_state["a1_ack"] = {"state": "durable_attempt_prepared",
            "attempt_directory": str(a1_store.directory),
            "attempt_sha256": hashlib.sha256(a1_store.path.read_bytes()).hexdigest(),
            "selection_sha256": a1_prepared.selection_sha256,
            "request_sha256": a1_prepared.request_sha256,
            "manifest_sha256": a1_prepared.manifest_sha256,
            "receipt_sha256": a1_prepared.receipt_sha256,
            "publication_id": a1_prepared.publication_id}
        result["a1_ack_attempt"] = {key: recovery_state["a1_ack"][key]
            for key in ("selection_sha256", "request_sha256", "manifest_sha256",
                        "receipt_sha256", "publication_id")}
        _write_private_json(recovery_path, recovery_state)
        ack_attempted = True
        recovery_state["a1_ack"]["state"] = "dispatch_intent"
        _write_private_json(recovery_path, recovery_state)
        a1_settlement = holder.acknowledge_once(a1_store,
            attempt_directory=a1_store.directory, deadline=deadline)
        _validate_ack_settlement(a1_settlement, request_bytes=a1_request_bytes,
            manifest_bytes=a1_manifest_bytes, snapshot_bytes=a1_snapshot_bytes,
            next_checkpoint_bytes=a1_detached.source_page.next_bytes,
            receipt_bytes=a1_receipt_bytes)
        recovery_state["a1_ack"]["state"] = "acknowledged_exactly_reconciled"
        recovery_state["phase"] = "acknowledged_cleanup_pending"
        _write_private_json(recovery_path, recovery_state)
        result["source_holder_settlement"] = {"outcome": a1_settlement.outcome,
            "request_sha256": a1_settlement.request_sha256,
            "manifest_sha256": a1_settlement.manifest_sha256,
            "selection_sha256": a1_settlement.selection_sha256}
        verified_ack = True
        result["source_holder_closed"] = True
        _wait_source_group_absent(holder, deadline)
        recovery_state["source_postgres_cleanup"] = _cleanup_source_pgdata(
            holder_config, source_pg_ctl_lock, deadline)
        result["source_postgres_cleanup"] = recovery_state["source_postgres_cleanup"]
        _write_private_json(recovery_path, recovery_state)

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
        for uri in (genesis_uri, snapshot_uri, initial_uri, prefix_uri,
                    publisher_result_uri, selected_prefix_uri, a1_manifest_uri,
                    a1_result_uri, a1_prefix_uri, a1_selected_uri):
            api.remove_file("dbfs:" + uri)
        directory = api.call("get", "/api/2.0/fs/directories/"
                              + quote(volume_path, safe="/"))
        contents = directory.get("contents")
        require(type(contents) is list and contents == [],
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
        import shutil
        shutil.rmtree(pin_root)
        pin_root = None
        return result
    except BaseException:
        cleanup_evidence = recovery_state.setdefault("postgres_cleanup", [])
        if pin_stages is not None:
            try: pin_stages.close_on_error()
            except Exception as error:
                cleanup_evidence.append({"owner": "pin", "stage": "handle_close",
                    "error": f"{type(error).__name__}: {error}"[:500]})
        if pin_owner is not None:
            try: pin_owner.__exit__(*sys.exc_info())
            except Exception as error:
                cleanup_evidence.append({"owner": "pin", "stage": "owner_exit",
                    "error": f"{type(error).__name__}: {error}"[:500]})
        if pin_owner is not None and pin_pg_ctl_lock is not None:
            try:
                cleanup_evidence.append({"owner": "pin", **_cleanup_pin_pgdata(
                    pin_owner, pin_pg_ctl_lock, deadline)})
            except Exception as error:
                cleanup_evidence.append({"owner": "pin", "state": "unknown",
                    "error": f"{type(error).__name__}: {error}"[:500]})
        if holder is not None:
            try:
                if ack_attempted:
                    holder.owner.terminate()
                else:
                    try: holder.owner.close_before_ack()
                    except Exception: holder.owner.terminate()
                _wait_source_group_absent(holder, deadline)
                cleanup_evidence.extend({"owner": "source", **item}
                    for item in _cleanup_source_pgdata(holder_config, source_pg_ctl_lock, deadline))
            except Exception as error:
                cleanup_evidence.append({"owner": "source", "state": "unknown",
                    "error": f"{type(error).__name__}: {error}"[:500]})
        # Keep schema and volume if either run or source ACK has an unclear result.
        result["status"] = "recovery_required" if not verified_ack else "cleanup_incomplete"
        recovery_state["phase"] = result["status"]
        recovery_state["known_run_ids"] = dict(run_ids)
        recovery_state["pin_root"] = str(pin_root) if pin_root is not None else None
        recovery_state["ack_attempted"] = ack_attempted
        try: _write_private_json(recovery_root / "recovery.json", recovery_state)
        except Exception: pass
        raise


def run(config: dict, payload: dict, profile: str, *, launch: bool) -> dict:
    # Static mode deliberately does not even resolve CLI credentials.
    if not launch:
        return {"status": "validated_only", "profile": profile,
                "workspace": config["host"], "payload": payload}
    require(threading.current_thread() is threading.main_thread(),
            "Bounded launch must run on the main thread for outer-deadline supervision")
    start = time.monotonic()
    recovery_root = Path(tempfile.mkdtemp(prefix="ashlar-a1-recovery-"))
    os.chmod(recovery_root, 0o700)
    recovery_path = recovery_root / "recovery.json"
    recovery_state = {"profile": "ashlar-managed-a1-local-recovery/0.1",
        "trial_id": config["trial_id"], "workspace": config["host"],
        "started_at_epoch_ms": int(time.time() * 1000), "phase": "launch_gate",
        "deadline_epoch_ms": int(time.time() * 1000) +
            config["maximum_wall_seconds"] * 1000,
        "jobs": {}, "known_run_ids": {}, "ack_attempted": False,
        "recovery_path": str(recovery_path)}
    _write_private_json(recovery_path, recovery_state)
    previous_handler = signal.getsignal(signal.SIGALRM)
    previous_timer = signal.getitimer(signal.ITIMER_REAL)
    def expired(_signum, _frame):
        raise TimeoutError("Whole managed A1 wall deadline expired")
    signal.signal(signal.SIGALRM, expired)
    signal.setitimer(signal.ITIMER_REAL, config["maximum_wall_seconds"])
    try:
        result = _launch_trial(config, payload, profile, recovery_root=recovery_root,
            recovery_state=recovery_state, start=start)
        recovery_state.update({"phase": "complete", "status": result.get("status"),
            "known_run_ids": dict(recovery_state.get("known_run_ids", {}))})
        _write_private_json(recovery_path, recovery_state)
        import shutil
        shutil.rmtree(recovery_root)
        return result
    except BaseException as error:
        recovery_state.setdefault("phase", "recovery_required")
        recovery_state["failure_type"] = type(error).__name__
        recovery_state["failure"] = str(error)[:1500]
        recovery_state["failure_notes"] = list(getattr(error, "__notes__", ()))[:16]
        recovery_state["known_run_ids"] = dict(recovery_state.get("known_run_ids", {}))
        try: _write_private_json(recovery_path, recovery_state)
        except Exception: pass
        raise RuntimeError(f"Managed A1 stopped; private recovery receipt retained at {recovery_path}") from error
    finally:
        signal.setitimer(signal.ITIMER_REAL, 0)
        signal.signal(signal.SIGALRM, previous_handler)
        if previous_timer[0] > 0:
            signal.setitimer(signal.ITIMER_REAL, previous_timer[0], previous_timer[1])


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
