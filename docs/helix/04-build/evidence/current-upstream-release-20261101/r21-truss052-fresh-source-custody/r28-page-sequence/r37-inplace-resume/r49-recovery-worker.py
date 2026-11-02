#!/usr/bin/env python3
"""R49 bounded no-replay recovery for the committed R42 seed page.

Preflight validates the exact pending receipt, original R36 journal/targets,
retained page and the real resume parser. Native mode restores the already
applied artifact, reads its exact committed Delta vector, persists a completion
marker, and only then permits the same-session protected Truss ACK.
"""
from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.metadata
import importlib.util
import json
import os
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[7]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))
R42_WORKER = HERE / "r42-local-worker.py"
R42_SCOPE = HERE / "r42-host-scope.json"
PENDING_RECEIPT = Path("/private/tmp/ashlar-r36-truss052-page-sequence-20261101/delta/publication-readback-pending-8d6db42c4d167a36468e261cf2b495fdaefedf1a4a963dbd09cae91f39a3c9ea.json")
PROFILE = "ashlar-r49-truss052-committed-publication-recovery/0.1"


class Refusal(RuntimeError):
    pass


class RecoveryInputShapeValidated(RuntimeError):
    """Effect-free sentinel proving recovery inputs validated before API calls."""
    def __init__(self, inputs):
        super().__init__("recovery inputs validated before public calls")
        self.inputs = inputs


class RecoveryInputShapeValidated(RuntimeError):
    """Effect-free sentinel proving recovery inputs passed before public calls."""
    def __init__(self, inputs):
        super().__init__("recovery inputs validated before public calls")
        self.inputs = inputs


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


def strict_json(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise Refusal("duplicate JSON key")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs)
    if type(value) is not dict:
        raise Refusal("closed JSON object required")
    return value


def write_json(path, value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"),
                       ensure_ascii=False, allow_nan=False) + "\n").encode()
    path = Path(path)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        offset = 0
        while offset < len(raw):
            offset += os.write(fd, raw[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)
    dfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try:
        os.fsync(dfd)
    finally:
        os.close(dfd)
    return sha(raw)


def load_r42():
    spec = importlib.util.spec_from_file_location("ashlar_r42_recovery_helpers", R42_WORKER)
    if spec is None or spec.loader is None:
        raise Refusal("retained R42 verifier unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def load_module(path, module_name):
    spec = importlib.util.spec_from_file_location(module_name, path)
    if spec is None or spec.loader is None:
        raise Refusal("retained R36 helper module unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def _scope_checks(scope_path, scope, *, preflight):
    if scope.get("profile") != "ashlar-r37-truss052-inplace-resume/0.1":
        raise Refusal("exact bounded R37 supervisor scope profile required")
    status = scope.get("status")
    retained = scope.get("recovery", {}).get("retained_r48")
    if type(retained) is not dict:
        raise Refusal("R48 supervisor refusal and reconciliation bindings required")
    for label in ("scope", "worker", "supervisor", "receipt", "stdout", "stderr", "reconciliation"):
        path = Path(retained[f"{label}_path"])
        if (not path.is_absolute() or path.resolve(strict=True) != path or path.is_symlink()
                or sha(path.read_bytes()) != retained[f"{label}_sha256"]):
            raise Refusal(f"retained R48 {label} evidence changed")
    r48_receipt = strict_json(Path(retained["receipt_path"]).read_bytes())
    r48_reconciliation = strict_json(Path(retained["reconciliation_path"]).read_bytes())
    if (r48_receipt.get("status") != "supervisor_failed_or_stopped"
            or "PermissionError" not in r48_receipt.get("failure_reason", "")
            or "/bin/ps" not in r48_receipt.get("failure_reason", "")
            or r48_receipt.get("returncode") != -15
            or Path(r48_receipt.get("logs", {}).get("stdout", "")).stat().st_size != 0
            or Path(r48_receipt.get("logs", {}).get("stderr", "")).stat().st_size != 0
            or r48_reconciliation.get("status") != "supervisor_refused_before_worker_no_source_or_destination_effects"
            or r48_reconciliation.get("attempt_root_absent") is not True
            or r48_reconciliation.get("source", {}).get("tree_sha256") != scope["recovery"]["pgdata_tree"]["expected"]["sha256"]
            or r48_reconciliation.get("delta", {}).get("journal_sha256") != scope["recovery"]["journal"]["sha256"]
            or r48_reconciliation.get("delta", {}).get("counts") != scope["recovery"]["journal"]["counts"]
            or r48_reconciliation.get("delta", {}).get("heads") != retained["delta_heads"]
            or r48_reconciliation.get("delta", {}).get("new_ack_row") is not False
            or r48_reconciliation.get("delta", {}).get("new_delta_publication") is not False
            or r48_reconciliation.get("independent_custody", {}).get("postmaster_pid_present") is not False
            or r48_reconciliation.get("independent_custody", {}).get("matching_postgres_processes_for_pgdata") != []):
        raise Refusal("R48 refused before worker and reconciles unchanged, stopped source/destination")
    if Path(retained["attempt_root"]).exists():
        raise Refusal("R48 attempt root unexpectedly exists")
    if ((preflight and status not in ("pending_sol_review", "sol_reviewed_and_dispatchable"))
            or (not preflight and status != "sol_reviewed_and_dispatchable")):
        raise Refusal("R45 scope status does not match preflight or reviewed native mode")
    if scope.get("authority_class") != "local_development_authority" or scope.get("cloud") is not False:
        raise Refusal("local-development-only scope required")
    limits = scope.get("limits", {})
    if (limits.get("wall_seconds") != 900 or limits.get("process_rss_bytes") != 2 * 1024**3
            or limits.get("attempt_root_bytes") != 512 * 1024**2
            or limits.get("attempts") != 1 or limits.get("automatic_retries") != 0
            or limits.get("cloud") is not False or limits.get("network_listen") is not False):
        raise Refusal("exact bounded local recovery limits required")
    paths = scope["paths"]
    output = Path(paths["output_root"])
    supervisor = Path(paths["supervisor_root"])
    pgdata = Path(paths["data_directory"])
    delta = Path(paths["publication_directory"])
    if any(not item.is_absolute() or item.resolve() != item for item in (output, supervisor, pgdata, delta)):
        raise Refusal("canonical R46 local paths required")
    if not pgdata.is_dir() or pgdata.is_symlink() or not delta.is_dir() or delta.is_symlink():
        raise Refusal("retained stopped PGDATA and Delta destination required")
    if (pgdata / "postmaster.pid").exists():
        raise Refusal("retained PGDATA must be stopped before recovery")
    if preflight and (output.exists() or supervisor.exists()):
        raise Refusal("fresh R46 attempt and supervisor roots required")
    worker = Path(scope["worker"]["path"])
    if worker != Path(__file__).resolve() or sha(worker.read_bytes()) != scope["worker"]["sha256"]:
        raise Refusal("exact R46 worker binding required")
    bound_supervisor = Path(scope["supervisor"]["path"])
    if not bound_supervisor.is_file() or sha(bound_supervisor.read_bytes()) != scope["supervisor"]["sha256"]:
        raise Refusal("exact reviewed R37 supervisor binding required")
    for name, digest in scope["recovery"]["source_bindings"].items():
        selected = REPO / name
        if not selected.is_file() or sha(selected.read_bytes()) != digest:
            raise Refusal(f"R43 source binding differs: {name}")
    pending_path = Path(scope["recovery"]["pending_receipt_path"])
    if (not pending_path.is_absolute() or pending_path.resolve(strict=True) != pending_path
            or pending_path.is_symlink()):
        raise Refusal("canonical immutable pending receipt path required")
    pending_raw = pending_path.read_bytes()
    if sha(pending_raw) != scope["recovery"]["pending_receipt_sha256"]:
        raise Refusal("R42 immutable pending receipt changed")
    pending = strict_json(pending_raw)
    prior = scope["retained_r43"]
    for label in ("failure", "supervisor"):
        retained_path = Path(prior[f"{label}_receipt_path"])
        retained_raw = retained_path.read_bytes()
        if sha(retained_raw) != prior[f"{label}_receipt_sha256"]:
            raise Refusal(f"retained R43 {label} receipt changed")
    prior_failure = strict_json(Path(prior["failure_receipt_path"]).read_bytes())
    if (prior_failure.get("stage") != "local_postgres_start"
            or prior_failure.get("ack_outcome") != "not_attempted"
            or prior_failure.get("effects", {}).get("source_ack_attempted") is not False
            or prior_failure.get("effects", {}).get("delta_effects_may_remain") is not False):
        raise Refusal("R43 must remain a verified pre-Delta, pre-ACK failure")
    reconciliation_path = Path(prior["reconciliation_path"])
    if (not reconciliation_path.is_absolute()
            or reconciliation_path.resolve(strict=True) != reconciliation_path
            or sha(reconciliation_path.read_bytes()) != prior["reconciliation_sha256"]):
        raise Refusal("retained R43 read-only reconciliation changed")
    reconciliation = strict_json(reconciliation_path.read_bytes())
    if reconciliation.get("status") != "source_and_destination_baseline_unchanged":
        raise Refusal("R43 reconciliation does not establish unchanged recovery baseline")
    prior44 = scope["retained_r44"]
    for label in ("failure", "supervisor", "stderr", "configuration"):
        retained_path = Path(prior44[f"{label}_path"])
        raw_retained = retained_path.read_bytes()
        if sha(raw_retained) != prior44[f"{label}_sha256"]:
            raise Refusal(f"retained R44 {label} evidence changed")
    r44_failure = strict_json(Path(prior44["failure_path"]).read_bytes())
    if (r44_failure.get("stage") != "restore_readback_and_protected_ack"
            or r44_failure.get("ack_outcome") != "not_attempted"
            or r44_failure.get("effects", {}).get("source_ack_attempted") is not False):
        raise Refusal("R44 must remain a verified no-ACK recovery refusal")
    r44_reconciliation_path = Path(prior44["reconciliation_path"])
    if (not r44_reconciliation_path.is_absolute()
            or r44_reconciliation_path.resolve(strict=True) != r44_reconciliation_path
            or sha(r44_reconciliation_path.read_bytes()) != prior44["reconciliation_sha256"]):
        raise Refusal("retained R44 read-only reconciliation changed")
    if strict_json(r44_reconciliation_path.read_bytes()).get("status") != "no_delta_or_ack_mutation_confirmed":
        raise Refusal("R44 read-only reconciliation does not establish a safe resume state")
    prior47 = scope["recovery"]["retained_r47"]
    for label in ("failure", "supervisor", "stderr", "reconciliation", "scope", "worker"):
        retained_path = Path(prior47[f"{label}_path"])
        if (not retained_path.is_absolute() or retained_path.resolve(strict=True) != retained_path
                or sha(retained_path.read_bytes()) != prior47[f"{label}_sha256"]):
            raise Refusal(f"retained R47 {label} evidence changed")
    r47_failure = strict_json(Path(prior47["failure_path"]).read_bytes())
    r47_supervisor = strict_json(Path(prior47["supervisor_path"]).read_bytes())
    r47_reconciliation = strict_json(Path(prior47["reconciliation_path"]).read_bytes())
    if (r47_failure.get("failure_type") != "ValueError"
            or r47_failure.get("stage") != "restore_readback_and_protected_ack"
            or r47_failure.get("effects", {}).get("source_ack_attempted") is not False
            or r47_failure.get("ack_outcome") != "not_attempted"
            or r47_reconciliation.get("status") != "failed_before_recovery_completion_or_ack"
            or r47_reconciliation.get("delta", {}).get("new_delta_version") is not False
            or r47_supervisor.get("process_group_remaining") != []
            or r47_supervisor.get("postgres_cluster_cleanup", {}).get("status") != "absent"):
        raise Refusal("R47 must remain reconciled as no-publication, no-ACK")
    if any((Path(prior47["attempt_root"]) / "receipts" / name).exists()
           for name in ("protected-ack-attempt.json", "protected-ack-reconciliation.json",
                        "publication-readback-complete.json", "recovery-result.json")):
        raise Refusal("R47 recovery completion or ACK receipt appeared after reconciliation")
    prior46 = scope["recovery"]["retained_r46"]
    for label in ("failure", "supervisor", "reconciliation"):
        retained_path = Path(prior46[f"{label}_path"])
        if (not retained_path.is_absolute() or retained_path.resolve(strict=True) != retained_path
                or sha(retained_path.read_bytes()) != prior46[f"{label}_sha256"]):
            raise Refusal(f"retained R46 {label} evidence changed")
    r46_failure = strict_json(Path(prior46["failure_path"]).read_bytes())
    r46_supervisor = strict_json(Path(prior46["supervisor_path"]).read_bytes())
    r46_reconciliation = strict_json(Path(prior46["reconciliation_path"]).read_bytes())
    if (r46_failure.get("failure_type") != "ValueError"
            or r46_failure.get("stage") != "restore_readback_and_protected_ack"
            or r46_failure.get("effects", {}).get("source_ack_attempted") is not False
            or r46_failure.get("ack_outcome") != "not_attempted"
            or r46_reconciliation.get("status") != "failed_before_restore_or_ack_with_unchanged_delta_baseline"
            or r46_reconciliation.get("delta", {}).get("new_delta_publication") is not False
            or r46_supervisor.get("process_group_remaining") != []
            or r46_supervisor.get("postgres_cluster_cleanup", {}).get("status") != "absent"):
        raise Refusal("R46 must remain reconciled as no-publication, no-ACK")
    if any((Path(prior46["attempt_root"]) / "receipts" / name).exists()
           for name in ("protected-ack-attempt.json", "protected-ack-reconciliation.json",
                        "publication-readback-complete.json", "recovery-result.json")):
        raise Refusal("R46 recovery completion or ACK receipt appeared after reconciliation")
    prior45 = scope["recovery"]["retained_r45"]
    for label in ("failure_receipt", "supervisor_receipt", "reconciliation"):
        path = Path(prior45[f"{label}_path"])
        if (not path.is_absolute() or path.resolve(strict=True) != path
                or sha(path.read_bytes()) != prior45[f"{label}_sha256"]):
            raise Refusal(f"retained R45 {label} changed")
    r45_failure = strict_json(Path(prior45["failure_receipt_path"]).read_bytes())
    r45_supervisor = strict_json(Path(prior45["supervisor_receipt_path"]).read_bytes())
    r45_reconciliation = strict_json(Path(prior45["reconciliation_path"]).read_bytes())
    if (r45_failure.get("failure_type") != "KeyError"
            or r45_failure.get("stage") != "restore_readback_and_protected_ack"
            or r45_failure.get("effects", {}).get("source_ack_attempted") is not False
            or r45_failure.get("ack_outcome") != "not_attempted"
            or r45_reconciliation.get("status") != "failed_before_source_page_custody_or_ack"
            or r45_reconciliation.get("source_ack_attempted") is not False
            or r45_reconciliation.get("delta", {}).get("new_delta_publication") is not False
            or r45_supervisor.get("process_group_remaining") != []
            or r45_supervisor.get("postgres_cluster_cleanup", {}).get("status") != "absent"):
        raise Refusal("R45 must remain reconciled as a no-publication, no-ACK failure")
    if any((Path(prior45["attempt_root"]) / "receipts" / name).exists()
           for name in ("protected-ack-attempt.json", "protected-ack-reconciliation.json",
                        "publication-readback-complete.json", "recovery-result.json")):
        raise Refusal("R45 recovery completion or ACK receipt appeared after reconciliation")
    if (pending.get("status") != "publication_returned_native_readback_pending"
            or pending.get("acknowledgement") != "not_attempted"
            or pending.get("request_digest") != scope["recovery"]["request_digest"]
            or pending.get("publication_id") != scope["recovery"]["publication_id"]):
        raise Refusal("R42 unacknowledged committed-publication receipt required")
    intent = Path(scope["recovery"]["publication_intent_path"])
    if (not intent.is_absolute() or intent.resolve(strict=True) != intent
            or intent.is_symlink()):
        raise Refusal("canonical original publication intent path required")
    intent_raw = intent.read_bytes()
    if sha(intent_raw) != scope["recovery"]["publication_intent_sha256"]:
        raise Refusal("Original publication intent changed")
    intent_value = strict_json(intent_raw)
    if (intent_value.get("request_digest") != pending["request_digest"]
            or intent_value.get("publication_id") != pending["publication_id"]
            or intent_value.get("source_page_sha256") != pending["source_page"]["archive_sha256"]):
        raise Refusal("Original intent differs from the immutable pending receipt")
    page_path = Path(pending["source_page"]["path"])
    if (page_path != Path(scope["recovery"]["source_page_path"])
            or not page_path.is_absolute() or page_path.resolve(strict=True) != page_path
            or page_path.is_symlink()
            or sha(page_path.read_bytes()) != pending["source_page"]["archive_sha256"]):
        raise Refusal("Original archived public page changed")
    # Decode the exact retained archive during effect-free admission. The
    # carrier digest and FeedPage.content digest are different commitments.
    from ashlar_host.truss_page_archive import decode_truss_page
    archived_page = decode_truss_page(
        page_path.read_bytes(), pending["source_page"]["archive_sha256"], pending["feed"])
    archived_source = pending["source_page"]
    if (archived_page.sha256 != archived_source["page_sha256"]
            or sha(archived_page.content) != archived_source["content_sha256"]
            or sha(archived_page.prior.content) != archived_source["prior_sha256"]
            or sha(archived_page.next.content) != archived_source["next_sha256"]
            or archived_page.registration_id != archived_source["registration_id"]
            or archived_page.writer_xid != archived_source["writer_xid"]
            or archived_page.consumer_id != pending["consumer_id"]):
        raise Refusal("Decoded retained public page differs from pending source-page bindings")
    if (Path(scope["recovery"]["pgdata_tree"]["path"]) != pgdata
            or not Path(scope["recovery"]["pgdata_tree"]["path"]).is_dir()):
        raise Refusal("R42 stopped source path unavailable")
    base = load_r42()
    resume_evidence = base.load_base().validate_resume_evidence(scope)
    evidence = resume_evidence.get("evidence")
    if type(evidence) is not dict:
        raise Refusal("validated retained acceptance evidence is unavailable")
    r36_worker_path = Path(scope["retained_r36"]["base_worker_path"])
    if (not r36_worker_path.is_file()
            or sha(r36_worker_path.read_bytes()) != scope["retained_r36"]["base_worker_sha256"]):
        raise Refusal("exact retained R36 public API helper binding required")
    base_r36 = load_module(r36_worker_path, "ashlar_r46_r36_public_api_helpers")
    actual_pg = base.tree_digest(pgdata, private=True, max_bytes=512 * 1024 * 1024)
    if actual_pg != scope["recovery"]["pgdata_tree"]["expected"]:
            raise Refusal("R46 retained stopped PGDATA tree differs")
    # The journal is read through SQLite's normal WAL-aware read-only mode.
    # R44 left an empty WAL and a transient 32 KiB SHM index; retain both and
    # bind their safe metadata rather than using immutable=1 or deleting files.
    journal = delta / "operations.sqlite"
    import sqlite3, stat
    expected_journal = scope["recovery"]["journal"]
    def sidecar_snapshot():
        observed = {}
        for suffix in ("-wal", "-shm"):
            path = Path(str(journal) + suffix)
            try:
                before = path.lstat()
            except FileNotFoundError:
                observed[suffix] = {"present": False, "bytes": 0, "dev": None, "ino": None, "mode": None}
                continue
            if (not stat.S_ISREG(before.st_mode) or before.st_uid != os.getuid()
                    or stat.S_IMODE(before.st_mode) != 0o600 or before.st_size > 65536):
                raise Refusal("R46 SQLite sidecar must be bounded, owner-private regular file")
            fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                opened = os.fstat(fd)
                if (not stat.S_ISREG(opened.st_mode) or opened.st_uid != os.getuid()
                        or stat.S_IMODE(opened.st_mode) != 0o600
                        or (opened.st_dev, opened.st_ino, opened.st_size) !=
                           (before.st_dev, before.st_ino, before.st_size)):
                    raise Refusal("R46 SQLite sidecar changed during safe open")
                digest = None
                if suffix == "-wal":
                    content = bytearray()
                    while len(content) <= opened.st_size:
                        chunk = os.read(fd, min(16384, opened.st_size + 1 - len(content)))
                        if not chunk:
                            break
                        content.extend(chunk)
                    if len(content) != opened.st_size:
                        raise Refusal("R46 SQLite WAL changed during bounded read")
                    digest = sha(bytes(content))
                    if opened.st_size != 0:
                        raise Refusal("R46 SQLite WAL must be empty before journal admission")
                observed[suffix] = {"present": True, "bytes": opened.st_size,
                    "dev": opened.st_dev, "ino": opened.st_ino, "mode": stat.S_IMODE(opened.st_mode),
                    "uid": opened.st_uid, "sha256": digest}
            finally:
                os.close(fd)
        return observed
    sidecars_before = sidecar_snapshot()
    expected_sidecars = expected_journal["sidecars"]
    for suffix, label in (("-wal", "operations.sqlite-wal"), ("-shm", "operations.sqlite-shm")):
        actual = sidecars_before[suffix]
        expected = expected_sidecars[label]
        if (actual["present"] != expected["present"] or actual["bytes"] != expected["bytes"]
                or (suffix == "-wal" and actual.get("sha256") != expected.get("sha256"))):
            raise Refusal("R46 retained SQLite sidecar state differs")
    main_before = sha(journal.read_bytes())
    if main_before != expected_journal["sha256"]:
        raise Refusal("R46 retained SQLite main journal changed")
    uri = "file:" + str(journal) + "?mode=ro"
    connection = sqlite3.connect(uri, uri=True, timeout=2)
    try:
        connection.execute("PRAGMA query_only=ON")
        integrity = connection.execute("PRAGMA integrity_check").fetchone()[0]
        if integrity != "ok":
            raise Refusal("R46 retained Delta coordination journal integrity failed")
        artifact = connection.execute(
            "SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?",
            (pending["request_digest"],)).fetchall()
        if len(artifact) != 1:
            raise Refusal("exact committed R42 applied artifact required")
        if strict_json(artifact[0][0].encode()) != strict_json(
                base64.b64decode(pending["request_bytes_base64"], validate=True)):
            raise Refusal("R42 applied artifact request differs from retained receipt")
        counts = {table: connection.execute("SELECT count(*) FROM " + table).fetchone()[0]
                  for table in ("local_operation", "local_plan", "local_source_plan", "local_publication_artifact")}
        if counts != expected_journal["counts"]:
            raise Refusal("R46 retained coordination journal counts differ")
        if (integrity != expected_journal["integrity_check"]
                or journal.stat().st_size != expected_journal["bytes"]
                or len(artifact) != expected_journal["matching_request_artifacts"]):
            raise Refusal("R46 retained read-only journal identity differs")
    finally:
        connection.close()
    sidecars_after = sidecar_snapshot()
    for suffix in ("-wal", "-shm"):
        before, after = sidecars_before[suffix], sidecars_after[suffix]
        stable_keys = ("present", "bytes", "dev", "ino", "mode", "uid")
        if any(before.get(key) != after.get(key) for key in stable_keys):
            raise Refusal("R46 SQLite sidecar identity changed during read-only inspection")
        if suffix == "-wal" and before.get("sha256") != after.get("sha256"):
            raise Refusal("R46 SQLite WAL changed during read-only inspection")
    if sha(journal.read_bytes()) != main_before:
        raise Refusal("R46 SQLite main journal changed during read-only inspection")
    journal_readonly_receipt = {"connection_mode": "mode=ro with WAL awareness",
        "integrity_check": integrity, "counts": counts, "matching_request_artifacts": len(artifact),
        "main_sha256_before_after": main_before, "sidecars_before": sidecars_before,
        "sidecars_after": sidecars_after, "no_checkpoint_or_delete": True}
    checked = {"scope": scope, "pending": pending, "pending_raw": pending_raw,
            "intent": intent_value, "page_path": page_path, "base": base,
            "resume_evidence": evidence, "pgdata": pgdata, "delta": delta, "output": output,
            "supervisor": supervisor, "base_r36": base_r36,
            "journal_readonly": journal_readonly_receipt,
            "archived_page": {"archive_sha256": pending["source_page"]["archive_sha256"],
                "page_sha256": archived_page.sha256, "content_sha256": sha(archived_page.content),
                "feed": pending["feed"], "registration_id": archived_page.registration_id,
                "consumer_id": archived_page.consumer_id, "writer_xid": archived_page.writer_xid}}
    _prepare_recovery_inputs(scope, pending, evidence)
    return checked


def _resume_config(scope, info, output):
    import psycopg
    source = scope["source"]
    params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
    reader = psycopg.conninfo.make_conninfo(**dict(params, user=source["ordinary_reader_role"]))
    registrar = psycopg.conninfo.make_conninfo(**dict(params, user=source["registrar_login_role"]))
    pending = strict_json(Path(scope["recovery"]["pending_receipt_path"]).read_bytes())
    intent_path = Path(scope["recovery"]["publication_intent_path"])
    jars = [item["path"] for item in scope["spark_runtime"]["jars"]]
    return {"connection_uri": reader, "catalog_connection_uri": registrar,
            "publication_directory": scope["paths"]["publication_directory"], "jars": jars,
            "publication_intent": {"path": str(intent_path),
                                   "sha256": scope["recovery"]["publication_intent_sha256"]},
            "destination_id": pending["destination_id"],
            "carrier_profile": scope["publication"]["carrier_profile"],
            "runtime_profile": {"profile": scope["spark_runtime"]["runtime_profile"],
                "java_home": scope["spark_runtime"]["java_home"],
                "jar_inventory": {item["name"]: item["sha256"] for item in scope["spark_runtime"]["jars"]}},
            "archived_acceptance_files": []}


def _prepare_recovery_inputs(scope, pending, evidence):
    """Validate every nested recovery input against the retained R24 receipts."""
    def require_object(value, keys, label):
        if type(value) is not dict or not set(keys) <= set(value):
            raise Refusal(f"R46 {label} fields are incomplete")
        return value

    source = require_object(scope.get("source"), (
        "registrar_login_role", "ordinary_reader_role", "consumer_id", "source_holder_id",
        "ack_registration_id", "ack_scope_id"), "source")
    resume = require_object(scope.get("resume"), ("source_postconditions",), "resume")
    post = require_object(resume["source_postconditions"], (
        "installation_id", "accepted_document_sha256", "accepted_registration_id",
        "accepted_report_sha256", "consumer_registration_id", "consumer_checkpoint_sha256"),
        "resume source postconditions")
    model = require_object(scope.get("model"), ("derived_path", "derived_sha256", "document_id",
        "document_revision"), "model")
    publication = require_object(scope.get("publication"),
        ("maximum_page_bytes", "feed", "destination_id", "predecessor"), "publication")
    recovery = require_object(scope.get("recovery"), ("publication_intent_path",
        "publication_intent_sha256", "request_digest", "publication_id", "source_page_path",
        "source_page_archive_sha256"), "recovery")
    retained_r36 = require_object(scope.get("retained_r36"), ("ack_checkpoint_sha256",), "retained R36 ACK")
    runtime = require_object(scope.get("spark_runtime"), ("jars", "java_home", "runtime_profile"), "runtime")
    runtime_candidate = require_object(scope.get("runtime_candidate"), ("release_id",), "Truss runtime candidate")
    acceptance = require_object(evidence.get("acceptance.json") if type(evidence) is dict else None,
        ("document_id", "document_revision", "model_sha256", "registration_id", "report_sha256", "revision"),
        "validated acceptance receipt")
    installation = require_object(evidence.get("installation.json"), ("installation_id",), "installation receipt")
    consumer_registration = require_object(evidence.get("consumer-registration.json"),
        ("consumer_id", "registration_id", "checkpoint_sha256"), "consumer-registration receipt")
    page = require_object(pending.get("source_page"), ("registration_id", "page_sha256", "archive_sha256",
        "prior_sha256", "next_sha256", "writer_xid"), "pending source page")
    require_object(pending, ("status", "acknowledgement", "request_digest", "request_sha256",
        "publication_id", "request_bytes_base64", "destination_id", "consumer_id", "feed",
        "source_page"), "pending publication")
    if type(pending["request_bytes_base64"]) is not str or len(pending["request_bytes_base64"]) > 4 * 1024 * 1024:
        raise Refusal("R46 retained canonical request bytes are missing or over bound")
    try:
        request_raw = base64.b64decode(pending["request_bytes_base64"], validate=True)
        request = strict_json(request_raw)
    except (ValueError, TypeError) as error:
        raise Refusal("R46 retained request bytes are not canonical JSON input") from error
    if (sha(request_raw) != pending["request_sha256"]
            or request.get("request_digest") != pending["request_digest"]
            or request.get("batch_id") != page["writer_xid"]
            or request.get("predecessor") != publication["predecessor"]
            or pending["request_digest"] != recovery["request_digest"]
            or pending["publication_id"] != recovery["publication_id"]
            or pending["destination_id"] != publication["destination_id"]
            or pending["feed"] != publication["feed"]
            or page["path"] != recovery["source_page_path"]
            or page["archive_sha256"] != recovery["source_page_archive_sha256"]
            or not page["writer_xid"].isascii() or not page["writer_xid"].isdigit()
            or page["writer_xid"].startswith("0")):
        raise Refusal("R46 retained request, publication, page or destination binding differs")
    if (type(acceptance["revision"]) is not int or acceptance["revision"] < 1
            or str(acceptance["document_revision"]) != str(model["document_revision"])
            or acceptance["document_id"] != model["document_id"]
            or acceptance["model_sha256"] != model["derived_sha256"]
            or acceptance["report_sha256"] != post["accepted_report_sha256"]
            or acceptance["registration_id"] != post["accepted_registration_id"]
            or acceptance["model_sha256"] != post["accepted_document_sha256"]
            or installation["installation_id"] != post["installation_id"]
            or consumer_registration["consumer_id"] != source["consumer_id"]
            or consumer_registration["registration_id"] != post["consumer_registration_id"]
            or consumer_registration["checkpoint_sha256"] != post["consumer_checkpoint_sha256"]
            or page["registration_id"] != consumer_registration["registration_id"]
            or page["prior_sha256"] != retained_r36["ack_checkpoint_sha256"]
            or pending["consumer_id"] != source["consumer_id"]
            or pending["feed"] != publication["feed"]
            or type(publication["maximum_page_bytes"]) is not int
            or not 1 <= publication["maximum_page_bytes"] <= 1048576
            or type(runtime["jars"]) is not list or len(runtime["jars"]) != 2
            or type(runtime_candidate["release_id"]) is not str or not runtime_candidate["release_id"]):
        raise Refusal("R46 retained acceptance, consumer, page, model or runtime bindings differ")
    if (pending.get("status") != "publication_returned_native_readback_pending"
            or pending.get("acknowledgement") != "not_attempted"):
        raise Refusal("R46 requires the exact unacknowledged committed publication")
    return {"source": source, "postconditions": post, "model": model,
        "publication": publication, "recovery": recovery, "runtime": runtime,
        "runtime_candidate": runtime_candidate,
        "acceptance": acceptance, "installation": installation,
        "consumer_registration": consumer_registration, "retained_r36": retained_r36,
        "pending": pending, "page": page}


def _public_recovery_operation(scope, pending_raw, receipts, connect, base, evidence):
    def operation(driver, config, *, validate_only=False):
        pending = strict_json(pending_raw)
        inputs = _prepare_recovery_inputs(scope, pending, evidence)
        if validate_only:
            raise RecoveryInputShapeValidated(inputs)
        import psycopg
        from truss import CatalogRuntimeConfiguration, CatalogRuntimeStatus, adopt_psycopg_host, create_reference_assembly
        from truss.catalog_results import CatalogRegistration
        from truss.feed import FeedConsumer, FeedPage
        from truss.reader_catalog import AcceptedDocument
        from ashlar_host.truss_note_publication import read_page
        from ashlar_host.truss_source_custody_selection import select_truss_local_dev_source_custody_anchor
        from ashlar_host.truss052_source_custody import Truss052SourceCustodySession
        from ashlar_host.truss_local_publication_recovery import recover_committed_truss_publication_and_ack

        source = inputs["source"]
        acceptance = inputs["acceptance"]
        model = inputs["model"]
        publication = inputs["publication"]
        recovery = inputs["recovery"]
        page_proof = inputs["page"]
        expected_model = Path(model["derived_path"])
        if not expected_model.is_absolute():
            expected_model = HERE.parent / expected_model
        expected_model_raw = expected_model.read_bytes()

        def tx(connection, action, read_only=True):
            return base._transaction(connection, action, read_only=read_only, timeout_ms=10000)

        def status_provider():
            with connect(source["registrar_login_role"], True) as connection:
                status = tx(connection, lambda assembly, transaction:
                    base._value(assembly.status_in_transaction(transaction), "fresh registrar runtime status"))
                connection.rollback()
            if (type(status) is not CatalogRuntimeStatus or status.installed is not True
                    or status.installation_id != inputs["installation"]["installation_id"]
                    or status.release_id != inputs["runtime_candidate"]["release_id"]):
                raise Refusal("fresh public Truss runtime status differs from selected installation")
            return status

        with connect(source["registrar_login_role"], True) as connection:
            status, report = tx(connection, lambda assembly, transaction: (
                base._value(assembly.status_in_transaction(transaction), "registrar status"),
                base._value(assembly.report_in_transaction(transaction, acceptance["revision"]),
                            "registrar acceptance report")))
            connection.rollback()
        if (type(status) is not CatalogRuntimeStatus or status.installed is not True
                or status.installation_id != inputs["installation"]["installation_id"]
                or status.release_id != inputs["runtime_candidate"]["release_id"]
                or sha(report.original_bytes) != acceptance["report_sha256"]):
            raise Refusal("public registrar status/report differs from retained acceptance")
        base.validate_retained_report(report.original_bytes,
            registration_id=acceptance["registration_id"], revision=acceptance["revision"])
        with connect(source["ordinary_reader_role"], True) as connection:
            document = tx(connection, lambda assembly, transaction:
                base._value(assembly.accepted_document_in_transaction(
                    transaction, document_id=model["document_id"], maximum_bytes=1048576),
                    "ordinary-reader accepted document"))
            connection.rollback()
        if (type(document) is not AcceptedDocument
                or document.document_id != model["document_id"]
                or sha(document.content) != sha(expected_model_raw)):
            raise Refusal("public ordinary-reader accepted document differs")
        registration = CatalogRegistration(revision=acceptance["revision"],
            registration_id=acceptance["registration_id"],
            report_bytes=report.original_bytes, replayed=False)
        reader_factory = base.ordinary_reader_connection_factory(
            psycopg, psycopg.conninfo.conninfo_to_dict(config["connection_uri"]),
            source["ordinary_reader_role"])
        page = read_page(reader_factory, source["consumer_id"],
                         maximum_bytes=publication["maximum_page_bytes"])
        if (type(page) is not FeedPage or page.consumer_id != source["consumer_id"]
                or page.registration_id != page_proof["registration_id"]
                or page.sha256 != page_proof["page_sha256"]
                or sha(page.prior.content) != page_proof["prior_sha256"]
                or sha(page.next.content) != page_proof["next_sha256"]
                or type(page.writer_xid) is not str
                or not page.writer_xid.isascii() or not page.writer_xid.isdigit()
                or page.writer_xid.startswith("0")
                or page.writer_xid != page_proof["writer_xid"]):
            raise Refusal("fresh public Truss source page differs from committed publication receipt")
        # .52 has no non-mutating consumer getter. This is a typed snapshot
        # derived from the public FeedPage; protected custody open is the
        # authoritative check of the live consumer row/checkpoint and rereads
        # the exact page before ACK. Do not re-enroll the consumer here.
        consumer = FeedConsumer(consumer_id=page.consumer_id,
            registration_id=page.registration_id, checkpoint=page.prior)
        request_raw = base64.b64decode(pending["request_bytes_base64"], validate=True)
        anchor = select_truss_local_dev_source_custody_anchor(
            registration=registration, accepted_document=document, consumer=consumer,
            runtime_status=status, page=page, feed=pending["feed"],
            source_holder_id=source["source_holder_id"],
            document_id=model["document_id"],
            destination_id=pending["destination_id"],
            ack_registration_id=source["ack_registration_id"],
            ack_scope_id=source["ack_scope_id"], publication_id=pending["publication_id"],
            request_bytes=request_raw,
            predecessor_id=strict_json(request_raw)["predecessor"],
            destination_transport=driver.transport)
        context = object()
        def custody_factory(selection):
            return Truss052SourceCustodySession(
                connection_factory=lambda _ctx: connect(source["ordinary_reader_role"], False),
                selection=selection, context=context, runtime_status_provider=status_provider,
                statement_timeout_ms=10000)
        result = recover_committed_truss_publication_and_ack(
            driver, pending["request_digest"], pending_raw, anchor,
            spark=driver.transport.spark, custody_session_factory=custody_factory,
            completion_path=receipts / "publication-readback-complete.json",
            ack_attempt_path=receipts / "protected-ack-attempt.json",
            ack_reconciliation_path=receipts / "protected-ack-reconciliation.json",
            publication_intent_path=Path(recovery["publication_intent_path"]),
            publication_intent_sha256=recovery["publication_intent_sha256"])
        return {"profile": PROFILE, "status": "matched_local_development_only",
                "request_digest": pending["request_digest"],
                "publication_id": pending["publication_id"],
                "completion_marker_sha256": result["completion_marker_sha256"],
                "ack_attempt_marker_sha256": result["ack_attempt_marker_sha256"],
                "ack_reconciliation_sha256": result["ack_reconciliation_sha256"],
                "publication_replayed": False, "production_authority": False, "cloud": False}
    return operation


def install_selected_runtime(scope):
    """Install and prove the exact .52 wheel and local extras before Truss imports."""
    runtime = scope["runtime_candidate"]
    overlay = Path(runtime["new_overlay_or_venv"]).resolve(strict=True) / "site-packages"
    extras = Path(runtime["local_extra_overlay"]).resolve(strict=True)
    if not overlay.is_dir() or not extras.is_dir():
        raise Refusal("exact pinned .52 runtime overlay and local extras required")
    for selected in (overlay, extras):
        text = str(selected)
        while text in sys.path:
            sys.path.remove(text)
    sys.path[:0] = [str(overlay), str(extras)]
    if "truss" in sys.modules:
        origin = Path(getattr(sys.modules["truss"], "__file__", "")).resolve()
        if overlay not in origin.parents:
            raise Refusal("Truss was imported before selected .52 overlay admission")
    import importlib.metadata
    if importlib.metadata.version("umf-truss") != "0.0.52":
        raise Refusal("exact installed Truss 0.0.52 required")
    distribution = importlib.metadata.distribution("umf-truss")
    direct = json.loads(distribution.read_text("direct_url.json") or "{}")
    if (direct.get("url") != Path(runtime["wheel_path"]).resolve().as_uri()
            or direct.get("archive_info", {}).get("hashes", {}).get("sha256") != runtime["wheel_sha256"]):
        raise Refusal("installed Truss .52 wheel origin/hash mismatch")
    expected_extras = {"pgserver-pg18": "18.6.1", "fasteners": "0.20",
                       "platformdirs": "4.12.4", "psutil": "7.2.2"}
    if {name: importlib.metadata.version(name) for name in expected_extras} != expected_extras:
        raise Refusal("Truss .52 local-extra pin mismatch")
    import truss
    truss_origin = Path(truss.__file__).resolve()
    if overlay not in truss_origin.parents:
        raise Refusal("selected Truss module did not resolve from pinned overlay")
    from ashlar_host.truss_runtime import require_truss_psycopg_runtime
    require_truss_psycopg_runtime()
    return {"overlay": str(overlay), "extras": str(extras),
            "truss_origin": str(truss_origin), "truss_version": "0.0.52",
            "wheel_sha256": runtime["wheel_sha256"], "local_extra_versions": expected_extras}


def configure_selected_spark_environment(scope, base):
    """Mirror R42's pinned JDK/Python/loopback setup without starting Spark."""
    runtime = scope["spark_runtime"]
    java_home = Path(runtime["java_home"]).resolve(strict=True)
    python = Path(scope["runtime_candidate"]["local_runtime_base_python"]).resolve(strict=True)
    if Path(sys.executable).resolve() != python:
        raise Refusal("selected Spark Python differs from the admitted Truss runtime Python")
    forbidden = ("SPARK_REMOTE", "MASTER", "SPARK_CONNECT_MODE_ENABLED",
                 "PYSPARK_SUBMIT_ARGS", "PYSPARK_GATEWAY_PORT", "PYSPARK_GATEWAY_SECRET")
    present = [name for name in forbidden if name in os.environ]
    if present:
        raise Refusal("configured-local Spark environment contains forbidden runtime override")
    spark_env, network = base.spark_loopback_settings(scope)
    if spark_env != {"SPARK_LOCAL_IP": "127.0.0.1"}:
        raise Refusal("exact R36 Spark loopback environment required")
    if network != {"spark.driver.host": "127.0.0.1",
                   "spark.driver.bindAddress": "127.0.0.1",
                   "spark.ui.bindAddress": "127.0.0.1"}:
        raise Refusal("exact loopback Spark listener configuration required")
    expected_jars = {item["name"]: item["sha256"] for item in runtime["jars"]}
    jars = [Path(item["path"]).resolve(strict=True) for item in runtime["jars"]]
    for item, jar in zip(runtime["jars"], jars):
        if jar.name != item["name"] or sha(jar.read_bytes()) != item["sha256"]:
            raise Refusal("selected Delta JAR path/hash differs")
    if len(jars) != 2 or len({jar.parent for jar in jars}) != 1:
        raise Refusal("exact two-JAR configured-local runtime required")
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ.update(spark_env)
    os.environ["PYSPARK_PYTHON"] = str(python)
    os.environ["PYSPARK_DRIVER_PYTHON"] = str(python)
    from types import SimpleNamespace
    from ashlar_host.configured_local_runtime_profile import attest
    identity = SimpleNamespace(java_home=java_home, jars=jars[0].parent, jar_inventory=expected_jars)
    profile, runtime_identity, attested_jars = attest(identity)
    if (profile != runtime["runtime_profile"] or attested_jars != jars
            or runtime_identity.get("jdk") != "21"):
        raise Refusal("configured-local JDK/JAR/runtime attestation differs")
    return {"profile": profile, "runtime_identity": runtime_identity,
            "java_home": str(java_home), "python": str(python),
            "environment": {"JAVA_HOME": os.environ["JAVA_HOME"],
                "SPARK_LOCAL_IP": os.environ["SPARK_LOCAL_IP"],
                "PYSPARK_PYTHON": os.environ["PYSPARK_PYTHON"],
                "PYSPARK_DRIVER_PYTHON": os.environ["PYSPARK_DRIVER_PYTHON"]},
            "loopback_spark_config": network, "jars": [str(path) for path in jars]}


def preflight_configured_local_runtime(scope, base, parsed_configuration):
    """Exercise the exact `_runtime` admission used immediately before Spark."""
    from ashlar_host.configured_local_runtime import _runtime
    runtime = parsed_configuration.get("_current_runtime")
    if runtime is None:
        raise Refusal("resume parser did not create its current-runtime admission")
    selected = _runtime(runtime)
    expected = [Path(item["path"]).resolve(strict=True) for item in scope["spark_runtime"]["jars"]]
    if selected != expected:
        raise Refusal("configured-local `_runtime` selected different JARs")
    return {"runtime_admission": "configured_local_runtime._runtime passed",
            "selected_jars": [str(path) for path in selected]}


def structural_preflight(scope_path, scope, *, mode):
    runtime_receipt = install_selected_runtime(scope)
    checked = _scope_checks(scope_path, scope, preflight=True)
    base = checked["base"]
    environment_receipt = configure_selected_spark_environment(scope, checked["base_r36"])
    from truss import CatalogRuntimeConfiguration, CatalogRuntimeStatus, adopt_psycopg_host, create_reference_assembly
    from ashlar_host.truss_resume_cli import parse_resume_truss_configuration
    from ashlar_host.truss_local_publication_recovery import recover_committed_truss_publication_and_ack
    if not {"publication_intent_path", "publication_intent_sha256"} <= set(
            __import__("inspect").signature(recover_committed_truss_publication_and_ack).parameters):
        raise Refusal("recovery API lacks immutable publication-intent binding parameters")
    import_scope = scope
    output = Path(scope["paths"]["output_root"])
    runtime = scope["spark_runtime"]
    config = {"connection_uri": "postgresql://ashlar_local_dev_reader@localhost/ashlar",
        "catalog_connection_uri": "postgresql://ashlar_local_dev_registrar@localhost/ashlar",
        "publication_directory": scope["paths"]["publication_directory"],
        "jars": [item["path"] for item in runtime["jars"]],
        "publication_intent": {"path": scope["recovery"]["publication_intent_path"],
            "sha256": scope["recovery"]["publication_intent_sha256"]},
        "destination_id": scope["publication"]["destination_id"],
        "carrier_profile": scope["publication"]["carrier_profile"],
        "runtime_profile": {"profile": runtime["runtime_profile"], "java_home": runtime["java_home"],
            "jar_inventory": {item["name"]: item["sha256"] for item in runtime["jars"]}},
        "archived_acceptance_files": []}
    fd, path = tempfile.mkstemp(prefix="ashlar-r43-parser-", suffix=".json", dir="/private/tmp")
    try:
        os.fchmod(fd, 0o600)
        raw = (json.dumps(config, sort_keys=True, separators=(",", ":")) + "\n").encode()
        os.write(fd, raw); os.fsync(fd); os.close(fd); fd = -1
        parsed_configuration = parse_resume_truss_configuration(
            Path(path), Path("/private/tmp/ashlar-r45-parser-output"))[0]
        runtime_preflight = preflight_configured_local_runtime(scope, base, parsed_configuration)
    finally:
        if fd >= 0: os.close(fd)
        Path(path).unlink(missing_ok=True)
    shape_operation = _public_recovery_operation(scope, checked["pending_raw"],
        output / "receipts", lambda *_args, **_kwargs: None, checked["base_r36"],
        checked["resume_evidence"])
    try:
        shape_operation(None, {}, validate_only=True)
    except RecoveryInputShapeValidated as validated:
        recovery_input_shape = {"status": "passed_effect_free_recovery_operation_input_sentinel",
            "acceptance_revision": validated.inputs["acceptance"]["revision"],
            "acceptance_registration_id": validated.inputs["acceptance"]["registration_id"],
            "pending_request_digest": validated.inputs["pending"]["request_digest"],
            "public_api_calls": False, "postgres_opened": False, "spark_started": False,
            "delta_written": False, "source_ack_attempted": False}
    else:
        raise Refusal("recovery operation input sentinel was not reached")
    return {"profile": PROFILE, "status": ("passed_effect_free_native_structural_preflight"
                if mode == "structural" else "passed_effect_free_inplace_preflight"),
            "scope_sha256": sha(scope_path.read_bytes()), "native_effects": False,
            "database_opened": False, "spark_started": False, "delta_written": False,
            "source_acknowledgement": False, "cloud": False,
            "pending_receipt_sha256": scope["recovery"]["pending_receipt_sha256"],
            "publication_replayed": False, "runtime": runtime_receipt,
            "configured_spark_environment": environment_receipt,
            "configured_runtime_preflight": runtime_preflight,
            "journal_readonly_preflight": checked["journal_readonly"],
            "retained_page_archive_preflight": checked["archived_page"],
            "recovery_input_shape": recovery_input_shape}


def native(scope_path, scope, attempt_root, supervisor_root):
    for key, expected in (("ASHLAR_R36_SUPERVISED", "1"),
                          ("ASHLAR_R36_SCOPE_SHA256", sha(scope_path.read_bytes())),
                          ("ASHLAR_R36_ATTEMPT_ROOT", str(attempt_root)),
                          ("ASHLAR_R36_SUPERVISOR_ROOT", str(supervisor_root)),
                          ("ASHLAR_R36_CLOUD", "0")):
        if os.environ.get(key) != expected:
            raise Refusal("R45 native worker requires exact R36 supervisor environment")
    runtime_receipt = install_selected_runtime(scope)
    checked = _scope_checks(scope_path, scope, preflight=False)
    output = checked["output"]
    output.mkdir(mode=0o700)
    os.chmod(output, 0o700)
    receipts = output / "receipts"
    receipts.mkdir(mode=0o700)
    effects = {"database_effects_may_remain": False, "delta_effects_may_remain": False,
               "source_ack_attempted": False, "cloud": False, "publication_replayed": False}
    write_json(receipts / "recovery-started.json", {"profile": PROFILE,
        "status": "started_existing_artifact_recovery", "pending_receipt_sha256": sha(checked["pending_raw"]),
        "publication_replayed": False, "effects": effects})
    runtime = None
    primary = None
    stage = "local_postgres_start"
    try:
        environment_receipt = configure_selected_spark_environment(scope, checked["base_r36"])
        from truss import LocalPostgres
        import psycopg
        from ashlar_host.truss_resume_cli import run_selected_truss_command
        runtime = LocalPostgres(checked["pgdata"])
        effects["database_effects_may_remain"] = True
        runtime.__enter__()
        info = runtime.info
        if info.server_version != "18.6" or Path(info.data_directory) != checked["pgdata"]:
            raise Refusal("exact retained R42 PostgreSQL identity required")
        params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
        source = scope["source"]
        def connect(role, autocommit=False):
            return psycopg.connect(**dict(params, user=role), autocommit=autocommit,
                connect_timeout=5, options="-c statement_timeout=10000")
        config = _resume_config(scope, info, output)
        # Parser executes before Spark session/driver creation.
        stage = "resume_configuration_preflight"
        from ashlar_host.truss_resume_cli import parse_resume_truss_configuration
        parse_resume_truss_configuration(config_path := _persist_config(output, config),
                                         output / "resume-operation-result.json")
        operation = _public_recovery_operation(scope, checked["pending_raw"], receipts, connect,
            checked["base_r36"], checked["resume_evidence"])
        effects["delta_effects_may_remain"] = True
        stage = "restore_readback_and_protected_ack"
        result = run_selected_truss_command(config_path, output / "resume-operation-result.json", operation)
        result_raw = (json.dumps(result, sort_keys=True, separators=(",", ":")) + "\n").encode()
        write_json(receipts / "recovery-result.json", json.loads(result_raw))
        return 0
    except BaseException as error:
        primary = error
        try:
            effects["source_ack_may_have_been_attempted"] = (
                (receipts / "protected-ack-attempt.json").is_file())
            write_json(receipts / "failure.json", {"profile": PROFILE, "status": "stopped",
                "failure_type": type(error).__name__, "stage": stage,
                "effects": effects, "publication_replayed": False,
                "ack_outcome": ("may_be_unknown" if effects["source_ack_may_have_been_attempted"]
                                else "not_attempted")})
        except BaseException:
            pass
        raise
    finally:
        if runtime is not None:
            runtime.__exit__(type(primary) if primary else None, primary,
                             primary.__traceback__ if primary else None)


def _persist_config(output, config):
    path = output / "resume-config.json"
    write_json(path, config)
    return path


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", required=True, type=Path)
    parser.add_argument("--attempt-root", type=Path)
    parser.add_argument("--supervisor-root", type=Path)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--structural-preflight-only", action="store_true")
    args = parser.parse_args()
    try:
        scope_path = args.scope.resolve(strict=True)
        scope = strict_json(scope_path.read_bytes())
        if args.preflight_only or args.structural_preflight_only:
            result = structural_preflight(scope_path, scope,
                mode="structural" if args.structural_preflight_only else "preflight")
            print(json.dumps(result, sort_keys=True, separators=(",", ":")))
            return 0
        if args.attempt_root is None or args.supervisor_root is None:
            raise Refusal("exact supervised attempt and supervisor roots required")
        native(scope_path, scope, args.attempt_root.resolve(), args.supervisor_root.resolve())
        return 0
    except BaseException as error:
        print(f"{type(error).__name__}: {str(error)[:1200]}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
