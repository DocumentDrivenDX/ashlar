#!/usr/bin/env python3
"""One-use B install/accept/genesis-registration worker (execution is gated)."""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import sys
import time

HERE = Path(__file__).resolve().parent
CONFIG = HERE / "b-install-config.json"
APPROVAL = HERE / "execution-approval.json"
SUPERVISOR = HERE / "b_install_supervisor.py"
PREPARATION = HERE / "b-offline-preparation-assessment-20261023.json"
MAX_SOURCE = 1_048_576
MAX_ORIGIN = 65_536
MAX_METADATA = 1_048_576


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def strict_json_object(raw: bytes) -> dict:
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError("duplicate JSON object member")
            result[key] = value
        return result
    value = json.loads(raw, object_pairs_hook=pairs)
    if type(value) is not dict:
        raise ValueError("JSON object required")
    return value


def genesis_cursor_evidence(consumer, expected_consumer_id: str,
                           expected_installation_id: str | None = None) -> dict:
    """Validate the public FeedConsumer.checkpoint.content bytes exactly."""
    if consumer.consumer_id != expected_consumer_id or consumer.durability != "pending":
        raise ValueError("public consumer identity/durability differs")
    raw = consumer.checkpoint.content
    if type(raw) is not bytes or len(raw) > 16_384:
        raise ValueError("bounded exact FeedCursor.content bytes required")
    cursor = strict_json_object(raw)
    required = {"profile", "installationId", "sourceEpoch", "consumerId", "registrationId", "nextXid"}
    if set(cursor) != required or cursor["profile"] != "truss-caller-asserted-feed/0.1":
        raise ValueError("exact Truss feed cursor schema required")
    if (cursor["consumerId"] != expected_consumer_id
            or cursor["registrationId"] != consumer.registration_id
            or cursor["nextXid"] != "0"):
        raise ValueError("feed cursor identity or genesis checkpoint differs")
    if (expected_installation_id is not None
            and cursor["installationId"] != expected_installation_id):
        raise ValueError("genesis cursor belongs to a different installation")
    if any(type(cursor[k]) is not str or not cursor[k]
           for k in ("installationId", "sourceEpoch", "consumerId", "registrationId", "nextXid")):
        raise ValueError("feed cursor identity fields must be nonempty strings")
    return {"consumer_id": consumer.consumer_id,
            "registration_id": consumer.registration_id,
            "installation_id": cursor["installationId"],
            "source_epoch": cursor["sourceEpoch"],
            "cursor_sha256": sha(raw), "cursor_bytes": raw.decode("utf-8")}


def validate_accepted_observations(*, status_revision, accepted, report_bytes,
                                   registration, view, source, document_id, document_revision,
                                   expected_source_catalog_revision, complete_scope):
    expected_sha = sha(source)
    expected_inventory = expected_catalog_definition_inventory(source, document_id)
    actual_inventory = []
    for definition in view.definitions:
        reference = definition.reference
        provenance = definition.provenance
        actual_inventory.append((reference.kind, reference.owner.document_id,
                                 reference.owner.module_id, reference.authored_identity,
                                 provenance.authored_pointer))
    if (status_revision != 1 or accepted.document_id != document_id
            or accepted.content != source
            or accepted.document_revision != document_revision
            or accepted.catalog_revision != 1
            or accepted.source_catalog_revision != expected_source_catalog_revision
            or accepted.registration_id != registration.registration_id
            or accepted.sha256 != expected_sha
            or registration.report_bytes != report_bytes
            or not complete_scope or str(view.catalog_revision) != "1"
            or len(actual_inventory) != len(expected_inventory)
            or len(set(actual_inventory)) != len(actual_inventory)
            or set(actual_inventory) != expected_inventory):
        raise RuntimeError("accepted source/report/complete view differs from frozen pins")
    for definition in view.definitions:
        provenance = definition.provenance
        if (provenance.accepted_catalog_revision != "1"
                or provenance.document_revision != document_revision
                or provenance.accepted_document_sha256 != expected_sha):
            raise RuntimeError("catalog view provenance differs from frozen source pins")
    return {"revision": 1, "source_sha256": accepted.sha256,
            "document_revision": accepted.document_revision,
            "source_catalog_revision": accepted.source_catalog_revision,
            "registration_id": accepted.registration_id,
            "report_sha256": sha(report_bytes),
            "complete_view_sha256": sha(view.original_bytes)}


def expected_catalog_definition_inventory(source: bytes, document_id: str) -> set[tuple[str, str, str, str, str]]:
    """Derive exact Truss view identities/pointers from the frozen UMF bytes.

    Truss exposes one view entry for each record (type), field (property),
    authored key, and relationship. Provider-native numeric IDs are deliberately
    excluded; accepted identity is bound by kind, document/module, authored ID,
    and the exact source pointer instead.
    """
    document = strict_json_object(source)
    if type(document_id) is not str or not document_id:
        raise ValueError("frozen document ID required")
    modules = document.get("modules")
    if type(modules) is not list or not modules:
        raise ValueError("UMF module inventory required")
    expected = set()
    for module_index, module in enumerate(modules):
        if type(module) is not dict or type(module.get("id")) is not str or not module["id"]:
            raise ValueError("bounded UMF module identity required")
        module_id = module["id"]
        elements = module.get("elements", [])
        relationships = module.get("relationships", [])
        if type(elements) is not list or type(relationships) is not list:
            raise ValueError("UMF element/relationship arrays required")
        for element_index, element in enumerate(elements):
            if type(element) is not dict or type(element.get("id")) is not str or not element["id"]:
                raise ValueError("UMF authored element identity required")
            pointer = f"/modules/{module_index}/elements/{element_index}"
            kind = element.get("kind")
            if kind == "record":
                expected.add(("type", document_id, module_id, element["id"], pointer))
                keys = element.get("keys", [])
                if type(keys) is not list:
                    raise ValueError("UMF record keys must be an array")
                for key_index, key in enumerate(keys):
                    if type(key) is not dict or type(key.get("id")) is not str or not key["id"]:
                        raise ValueError("UMF authored key identity required")
                    expected.add(("key", document_id, module_id, key["id"],
                                  f"{pointer}/keys/{key_index}"))
            elif kind == "field":
                expected.add(("property", document_id, module_id, element["id"], pointer))
            else:
                raise ValueError("unsupported UMF source definition kind in accepted inventory")
        for relationship_index, relationship in enumerate(relationships):
            if (type(relationship) is not dict or type(relationship.get("id")) is not str
                    or not relationship["id"]):
                raise ValueError("UMF authored relationship identity required")
            expected.add(("relationship", document_id, module_id, relationship["id"],
                          f"/modules/{module_index}/relationships/{relationship_index}"))
    if not expected:
        raise ValueError("nonempty UMF definition inventory required")
    return expected


def validate_installation_readback(status, manifest: dict, manifest_sha256: str,
                                   expected_installation_id: str) -> dict:
    actual_layout = {"identity": status.layout_profile.identity,
                     "version": status.layout_profile.version,
                     "sha256": status.layout_profile.sha256}
    result = {"installation_id": status.installation_id,
              "release_id": status.release_id,
              "release_sha256": status.release_sha256,
              "layout_profile": actual_layout}
    if (not status.installed or result["installation_id"] != expected_installation_id
            or result["release_id"] != manifest["releaseId"]
            or result["release_sha256"] != manifest_sha256
            or actual_layout != manifest["layoutProfile"]):
        raise RuntimeError("installation status differs from B identity or pinned runtime manifest")
    return result


def _read(path: Path, limit: int) -> bytes:
    if not path.is_absolute() or path.resolve() != path or path.is_symlink():
        raise ValueError("canonical non-symlink absolute path required")
    with path.open("rb") as f:
        raw = f.read(limit + 1)
    if len(raw) > limit:
        raise ValueError("input byte bound exceeded")
    return raw


def load_config(path: Path = CONFIG) -> tuple[dict, bytes, bytes]:
    config = json.loads(_read(path, 65_536))
    if type(config) is not dict or config.get("profile") != "ashlar-r04-b-install-attempt/0.1":
        raise ValueError("unknown B attempt profile")
    source_cfg, origin_cfg = config["source"], config["origin"]
    source = _read(Path(source_cfg["path"]), MAX_SOURCE)
    origin_path = Path(origin_cfg["path"])
    if not origin_path.is_absolute() or origin_path.resolve() != origin_path or origin_path.is_symlink():
        raise ValueError("origin must be an external canonical absolute file")
    origin = _read(origin_path, MAX_ORIGIN)
    if sha(source) != source_cfg["sha256"] or sha(origin) != origin_cfg["sha256"]:
        raise ValueError("pinned source/origin digest mismatch")
    source_doc, origin_doc = json.loads(source), json.loads(origin)
    if (source_doc.get("id") != source_cfg["document_id"]
            or source_doc.get("umf") != source_cfg["umf_version"]):
        raise ValueError("source identity/version mismatch")
    if "revision" in source_doc and source_doc["revision"] != source_cfg["document_revision"]:
        raise ValueError("source revision disagrees with external revision pin")
    if origin_doc != {"actor": "ashlar-r04-b-source-acceptance",
                      "reason": "independent R04 B source acceptance using the retained original UMF 0.10.0 model bytes"}:
        raise ValueError("origin bytes do not match the exact B candidate")
    return config, source, origin


def _file_sha(path: Path, maximum=MAX_METADATA) -> str:
    if path.is_symlink() or not path.is_file():
        raise ValueError("pinned regular file required: " + str(path))
    raw = path.read_bytes()
    if len(raw) > maximum:
        raise ValueError("pinned metadata exceeds byte bound")
    return sha(raw)


def _large_file_sha(path: Path, maximum=134_217_728) -> str:
    if path.is_symlink() or not path.is_file() or path.stat().st_size > maximum:
        raise ValueError("bounded pinned executable required")
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def validate_dependency_lock(config: dict) -> str:
    lock_cfg = config["dependency_lock"]
    lock_raw = _read(Path(lock_cfg["path"]), MAX_METADATA)
    if sha(lock_raw) != lock_cfg["sha256"]:
        raise ValueError("selected R4 dependency lock digest mismatch")
    lock = strict_json_object(lock_raw)
    if (lock.get("artifact_count") != lock_cfg["artifact_count"]
            or lock.get("python_target") != lock_cfg["python_target"]
            or lock.get("platform_target") != lock_cfg["platform_target"]
            or type(lock.get("wheels")) is not list
            or len(lock["wheels"]) != lock_cfg["artifact_count"]):
        raise ValueError("selected R4 dependency lock target/count mismatch")
    locked = {wheel.get("distribution"): (wheel.get("version"), wheel.get("sha256"))
              for wheel in lock["wheels"] if type(wheel) is dict}
    expected_direct = {
        "truss-toolkit": ("0.0.49", config["runtime"]["truss_wheel_sha256"]),
        "umf_access": ("0.1.0.dev7", "a78053ab8f35f55f23661de780f351f90e71299773774b62938ffb09270f3597"),
        "umf-core": ("0.10.0", "49633924b45a3c8499d4fcc1a24c30d5a8fcb34d974ab019bbda987cb47da0f9"),
        "weft-sql": ("0.4.52", "598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46"),
    }
    if any(locked.get(name) != pin for name, pin in expected_direct.items()):
        raise ValueError("selected R4 lock does not contain the exact B direct wheel cohort")
    return lock_cfg["sha256"]


def validate_runtime_receipt_lock(receipt: dict, expected_lock_sha256: str) -> None:
    if receipt.get("dependency_lock_sha256") != expected_lock_sha256:
        raise RuntimeError("B runtime receipt does not bind the selected dependency lock")


def verify_preparation_and_runtime(config: dict) -> dict:
    prep_cfg = config["preparation"]
    lock_cfg = config["dependency_lock"]
    lock_sha256 = validate_dependency_lock(config)
    prep_path = (HERE / prep_cfg["assessment_path"]).resolve()
    if prep_path != PREPARATION or _file_sha(prep_path) != prep_cfg["assessment_sha256"]:
        raise ValueError("offline preparation assessment pin mismatch")
    prep = strict_json_object(prep_path.read_bytes())
    if prep.get("status") != "top_level_pure_preparation_passed_offline_assembly_route_unavailable":
        raise ValueError("pinned public pure preparation result is not ready")
    if prep["input"]["bytes_sha256"] != config["source"]["sha256"]:
        raise ValueError("preparation assessment source hash differs")
    if prep["b_origin_candidate"]["sha256"] != config["origin"]["sha256"]:
        raise ValueError("preparation assessment origin hash differs")
    for pathkey, hashkey in (("prepared_input_path", "prepared_input_sha256"),
                             ("prepared_report_path", "prepared_report_sha256")):
        if _file_sha(Path(prep_cfg[pathkey])) != prep_cfg[hashkey]:
            raise ValueError("prepared Truss artifact pin mismatch: " + pathkey)
    bun_path = Path(prep["preparation"]["bun_executable"])
    bun_sha = _large_file_sha(bun_path)
    if (bun_path.resolve() != Path(prep_cfg["bun_path"]).resolve()
            or bun_sha != prep_cfg["bun_sha256"]):
        raise ValueError("prepared Bun executable path/hash differs from frozen pin")
    receipt_path = Path(config["runtime"]["receipt_path"])
    if _file_sha(receipt_path) != config["runtime"]["runtime_manifest_sha256"]:
        raise ValueError("selected .49 runtime receipt pin mismatch")
    receipt = strict_json_object(receipt_path.read_bytes())
    validate_runtime_receipt_lock(receipt, lock_sha256)
    if (receipt.get("no_native_execution") is not True
            or receipt.get("pg_started") is not False or receipt.get("spark_started") is not False
            or receipt.get("versions", {}).get("truss-toolkit") != config["runtime"]["truss_version"]
            or receipt.get("versions", {}).get("umf-core") != config["runtime"]["umf_core_version"]
            or receipt.get("direct_wheels", {}).get("truss-toolkit", {}).get("wheel_sha256")
               != config["runtime"]["truss_wheel_sha256"]):
        raise ValueError("Truss runtime version pin mismatch")
    import importlib.resources as resources
    import importlib.metadata as metadata
    runtime_venv = Path(config["runtime"]["venv"]).resolve()
    if Path(sys.executable).resolve() != (runtime_venv / "bin" / "python").resolve():
        raise ValueError("pinned isolated interpreter required")
    if metadata.version("truss-toolkit") != config["runtime"]["truss_version"]:
        raise ValueError("installed Truss distribution version differs")
    catalog_manifest = resources.files("truss").joinpath("_catalog_runtime", "manifest.json").read_bytes()
    if sha(catalog_manifest) != config["runtime"]["catalog_manifest_sha256"]:
        raise ValueError("installed Truss catalog release manifest hash differs")
    pg_ctl = Path(str(resources.files("pgserver").joinpath("pginstall", "bin", "pg_ctl")))
    if (not pg_ctl.is_file() or pg_ctl.is_symlink()
            or pg_ctl.resolve() != Path(config["runtime"]["pg_ctl_path"]).resolve()):
        raise ValueError("selected runtime pg_ctl binary unavailable")
    ctl_version = __import__("subprocess").run([str(pg_ctl), "--version"],
        stdin=__import__("subprocess").DEVNULL, capture_output=True, text=True, timeout=5)
    if ctl_version.returncode or ctl_version.stdout.strip() != "pg_ctl (PostgreSQL) 18.6":
        raise ValueError("selected PostgreSQL cleanup binary must be 18.6")
    pg_ctl_sha = _large_file_sha(pg_ctl)
    if pg_ctl_sha != config["runtime"]["pg_ctl_sha256"]:
        raise ValueError("selected PostgreSQL cleanup binary hash differs")
    return {"dependency_lock_sha256": lock_sha256,
            "preparation_sha256": prep_cfg["assessment_sha256"],
            "prepared_input_sha256": prep_cfg["prepared_input_sha256"],
            "prepared_report_sha256": prep_cfg["prepared_report_sha256"],
            "runtime_receipt_sha256": config["runtime"]["runtime_manifest_sha256"],
            "catalog_manifest_sha256": config["runtime"]["catalog_manifest_sha256"],
            "pg_ctl_path": str(pg_ctl.resolve()), "pg_ctl_sha256": pg_ctl_sha,
            "bun_path": str(bun_path.resolve()), "bun_sha256": bun_sha}


def worker_approval_payload(config: dict, config_raw: bytes, source: bytes,
                            origin: bytes, pins: dict, admission_sha256: str) -> dict:
    return {"profile": "ashlar-r04-b-install-source-execution-approval/0.1",
            "scope": "one B-local Truss install, one exact source acceptance, one genesis consumer; no feed read, destination, Spark or ACK",
            "worker_sha256": _file_sha(Path(__file__)),
            "supervisor_sha256": _file_sha(SUPERVISOR),
            "config_sha256": sha(config_raw),
            "dependency_lock_sha256": pins["dependency_lock_sha256"],
            "preparation_sha256": pins["preparation_sha256"],
            "prepared_input_sha256": pins["prepared_input_sha256"],
            "prepared_report_sha256": pins["prepared_report_sha256"],
            "runtime_receipt_sha256": pins["runtime_receipt_sha256"],
            "catalog_manifest_sha256": pins["catalog_manifest_sha256"],
            "pg_ctl_path": pins["pg_ctl_path"], "pg_ctl_sha256": pins["pg_ctl_sha256"],
            "bun_path": pins["bun_path"], "bun_sha256": pins["bun_sha256"],
            "source_sha256": sha(source), "origin_sha256": sha(origin),
            "admission_gates_sha256": admission_sha256}


def worker_preflight(config_path: Path = CONFIG, admission_sha256: str | None = None) -> dict:
    config, source, origin = load_config(config_path)
    pins = verify_preparation_and_runtime(config)
    payload = worker_approval_payload(config, _read(config_path, MAX_METADATA), source,
                                      origin, pins, admission_sha256 or "")
    return {"profile": "ashlar-r04-b-worker-preflight/0.2",
            "source_sha256": sha(source), "origin_sha256": sha(origin),
            "preparation": pins, "expected_approval": payload,
            "pg_started": False, "spark_started": False,
            "acceptance_invoked": False, "consumer_registered": False,
            "execution_approval": None}


def _write_marker(directory: Path, name: str, value: dict) -> None:
    raw = (json.dumps(value, sort_keys=True, indent=2) + "\n").encode()
    fd = os.open(directory / name, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        if stream.write(raw) != len(raw):
            raise OSError("incomplete marker write")
        stream.flush()
        os.fsync(stream.fileno())
    dfd = os.open(directory, os.O_RDONLY)
    try: os.fsync(dfd)
    finally: os.close(dfd)


def _execute(config_path: Path = CONFIG) -> dict:
    config, source, origin = load_config(config_path)
    pins = verify_preparation_and_runtime(config)
    admission_sha = os.environ.get("ASHLAR_B_ADMISSION_GATES_SHA256")
    if not admission_sha or len(admission_sha) != 64:
        raise PermissionError("supervisor admission-gate binding missing")
    payload = worker_approval_payload(config, _read(config_path, MAX_METADATA),
                                      source, origin, pins, admission_sha)
    if config["execution"]["approval"] is not None:
        raise PermissionError("execution approval must remain in exact separate approval sidecar")
    if (config["origin"]["approved"] is not True
            or config["origin"]["approval_record"] is None
            or config["scope"]["arras_window_released"] is not True
            or config["scope"]["source_acceptance"] is not True
            or config["scope"]["feed_reads"] is not False
            or config["scope"]["source_ack"] is not False
            or config["scope"]["spark"] is not False):
        raise PermissionError("B source/Arras/scope approval pins are incomplete")
    install = config["installation"]
    required = ("root_path", "pgdata_path", "attempt_path", "owner_role", "registrar_role",
                "registrar_login", "reader_login", "consumer_id", "request_key",
                "exclusive_port_admission", "retention_owner")
    if any(install.get(k) is None for k in required):
        raise PermissionError("B path/role/request/retention pins are incomplete")
    if install["exclusive_port_admission"] != admission_sha:
        raise PermissionError("exclusive shared LocalPostgres admission digest differs")
    if install["expected_catalog_revision"] != 0 or install["expected_document_source_revision"] != 0:
        raise PermissionError("only fresh catalog and source CAS revision zero are admitted")
    role_names = [install[k] for k in ("owner_role", "registrar_role", "registrar_login", "reader_login")]
    if len(set(role_names)) != len(role_names) or any(
            __import__("re").fullmatch(r"[a-z][a-z0-9_]{2,62}", role) is None for role in role_names):
        raise PermissionError("distinct bounded B role names required")
    approval_raw = _read(APPROVAL, 65_536)
    approval = strict_json_object(approval_raw)
    if approval != payload:
        raise PermissionError("exact hash-bound execution approval does not match worker contract")
    attempt = Path(install["attempt_path"])
    root = Path(install["root_path"])
    pgdata = Path(install["pgdata_path"])
    if any(not p.is_absolute() or p.resolve() != p or p.is_symlink()
           for p in (root, attempt, pgdata)):
        raise PermissionError("canonical absolute non-symlink B paths required")
    if not root.is_dir() or root.stat().st_uid != os.getuid() or stat.S_IMODE(root.stat().st_mode) != 0o700:
        raise PermissionError("current-user-owned mode-0700 B root required")
    if (not attempt.is_dir() or attempt.stat().st_uid != os.getuid()
            or stat.S_IMODE(attempt.stat().st_mode) != 0o700):
        raise PermissionError("current-user-owned mode-0700 B attempt directory required")
    if (not attempt.is_relative_to(root) or not pgdata.is_relative_to(root)
            or attempt == pgdata or attempt.is_relative_to(pgdata) or pgdata.is_relative_to(attempt)):
        raise PermissionError("attempt and PGDATA must be disjoint descendants of B root")
    if {entry.name for entry in root.iterdir()} != {attempt.name, "execution-started-b.json"}:
        raise PermissionError("B root contains unexpected entries")
    started = _read(root / "execution-started-b.json", 65_536)
    started_doc = strict_json_object(started)
    if (started_doc.get("profile") != "ashlar-r04-b-execution-started/0.1"
            or started_doc.get("approval_sha256") != sha(approval_raw)
            or started_doc.get("worker_sha256") != _file_sha(Path(__file__))
            or started_doc.get("supervisor_sha256") != _file_sha(SUPERVISOR)
            or started_doc.get("admission_gates_sha256") != admission_sha):
        raise PermissionError("fsynced supervisor start marker does not match exact worker approval")
    expected_logs = {"worker.stdout.log", "worker.stderr.log"}
    if {entry.name for entry in attempt.iterdir()} != expected_logs:
        raise PermissionError("only the supervisor-created bounded worker logs may preexist")
    for log in attempt.iterdir():
        if log.is_symlink() or not log.is_file() or log.stat().st_uid != os.getuid() or log.stat().st_size != 0:
            raise PermissionError("fresh current-user-owned worker logs required")
    if pgdata.exists() or pgdata.is_symlink():
        raise PermissionError("fresh distinct B PGDATA required; never reopen or replay")
    if _large_file_sha(Path(pins["bun_path"])) != pins["bun_sha256"]:
        raise PermissionError("pinned preparation executable changed before execution")
    prepared = prepare(config, source, pins["bun_path"])
    sequence = 0
    def mark(stage, value):
        nonlocal sequence
        sequence += 1
        _write_marker(attempt, f"{sequence:02d}-{stage}.json", value)
    try:
        config["execution"]["approval"] = payload
        result = ordered_attempt(Truss049Adapter(config), config, prepared, source, origin, mark)
        return result
    except BaseException as error:
        try:
            _write_marker(attempt, f"{sequence + 1:02d}-failure.json", {
                "profile": "ashlar-r04-b-attempt-failure/0.1",
                "exception_type": type(error).__name__,
                "automatic_retry": False,
                "retain_pgdata": True})
        except BaseException:
            pass
        raise


def prepare(config: dict, source: bytes, bun_executable: str | None = None):
    """Pure public preparation; does not create a database or attempt files."""
    runtime = Path(config["runtime"]["venv"]).resolve()
    if Path(sys.executable).resolve() != (runtime / "bin" / "python").resolve():
        raise RuntimeError("selected isolated Truss runtime interpreter required")
    import importlib.metadata as md
    if md.version("truss-toolkit") != "0.0.49":
        raise RuntimeError("Truss 0.0.49 required")
    from truss import CatalogDocument, prepare_catalog
    doc = CatalogDocument(document_id=config["source"]["document_id"],
                          document_revision=config["source"]["document_revision"],
                          content=source)
    return prepare_catalog((doc,), source_version=config["runtime"]["umf_core_version"],
                           bun_executable=bun_executable or "bun")


class Truss049Adapter:
    """Thin caller-owned transaction adapter over Truss 0.0.49 public APIs."""
    def __init__(self, config=None):
        self.runtime = self.connection = self.host = self.assembly = self.tx = None
        self._pending_registration = None
        self.config = config

    @staticmethod
    def _value(outcome):
        from truss import Ok
        if not isinstance(outcome, Ok):
            raise RuntimeError("public Truss operation did not return Ok")
        return outcome.value

    def open(self, pgdata, *, port):
        import importlib.resources as resources
        from truss import LocalPostgres
        import psycopg
        if port != 5432:
            raise ValueError("Truss 0.0.49 LocalPostgres fixes port 5432")
        self.runtime = LocalPostgres(pgdata)
        self.runtime.__enter__()
        info = self.runtime.info
        if info.port != 5432 or not info.server_version.startswith("18.6"):
            raise RuntimeError("unexpected local PostgreSQL runtime profile")
        params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
        self._params = params
        manifest_raw = resources.files("truss").joinpath("_catalog_runtime", "manifest.json").read_bytes()
        expected_manifest = (self.config or {}).get("runtime", {}).get("catalog_manifest_sha256")
        if expected_manifest is None or sha(manifest_raw) != expected_manifest:
            raise RuntimeError("installed catalog manifest differs from the exact B runtime pin")
        self._manifest = json.loads(manifest_raw)
        self._manifest_sha256 = sha(manifest_raw)
        self.connection = psycopg.connect(**params, autocommit=True,
                                          connect_timeout=5,
                                          options="-c statement_timeout=5000")

    def _begin(self, user=None, *, readonly=False):
        from truss import CatalogRuntimeConfiguration, adopt_psycopg_host, create_reference_assembly
        import psycopg
        params = dict(self._params)
        if user:
            params["user"] = user
        params.update(autocommit=False, connect_timeout=5,
                      options="-c statement_timeout=5000")
        self.connection = psycopg.connect(**params)
        self.connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED " +
                                ("READ ONLY" if readonly else "READ WRITE"))
        self.connection.execute("SET LOCAL statement_timeout='5000ms'")
        self.host = adopt_psycopg_host(self.connection, statement_timeout_ms=5000,
                                       operation_timeout_ms=30000)
        self.tx = self._value(self.host.adopt_transaction(
            access_mode="read_only" if readonly else "read_write"))
        self.assembly = self._value(create_reference_assembly(
            CatalogRuntimeConfiguration(), self.host))

    def _finish(self, commit):
        primary = None
        try:
            if self.assembly is not None:
                self.assembly.dispose()
            if self.host is not None and not self.host.detach():
                raise RuntimeError("public Truss host detach refused")
            if commit:
                self.connection.commit()
            else:
                self.connection.rollback()
        except BaseException as error:
            primary = error
            try: self.connection.rollback()
            except BaseException: pass
        finally:
            try: self.connection.close()
            except BaseException: pass
            self.host = self.assembly = self.tx = None
        if primary is not None:
            raise primary

    def bootstrap_roles(self, registrar_login, reader_login):
        import re
        from psycopg import sql
        if any(re.fullmatch(r"[a-z][a-z0-9_]{2,62}", n) is None
               for n in (registrar_login, reader_login)) or registrar_login == reader_login:
            raise ValueError("two distinct bounded B login role names required")
        self.connection.autocommit = False
        self.connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ WRITE")
        self.connection.execute("SET LOCAL statement_timeout='5000ms'")
        self.connection.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(registrar_login)))
        self.connection.execute(sql.SQL("CREATE ROLE {} LOGIN").format(sql.Identifier(reader_login)))
        self._registrar_login = registrar_login

    def commit(self):
        self._finish(True)

    def install(self, owner, registrar):
        from truss import PendingCatalogInstallation
        self._begin()
        status = self._value(self.assembly.status_in_transaction(self.tx))
        if status.installed:
            raise RuntimeError("installation already exists; one-use attempt refuses replay")
        pending = self._value(self.assembly.install_in_transaction(
            self.tx, owner_role=owner, registrar_role=registrar))
        if not isinstance(pending, PendingCatalogInstallation):
            raise RuntimeError("public install did not return PendingCatalogInstallation")
        self._pending_installation_id = pending.installation_id
        return {"installation_id": pending.installation_id,
                "release_id": pending.release_id,
                "release_sha256": self._manifest_sha256,
                "layout_profile": self._manifest["layoutProfile"]}

    def read_installation(self):
        self._begin(readonly=True)
        status = self._value(self.assembly.status_in_transaction(self.tx))
        value = validate_installation_readback(status, self._manifest,
                                               self._manifest_sha256,
                                               self._pending_installation_id)
        self._finish(False)
        return value

    def grant_registrar(self, role):
        from psycopg import sql
        self._begin()
        self.connection.execute(sql.SQL("GRANT {} TO {} WITH SET FALSE").format(
            sql.Identifier(role), sql.Identifier(self._registrar_login)))

    def accept(self, prepared, source, origin, request_key, document_id):
        from truss import CatalogDocumentExpectation, CatalogRegistration
        self._begin(user=self._registrar_login)
        registration = self._value(self.assembly.accept_in_transaction(
            self.tx, prepared, request_key=request_key, origin_bytes=origin,
            expected_revision=0,
            expected_documents=(CatalogDocumentExpectation(
                document_id=document_id, source_catalog_revision=0),)))
        if not isinstance(registration, CatalogRegistration) or registration.replayed or registration.revision != 1:
            raise RuntimeError("public source acceptance did not create exactly revision 1")
        self._pending_registration = registration
        self._accepted_source_bytes = source
        return {"registration_id": registration.registration_id,
                "revision": registration.revision,
                "source_sha256": sha(source),
                "report_sha256": sha(registration.report_bytes)}

    def read_acceptance(self, source, document_id, document_revision, expected_source_catalog_revision):
        from dataclasses import replace
        from truss import CatalogViewAvailable, CompleteCatalogScope, HistoricalCatalogSelection
        self._begin(user=self._registrar_login, readonly=True)
        reg = self._pending_registration
        status = self._value(self.assembly.status_in_transaction(self.tx))
        report = self._value(self.assembly.report_in_transaction(self.tx, 1))
        accepted = self._value(self.assembly.accepted_document_in_transaction(
            self.tx, document_id=document_id, maximum_bytes=MAX_SOURCE))
        request = replace(self.assembly.catalog_view_request(
            definition_count="65536", encoded_bytes="16777216"),
            selection=HistoricalCatalogSelection(catalog_revision="1"))
        view = self._value(self.assembly.catalog_view_in_transaction(self.tx, request))
        if (not isinstance(view, CatalogViewAvailable)
                or not isinstance(view.view.scope, CompleteCatalogScope)
                or str(view.view.catalog_revision) != "1"):
            raise RuntimeError("complete catalog-view readback unavailable")
        if accepted.content != self._accepted_source_bytes:
            raise RuntimeError("accepted bytes differ from originally prepared source")
        result = validate_accepted_observations(
            status_revision=status.revision, accepted=accepted,
            report_bytes=report.original_bytes, registration=reg, view=view.view,
            source=source, document_id=document_id, document_revision=document_revision,
            expected_source_catalog_revision=expected_source_catalog_revision,
            complete_scope=isinstance(view.view.scope, CompleteCatalogScope))
        self._finish(False)
        return result

    def register_consumer(self, consumer_id, reader_role):
        self._begin()
        result = self._value(self.assembly.register_feed_consumer_in_transaction(
            self.tx, consumer_id=consumer_id, reader_role=reader_role))
        return genesis_cursor_evidence(result, consumer_id,
                                       self._pending_installation_id)

    def close(self):
        cleanup_error = None
        # On any pre-commit failure detach the borrowed host before stopping the
        # LocalPostgres owner. Roll back the caller transaction while the host is
        # still alive, and preserve PGDATA regardless of cleanup outcome.
        if self.assembly is not None:
            try: self.assembly.dispose()
            except BaseException as error: cleanup_error = error
            self.assembly = None
        if self.host is not None:
            try:
                if not self.host.detach():
                    raise RuntimeError("public Truss host detach refused during cleanup")
            except BaseException as error:
                cleanup_error = cleanup_error or error
            self.host = self.tx = None
        if self.connection is not None:
            try:
                try: self.connection.rollback()
                except BaseException: pass
                self.connection.close()
            except BaseException as error:
                cleanup_error = cleanup_error or error
            finally: self.connection = None
        if self.runtime is not None:
            runtime, self.runtime = self.runtime, None
            try: runtime.__exit__(None, None, None)
            except BaseException as error: cleanup_error = cleanup_error or error
        if cleanup_error is not None:
            raise cleanup_error


class UncertainAttempt(RuntimeError):
    """A commit may have settled; caller must preserve PGDATA and never retry."""


def _ordered_attempt(adapter, config: dict, prepared, source: bytes, origin: bytes,
                     mark, *, clock=time.monotonic):
    """Run one ordered attempt through a narrow host adapter; never retries.

    Adapter methods own only public Truss calls/transaction boundaries. Every
    write is preceded by an fsynced marker. Any commit exception is uncertain.
    """
    started = clock()
    deadline = started + config["bounds"]["worker_deadline_seconds"]
    def check():
        if clock() >= deadline:
            raise TimeoutError("B attempt deadline exceeded")
    if config["execution"]["approval"] is None:
        raise PermissionError("execution approval is null")
    install = config["installation"]
    required = ("root_path", "pgdata_path", "attempt_path", "owner_role", "registrar_role",
                "registrar_login", "reader_login", "consumer_id", "request_key",
                "exclusive_port_admission", "retention_owner")
    if any(install.get(k) is None for k in required) or not config["scope"]["arras_window_released"]:
        raise PermissionError("B-owned execution pins or exclusive 5432 admission are incomplete")
    check()
    mark("preparation_verified", {"source_sha256": sha(source), "origin_sha256": sha(origin)})
    if isinstance(adapter, Truss049Adapter):
        adapter.config = config
    adapter.open(install["pgdata_path"], port=5432)
    mark("postgres_started", {"port": 5432, "pgdata_path": install["pgdata_path"]})
    check()
    mark("bootstrap_commit_started", {})
    try:
        adapter.bootstrap_roles(install["registrar_login"], install["reader_login"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("bootstrap transaction settlement uncertain") from exc
    mark("bootstrap_committed", {})
    check()
    mark("install_commit_started", {})
    try:
        install_result = adapter.install(install["owner_role"], install["registrar_role"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("catalog install settlement uncertain") from exc
    mark("install_committed", {"installation_id": install_result["installation_id"]})
    observed_install = adapter.read_installation()
    if observed_install != install_result:
        raise RuntimeError("installation readback differs; retain PGDATA")
    mark("install_readback", observed_install)
    mark("registrar_grant_commit_started", {})
    try:
        adapter.grant_registrar(install["registrar_role"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("registrar grant settlement uncertain") from exc
    mark("registrar_grant_committed", {})
    check()
    mark("accept_commit_started", {"request_key": install["request_key"]})
    try:
        acceptance = adapter.accept(prepared, source, origin, install["request_key"],
                                    config["source"]["document_id"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("source acceptance settlement uncertain") from exc
    mark("accept_committed", acceptance)
    accepted = adapter.read_acceptance(
        source, config["source"]["document_id"], config["source"]["document_revision"],
        install["expected_document_source_revision"])
    if accepted.get("source_sha256") != sha(source) or accepted.get("revision") != 1:
        raise RuntimeError("accepted source readback mismatch; retain PGDATA")
    if (accepted.get("document_revision") != config["source"]["document_revision"]
            or accepted.get("source_catalog_revision") != install["expected_document_source_revision"]
            or accepted.get("registration_id") != acceptance.get("registration_id")
            or accepted.get("report_sha256") != acceptance.get("report_sha256")):
        raise RuntimeError("accepted source/report readback does not match frozen pins")
    mark("accept_readback", accepted)
    check()
    mark("consumer_commit_started", {"consumer_id": install["consumer_id"]})
    try:
        consumer = adapter.register_consumer(install["consumer_id"], install["reader_login"])
        adapter.commit()
    except BaseException as exc:
        raise UncertainAttempt("consumer registration settlement uncertain") from exc
    # Public Truss 0.0.49 returns the provider registration ID and exact genesis
    # cursor, but has no independent consumer-registration readback operation.
    if consumer.get("installation_id") != install_result.get("installation_id"):
        raise RuntimeError("genesis cursor installation identity differs from installed B")
    mark("consumer_registration_committed", consumer)
    check()
    adapter.close()
    mark("terminal_stopped", {"pgdata_retained": True, "feed_reads": 0, "acks": 0})
    return {"status": "installed_accepted_genesis_registered_stopped", "consumer": consumer}


def ordered_attempt(adapter, config: dict, prepared, source: bytes, origin: bytes,
                    mark, *, clock=time.monotonic):
    """Ensure any opened host is closed after failure; never delete its PGDATA."""
    try:
        return _ordered_attempt(adapter, config, prepared, source, origin, mark, clock=clock)
    except BaseException:
        try:
            adapter.close()
        except BaseException:
            pass
        raise


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--execute", action="store_true")
    args = parser.parse_args()
    if args.preflight == args.execute:
        parser.error("select exactly one mode")
    if args.preflight:
        gate = os.environ.get("ASHLAR_B_ADMISSION_GATES_SHA256")
        print(json.dumps(worker_preflight(admission_sha256=gate), sort_keys=True))
        return 0
    result = _execute()
    print(json.dumps(result, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
