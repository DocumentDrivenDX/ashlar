#!/usr/bin/env python3
"""R54 bounded continuation worker with effect-free preflight and gated native path.

Native execution is reachable only through the R37 Sol-reviewed dispatch
handshake for this exact scope and worker. Preflight itself remains effect-free.
"""
from __future__ import annotations
import argparse, hashlib, importlib.metadata, importlib.util, json, os, sqlite3, stat, sys
from pathlib import Path
from urllib.parse import quote

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[7]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))
SCOPE_DEFAULT = HERE / "r54-host-scope.json"
PROFILE = "ashlar-r54-effect-free-two-change-preflight/0.1"

class Refusal(RuntimeError):
    pass

def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()

def read_json(path: Path):
    def pairs(items):
        out = {}
        for key, value in items:
            if key in out:
                raise Refusal(f"duplicate JSON key: {key}")
            out[key] = value
        return out
    value = json.loads(path.read_bytes(), object_pairs_hook=pairs)
    if type(value) is not dict:
        raise Refusal(f"closed object required: {path}")
    return value

def write_json(path: Path, value):
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False) + "\n").encode()
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        offset = 0
        while offset < len(raw): offset += os.write(fd, raw[offset:])
        os.fsync(fd)
    finally: os.close(fd)
    dfd = os.open(path.parent, os.O_RDONLY | getattr(os, "O_DIRECTORY", 0))
    try: os.fsync(dfd)
    finally: os.close(dfd)
    return sha(raw)

def tree_digest(root: Path, maximum: int):
    if not root.is_absolute() or root.is_symlink() or root.resolve(strict=True) != root or not root.is_dir():
        raise Refusal(f"canonical directory required: {root}")
    h = hashlib.sha256(); files = total = 0
    entries = sorted(root.rglob("*"), key=lambda p: p.relative_to(root).as_posix().encode())
    if len(entries) > 200000:
        raise Refusal("tree entry bound exceeded")
    for path in entries:
        st = path.lstat()
        if st.st_uid != os.getuid() or stat.S_ISLNK(st.st_mode):
            raise Refusal("foreign or symlink source-tree entry")
        rel = path.relative_to(root).as_posix().encode()
        if path.is_dir(): kind = b"D"
        elif path.is_file():
            kind = b"F"; files += 1; total += st.st_size
            if total > maximum: raise Refusal("source tree exceeds bounded byte count")
        else: raise Refusal("special source-tree entry")
        h.update(kind + len(rel).to_bytes(4, "big") + rel + (st.st_mode & 0o777).to_bytes(4, "big") + st.st_size.to_bytes(8, "big"))
        if kind == b"F":
            with path.open("rb") as stream:
                while chunk := stream.read(1024 * 1024): h.update(chunk)
    return {"sha256": h.hexdigest(), "files": files, "bytes": total}

def sidecar(path: Path):
    try:
        st = path.lstat()
        if not stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode): raise Refusal("invalid SQLite sidecar")
        return {"present": True, "bytes": st.st_size, "dev": st.st_dev, "ino": st.st_ino,
                "mode": st.st_mode & 0o777, "sha256": sha(path.read_bytes()) if path.suffix == ".wal" else None}
    except FileNotFoundError:
        return {"present": False, "bytes": 0, "dev": None, "ino": None, "mode": None, "sha256": None}

def valid_empty_page_from_prior(page, feed_page_type, consumer_id: str, prior_sha256: str) -> bool:
    """Match an empty page to its requested prior cursor; next may encode another bound."""
    return (type(page) is feed_page_type and page.outcome == "empty_interval" and
            page.consumer_id == consumer_id and sha(page.prior.content) == prior_sha256 and
            page.writer_xid is None)

def preflight(scope_path: Path, *, require_absent=True, supervisor_root_created=None):
    raw = scope_path.read_bytes(); scope = read_json(scope_path)
    if scope.get("profile") != "ashlar-r37-truss052-inplace-resume/0.1" or scope.get("status") not in {"pending_sol_review", "sol_reviewed_and_dispatchable"}:
        raise Refusal("exact R54 preflight/dispatchable scope required")
    limits = scope.get("limits", {})
    if (limits.get("wall_seconds"), limits.get("process_rss_bytes"), limits.get("attempt_root_bytes"), limits.get("attempts"), limits.get("automatic_retries"), limits.get("cloud")) != (900, 2147483648, 536870912, 1, 0, False):
        raise Refusal("R54 finite local bounds differ")
    dispatchable = scope.get("status") == "sol_reviewed_and_dispatchable"
    if (scope.get("cloud") is not False or scope.get("production_authority") is not False or
            type(scope.get("native_dispatch")) is not bool or scope.get("native_dispatch") is not dispatchable):
        raise Refusal("R54 local preflight boundary differs")
    paths = scope["paths"]
    attempt = Path(paths["output_root"]); supervisor = Path(paths["supervisor_root"])
    if not attempt.is_absolute() or not supervisor.is_absolute() or attempt.exists():
        raise Refusal("fresh R54 attempt root must be absent")
    if require_absent and supervisor.exists():
        raise Refusal("fresh R54 supervisor root must be absent")
    if not require_absent:
        if supervisor_root_created is None or Path(supervisor_root_created) != supervisor or not supervisor.is_dir() or supervisor.is_symlink():
            raise Refusal("exact supervisor-created R54 root required")
        if {p.name for p in supervisor.iterdir()} != {"child.stdout.log", "child.stderr.log"}:
            raise Refusal("exact supervisor log set required")
        for log in supervisor.iterdir():
            st = log.lstat()
            if not stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode) or st.st_uid != os.getuid() or st.st_mode & 0o077:
                raise Refusal("supervisor logs must be private owned regular files")
    r36 = scope["r36_supervisor"]
    if sha(Path(r36["path"]).read_bytes()) != r36["sha256"] or r36["sha256"] != "429a56003e337c271549fa6310a7c4ac61d7605d293e8f7ed1bcc4e532058f3a":
        raise Refusal("exact reviewed R36 supervisor fix changed")
    if sha(Path(scope["retained_r36"]["base_worker_path"]).read_bytes()) != scope["retained_r36"]["base_worker_sha256"]:
        raise Refusal("hash-bound R36 source/page helper worker changed")
    if sha(Path(scope["worker"]["path"]).read_bytes()) != scope["worker"]["sha256"]:
        raise Refusal("R54 worker hash differs from scope")
    group_helper_binding = scope["group_helper"]
    if sha(Path(group_helper_binding["path"]).read_bytes()) != group_helper_binding["sha256"]:
        raise Refusal("R54 transaction-safe group helper hash differs from scope")
    if sha(Path(scope["supervisor"]["path"]).read_bytes()) != scope["supervisor"]["sha256"]:
        raise Refusal("R37 supervisor hash differs from scope")
    repo = HERE.parents[7]
    for relative, expected in scope["ashlar_source_bindings"].items():
        selected = repo / relative
        if selected.resolve(strict=True) != selected or sha(selected.read_bytes()) != expected:
            raise Refusal(f"Ashlar source binding changed: {relative}")
    runtime = scope["runtime_candidate"]
    python = Path(runtime["local_runtime_base_python"])
    wheel = Path(runtime["wheel_path"])
    if not python.is_file() or not os.access(python, os.X_OK) or sha(wheel.read_bytes()) != runtime["wheel_sha256"]:
        raise Refusal("selected Truss runtime executable or exact .52 wheel changed")
    old = scope["retained_r49"]
    for path_key, digest_key in (("scope_path", "scope_sha256"), ("worker_path", "worker_sha256"), ("reconciliation_path", "reconciliation_sha256")):
        p = Path(old[path_key])
        if p.resolve(strict=True) != p or sha(p.read_bytes()) != old[digest_key]:
            raise Refusal(f"retained R49 {path_key} changed")
    recon = read_json(Path(old["reconciliation_path"]))
    ack = recon.get("ack", {})
    if ack.get("status") != "matched_local_development_only" or ack.get("checkpoint_sha256") != scope["predecessor"]["checkpoint_sha256"] or ack.get("checkpoint_sha256") != "506d67a8dc1f8af88beb73bcd2b899afda8e8bebc62604561b4246a934e88c94":
        raise Refusal("R49 matched seed ACK checkpoint is not exact")
    if recon.get("publication_replayed") is not False or recon.get("source_ack_replayed") is not False:
        raise Refusal("R49 replay prohibition is not evidenced")
    predecessor = scope["predecessor"]
    r51 = scope["retained_r51"]
    failure_receipt = Path(r51["attempt_root"]) / "receipts" / "failure.json"
    if sha(failure_receipt.read_bytes()) != r51["failure_receipt_sha256"]:
        raise Refusal("preserved R51 failure receipt changed")
    if sha(Path(scope["failure_reconciliation"]["path"]).read_bytes()) != scope["failure_reconciliation"]["sha256"]:
        raise Refusal("R51 failure reconciliation changed")
    if sha(Path(scope["read_only_inspection"]["path"]).read_bytes()) != scope["read_only_inspection"]["sha256"]:
        raise Refusal("R51 read-only inspection changed")
    previous = scope["prior_r53_attempt"]
    for path_key, digest_key in (("reconciliation_path", "reconciliation_sha256"),
                                 ("failure_receipt_path", "failure_receipt_sha256"),
                                 ("supervisor_receipt_path", "supervisor_receipt_sha256")):
        p = Path(previous[path_key])
        if p.resolve(strict=True) != p or sha(p.read_bytes()) != previous[digest_key]:
            raise Refusal(f"preserved R53 {path_key} changed")
    prior = read_json(Path(previous["reconciliation_path"]))
    prior_failure = read_json(Path(previous["failure_receipt_path"]))
    prior_supervisor = read_json(Path(previous["supervisor_receipt_path"]))
    if (prior.get("status") != "pre_group_feed_cursor_check_refused" or
            prior_failure.get("group_invoked") is not False or
            prior_failure.get("postgres_opened") is not True or
            prior_failure.get("spark_started") is not False or
            prior_failure.get("delta_written") is not False or
            prior_failure.get("source_ack_attempted") is not False or
            prior_supervisor.get("status") != "child_failed" or
            prior_supervisor.get("process_group_remaining") != []):
        raise Refusal("R53 pre-group no-effect reconciliation differs")
    failure = read_json(failure_receipt)
    inspected = read_json(Path(scope["read_only_inspection"]["path"]))
    feed = inspected.get("public_feed_page", {})
    if (failure.get("postgres_opened") is not True or failure.get("spark_started") is not False or
            failure.get("delta_written") is not False or failure.get("source_ack_attempted") is not False or
            inspected.get("read_only_sql", {}).get("matching_request_receipts") != [] or
            inspected.get("delta_written") is not False or inspected.get("source_ack_attempted") is not False or
            feed.get("outcome") != "empty_interval" or feed.get("prior_cursor_matches_R49_ACK") is not True or
            feed.get("prior_cursor_sha256") != predecessor["checkpoint_sha256"]):
        raise Refusal("R51 no-commit/no-ACK reconciliation is not exact")
    r52 = scope["retained_r52"]
    for key, digest_key in (("scope_path", "scope_sha256"), ("worker_path", "worker_sha256"),
                            ("group_helper_path", "group_helper_sha256"),
                            ("dispatch_path", "dispatch_sha256"),
                            ("group_result_path", "group_result_sha256"),
                            ("failure_path", "failure_sha256"),
                            ("supervisor_receipt_path", "supervisor_receipt_sha256")):
        path = Path(r52[key])
        if path.resolve(strict=True) != path or sha(path.read_bytes()) != r52[digest_key]:
            raise Refusal(f"retained R52 {key} changed")
    r52_scope = read_json(Path(r52["scope_path"]))
    r52_result = read_json(Path(r52["group_result_path"]))
    r52_failure = read_json(Path(r52["failure_path"]))
    r52_supervisor = read_json(Path(r52["supervisor_receipt_path"]))
    if (r52_scope.get("status") != "sol_reviewed_and_dispatchable" or
            r52_scope.get("native_dispatch") is not True or
            r52_result.get("status") != "result_refused_before_commit" or
            r52_result.get("public_result_wrapper") != "Ok" or
            r52_result.get("group_variant") != "GroupFailed" or
            r52_result.get("transaction_rolled_back") is not True or
            r52_result.get("diagnostic_written_after_rollback") is not True or
            r52_failure.get("spark_started") is not False or
            r52_failure.get("delta_written") is not False or
            r52_failure.get("source_ack_attempted") is not False or
            r52_supervisor.get("status") != "child_failed" or
            r52_supervisor.get("process_group_remaining") != []):
        raise Refusal("R52 GroupFailed rollback and no-publication reconciliation differs")
    if (sha(Path(r52["r51_inspection_path"]).read_bytes()) != r52["r51_inspection_sha256"] or
            sha(Path(r52["r49_reconciliation_path"]).read_bytes()) != r52["r49_reconciliation_sha256"]):
        raise Refusal("R52 retained R49/R51 source-custody evidence changed")
    pub = recon["publication"]
    for field in ("publication_id", "request_digest", "manifest_sha256", "receipt_sha256", "pending_receipt_sha256", "vector", "roles"):
        if pub.get(field) != predecessor.get(field): raise Refusal(f"R49 predecessor binding differs: {field}")
    for name, binding in old["receipts"].items():
        p = Path(binding["path"])
        if p.resolve(strict=True) != p or sha(p.read_bytes()) != binding["sha256"]:
            raise Refusal(f"R49 retained receipt changed: {name}")
    request = HERE.parent / scope["continuation"]["path"]
    request_raw = request.read_bytes(); req = read_json(request)
    if sha(request_raw) != scope["continuation"]["sha256"] or len(req.get("operations", [])) != 2 or [op.get("op") for op in req["operations"]] != ["create_object", "create_object"] or [op.get("type") for op in req["operations"]] != ["Person", "Place"]:
        raise Refusal("frozen two-operation continuation request differs")
    if req.get("historical_A_seed_bytes_reused") is not False or req.get("request_key") != scope["continuation"]["request_key"]:
        raise Refusal("continuation identity/seed reuse boundary differs")
    model_path = HERE.parent / scope["model"]["derived_path"]
    if sha(model_path.read_bytes()) != scope["model"]["derived_sha256"]:
        raise Refusal("accepted model source bytes changed")
    bun = Path(scope["model"]["bun_path"])
    if not bun.is_file() or not os.access(bun, os.X_OK) or sha(bun.read_bytes()) != scope["model"]["bun_sha256"]:
        raise Refusal("pinned local group authoring runtime changed")
    resume_dir = HERE.parent / scope["resume"]["receipt_directory"]
    for name, digest in scope["resume"]["receipt_sha256"].items():
        if sha((resume_dir / name).read_bytes()) != digest: raise Refusal(f"retained R24 receipt changed: {name}")
    pg = Path(paths["data_directory"]); delta = Path(paths["publication_directory"])
    pgt = tree_digest(pg, 128 * 1024 * 1024)
    expected_pg = scope["current_source_state_binding"]["expected"]
    if (pgt != expected_pg or (pg / "postmaster.pid").exists() or
            sha((pg / "global" / "pg_control").read_bytes()) != scope["current_source_state_binding"]["pg_control_sha256"]):
        raise Refusal("current stopped PGDATA differs from R54 current source-state binding")
    journal = Path(scope["retained_r49"]["journal"]["path"])
    if journal != delta / "operations.sqlite" or sha(journal.read_bytes()) != scope["retained_r49"]["journal"]["sha256"]:
        raise Refusal("current Delta coordination journal differs from R49")
    before = {s: sidecar(Path(str(journal) + s)) for s in ("-wal", "-shm")}
    uri = "file:" + quote(str(journal)) + "?mode=ro"
    with sqlite3.connect(uri, uri=True, timeout=5) as db:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        counts = {table: db.execute(f"SELECT count(*) FROM {table}").fetchone()[0] for table in ("local_operation", "local_plan", "local_publication_artifact", "local_source_plan")}
    after = {s: sidecar(Path(str(journal) + s)) for s in ("-wal", "-shm")}
    if integrity != "ok" or counts != scope["retained_r49"]["journal"]["counts"] or before != after:
        raise Refusal("read-only WAL-aware journal check or sidecar preservation failed")
    return {"profile": PROFILE, "status": "passed_effect_free_inplace_preflight", "scope_sha256": sha(raw),
            "worker_sha256": scope["worker"]["sha256"],
            "r49_ack_checkpoint_sha256": ack["checkpoint_sha256"], "r49_publication_id": recon["publication"]["publication_id"],
            "continuation_request_sha256": sha(request_raw), "continuation_operations": 2,
            "pgdata_tree": pgt, "postmaster_stopped": True, "journal_sha256": sha(journal.read_bytes()),
            "journal_integrity": integrity, "journal_counts": counts, "sqlite_sidecars_before": before,
            "sqlite_sidecars_after": after, "postgres_opened": False, "spark_started": False,
            "delta_written": False, "source_ack_attempted": False, "native_dispatch": False,
            "cloud": False, "output_roots_absent": require_absent}

def native(scope_path: Path, supervisor_root: Path):
    scope = read_json(scope_path); raw_scope = scope_path.read_bytes()
    preflight(scope_path, require_absent=False, supervisor_root_created=supervisor_root)
    paths = scope["paths"]; attempt = Path(paths["output_root"])
    attempt.mkdir(mode=0o700); os.chmod(attempt, 0o700)
    receipts = attempt / "receipts"; receipts.mkdir(mode=0o700)
    effects = {"postgres_opened":False,"group_invoked":False,"transaction_rolled_back":False,
        "spark_started":False,"delta_written":False,"source_ack_attempted":False,"cloud":False}
    runtime = None; primary = None
    try:
        helper_spec = importlib.util.spec_from_file_location("ashlar_r37_page_helpers", HERE / "r37-local-worker.py")
        helper = importlib.util.module_from_spec(helper_spec); helper_spec.loader.exec_module(helper)
        base = helper.load_base()
        probe_spec = importlib.util.spec_from_file_location("ashlar_r54_probe", HERE / "r54-group-diagnostic-probe.py")
        probe = importlib.util.module_from_spec(probe_spec); probe_spec.loader.exec_module(probe)
        runtime_selection = scope["runtime_candidate"]
        overlay = Path(runtime_selection["new_overlay_or_venv"]).resolve(strict=True) / "site-packages"
        extras = Path(runtime_selection["local_extra_overlay"]).resolve(strict=True)
        sys.path[:0] = [str(overlay), str(extras)]
        if importlib.metadata.version("umf-truss") != "0.0.52": raise Refusal("selected Truss 0.0.52 runtime required")
        distribution = importlib.metadata.distribution("umf-truss")
        direct = json.loads(distribution.read_text("direct_url.json") or "{}")
        if (direct.get("url") != Path(runtime_selection["wheel_path"]).resolve().as_uri() or
                direct.get("archive_info", {}).get("hashes", {}).get("sha256") != runtime_selection["wheel_sha256"]):
            raise Refusal("selected Truss wheel origin/hash differs")
        if {name: importlib.metadata.version(name) for name in
                ("pgserver-pg18", "fasteners", "platformdirs", "psutil")} != {
                "pgserver-pg18":"18.6.1", "fasteners":"0.20", "platformdirs":"4.12.4", "psutil":"7.2.2"}:
            raise Refusal("selected Truss .52 local extra pins changed")
        import psycopg
        from truss import CatalogRuntimeStatus, LocalPostgres
        from truss.feed import FeedPage
        from truss.reader_catalog import AcceptedDocument
        from ashlar_host.truss_note_publication import read_page
        runtime = LocalPostgres(Path(paths["data_directory"]))
        runtime.__enter__(); effects["postgres_opened"] = True
        info = runtime.info
        if info.server_version != "18.6" or Path(info.data_directory) != Path(paths["data_directory"]):
            raise Refusal("R54 resumed PostgreSQL identity mismatch")
        params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
        names = scope["source"]
        def connect(role=None, autocommit=False):
            values = dict(params, autocommit=autocommit, connect_timeout=5,
                          options="-c statement_timeout=10000")
            if role is not None: values["user"] = role
            return psycopg.connect(**values)
        def tx(connection, fn, read_only=True):
            return base._transaction(connection, fn, read_only=read_only, timeout_ms=10000)
        with connect(names["registrar_login_role"], True) as c:
            status, report = tx(c, lambda a,t:(base._value(a.status_in_transaction(t),"registrar status"),
                base._value(a.report_in_transaction(t,scope["acceptance"]["revision"]),"acceptance report")))
            c.rollback()
        if (type(status) is not CatalogRuntimeStatus or not status.installed or
                status.installation_id != scope["installation"]["installation_id"] or
                status.release_id != runtime_selection["release_id"] or
                sha(report.original_bytes) != scope["acceptance"]["report_sha256"]):
            raise Refusal("R54 public Truss runtime/acceptance status changed")
        base.validate_retained_report(report.original_bytes, registration_id=scope["acceptance"]["registration_id"],
                                      revision=scope["acceptance"]["revision"])
        with connect(names["ordinary_reader_role"], True) as c:
            document = tx(c, lambda a,t:base._value(a.accepted_document_in_transaction(
                t,document_id=scope["model"]["document_id"],maximum_bytes=1048576),"accepted document"))
            c.rollback()
        model_raw=(HERE.parent / scope["model"]["derived_path"]).read_bytes()
        if type(document) is not AcceptedDocument or document.document_id != scope["model"]["document_id"] or sha(document.content) != sha(model_raw):
            raise Refusal("R54 public accepted document differs from frozen model")
        request_path=HERE.parent / scope["continuation"]["path"]
        request_raw=request_path.read_bytes()
        continuation=read_json(request_path)
        if sha(request_raw) != scope["continuation"]["sha256"]:
            raise Refusal("R54 frozen request digest changed at runtime")
        reader_factory = base.ordinary_reader_connection_factory(psycopg, params, names["ordinary_reader_role"])
        page=read_page(reader_factory,names["consumer_id"],maximum_bytes=scope["publication"]["maximum_page_bytes"])
        if not valid_empty_page_from_prior(page, FeedPage, names["consumer_id"],
                                           scope["predecessor"]["checkpoint_sha256"]):
            raise Refusal("R54 public FeedPage is not empty from the exact R49 prior cursor")
        request_key=continuation["request_key"]
        with connect(None, False) as c:
            c.execute("SET TRANSACTION READ ONLY")
            rows=c.execute("SELECT request_id_utf8 FROM truss.request_receipt WHERE request_id_utf8=%s",(request_key,)).fetchmany(2)
            c.rollback()
        if rows:
            raise Refusal("R54 refused: exact continuation request key already has a receipt")
        # Hold the exact public transaction result in memory; probe_group raises its sentinel
        # inside base._transaction so this public apply call can never commit.
        effects["group_invoked"] = True
        outcome=probe.probe_group(base,lambda _role:dict(params,user=names["writer_login_role"]),
            model=json.loads(model_raw),document_id=scope["model"]["document_id"],
            document_revision=scope["model"]["document_revision"],request_key=request_key,
            origin=continuation["origin"],operations=continuation["operations"],
            bun=Path(scope["model"]["bun_path"]),receipt_path=receipts/"group-diagnostic.json",
            expected_count=2)
        effects["transaction_rolled_back"] = True
        # Read-only custody checks after rollback prove the probe left request and feed absent.
        with connect(None, False) as c:
            c.execute("SET TRANSACTION READ ONLY")
            rows_after=c.execute("SELECT request_id_utf8 FROM truss.request_receipt WHERE request_id_utf8=%s",(request_key,)).fetchmany(2)
            c.rollback()
        page_after=read_page(reader_factory,names["consumer_id"],maximum_bytes=scope["publication"]["maximum_page_bytes"])
        if rows_after or not valid_empty_page_from_prior(
                page_after, FeedPage, names["consumer_id"], scope["predecessor"]["checkpoint_sha256"]):
            raise Refusal("R54 post-rollback readback found request receipt or non-empty page from R49 prior cursor")
        write_json(receipts/"probe-completed.json",{"profile":"ashlar-r54-truss-group-diagnostic-probe/0.1",
            "status":"observed_and_rolled_back","request_key":request_key,
            "failure_variant":outcome.get("failure_variant"),"operation_index":outcome.get("operation_index"),
            "operation_code":outcome.get("operation_code"),"diagnostic_artifact_sha256":(outcome.get("diagnostic_artifact") or {}).get("sha256"),
            "r49_checkpoint_sha256":scope["predecessor"]["checkpoint_sha256"],
            "request_receipt_absent_before_and_after":True,"feed_empty_before_and_after":True,
            "spark_started":False,"delta_written":False,"source_ack_attempted":False,"cloud":False})
    except BaseException as exc:
        primary=exc
        try:
            write_json(receipts/"failure.json",{"profile":"ashlar-r54-truss-group-diagnostic-probe/0.1",
                "status":"failed_or_interrupted","failure_type":type(exc).__name__,"failure":str(exc)[:800],**effects})
        except BaseException: pass
        raise
    finally:
        if runtime is not None:
            try: runtime.__exit__(None,None,None)
            except BaseException:
                if primary is None: raise

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--scope", type=Path, default=SCOPE_DEFAULT)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--attempt-root", type=Path)
    parser.add_argument("--supervisor-root", type=Path)
    args = parser.parse_args()
    try:
        if args.preflight_only:
            print(json.dumps(preflight(args.scope), sort_keys=True, separators=(",", ":")))
            return 0
        scope = read_json(args.scope)
        raw_scope = args.scope.read_bytes()
        if (args.attempt_root != Path(scope["paths"]["output_root"]) or
                args.supervisor_root != Path(scope["paths"]["supervisor_root"]) or
                os.environ.get("ASHLAR_R36_SUPERVISED") != "1" or
                os.environ.get("ASHLAR_R36_SCOPE_SHA256") != sha(raw_scope) or
                scope.get("status") != "sol_reviewed_and_dispatchable"):
            raise Refusal("R54 native execution requires exact reviewed supervisor handshake")
        native(args.scope, args.supervisor_root)
        return 0
    except Exception as exc:
        print(json.dumps({"profile": PROFILE, "status": "refused", "reason": f"{type(exc).__name__}: {exc}"[:1200], "native_dispatch": False}, sort_keys=True, separators=(",", ":")))
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
