#!/usr/bin/env python3
"""R52 bounded continuation worker with effect-free preflight and gated native path.

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
SCOPE_DEFAULT = HERE / "r52-host-scope.json"
PROFILE = "ashlar-r52-effect-free-two-change-preflight/0.1"

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

def preflight(scope_path: Path, *, require_absent=True, supervisor_root_created=None):
    raw = scope_path.read_bytes(); scope = read_json(scope_path)
    if scope.get("profile") != "ashlar-r37-truss052-inplace-resume/0.1" or scope.get("status") not in {"pending_sol_review", "sol_reviewed_and_dispatchable"}:
        raise Refusal("exact R52 preflight/dispatchable scope required")
    limits = scope.get("limits", {})
    if (limits.get("wall_seconds"), limits.get("process_rss_bytes"), limits.get("attempt_root_bytes"), limits.get("attempts"), limits.get("automatic_retries"), limits.get("cloud")) != (900, 2147483648, 536870912, 1, 0, False):
        raise Refusal("R52 finite local bounds differ")
    dispatchable = scope.get("status") == "sol_reviewed_and_dispatchable"
    if (scope.get("cloud") is not False or scope.get("production_authority") is not False or
            type(scope.get("native_dispatch")) is not bool or scope.get("native_dispatch") is not dispatchable):
        raise Refusal("R52 local preflight boundary differs")
    paths = scope["paths"]
    attempt = Path(paths["output_root"]); supervisor = Path(paths["supervisor_root"])
    if not attempt.is_absolute() or not supervisor.is_absolute() or attempt.exists():
        raise Refusal("fresh R52 attempt root must be absent")
    if require_absent and supervisor.exists():
        raise Refusal("fresh R52 supervisor root must be absent")
    if not require_absent:
        if supervisor_root_created is None or Path(supervisor_root_created) != supervisor or not supervisor.is_dir() or supervisor.is_symlink():
            raise Refusal("exact supervisor-created R52 root required")
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
        raise Refusal("R52 worker hash differs from scope")
    group_helper_binding = scope["group_helper"]
    if sha(Path(group_helper_binding["path"]).read_bytes()) != group_helper_binding["sha256"]:
        raise Refusal("R52 transaction-safe group helper hash differs from scope")
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
    for jar in scope["spark_runtime"]["jars"]:
        path = Path(jar["path"])
        if not path.is_file() or sha(path.read_bytes()) != jar["sha256"]:
            raise Refusal("selected Spark/Delta JAR hash changed")
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
    expected_pg = scope["retained_r49"]["pgdata_tree"]["expected"]
    if pgt != expected_pg or (pg / "postmaster.pid").exists():
        raise Refusal("current stopped PGDATA differs from R49 read-only binding")
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
    scope = read_json(scope_path)
    preflight(scope_path, require_absent=False, supervisor_root_created=supervisor_root)
    # R37's parser and public page matcher are reused; its seed-page and seed-ACK
    # stages are intentionally not called from this continuation-only path.
    helper_path = HERE / "r37-local-worker.py"
    spec = importlib.util.spec_from_file_location("ashlar_r37_page_helpers", helper_path)
    if spec is None or spec.loader is None: raise Refusal("R37 bounded page helper is unavailable")
    helper = importlib.util.module_from_spec(spec); spec.loader.exec_module(helper)
    base = helper.load_base()
    group_spec = importlib.util.spec_from_file_location("ashlar_r52_group_helper", HERE / "r52-group-helper.py")
    group_helper = importlib.util.module_from_spec(group_spec); group_spec.loader.exec_module(group_helper)
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
    if {name: importlib.metadata.version(name) for name in ("pgserver-pg18", "fasteners", "platformdirs", "psutil")} != {
            "pgserver-pg18":"18.6.1", "fasteners":"0.20", "platformdirs":"4.12.4", "psutil":"7.2.2"}:
        raise Refusal("selected Truss .52 local extra pins changed")
    import base64, os
    import psycopg
    from truss import CatalogRuntimeStatus, LocalPostgres, create_reference_assembly
    from truss.catalog_results import CatalogRegistration
    from truss.feed import FeedConsumer, FeedPage
    from truss.reader_catalog import AcceptedDocument
    from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
    from ashlar_host.truss_note_publication import read_page
    from ashlar_host.truss052_source_custody import Truss052SourceCustodySession
    from ashlar_host.truss_source_custody_selection import select_truss_local_dev_source_custody_anchor, read_local_native_delta_publication_observation
    from pyspark import SparkContext
    from pyspark.sql import SparkSession

    paths = scope["paths"]; attempt = Path(paths["output_root"])
    attempt.mkdir(mode=0o700); os.chmod(attempt, 0o700)
    receipts = attempt / "receipts"; receipts.mkdir(mode=0o700)
    effects = {"postgres_opened": False, "spark_started": False, "delta_written": False,
               "source_ack_attempted": False, "publication_replayed": False, "ack_replayed": False,
               "cloud": False, "production_authority": False}
    runtime = LocalPostgres(Path(paths["data_directory"]))
    spark = None; primary = None
    state = {}
    try:
        resume_dir = HERE.parent / scope["resume"]["receipt_directory"]
        resume_receipts = {}
        for name, digest in scope["resume"]["receipt_sha256"].items():
            retained = resume_dir / name
            if sha(retained.read_bytes()) != digest: raise Refusal(f"retained R24 receipt changed: {name}")
            resume_receipts[name] = read_json(retained)
        acceptance = resume_receipts["acceptance.json"]
        installation = resume_receipts["installation.json"]
        consumer_registration = resume_receipts["consumer-registration.json"]
        if acceptance != scope["acceptance"] or installation != scope["installation"]:
            raise Refusal("R24 acceptance/installation scope differs from retained receipts")
        runtime.__enter__(); effects["postgres_opened"] = True
        info = runtime.info
        if info.server_version != "18.6" or Path(info.data_directory) != Path(paths["data_directory"]):
            raise Refusal("R52 resumed PostgreSQL identity mismatch")
        params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
        names = scope["source"]
        def connect(role, autocommit=False):
            return psycopg.connect(**dict(params, user=role), autocommit=autocommit,
                                   connect_timeout=5, options="-c statement_timeout=10000")
        def tx(connection, fn): return base._transaction(connection, fn, read_only=True, timeout_ms=10000)
        with connect(names["registrar_login_role"], True) as c:
            status, report = tx(c, lambda a,t: (base._value(a.status_in_transaction(t), "registrar status"),
                base._value(a.report_in_transaction(t, scope["acceptance"]["revision"]), "acceptance report")))
            c.rollback()
        if (type(status) is not CatalogRuntimeStatus or status.installed is not True or
                status.installation_id != installation["installation_id"] or
                status.release_id != scope["runtime_candidate"]["release_id"] or
                sha(report.original_bytes) != scope["acceptance"]["report_sha256"]):
            raise Refusal("current public Truss runtime/report differs from R49 acceptance custody")
        base.validate_retained_report(report.original_bytes, registration_id=acceptance["registration_id"],
                                      revision=acceptance["revision"])
        with connect(names["ordinary_reader_role"], True) as c:
            document = tx(c, lambda a,t: base._value(a.accepted_document_in_transaction(
                t, document_id=scope["model"]["document_id"], maximum_bytes=1048576), "accepted document"))
            c.rollback()
        model_raw = (HERE.parent / scope["model"]["derived_path"]).read_bytes()
        if type(document) is not AcceptedDocument or document.document_id != scope["model"]["document_id"] or sha(document.content) != sha(model_raw):
            raise Refusal("public accepted document differs from exact model binding")
        registration = CatalogRegistration(revision=acceptance["revision"],
            registration_id=acceptance["registration_id"], report_bytes=report.original_bytes, replayed=False)
        continuation = read_json(HERE.parent / scope["continuation"]["path"])
        model = json.loads(model_raw)
        group = group_helper.apply_group(base,lambda _role: dict(params, user=names["writer_login_role"]),
            model=model, document_id=scope["model"]["document_id"],
            document_revision=scope["model"]["document_revision"],
            request_key=continuation["request_key"], origin=continuation["origin"],
            operations=continuation["operations"], bun=Path(scope["model"]["bun_path"]),
            receipt_path=receipts / "continuation-group-commit.json", expected_count=2)
        if len(group.response.semantic.results) != 2:
            raise Refusal("frozen R52 source group did not commit exactly two operations")
        reader_factory = base.ordinary_reader_connection_factory(psycopg, params, names["ordinary_reader_role"])
        page = read_page(reader_factory, names["consumer_id"], maximum_bytes=scope["publication"]["maximum_page_bytes"])
        if (type(page) is not FeedPage or page.outcome != "transaction" or
                page.consumer_id != names["consumer_id"] or
                page.registration_id != consumer_registration["registration_id"]):
            raise Refusal("next public Truss page is not the selected consumer transaction")
        if sha(page.prior.content) != scope["predecessor"]["checkpoint_sha256"]:
            raise Refusal("fresh page prior cursor is not the matched R49 ACK checkpoint")
        parsed, shape = helper.parse_page(base, page, names["feed_selection"])
        if shape.get("kind") != "entity" or len(parsed.changes) != 2 or shape.get("typed_entity_changes") != 2:
            raise Refusal("fresh Truss page is not exactly two typed entity changes")
        state = {"page": page}
        from ashlar_host.configured_local_runtime_profile import attest
        rt = scope["spark_runtime"]; java_home = Path(rt["java_home"]); jars = [Path(i["path"]) for i in rt["jars"]]
        for item, jar in zip(rt["jars"], jars):
            if not jar.is_file() or sha(jar.read_bytes()) != item["sha256"]: raise Refusal("R52 pinned Spark/Delta JAR changed")
        os.environ["JAVA_HOME"] = str(java_home)
        os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
        os.environ["PYSPARK_PYTHON"] = scope["runtime_candidate"]["local_runtime_base_python"]
        os.environ["PYSPARK_DRIVER_PYTHON"] = scope["runtime_candidate"]["local_runtime_base_python"]
        runtime_identity = type("R52Runtime", (), {"java_home":java_home,"jars":jars[0].parent,
            "jar_inventory":{i["name"]:i["sha256"] for i in rt["jars"]}})()
        _, spark_identity, verified = attest(runtime_identity)
        if tuple(verified) != tuple(jars) or spark_identity.get("jdk") != "21": raise Refusal("R52 local Spark runtime attestation differs")
        def select_page(**kw):
            p = kw["page"]
            if p.sha256 != page.sha256 or p.prior.content != page.prior.content or p.next.content != page.next.content:
                raise Refusal("R52 public page changed between selection and publication")
            typed_page, page_shape = helper.parse_page(base, p, names["feed_selection"])
            if page_shape.get("kind") != "entity" or len(typed_page.changes) != 2:
                raise Refusal("R52 page changed from exact two-change shape")
            match = base.validate_group_page_against_frozen_request(typed_page, kw["admission"],
                continuation, document_id=scope["model"]["document_id"],
                revision=acceptance["revision"], expected_count=2)
            request_raw = json.dumps(kw["request"], sort_keys=True, separators=(",", ":"),
                ensure_ascii=False, allow_nan=False).encode()
            consumer = FeedConsumer(consumer_id=p.consumer_id, registration_id=p.registration_id, checkpoint=p.prior)
            anchor = select_truss_local_dev_source_custody_anchor(registration=registration,
                accepted_document=document, consumer=consumer, runtime_status=status, page=p,
                feed=scope["publication"]["feed"], source_holder_id=names["source_holder_id"],
                document_id=scope["model"]["document_id"], destination_id=scope["publication"]["destination_id"],
                ack_registration_id=names["ack_registration_id"], ack_scope_id=names["ack_scope_id"],
                publication_id=kw["publication_id"], request_bytes=request_raw,
                predecessor_id=kw["request"]["predecessor"], destination_transport=kw["destination_transport"])
            state.update({"anchor":anchor,"request_bytes":request_raw,"typed_page":typed_page})
            write = {"profile":"ashlar-r52-page-selection/0.1","page_sha256":p.sha256,
                "prior_cursor_sha256":sha(p.prior.content),"next_cursor_sha256":sha(p.next.content),
                "typed_changes":2,"group_correspondence":match,"publication_id":kw["publication_id"],
                "source_ack_attempted":False}
            write_json(receipts / "page-selection.json", write)
            def postpublication(**post):
                observation = read_local_native_delta_publication_observation(anchor=anchor,
                    spark=post["destination_transport"].spark, destination_transport=post["destination_transport"],
                    manifest_bytes=post["manifest_bytes"], vector=post["vector"],
                    receipt_bytes=post["receipt_bytes"], expected_post_state=post["expected_post_state"])
                selection = anchor.bind_local_native_publication(manifest_bytes=post["manifest_bytes"],
                    vector=post["vector"], receipt_bytes=post["receipt_bytes"],
                    expected_post_state=post["expected_post_state"], observation=observation)
                state.update({"selection":selection,"receipt":post["receipt_bytes"]})
                out = {"profile":"ashlar-r52-native-readback/0.1","publication_id":observation.publication_id,
                    "manifest_sha256":observation.manifest_sha256,"receipt_sha256":observation.receipt_sha256,
                    "vector":{k:list(v) for k,v in observation.vector.targets.items()},
                    "roles":{r:{"table":o.table,"uuid":o.uuid,"version":o.version,"head_version":o.head_version,
                        "head_json_sha256":o.head_json_sha256,"row_count":o.row_count,"row_bag_sha256":o.row_bag_sha256}
                        for r,o in observation.roles.items()},"independent_native_readback":True,
                    "ack_attempted":False,"publication_replayed":False}
                write_json(receipts / "native-readback.json", out)
            return postpublication
        intent = scope["predecessor"]["publication_intent"]
        if sha(Path(intent["path"]).read_bytes()) != intent["sha256"]: raise Refusal("R49 seed publication intent changed")
        config = {"connection_uri":psycopg.conninfo.make_conninfo(**dict(params,user=names["ordinary_reader_role"])),
            "catalog_connection_uri":psycopg.conninfo.make_conninfo(**dict(params,user=names["registrar_login_role"])),
            "publication_directory":paths["publication_directory"],"jars":[str(j) for j in jars],
            "publication_intent":intent,"destination_id":scope["publication"]["destination_id"],
            "carrier_profile":scope["publication"]["carrier_profile"],
            "runtime_profile":{"profile":rt["runtime_profile"],"java_home":str(java_home),
                "jar_inventory":{i["name"]:i["sha256"] for i in rt["jars"]}},"archived_acceptance_files":[]}
        config_path = attempt / "continuation-config-r52.json"
        write_json(config_path, config)
        from ashlar_host.truss_resume_cli import parse_resume_truss_configuration
        parse_resume_truss_configuration(config_path, attempt / "continuation-output-r52")
        effects["spark_started"] = True
        effects["delta_written"] = True
        result = run_continue_truss_graph_command(config_path, attempt / "continuation-result-r52.json",
            maximum_pages=1, wait_seconds=0, maximum_page_bytes=scope["publication"]["maximum_page_bytes"],
            defer_source_acknowledgement=True, prepublication_source_custody=select_page)
        if result.get("source_acknowledgement") != "deferred_not_attempted" or len(result.get("publications",())) != 1 or "selection" not in state:
            raise Refusal("R52 continuation did not produce one verified deferred-ACK publication")
        pub = result["publications"][0]; side = pub["deferred_ack_intent"]
        if sha(Path(side["path"]).read_bytes()) != side["sha256"]: raise Refusal("R52 durable deferred ACK intent changed")
        context = object()
        def status_provider():
            with connect(names["registrar_login_role"], True) as c:
                current = tx(c, lambda a,t: base._value(a.status_in_transaction(t), "runtime status")); c.rollback()
            if type(current) is not CatalogRuntimeStatus or not current.installed or current.installation_id != installation["installation_id"] or current.release_id != scope["runtime_candidate"]["release_id"]:
                raise Refusal("R52 runtime status changed before protected ACK")
            return current
        custody = Truss052SourceCustodySession(connection_factory=lambda _ctx:connect(names["ordinary_reader_role"],False),
            selection=state["selection"], context=context, runtime_status_provider=status_provider, statement_timeout_ms=10000)
        with custody:
            custody.open(); effects["source_ack_attempted"] = True; custody.acknowledge(); ack = custody.reconcile()
            if ack.outcome != "matched" or ack.checkpoint.content != page.next.content: raise Refusal("R52 protected ACK checkpoint mismatch")
            custody.close()
        ack_result = {"profile":"ashlar-r52-protected-ack/0.1","outcome":ack.outcome,
            "publication_id":state["selection"].publication_id,"page_sha256":page.sha256,
            "receipt_sha256":sha(state["receipt"]),"checkpoint_sha256":sha(ack.checkpoint.content),
            "same_session_open_reconcile_close":True,"source_ack_replayed":False,"authority":"local-development"}
        write_json(receipts / "protected-ack-reconciliation.json", ack_result)
        write_json(receipts / "completed.json", {"profile":"ashlar-r52-two-change-native-attempt/0.1",
            "status":"matched_local_development_only","r49_seed_ack_replayed":False,"r49_seed_publication_replayed":False,
            "continuation_operations":2,"page_changes":2,"native_readback_before_ack":True,
            "continuation_ack":"matched","production_authority":False,"cloud":False})
    except BaseException as exc:
        primary = exc
        try:
            write_json(receipts / "failure.json", {"profile": "ashlar-r52-two-change-native-attempt/0.1",
                "status": "failed_or_interrupted", "failure_type": type(exc).__name__,
                "failure": str(exc)[:1200], **effects})
        except BaseException: pass
        raise
    finally:
        if spark is not None: spark.stop()
        try: runtime.__exit__(None, None, None)
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
            raise Refusal("R52 native execution requires exact reviewed supervisor handshake")
        native(args.scope, args.supervisor_root)
        return 0
    except Exception as exc:
        print(json.dumps({"profile": PROFILE, "status": "refused", "reason": f"{type(exc).__name__}: {exc}"[:1200], "native_dispatch": False}, sort_keys=True, separators=(",", ":")))
        return 2

if __name__ == "__main__":
    raise SystemExit(main())
