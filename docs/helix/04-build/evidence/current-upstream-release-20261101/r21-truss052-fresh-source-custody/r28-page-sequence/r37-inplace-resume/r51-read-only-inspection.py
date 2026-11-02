#!/usr/bin/env python3
"""One bounded R51 Truss/Feed inspection; no source or destination writes."""
from __future__ import annotations
import hashlib, importlib.util, json, os, re, signal, sqlite3, stat, sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[7]
SCOPE = HERE / "r51-host-scope.json"
OUTPUT = HERE / "r51-read-only-inspection-v2.json"
RUNTIME = Path("/private/tmp/ashlar-r21-truss052-runtime-candidate-20261101")
EXTRAS = Path("/private/tmp/ashlar-r9-truss051-current-runtime-20261101/site-packages")
MAX_SECONDS = 180
MAX_RSS = 2 * 1024**3

def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()

def read_json(path: Path):
    def pairs(items):
        value = {}
        for key, child in items:
            if key in value: raise ValueError("duplicate JSON key")
            value[key] = child
        return value
    value = json.loads(path.read_bytes(), object_pairs_hook=pairs)
    if type(value) is not dict: raise ValueError("closed JSON object required")
    return value

def tree(root: Path):
    h = hashlib.sha256(); files = total = 0
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix().encode()):
        info = path.lstat(); rel = path.relative_to(root).as_posix().encode()
        if stat.S_ISLNK(info.st_mode) or not (path.is_dir() or path.is_file()):
            raise ValueError("PGDATA contains a link or special entry")
        kind = b"D" if path.is_dir() else b"F"
        h.update(kind + len(rel).to_bytes(4, "big") + rel + (info.st_mode & 0o777).to_bytes(4, "big") + info.st_size.to_bytes(8, "big"))
        if kind == b"F":
            files += 1; total += info.st_size
            if total > 128 * 1024**2: raise ValueError("PGDATA inspection byte bound exceeded")
            with path.open("rb") as stream:
                while chunk := stream.read(1024 * 1024): h.update(chunk)
    return {"sha256":h.hexdigest(), "files":files, "bytes":total}

def sidecar(path: Path):
    try:
        info = path.lstat()
        return {"present":True,"bytes":info.st_size,"dev":info.st_dev,"ino":info.st_ino,
                "mode":info.st_mode & 0o777,
                "sha256":sha(path.read_bytes()) if path.suffix == ".wal" else None}
    except FileNotFoundError:
        return {"present":False,"bytes":0,"dev":None,"ino":None,"mode":None,"sha256":None}

def timeout(_signum, _frame): raise TimeoutError("read-only inspection wall bound expired")

def main():
    signal.signal(signal.SIGALRM, timeout); signal.alarm(MAX_SECONDS)
    scope = read_json(SCOPE)
    if (scope.get("status") != "sol_reviewed_and_dispatchable" or
            scope.get("native_dispatch") is not True or scope.get("cloud") is not False or
            scope.get("production_authority") is not False):
        raise ValueError("exact retained R51 dispatch scope required for source identity")
    paths = scope["paths"]; pg = Path(paths["data_directory"]); delta = Path(paths["publication_directory"])
    if (pg.resolve(strict=True) != pg or pg.is_symlink() or (pg / "postmaster.pid").exists() or
            not pg.is_dir() or not delta.is_dir()):
        raise ValueError("canonical stopped PGDATA and existing Delta source required")
    before_tree = tree(pg)
    failure_reconciliation = read_json(HERE / "r51-read-only-failure-reconciliation.json")
    if failure_reconciliation["postgres_read_only"]["cluster_state"].strip() != "shut down":
        raise ValueError("R51 failed source is not in the expected stopped state")
    journal = delta / "operations.sqlite"
    before_sidecars = {suffix:sidecar(Path(str(journal) + suffix)) for suffix in ("-wal", "-shm")}
    before_journal_sha = sha(journal.read_bytes())
    attempt = Path(paths["output_root"]); supervisor = Path(paths["supervisor_root"])
    if not attempt.is_dir() or not supervisor.is_dir(): raise ValueError("preserved R51 roots required")
    failure = read_json(attempt / "receipts" / "failure.json")
    if failure.get("postgres_opened") is not True or failure.get("spark_started") is not False or failure.get("delta_written") is not False or failure.get("source_ack_attempted") is not False:
        raise ValueError("R51 failure phase differs from public read-only inspection authorization")

    sys.path[:0] = [str(REPO / "src"), str(RUNTIME / "site-packages"), str(EXTRAS)]
    helper_spec = importlib.util.spec_from_file_location("r51_page_helpers", HERE / "r37-local-worker.py")
    helper = importlib.util.module_from_spec(helper_spec); helper_spec.loader.exec_module(helper)
    base = helper.load_base()
    import psycopg
    from truss import LocalPostgres
    from ashlar_host.truss_note_publication import read_page
    from ashlar_host.truss052_source_custody import Truss052SourceCustodySession
    from truss.feed import FeedPage

    request = read_json(HERE.parent / scope["continuation"]["path"])
    runtime = LocalPostgres(pg); primary = None; result = None; opened = False
    try:
        runtime.__enter__()
        opened = True
        info = runtime.info
        if info.server_version != "18.6" or Path(info.data_directory) != pg:
            raise ValueError("exact existing local PostgreSQL identity required")
        params = psycopg.conninfo.conninfo_to_dict(info.connection_uri)
        conn = psycopg.connect(info.connection_uri, autocommit=False, connect_timeout=5,
                               options="-c statement_timeout=10000")
        try:
            conn.execute("SET TRANSACTION READ ONLY")
            conn.execute("SET LOCAL statement_timeout = '10000ms'")
            current_user, readonly = conn.execute("SELECT current_user, current_setting('transaction_read_only')").fetchone()
            if readonly != "on": raise ValueError("inspection transaction is not read only")
            rows = conn.execute("""
                SELECT storage_row_id, payload_state, original_writer_xid::text,
                       encode(namespace_sha256,'hex'), encode(request_sha256,'hex'),
                       convert_from(namespace_identity_utf8,'UTF8'),
                       convert_from(request_id_utf8,'UTF8'),
                       convert_from(original_writer_context_bytes,'UTF8'),
                       octet_length(original_receipt_bytes),
                       encode(pg_catalog.sha256(original_receipt_bytes),'hex'),
                       CASE WHEN octet_length(original_receipt_bytes) <= 1048576
                            THEN convert_from(original_receipt_bytes,'UTF8') END
                  FROM truss.request_receipt
                 WHERE request_id_utf8 = convert_to(%s,'UTF8')
                 ORDER BY storage_row_id LIMIT 8
            """, (request["request_key"],)).fetchall()
            request_rows = []
            for row in rows:
                receipt = None
                if row[10] is not None:
                    try: receipt = json.loads(row[10])
                    except (ValueError, TypeError): receipt = {"parse":"invalid-json"}
                writer_context = json.loads(row[7])
                item = {"storage_row_id":row[0],"payload_state":row[1],"writer_xid":row[2],
                    "namespace_sha256":row[3],"request_sha256":row[4],"namespace_identity":row[5],
                    "request_id":row[6],"writer_context":{k:writer_context.get(k) for k in ("profile","attemptId","xid","actorOid")},
                    "original_receipt_bytes":row[8],"original_receipt_sha256":row[9]}
                if type(receipt) is dict:
                    item["receipt_result_shape"]={k:receipt.get(k) for k in ("outcome","disposition","durability","code","semantic") if k in receipt}
                    sem=item["receipt_result_shape"].get("semantic")
                    if type(sem) is dict: item["receipt_result_shape"]["semantic"]={k:sem.get(k) for k in ("interfaceVersion","executedCatalogRevision","results","aliases") if k in sem}
                request_rows.append(item)
            conn.rollback()
        finally:
            conn.close()

        reader_role = scope["source"]["ordinary_reader_role"]
        def reader_connection():
            return psycopg.connect(**dict(params,user=reader_role),autocommit=False,connect_timeout=5,
                options="-c statement_timeout=10000")
        page = read_page(reader_connection, scope["source"]["consumer_id"],
                         maximum_bytes=scope["publication"]["maximum_page_bytes"])
        if type(page) is not FeedPage: raise ValueError("public feed API did not return FeedPage")
        page_summary = {"outcome":page.outcome,"consumer_id":page.consumer_id,
            "registration_id":page.registration_id,"writer_xid":page.writer_xid,
            "page_sha256":page.sha256,"content_sha256":sha(page.content),
            "prior_cursor_sha256":sha(page.prior.content),"next_cursor_sha256":sha(page.next.content),
            "prior_cursor_matches_R49_ACK":sha(page.prior.content)==scope["predecessor"]["checkpoint_sha256"],
            "prior_cursor_bytes":len(page.prior.content),"next_cursor_bytes":len(page.next.content)}
        if page.outcome == "transaction":
            typed, shape = helper.parse_page(base, page, scope["publication"]["feed"])
            page_summary.update({"typed_change_count":len(typed.changes),"page_shape":shape,
                "event_sha256":[sha(raw) for raw in typed.originals],
                "event_summaries":[{"sha256":sha(raw),"event":json.loads(raw)} for raw in typed.originals]})
        result = {"profile":"ashlar-r51-read-only-truss-inspection/0.1","status":"inspected",
            "authority":"local-development-only","scope_sha256":sha(SCOPE.read_bytes()),
            "worker_sha256":sha(Path(__file__).read_bytes()),"read_only_sql":{"current_user":current_user,
                "transaction_read_only":readonly,"request_key":request["request_key"],"matching_request_receipts":request_rows},
            "public_feed_page":page_summary,"postgres_opened":opened,"spark_started":False,
            "delta_written":False,"source_ack_attempted":False,"cloud":False}
    except BaseException as exc:
        primary = exc
        result = {"profile":"ashlar-r51-read-only-truss-inspection/0.1","status":"inspection_failed",
            "error_type":type(exc).__name__,"error":str(exc)[:1200],"postgres_opened":opened,
            "spark_started":False,"delta_written":False,"source_ack_attempted":False,"cloud":False}
    finally:
        try:
            if opened: runtime.__exit__(None,None,None)
        except BaseException as stop_error:
            if primary is None: primary = stop_error
            result["stop_error"] = f"{type(stop_error).__name__}: {stop_error}"[:1200]
    signal.alarm(0)
    after_tree = tree(pg)
    after_sidecars = {suffix:sidecar(Path(str(journal) + suffix)) for suffix in ("-wal", "-shm")}
    with sqlite3.connect("file:"+str(journal)+"?mode=ro",uri=True,timeout=5) as db:
        integrity = db.execute("PRAGMA integrity_check").fetchone()[0]
        counts = {table:db.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                  for table in ("local_operation","local_plan","local_publication_artifact","local_source_plan")}
    result["postgres_after_stop"]={"postmaster_pid_absent":not (pg/"postmaster.pid").exists(),
        "pg_control_sha256":sha((pg/"global"/"pg_control").read_bytes()),"pgdata_tree_before":before_tree,
        "pgdata_tree_after":after_tree,"normal_start_stop_pgdata_delta":before_tree!=after_tree}
    result["delta_read_only_reconciliation"]={"journal_sha256_before":before_journal_sha,
        "journal_sha256_after":sha(journal.read_bytes()),"matches_R51_preflight":sha(journal.read_bytes())==before_journal_sha,
        "integrity":integrity,"counts":counts,"sqlite_sidecars_before":before_sidecars,
        "sqlite_sidecars_after":after_sidecars,"sidecars_unchanged":before_sidecars==after_sidecars}
    OUTPUT.write_text(json.dumps(result,sort_keys=True,indent=2,ensure_ascii=False)+"\n")
    print(json.dumps({"status":result["status"],"output":str(OUTPUT),
        "postgres_stopped":result["postgres_after_stop"]["postmaster_pid_absent"],
        "delta_unchanged":result["delta_read_only_reconciliation"]["matches_R51_preflight"],
        "source_ack_attempted":False},sort_keys=True))
    if primary is not None: raise primary

if __name__ == "__main__": main()
