#!/usr/bin/env python3
"""In-place R36 recovery: publish and verify only ACK-gated next Truss pages.

Preflight reads retained receipts, the R36 PGDATA tree, public resume parser,
and Delta journal without opening PostgreSQL, Spark, or write paths. Native
mode is supervisor-only and never recreates acceptance, enrollment, or seed.
"""
from __future__ import annotations
import argparse, base64, hashlib, importlib.util, json, os, sqlite3, sys, tempfile
from pathlib import Path
from urllib.parse import quote

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[7]
if str(REPO / "src") not in sys.path:
    sys.path.insert(0, str(REPO / "src"))
BASE_PACKET = HERE.parent
BASE_WORKER = BASE_PACKET / "r36-local-worker.py"
SCOPE_DEFAULT = HERE / "r40-host-scope.json"
PROFILE = "ashlar-r37-truss052-inplace-resume/0.1"
R37_SOURCE_BINDING_PATHS = frozenset({
    "pyproject.toml",
    "src/ashlar/truss_native_feed.py",
    "src/ashlar_host/configured_local_runtime.py",
    "src/ashlar_host/driver.py",
    "src/ashlar_host/local_current_carrier_profile.py",
    "src/ashlar_host/publication_planning.py",
    "src/ashlar_host/truss052_source_custody.py",
    "src/ashlar_host/truss_ack_journal.py",
    "src/ashlar_host/truss_backend.py",
    "src/ashlar_host/truss_catalog_reader.py",
    "src/ashlar_host/truss_continue.py",
    "src/ashlar_host/truss_continue_cli.py",
    "src/ashlar_host/truss_current_catalog.py",
    "src/ashlar_host/truss_feed_sessions.py",
    "src/ashlar_host/truss_graph_admission.py",
    "src/ashlar_host/truss_note_publication.py",
    "src/ashlar_host/truss_source_custody_selection.py",
})

class Refusal(RuntimeError): pass

def need(value, message):
    if not value: raise Refusal(message)

def sha(raw): return hashlib.sha256(raw).hexdigest()
def read_json(path):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out: raise Refusal(f"duplicate JSON member: {k}")
            out[k]=v
        return out
    return json.loads(Path(path).read_bytes(), object_pairs_hook=pairs)
def write_json(path, value):
    raw=(json.dumps(value,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False)+"\n").encode()
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,"wb") as stream:
        stream.write(raw); stream.flush(); os.fsync(stream.fileno())
    dfd=os.open(path.parent,os.O_RDONLY)
    try: os.fsync(dfd)
    finally: os.close(dfd)
    return sha(raw)

def read_json_from_bytes(raw):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out: raise Refusal(f"duplicate JSON member: {k}")
            out[k]=v
        return out
    return json.loads(raw,object_pairs_hook=pairs)

def verify_retained_r39_page_evidence(scope):
    evidence=scope.get("retained_r39_page_evidence")
    need(type(evidence) is dict and evidence.get("page_sha256")=="0248fd1607b971f267df71b1262668930a96925a767855dd3400fb0ecd1a6456",
         "exact retained R39 page evidence required")
    page_path=HERE/evidence["page_relative_path"]
    receipt_path=HERE/evidence["receipt_relative_path"]
    need(page_path.resolve(strict=True)==page_path and receipt_path.resolve(strict=True)==receipt_path
         and page_path.is_file() and receipt_path.is_file()
         and sha(page_path.read_bytes())==evidence["page_sha256"]
         and sha(receipt_path.read_bytes())==evidence["receipt_sha256"],
         "retained R39 page/receipt bytes differ")
    receipt=read_json(receipt_path); raw=page_path.read_bytes(); page=read_json(page_path)
    page_proof=receipt.get("page_reread",{})
    need(page_proof.get("page_sha256")==evidence["page_sha256"]
         and page_proof.get("prior_cursor_sha256")==evidence["prior_cursor_sha256"]
         and page_proof.get("next_cursor_sha256")==evidence["next_cursor_sha256"]
         and page_proof.get("typed_change_count")==7
         and page_proof.get("ack_attempted") is False
         and receipt.get("source_state",{}).get("no_page2_publication_or_ack") is True,
         "retained R39 page/ACK reconciliation differs")
    model_path=BASE_PACKET/scope["model"]["derived_path"]
    model_raw=model_path.read_bytes(); model=read_json(model_path)
    need(sha(model_raw)==evidence["accepted_model_sha256"]
         and receipt.get("accepted_model",{}).get("sha256")==evidence["accepted_model_sha256"],
         "retained accepted model source differs")
    quantity=[element for module in model["modules"] for element in module["elements"]
              if element.get("id")=="quantity"]
    need(len(quantity)==1 and quantity[0].get("scalarType")=="integer"
         and quantity[0].get("nullability")=="absent-allowed",
         "accepted quantity declaration differs from retained null proof")
    null_events=[]
    for index,change in enumerate(page.get("changes",[])):
        event_raw=base64.b64decode(change["new_value"]["bytesBase64"],validate=True)
        event=read_json_from_bytes(event_raw)
        for prop in event.get("after",{}).get("properties",[]):
            if prop.get("propertyId")=="3" and prop.get("value")=={"kind":"null"}:
                null_events.append((index,event,prop,sha(event_raw)))
    need(len(page.get("changes",[]))==7 and len(null_events)==1,
         "retained R39 seven-change page or explicit-null event differs")
    index,event,prop,event_digest=null_events[0]
    proof=receipt["null_event"][0]
    need(index==1 and event.get("after",{}).get("identity",{}).get("id")=="2"
         and prop.get("definitionPin")==proof.get("definition_pin")
         and event_digest==proof.get("event_sha256"),
         "retained R39 null event/property proof differs")
def load_base():
    spec=importlib.util.spec_from_file_location("ashlar_r36_helpers",BASE_WORKER)
    mod=importlib.util.module_from_spec(spec); spec.loader.exec_module(mod)
    return mod

def tree_digest(root, *, private=False, max_bytes=512*1024*1024):
    root=Path(root)
    need(root.is_absolute() and root.is_dir() and not root.is_symlink() and root.resolve()==root,
         f"canonical directory required: {root}")
    st=root.stat(); need(st.st_uid==os.getuid(),f"directory owner mismatch: {root}")
    if private: need(st.st_mode & 0o077 == 0,f"private directory required: {root}")
    h=hashlib.sha256(); count=0; total=0
    entries=sorted(root.rglob("*"),key=lambda p:p.relative_to(root).as_posix().encode())
    need(len(entries)<=200000,"tree entry bound exceeded")
    for p in entries:
        info=p.lstat(); need(info.st_uid==os.getuid() and not p.is_symlink(),"foreign/symlink tree entry")
        rel=p.relative_to(root).as_posix().encode()
        if p.is_dir(): kind=b"D"
        elif p.is_file():
            kind=b"F"; count+=1; total+=info.st_size
            need(total<=max_bytes,"tree byte bound exceeded")
        else: raise Refusal("special tree entry")
        h.update(kind+len(rel).to_bytes(4,"big")+rel+(info.st_mode&0o777).to_bytes(4,"big")+info.st_size.to_bytes(8,"big"))
        if kind==b"F":
            with p.open("rb") as f:
                while chunk:=f.read(1024*1024): h.update(chunk)
    return {"sha256":h.hexdigest(),"files":count,"bytes":total}

def verify_inputs(scope, *, require_absent_output=True, supervisor_root_created=None):
    base=load_base()
    verify_retained_r39_page_evidence(scope)
    source_bindings=scope.get("ashlar_source_bindings")
    need(type(source_bindings) is dict and set(source_bindings)==R37_SOURCE_BINDING_PATHS,
         "R37 closed Ashlar source-binding map differs")
    for relative, expected_digest in source_bindings.items():
        rel=Path(relative)
        selected=REPO/rel
        need(not rel.is_absolute() and ".." not in rel.parts and selected.resolve(strict=True)==selected
             and selected.is_file() and sha(selected.read_bytes())==expected_digest,
             f"R37 canonical Ashlar source binding differs: {relative}")
    runtime=scope["runtime_candidate"]
    overlay=Path(runtime["new_overlay_or_venv"])/"site-packages"
    extras=Path(runtime["local_extra_overlay"])
    if overlay.is_dir(): sys.path[:0]=[str(overlay),str(extras)]
    import inspect, importlib.metadata
    import psycopg
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    import truss
    from truss import CatalogReferenceAssembly, LocalPostgres
    from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
    from ashlar_host.truss_note_publication import publish_graph_pages, read_page
    from ashlar_host.truss052_source_custody import Truss052SourceCustodySession
    from ashlar_host.truss_source_custody_selection import (
        select_truss_local_dev_source_custody_anchor,
        read_local_native_delta_publication_observation)
    need(importlib.metadata.version("umf-truss")=="0.0.52","installed Truss distribution version mismatch")
    distribution=importlib.metadata.distribution("umf-truss")
    direct=json.loads(distribution.read_text("direct_url.json") or "{}")
    need(direct.get("url")==Path(runtime["wheel_path"]).resolve().as_uri() and
         direct.get("archive_info",{}).get("hashes",{}).get("sha256")==runtime["wheel_sha256"],
         "installed Truss .52 wheel origin/hash mismatch")
    need({name:importlib.metadata.version(name) for name in ("pgserver-pg18","fasteners","platformdirs","psutil")}==
         {"pgserver-pg18":"18.6.1","fasteners":"0.20","platformdirs":"4.12.4","psutil":"7.2.2"},
         "Truss .52 local-extra pin mismatch")
    need(set(inspect.signature(run_continue_truss_graph_command).parameters) >=
         {"configuration","output","maximum_pages","defer_source_acknowledgement","prepublication_source_custody"},
         "resume API does not expose one-page deferred-ACK source selection")
    need({"maximum_pages","defer_source_acknowledgement","prepublication_source_custody"} <= set(inspect.signature(publish_graph_pages).parameters),
         "publisher API does not expose one-page deferred-ACK source selection")
    need("data_directory" in inspect.signature(LocalPostgres).parameters and
         callable(read_page) and callable(Truss052SourceCustodySession) and
         callable(select_truss_local_dev_source_custody_anchor) and
         callable(read_local_native_delta_publication_observation),
         "required public/native resume APIs are unavailable")
    base.require_fresh_spark_session(SparkSession,SparkContext)
    paths=scope["paths"]
    pg=Path(paths["data_directory"]); delta=Path(paths["publication_directory"]); out=Path(paths["output_root"]); sup=Path(paths["supervisor_root"])
    need(scope.get("profile")==PROFILE and scope.get("authority_class")=="local_development_authority" and scope.get("cloud") is False and scope.get("production_authority") is False,"local R37 authority boundary required")
    need(scope.get("limits")=={"wall_seconds":900,"process_rss_bytes":2147483648,"attempt_root_bytes":536870912,"combined_log_bytes":16777216,"attempts":1,"automatic_retries":0,"network_listen":False,"cloud":False,"cleanup":False,"parallelism":"sequential local only","retain_source_and_all_receipts":True},"R37 local resource limits differ")
    need(scope["publication"].get("page_sequence")=={"profile":"ashlar-r37-inplace-two-page-resume/0.1","page_bound":2,"protected_ack_bound":2,"expected_order":["exact-seven-change-seed-page","exact-two-change-continuation-page"],"expected_change_counts":[7,2],"seed_group_replay":False,"genesis_replay":False,"genesis_ack_replay":False,"unknown_page_refused_before_publication":True,"ack_each_page_after_independent_native_readback":True,"continuation_ack":"matched_through_same_session_Truss_0.52_source_custody"},"R37 page/ACK sequence scope differs")
    need(paths.get("must_be_absent_before_native_run") is True and set(paths)=={"data_directory","publication_directory","output_root","supervisor_root","must_be_absent_before_native_run"},"R37 path schema differs")
    for p in (pg,delta): need(p.is_absolute() and p.resolve(strict=True)==p and not p.is_symlink(),"existing bound source/destination root required")
    need(pg==Path("/private/tmp/ashlar-r36-truss052-page-sequence-20261101/pgdata"),"only exact retained R36 PGDATA is eligible")
    need(delta==Path("/private/tmp/ashlar-r36-truss052-page-sequence-20261101/delta"),"only exact retained R36 Delta destination is eligible")
    need(not (pg/"postmaster.pid").exists(),"R36 PostgreSQL must be stopped before resume")
    need(out.is_absolute() and out.resolve(strict=False)==out and sup.is_absolute() and sup.resolve(strict=False)==sup,"canonical fresh R37 roots required")
    if require_absent_output:
        need(supervisor_root_created is None and not out.exists() and not sup.exists(),
             "R37 preflight output/supervisor roots must be absent")
    else:
        need(not out.exists(),"R37 output root must remain absent before worker effects")
        need(supervisor_root_created is not None and Path(supervisor_root_created)==sup
             and sup.is_dir() and not sup.is_symlink() and sup.resolve()==sup,
             "R37 native worker requires the exact supervisor-created root")
        root_stat=sup.lstat()
        need(root_stat.st_uid==os.getuid() and root_stat.st_mode&0o077==0,
             "R37 supervisor root owner/private mode mismatch")
        logs=tuple(sup.iterdir())
        need({p.name for p in logs}=={"child.stdout.log","child.stderr.log"},
             "R37 exact supervisor logs required")
        for log in logs:
            st=log.lstat()
            need(log.is_file() and not log.is_symlink() and st.st_uid==os.getuid()
                 and st.st_mode&0o077==0,
                 "R37 supervisor child log owner/private mode mismatch")
    b=scope["retained_r36"]
    pgt=base.private_tree_digest(pg)
    need(pgt==b["pgdata_tree"],"retained R36 PGDATA digest/count/size changed")
    dgt=tree_digest(delta,max_bytes=128*1024*1024)
    need(dgt==b["delta_tree"],"retained R36 Delta directory changed")
    receipts=Path(b["attempt_root"])/"receipts"
    for name,digest in b["receipt_sha256"].items():
        raw=(receipts/name).read_bytes(); need(sha(raw)==digest,"retained R36 receipt hash mismatch: "+name)
    ack=read_json(receipts/"genesis-ack-reconciliation.json")
    native=read_json(receipts/"genesis-native-readback.json")
    fail=read_json(receipts/"failure.json")
    need(ack["outcome"]=="matched" and ack["authority"]=="local-development" and ack["publication_id"]==native["publication_id"],"R36 genesis ACK/native readback mismatch")
    role_names={"edge_current","object_current","tombstone","whole_source_history"}
    roles=native.get("roles")
    need(type(roles) is dict and set(roles)==role_names and
         all(roles[r].get("version")==1 and roles[r].get("head_version")==1 and roles[r].get("row_count")==0 and
             roles[r].get("active_files")==0 and roles[r].get("physical_parquet_files")==0 for r in role_names),
         "R36 four-role empty v1 native baseline no longer matches")
    deferred=read_json(delta/"deferred-ack-cbf69ea847e30930ec586b2a3d508745f0430da3e9e1628f6b81149b9d58230b.json")
    need(native.get("receipt_sha256")==ack.get("receipt_sha256") and
         sha(base64.b64decode(deferred["destination_receipt_bytes_base64"],validate=True))==native.get("receipt_sha256"),
         "R36 source ACK receipt does not match retained native destination receipt")
    need(ack["checkpoint_sha256"]==b["ack_checkpoint_sha256"] and ack["page_sha256"]==native.get("page_sha256",ack["page_sha256"]),"R36 ACK checkpoint binding mismatch")
    need(fail["failure"]=="Original durable Truss ACK attempt required" and fail["source_ack_attempted"] is True,"R36 failure is not the reviewed pre-page-two state")
    need(not any((receipts/name).exists() for name in ("seed-page-native-readback.json","seed-page-ack-reconciliation.json","page-2-native-readback.json","page-2-ack-reconciliation.json")),"R36 already contains page-two effects; no replay path")
    config=Path(b["continuation_config_path"]); raw=config.read_bytes(); need(sha(raw)==b["continuation_config_sha256"],"retained continuation config changed")
    cfg=read_json(config); need(cfg.get("publication_directory")==str(delta) and cfg["publication_intent"]["sha256"]==b["genesis_intent_sha256"],"retained config does not select exact R36 intent/destination")
    intent_path=Path(cfg["publication_intent"]["path"]); intent_raw=intent_path.read_bytes(); need(sha(intent_raw)==b["genesis_intent_sha256"],"R36 publication intent digest mismatch")
    side=read_json(delta/"deferred-ack-cbf69ea847e30930ec586b2a3d508745f0430da3e9e1628f6b81149b9d58230b.json")
    need(side.get("publication_id")==ack["publication_id"] and side.get("acknowledgement")=="deferred_not_attempted","R36 deferred intent identity/state mismatch")
    from ashlar_host.truss_resume_cli import parse_resume_truss_configuration
    from ashlar_host.private_graph_custody import decode_custody_bytes
    from ashlar_host.delta_custody import DeltaTarget,LocalDeltaTransport
    with tempfile.TemporaryDirectory(prefix="ashlar-r37-parser-",dir="/private/tmp") as t:
        t=Path(t); config_copy=t/"config.json"; parsed_raw=dict(cfg); parsed_raw.pop("feed",None)
        config_copy.write_text(json.dumps(parsed_raw,sort_keys=True,separators=(",",":")))
        parsed,pub,page_raw,*_,out_path,_=parse_resume_truss_configuration(config_copy,t/"resume-out")
        journal=pub/"operations.sqlite"; need(journal==delta/"operations.sqlite" and journal.is_file(),"R36 journal path mismatch")
        need(sha(journal.read_bytes())==b["journal_sha256"],"R36 journal changed")
        with sqlite3.connect("file:"+quote(str(journal))+"?mode=ro",uri=True) as db:
            orig=db.execute("SELECT CAST(original AS BLOB) FROM local_installation WHERE id=1").fetchone()
            ops=db.execute("SELECT count(*) FROM local_operation WHERE state='committed'").fetchone()[0]
            plans=db.execute("SELECT operation FROM local_plan ORDER BY operation").fetchall()
            publication_count=db.execute("SELECT count(*) FROM local_publication_artifact").fetchone()[0]
            source_plan_count=db.execute("SELECT count(*) FROM local_source_plan").fetchone()[0]
        need(orig is not None and len(orig[0])<=65536 and ops==6,"R36 original six-role journal admission failed")
        reg=decode_custody_bytes(orig[0]); targets=tuple(DeltaTarget(i["table"],Path(i["path"]),i["uuid"]) for i in reg["targets"])
        LocalDeltaTransport.require_existing_journal(journal,reg["installation"],targets,capacity=reg.get("operation_capacity"),installation_profile=reg.get("installation_profile"))
        expected_effect_plan="effects:"+read_json(receipts/"genesis-ack-reconciliation.json")["publication_id"].removeprefix("truss-graph-")
        need(len(targets)==6 and parsed["destination_id"]==scope["publication"]["destination_id"] and not out_path.exists(),"R36 destination/parser admission mismatch")
        need(plans==[(expected_effect_plan,)] and not any(row[0].startswith("truss-ack:") for row in plans),"R36 external-ACK plan state differs; do not replay or synthesize local ACK intent")
        need(publication_count==1 and source_plan_count==1,"R36 journal has unexpected publication/source-plan rows")
    helper_path=Path(b["base_worker_path"]); need(helper_path.is_file() and sha(helper_path.read_bytes())==b["base_worker_sha256"],"hash-bound R36 helper worker mismatch")
    need(Path(scope["worker"]["path"]).resolve()==Path(__file__).resolve()
         and sha(Path(__file__).read_bytes())==scope["worker"]["sha256"],"R37 worker path/hash mismatch")
    base.validate_resume_evidence(scope)
    for relative,expected in ((scope["model"]["derived_path"],scope["model"]["derived_sha256"]),
                              (scope["seed"]["path"],scope["seed"]["sha256"]),
                              (scope["continuation"]["path"],scope["continuation"]["sha256"])):
        input_path=BASE_PACKET/relative
        need(input_path.is_file() and sha(input_path.read_bytes())==expected,"frozen model/seed/continuation input hash mismatch")
    return {"pgdata_tree":pgt,"delta_tree":dgt,"journal_sha256":sha(journal.read_bytes()),"journal_operation_rows":ops,"target_count":len(targets),"parser":"passed_actual_retained_intent","genesis_ack":"matched","genesis_publication_id":ack["publication_id"],"ack_checkpoint_sha256":ack["checkpoint_sha256"],"next_page_artifact_absent":True,"output_created":False,"postgres_opened":False,"spark_started":False,"delta_written":False,"ack_attempted":False,"cloud":False}

def parse_page(base, page, feed):
    from ashlar.truss_native_feed import typed_scalar_entity_changes,native_feed_batch
    from ashlar.source import SourceBatch
    parsed=typed_scalar_entity_changes(page.content,sha256=page.sha256,prior=page.prior.content,next=page.next.content,feed=feed)
    batch=native_feed_batch(page.content,sha256=page.sha256,prior=page.prior.content,next=page.next.content,feed=feed)
    need(type(batch) is SourceBatch and batch.begin==page.content and batch.commit==b"" and batch.records==() and batch.feed==feed and batch.batch_id==page.writer_xid,"public SourceBatch correspondence failed")
    need(batch.epoch==json.loads(page.prior.content)["sourceEpoch"] and batch.batch_id==page.writer_xid and base64.b64decode(batch.cursor_before,validate=True)==page.prior.content and base64.b64decode(batch.cursor_after,validate=True)==page.next.content and parsed.original==page.content and parsed.prior==page.prior.content and parsed.next==page.next.content,"public SourceBatch/typed parser cursor/epoch/writer mismatch")
    shape=base.page_shape(page.content,len(parsed.changes))
    return parsed,shape

def native(scope, supervisor_root):
    base=load_base(); pre=verify_inputs(scope,require_absent_output=False,
                                        supervisor_root_created=supervisor_root)
    import psycopg
    from truss import CatalogRuntimeConfiguration,CatalogRuntimeStatus,adopt_psycopg_host,create_reference_assembly
    from truss.catalog_results import CatalogRegistration
    from truss.feed import FeedConsumer,FeedPage
    from truss.reader_catalog import AcceptedDocument
    from ashlar_host.truss052_source_custody import Truss052SourceCustodySession
    from ashlar_host.truss_note_publication import publish_graph_pages,read_page
    from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
    from ashlar_host.truss_source_custody_selection import select_truss_local_dev_source_custody_anchor,read_local_native_delta_publication_observation
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    paths=scope["paths"]; root=Path(paths["output_root"]); receipts=root/"receipts"; root.mkdir(mode=0o700); os.chmod(root,0o700); receipts.mkdir(mode=0o700)
    effects={"database_effects_may_remain":False,"delta_effects_may_remain":False,"source_ack_attempted":False,"cloud":False}
    r36=scope["retained_r36"]; r36root=Path(r36["attempt_root"]); kept=read_json(r36root/"receipts"/"resume-source-reconciliation.json")
    r24=scope["resume"]; evidence=base.validate_resume_evidence(scope)["evidence"]
    installation=evidence["installation.json"]; accept=evidence["acceptance.json"]; consumer_reg=evidence["consumer-registration.json"]; group=evidence["genesis-group-commit.json"]
    model_raw=(BASE_PACKET/"r28-model.umf.json").read_bytes() if (BASE_PACKET/"r28-model.umf.json").exists() else (HERE.parent/"r28-model.umf.json").read_bytes()
    seed=read_json(HERE.parent/"r28-seed-request.json"); continuation=read_json(HERE.parent/"r28-continuation-request.json")
    runtime=__import__("truss").LocalPostgres(Path(paths["data_directory"]))
    spark=None; primary=None
    try:
        effects["database_effects_may_remain"]=True; runtime.__enter__(); info=runtime.info
        need(info.server_version=="18.6" and Path(info.data_directory)==Path(paths["data_directory"]),"R36 in-place PostgreSQL identity mismatch")
        params=psycopg.conninfo.conninfo_to_dict(info.connection_uri); names=scope["source"]
        def connect(role,autocommit=False): return psycopg.connect(**dict(params,user=role),autocommit=autocommit,connect_timeout=5,options="-c statement_timeout=10000")
        def txop(connection,fn,read_only=True): return base._transaction(connection,fn,read_only=read_only,timeout_ms=10000)
        def status_provider():
            with connect(names["registrar_login_role"],True) as c:
                s=txop(c,lambda a,t:base._value(a.status_in_transaction(t),"registrar status")); c.rollback()
            need(type(s) is CatalogRuntimeStatus and s.installed and s.installation_id==installation["installation_id"] and s.release_id==scope["runtime_candidate"]["release_id"],"R36 public registrar status changed")
            return s
        with connect(names["registrar_login_role"],True) as c:
            status,report=txop(c,lambda a,t:(base._value(a.status_in_transaction(t),"catalog status"),base._value(a.report_in_transaction(t,accept["revision"]),"catalog report"))); c.rollback()
        need(type(status) is CatalogRuntimeStatus and status.installed and status.installation_id==installation["installation_id"],"R36 public Truss installation mismatch")
        need(sha(report.original_bytes)==accept["report_sha256"],"R36 public catalog report changed")
        base.validate_retained_report(report.original_bytes,registration_id=accept["registration_id"],revision=accept["revision"])
        with connect(names["ordinary_reader_role"],True) as c:
            doc=txop(c,lambda a,t:base._value(a.accepted_document_in_transaction(t,document_id=scope["model"]["document_id"],maximum_bytes=1048576),"accepted document")); c.rollback()
        need(type(doc) is AcceptedDocument and doc.document_id==scope["model"]["document_id"] and sha(doc.content)==sha(model_raw),"R36 accepted model readback changed")
        registration=CatalogRegistration(revision=accept["revision"],registration_id=accept["registration_id"],report_bytes=report.original_bytes,replayed=False)
        reader_factory=base.ordinary_reader_connection_factory(psycopg,params,names["ordinary_reader_role"])
        page=read_page(reader_factory,names["consumer_id"],maximum_bytes=scope["publication"]["maximum_page_bytes"])
        from truss.feed import FeedConsumer
        need(type(page) is FeedPage and page.outcome=="transaction" and page.consumer_id==names["consumer_id"] and page.registration_id==consumer_reg["registration_id"],"R36 next feed page identity mismatch")
        retained_page=scope["retained_r39_page_evidence"]
        need(page.sha256==retained_page["page_sha256"]
             and sha(page.prior.content)==retained_page["prior_cursor_sha256"]
             and sha(page.next.content)==retained_page["next_cursor_sha256"],
             "public next page differs from the retained R39 exact page proof")
        need(sha(page.prior.content)==r36["ack_checkpoint_sha256"],"R36 next feed page does not start at matched genesis ACK checkpoint")
        parsed,shape=parse_page(base,page,names["feed_selection"])
        need(shape["kind"]=="entity" and len(parsed.changes)==7 and shape["typed_entity_changes"]==7 and group["result_count"]==7,"R36 next page is not exact seven-change seed")
        consumer=FeedConsumer(consumer_id=page.consumer_id,registration_id=page.registration_id,checkpoint=page.prior)
        write_json(receipts/"resume-public-page-selection.json",{"profile":"ashlar-r37-public-seed-page-selection/0.1","page_sha256":page.sha256,"prior_sha256":sha(page.prior.content),"next_sha256":sha(page.next.content),"typed_changes":len(parsed.changes),"page_kind":shape["kind"],"group_commit_result_count":group["result_count"],"genesis_ack_checkpoint_sha256":r36["ack_checkpoint_sha256"],"no_publication_yet":True})
        from ashlar_host.configured_local_runtime_profile import attest
        rt=scope["spark_runtime"]; java_home=Path(rt["java_home"]); jars=[Path(i["path"]) for i in rt["jars"]]
        for i,p in zip(rt["jars"],jars): need(p.is_file() and sha(p.read_bytes())==i["sha256"],"R37 pinned Spark jar mismatch")
        spark_env, network = base.spark_loopback_settings(scope)
        os.environ["JAVA_HOME"] = str(java_home)
        os.environ.update(spark_env)
        os.environ["PYSPARK_PYTHON"] = scope["runtime_candidate"]["local_runtime_base_python"]
        os.environ["PYSPARK_DRIVER_PYTHON"] = scope["runtime_candidate"]["local_runtime_base_python"]
        runtime_identity=type("R37Runtime",(),{"java_home":java_home,"jars":jars[0].parent,"jar_inventory":{i["name"]:i["sha256"] for i in rt["jars"]}})()
        _,spark_identity,verified=attest(runtime_identity)
        need(tuple(verified)==tuple(jars) and spark_identity["jdk"]=="21","R37 Spark runtime attestation mismatch")
        from ashlar_host.truss_source_custody_selection import select_truss_local_dev_source_custody_anchor,read_local_native_delta_publication_observation
        from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
        state={}
        def select_page(expected_count, group_input, label):
            def callback(**kw):
                p=kw["page"]
                need(p.sha256==page.sha256 and p.prior.content==page.prior.content and p.next.content==page.next.content if label=="seed" else True,
                     f"R37 {label} page changed between public read and publication")
                need(p.outcome=="transaction" and p.consumer_id==names["consumer_id"] and
                     p.registration_id==consumer_reg["registration_id"],f"R37 {label} consumer/registration identity mismatch")
                if label=="continuation":
                    need(p.prior.content==state["seed"].get("ack_checkpoint"),"R37 continuation page does not start at the matched seed ACK checkpoint")
                typed,sh=parse_page(base,p,names["feed_selection"])
                need(sh["kind"]=="entity" and len(typed.changes)==expected_count,f"R37 {label} page typed change count mismatch")
                if label=="seed": need(sh["payload_counts"]["changes"]==7 and expected_count==7,"R37 seed payload count mismatch")
                match=base.validate_group_page_against_frozen_request(typed,kw["admission"],group_input,
                    document_id=scope["model"]["document_id"],revision=accept["revision"],expected_count=expected_count)
                req=kw["request"]; req_raw=json.dumps(req,sort_keys=True,separators=(",",":"),ensure_ascii=False,allow_nan=False).encode()
                page_consumer=FeedConsumer(consumer_id=p.consumer_id,registration_id=p.registration_id,checkpoint=p.prior)
                anchor=select_truss_local_dev_source_custody_anchor(registration=registration,accepted_document=doc,
                    consumer=page_consumer,runtime_status=status,page=p,feed=scope["publication"]["feed"],
                    source_holder_id=names["source_holder_id"],document_id=scope["model"]["document_id"],
                    destination_id=scope["publication"]["destination_id"],ack_registration_id=names["ack_registration_id"],
                    ack_scope_id=names["ack_scope_id"],publication_id=kw["publication_id"],request_bytes=req_raw,
                    predecessor_id=req["predecessor"],destination_transport=kw["destination_transport"])
                state[label]={"anchor":anchor,"page":p,"request_bytes":req_raw,"group_match":match}
                write_json(receipts/f"{label}-page-selection.json",{"profile":"ashlar-r37-page-selection/0.1","page_kind":sh["kind"],"typed_changes":expected_count,"page_sha256":p.sha256,"prior_sha256":sha(p.prior.content),"next_sha256":sha(p.next.content),"request_sha256":sha(req_raw),"publication_id":kw["publication_id"],"group_correspondence":match,"acknowledgement_attempted":False})
                def sink(**post):
                    obs=read_local_native_delta_publication_observation(anchor=anchor,spark=post["destination_transport"].spark,
                        destination_transport=post["destination_transport"],manifest_bytes=post["manifest_bytes"],vector=post["vector"],
                        receipt_bytes=post["receipt_bytes"],expected_post_state=post["expected_post_state"])
                    selection=anchor.bind_local_native_publication(manifest_bytes=post["manifest_bytes"],vector=post["vector"],
                        receipt_bytes=post["receipt_bytes"],expected_post_state=post["expected_post_state"],observation=obs)
                    state[label].update({"selection":selection,"manifest":post["manifest_bytes"],"receipt":post["receipt_bytes"],"vector":post["vector"],"observation":obs})
                    write_json(receipts/f"{label}-native-readback.json",{"publication_id":obs.publication_id,"manifest_sha256":obs.manifest_sha256,"receipt_sha256":obs.receipt_sha256,"vector":{k:list(v) for k,v in obs.vector.targets.items()},"roles":{r:{"table":o.table,"uuid":o.uuid,"version":o.version,"head_version":o.head_version,"head_json_sha256":o.head_json_sha256,"active_files":o.active_files,"physical_parquet_files":o.physical_parquet_files,"row_bag_sha256":o.row_bag_sha256,"row_count":o.row_count} for r,o in obs.roles.items()},"binding":"publisher receipt/vector plus independent exact local Delta native reread"})
                return sink
            return callback
        def ack_page(label):
            item=state[label]; context=object()
            with Truss052SourceCustodySession(connection_factory=lambda _ctx:connect(names["ordinary_reader_role"],False),selection=item["selection"],context=context,runtime_status_provider=status_provider,statement_timeout_ms=10000) as custody:
                custody.open(); effects["source_ack_attempted"]=True; custody.acknowledge(); reconciled=custody.reconcile()
                need(reconciled.outcome=="matched",f"R37 {label} ACK did not match")
                custody.close()
            item["ack_checkpoint"]=reconciled.checkpoint.content
            write_json(receipts/f"{label}-ack-reconciliation.json",{"outcome":reconciled.outcome,"publication_id":item["selection"].publication_id,"page_sha256":item["page"].sha256,"receipt_sha256":sha(item["receipt"]),"checkpoint_sha256":sha(reconciled.checkpoint.content),"same_session_open_reconcile_close":True,"authority":"local-development"})
            return reconciled
        delta_root=Path(paths["publication_directory"])
        # The genesis page is already ACKed. Resume from its exact retained intent;
        # publish_graph_pages is deliberately not used because it admits only genesis.
        retained_config=read_json(Path(scope["retained_r36"]["continuation_config_path"]))
        genesis_intent=retained_config["publication_intent"]
        resume_config={"connection_uri":psycopg.conninfo.make_conninfo(**dict(params,user=names["ordinary_reader_role"])),
            "catalog_connection_uri":psycopg.conninfo.make_conninfo(**dict(params,user=names["registrar_login_role"])),
            "publication_directory":str(delta_root),"jars":[str(p) for p in jars],"publication_intent":genesis_intent,
            "destination_id":scope["publication"]["destination_id"],"carrier_profile":scope["publication"]["carrier_profile"],
            "runtime_profile":{"profile":scope["spark_runtime"]["runtime_profile"],"java_home":str(java_home),"jar_inventory":{i["name"]:i["sha256"] for i in rt["jars"]}},"archived_acceptance_files":[]}
        seed_config_path=root/"continuation-config-seed.json"; write_json(seed_config_path,resume_config)
        from ashlar_host.truss_resume_cli import parse_resume_truss_configuration
        parse_resume_truss_configuration(seed_config_path,root/"continuation-output-seed")
        effects["delta_effects_may_remain"]=True
        published=run_continue_truss_graph_command(seed_config_path,root/"continuation-result-seed.json",maximum_pages=1,wait_seconds=0,maximum_page_bytes=scope["publication"]["maximum_page_bytes"],defer_source_acknowledgement=True,prepublication_source_custody=select_page(7,seed,"seed"))
        need(published.get("source_acknowledgement")=="deferred_not_attempted" and len(published.get("publications",()))==1 and "selection" in state.get("seed",{}),"R37 seed publish did not return one verified deferred destination")
        seed_pub=published["publications"][0]; seed_side=seed_pub["deferred_ack_intent"]; seed_side_raw=Path(seed_side["path"]).read_bytes(); need(sha(seed_side_raw)==seed_side["sha256"],"R37 seed deferred ACK intent hash mismatch")
        seed_ack=ack_page("seed")
        need(seed_ack.checkpoint.content==page.next.content,"R37 seed ACK checkpoint mismatch")
        # The committed two-operation group is applied exactly once, only after seed ACK.
        follow=base._apply_group(lambda _role:dict(params,user=names["writer_login_role"]),model=model_raw,
            document_id=scope["model"]["document_id"],document_revision=scope["model"]["document_revision"],
            request_key=continuation["request_key"],origin=continuation["origin"],operations=continuation["operations"],
            bun=Path(scope["model"]["bun_path"]),receipt_path=receipts/"continuation-group-commit.json",expected_count=2)
        need(len(follow.response.semantic.results)==2,"R37 continuation GroupSuccess count mismatch")
        predecessor=seed_pub
        ack_receipt=read_json(receipts/"seed-ack-reconciliation.json")
        intent_path=Path(predecessor["publication_intent"]["path"])
        config={"connection_uri":psycopg.conninfo.make_conninfo(**dict(params,user=names["ordinary_reader_role"])),
            "catalog_connection_uri":psycopg.conninfo.make_conninfo(**dict(params,user=names["registrar_login_role"])),
            "publication_directory":str(delta_root),"jars":[str(p) for p in jars],"publication_intent":predecessor["publication_intent"],
            "destination_id":scope["publication"]["destination_id"],"carrier_profile":scope["publication"]["carrier_profile"],
            "runtime_profile":{"profile":scope["spark_runtime"]["runtime_profile"],"java_home":str(java_home),"jar_inventory":{i["name"]:i["sha256"] for i in rt["jars"]}},"archived_acceptance_files":[]}
        config_path=root/"continuation-config-page3.json"; write_json(config_path,config)
        # Run the exact product parser before asking Truss for the next page.
        from ashlar_host.truss_resume_cli import parse_resume_truss_configuration
        parsed_config=parse_resume_truss_configuration(config_path,root/"continuation-output-page3")
        need(sha(Path(predecessor["publication_intent"]["path"]).read_bytes())==predecessor["publication_intent"]["sha256"],"R37 exact page-2 intent changed")
        continuation_result_path=root/"continuation-result-page3.json"
        continued=run_continue_truss_graph_command(config_path,continuation_result_path,maximum_pages=1,wait_seconds=0,maximum_page_bytes=scope["publication"]["maximum_page_bytes"],defer_source_acknowledgement=True,prepublication_source_custody=select_page(2,continuation,"continuation"))
        need(continued.get("source_acknowledgement")=="deferred_not_attempted" and len(continued.get("publications",()))==1 and "selection" in state.get("continuation",{}),"R37 two-operation continuation did not return one readback-bound deferred destination")
        continuation_pub=continued["publications"][0]; cont_side=continuation_pub["deferred_ack_intent"]; need(sha(Path(cont_side["path"]).read_bytes())==cont_side["sha256"],"R37 continuation deferred intent is not durable")
        cont_ack=ack_page("continuation")
        need(cont_ack.checkpoint.content==state["continuation"]["page"].next.content,"R37 continuation ACK checkpoint mismatch")
        write_json(receipts/"completed.json",{"profile":PROFILE,"status":"completed_local_development_only","resumed_existing_R36_PGDATA":True,"reused_existing_R36_Delta_journal":True,"genesis_replayed":False,"genesis_ack_replayed":False,"seed_page_changes":7,"seed_ack":"matched","continuation_operations":2,"continuation_page_changes":2,"continuation_ack":"matched","publication_ids":[state["seed"]["selection"].publication_id,state["continuation"]["selection"].publication_id],"cloud":False,"production_authority":False})
    except BaseException as e:
        primary=e
        try: write_json(receipts/"failure.json",{"profile":PROFILE,"status":"failed_or_interrupted","failure_type":type(e).__name__,"failure":str(e)[:1200],**effects,"production_authority":False})
        except BaseException: pass
        raise
    finally:
        cleanup_error=None
        if spark is not None:
            try: spark.stop()
            except BaseException as e: cleanup_error=e
        try: runtime.__exit__(None,None,None)
        except BaseException as e:
            if cleanup_error is None: cleanup_error=e
        if cleanup_error is not None and primary is None:
            raise Refusal("R37 Spark/PostgreSQL cleanup failed") from cleanup_error

def main():
    p=argparse.ArgumentParser(); p.add_argument("--scope",type=Path,default=SCOPE_DEFAULT); p.add_argument("--attempt-root",type=Path); p.add_argument("--supervisor-root",type=Path); p.add_argument("--preflight-only",action="store_true"); a=p.parse_args()
    raw=a.scope.read_bytes(); scope=read_json(a.scope)
    need(os.environ.get("ASHLAR_R36_SUPERVISED")=="1" or a.preflight_only,"R37 native worker must be supervised")
    need(a.attempt_root is None or str(a.attempt_root)==scope["paths"]["output_root"],"R37 output root mismatch")
    if a.supervisor_root is not None:
        sr=Path(a.supervisor_root); need(str(sr)==scope["paths"]["supervisor_root"] and sr.is_dir() and not sr.is_symlink(),"R37 supervisor-created root mismatch")
        need({p.name for p in sr.iterdir()}=={"child.stdout.log","child.stderr.log"},"R37 exact supervisor logs required")
        for lp in sr.iterdir():
            st=lp.lstat(); need(lp.is_file() and not lp.is_symlink() and st.st_uid==os.getuid() and st.st_mode&0o077==0,"R37 owner-only supervisor log required")
    receipt=verify_inputs(scope) if a.preflight_only else verify_inputs(
        scope,require_absent_output=False,supervisor_root_created=a.supervisor_root)
    if a.preflight_only:
        print(json.dumps({"status":"passed_effect_free_inplace_preflight","scope_sha256":sha(raw),"worker_sha256":sha(Path(__file__).read_bytes()),"preflight":receipt},sort_keys=True,separators=(",",":"))); return 0
    need(a.supervisor_root is not None and os.environ.get("ASHLAR_R36_SCOPE_SHA256")==sha(raw) and scope.get("status")=="sol_reviewed_and_dispatchable","R37 native execution requires reviewed supervisor scope handshake")
    native(scope,a.supervisor_root); return 0
if __name__=="__main__":
    try: raise SystemExit(main())
    except Exception as error:
        print(json.dumps({"status":"refused_or_failed","error_type":type(error).__name__,"error":str(error)[:1200]},sort_keys=True),file=sys.stderr); raise
