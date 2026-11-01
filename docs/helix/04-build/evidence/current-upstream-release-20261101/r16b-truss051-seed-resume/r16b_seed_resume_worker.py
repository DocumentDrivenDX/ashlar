#!/usr/bin/env python3
"""Minimal R16b continuation from the committed R15 genesis consumer state."""
from __future__ import annotations

import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import re
import shutil
import stat
import time

HERE = Path(__file__).resolve().parent
SCOPE_PATH = HERE / "r16b-resume-scope.json"
R15_PACKET = HERE.parent / "r15-truss051-view-transaction"
R15_WORKER = R15_PACKET / "r15_truss051_local_worker.py"
R15_SCOPE_PATH = R15_PACKET / "r15-truss051-resume-scope.json"
R15_SOURCE = Path("/private/tmp/ashlar-r15-truss051-current-cohort-20261101/r13-state-continuation-clone-pgdata")
EXPECTED_PATHS = {
    "output_root": "/private/tmp/ashlar-r16b-truss051-seed-resume-20261101",
    "supervisor_root": "/private/tmp/ashlar-r16b-truss051-seed-resume-20261101-supervisor",
    "cold_backup_pgdata": "/private/tmp/ashlar-r16b-truss051-seed-resume-20261101/r15-stopped-state-backup-pgdata",
    "migration_clone_pgdata": "/private/tmp/ashlar-r16b-truss051-seed-resume-20261101/r15-seed-continuation-clone-pgdata",
    "recovery_registry": "/private/tmp/ashlar-r16b-truss051-seed-resume-20261101/unused-recovery-registry.sqlite3",
    "receipt_root": "/private/tmp/ashlar-r16b-truss051-seed-resume-20261101/receipts",
}
MAX_BYTES = 512 * 1024 * 1024


class StopRun(RuntimeError):
    pass


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def unique_pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise StopRun(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def read_json(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=unique_pairs)


def write_new(path: Path, value) -> str:
    raw = (json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n").encode()
    path.parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())
    return sha(raw)


def import_r15_worker():
    spec = importlib.util.spec_from_file_location("r15_truss051_worker_helpers", R15_WORKER)
    if spec is None or spec.loader is None:
        raise StopRun("R15 helper module unavailable")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def validate_authorization(scope, scope_raw, receipt, worker_raw, supervisor_raw, require_dispatch):
    expected = {
        "schema": "ashlar.standing-local-dispatch-binding/1",
        "authorization_basis": "standing_user_local_release_authorization",
        "user_authorization_statement": "User authorized fresh Truss source creation, a local seed, and a tested release; bounded local R16b continuation execution is within that standing authorization and requires no per-attempt user approval.",
        "scope_sha256": sha(scope_raw),
        "worker_sha256": sha(worker_raw),
        "supervisor_sha256": sha(supervisor_raw),
        "attempts": 1,
        "cloud": False,
    }
    for key, value in expected.items():
        if receipt.get(key) != value:
            raise StopRun(f"R16b standing authorization binding mismatch: {key}")
    if set(receipt) != set(expected) | {"sol_review_status", "dispatch_enabled"}:
        raise StopRun("R16b standing authorization receipt fields changed")
    if require_dispatch:
        if receipt["sol_review_status"] != "sol_reviewed" or receipt["dispatch_enabled"] is not True:
            raise StopRun("R16b native dispatch remains pending Sol review")
    elif receipt["sol_review_status"] not in ("pending_sol_review", "sol_reviewed") or type(receipt["dispatch_enabled"]) is not bool:
        raise StopRun("R16b preflight authorization state malformed")


def result_receipt(outcome):
    from truss import Ok
    if not isinstance(outcome, Ok):
        return {"outer_type": type(outcome).__name__, "outer_repr": repr(outcome)[:3000]}
    value = outcome.value
    record = {"outer_type": "Ok", "variant": type(value).__name__}
    if type(value).__name__ == "GroupFailed":
        failure = value.failure
        record.update(failure_type=type(failure).__name__, failure_code=getattr(failure, "code", None),
                      operation_code=getattr(failure, "operation_code", None),
                      operation_index=getattr(failure, "operation_index", None))
        diagnostic = getattr(failure, "diagnostic", None) or getattr(failure, "inner_error", None)
        if diagnostic is not None:
            record["diagnostic"] = {"identity": diagnostic.identity, "sha256": diagnostic.sha256,
                                    "bytes_base64": diagnostic.bytes_base64}
            try:
                import base64
                record["diagnostic"]["json"] = json.loads(base64.b64decode(diagnostic.bytes_base64))
            except (ValueError, TypeError):
                pass
    elif type(value).__name__ == "GroupUnavailable":
        record["reason"] = value.reason
    elif type(value).__name__ == "GroupSuccess":
        response = value.response
        semantic = response.semantic
        record.update(response_type=type(response).__name__, durability=getattr(response, "durability", None),
                      disposition=getattr(response, "disposition", None),
                      catalog_revision=semantic.executed_catalog_revision,
                      result_count=len(semantic.results),
                      results=[{"type": type(item).__name__, "operation": item.operation,
                                "outcome": item.outcome} for item in semantic.results],
                      aliases=[{"alias": item.alias, "identity": repr(item.identity)} for item in semantic.aliases])
    return record


def validate_runtime_imports_and_result_receipt_shape():
    """Resolve every native-use import and exercise each typed result variant."""
    from truss import (LocalPostgres, CatalogRuntimeConfiguration, CatalogDocument,
        adopt_psycopg_host, create_reference_assembly, Ok, RegistrarStatus,
        AcceptedDocument, PreparedGroup, FeedPage)
    from truss.contracts import (MapEntry, MapValue, NullValue, TextValue,
        ExactArtifact, ProfilePin, QualifiedOwner, TypedIdentity)
    from truss.groups import (AliasObjectReference, AppliedPending, AuthoredValue,
        CreateEdge, CreateObject, Created, CurrentCatalog, GroupFailed,
        GroupSemanticInput, GroupSemanticResult, GroupSuccess, GroupUnavailable,
        InvalidGroupInput, JournalEventReference, RootlessOwnership, TextOrderKey)

    pin = ProfilePin(identity="r16b-preflight", version="0.1", sha256="0" * 64)
    event = JournalEventReference(source_epoch="e", history_profile="h", xid="1", seq="1")
    identity = TypedIdentity(id="fixture", type_id="person", definition_pin="0" * 64,
                             owner=QualifiedOwner(document_id="fixture", module_id="m"))
    semantic = GroupSemanticResult(executed_catalog_revision="1", layout_profile=pin,
        mutation_profile=pin, results=(Created(operation="create_object", identity=identity,
            version="1", events=(event,)),), aliases=())
    success = result_receipt(Ok(GroupSuccess(response=AppliedPending(semantic=semantic))))
    if success.get("variant") != "GroupSuccess" or success.get("result_count") != 1:
        raise StopRun("Truss GroupSuccess receipt shape preflight failed")

    diagnostic_bytes = b'{"code":"invalid_input","outcome":"failed","nativeState":"22023"}'
    diagnostic_sha = sha(diagnostic_bytes)
    diagnostic = ExactArtifact(identity=diagnostic_sha, sha256=diagnostic_sha,
                               bytes_base64=base64.b64encode(diagnostic_bytes).decode("ascii"))
    failure = result_receipt(Ok(GroupFailed(failure=InvalidGroupInput(
        diagnostic_profile=pin, diagnostic=diagnostic))))
    if (failure.get("failure_type") != "InvalidGroupInput"
            or failure.get("diagnostic", {}).get("json", {}).get("nativeState") != "22023"):
        raise StopRun("Truss GroupFailed diagnostic receipt shape preflight failed")
    unavailable = result_receipt(Ok(GroupUnavailable(reason="resource")))
    if unavailable.get("reason") != "resource":
        raise StopRun("Truss GroupUnavailable receipt shape preflight failed")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--preflight", action="store_true")
    args = parser.parse_args()
    preflight = args.preflight
    scope_raw = SCOPE_PATH.read_bytes()
    scope = json.loads(scope_raw, object_pairs_hook=unique_pairs)
    worker_raw = Path(__file__).read_bytes()
    supervisor_path = HERE / "r16b_supervisor.py"
    supervisor_raw = supervisor_path.read_bytes()
    auth = read_json(HERE / scope["authorization_receipt"])
    if (scope.get("profile") != "ashlar-r16b-truss051-r15-consumer-seed-resume/0.1"
            or scope.get("status") != "standing_local_native_authorization"
            or scope.get("authorization") != "standing_user_local_release_authorization"
            or scope.get("paths") != EXPECTED_PATHS
            or scope.get("resume_source", {}).get("path") != str(R15_SOURCE)):
        raise StopRun("R16b exact reviewed scope identity/paths required")
    validate_authorization(scope, scope_raw, auth, worker_raw, supervisor_raw, not preflight)
    r15 = import_r15_worker()
    r15_scope = read_json(R15_SCOPE_PATH)
    if (scope["prior_r15"]["scope_sha256"] != sha(R15_SCOPE_PATH.read_bytes())
            or scope["prior_r15"]["worker_sha256"] != sha(R15_WORKER.read_bytes())
            or scope["prior_r15"]["supervisor_sha256"] != sha((R15_PACKET / "r15_truss051_supervisor.py").read_bytes())):
        raise StopRun("R15 source worker/supervisor lineage changed")
    r15.validate_runtime_paths(scope["runtime"])
    r15.validate_release_cohort(scope["release_cohort"], scope["runtime"])
    r15.validate_local_runtime_extras(Path(scope["runtime"]["local_extra_overlay"]))
    if scope["release_cohort"] != r15_scope["release_cohort"] or scope["runtime"] != r15_scope["runtime"]:
        raise StopRun("R16b must retain the exact R15 current runtime/cohort")
    seed_path = R15_PACKET / "fresh-local-seed-request.json"
    seed_raw = seed_path.read_bytes()
    seed = json.loads(seed_raw, object_pairs_hook=unique_pairs)
    if (sha(seed_raw) != scope["consumer_and_model"]["seed_request_sha256"]
            or len(seed.get("operations", [])) != 7
            or seed.get("request_key") != scope["continuation"]["seed_request_key"]):
        raise StopRun("R16b exact seven-operation semantic seed required")
    if scope["continuation"].get("request_identity_source") != "PreparedGroup.context.request(seed_request_key)":
        raise StopRun("R16b Truss-derived request identity contract changed")

    from truss import GroupRuntimeContext
    from truss.groups import RequestPresent
    import inspect
    if not callable(getattr(GroupRuntimeContext, "request", None)):
        raise StopRun("public PreparedGroup context request constructor unavailable")
    signature = inspect.signature(GroupRuntimeContext.request)
    if set(signature.parameters) != {"self", "request_id"}:
        raise StopRun("Truss group context request API changed")
    # Pure check: request identity must come from the Truss-provided context.
    from truss.contracts import ProfilePin
    fixture_pin = ProfilePin(identity="r16-preflight", version="0.1", sha256="0" * 64)
    fixture = GroupRuntimeContext(layout_profile=fixture_pin, mutation_profile=fixture_pin,
        value_profile=fixture_pin, request_fingerprint_profile=fixture_pin,
        authorized_scope_identity="truss-person:fixture-installation:fixture-oid",
        source_epoch="fixture", history_profile="fixture")
    request = fixture.request(seed["request_key"])
    if (not isinstance(request, RequestPresent)
            or request.identity.authorized_scope_identity != fixture.authorized_scope_identity
            or request.identity.request_id != seed["request_key"]):
        raise StopRun("Truss group context request identity pure check failed")
    validate_runtime_imports_and_result_receipt_shape()

    source = Path(scope["resume_source"]["path"])
    source_hash = scope["resume_source"]
    source_tree = r15.pg18_preflight(source)
    if source_tree != (source_hash["bytes"], source_hash["tree_sha256"], source_hash["files"]):
        raise StopRun("stopped R15 consumer source tree changed")
    custody = r15.check_process_custody(source)
    if scope["resume_source"]["reconciliation_sha256"] != sha((HERE / scope["resume_source"]["reconciliation_path"]).read_bytes()):
        raise StopRun("R15 stopped-source reconciliation digest mismatch")
    reconciliation = read_json(HERE / scope["resume_source"]["reconciliation_path"])
    diagnostic_path = HERE / "r15-group-diagnostic-result.json"
    diagnostic = read_json(diagnostic_path)
    diagnostic_repr = diagnostic.get("result", {}).get("diagnostic_repr", "")
    encoded_match = re.search(r"bytes_base64='([^']+)'", diagnostic_repr)
    diagnostic_json = (json.loads(base64.b64decode(encoded_match.group(1)))
                       if encoded_match else {})
    if (sha(diagnostic_path.read_bytes()) != reconciliation.get("diagnostic_apply_probe", {}).get("packet_copy_sha256")
            or diagnostic.get("rollback_confirmed") is not True
            or diagnostic.get("transaction_committed") is not False
            or diagnostic.get("result", {}).get("failure_type") != "InvalidGroupInput"
            or reconciliation.get("diagnostic_apply_probe", {}).get("variant") != "GroupFailed(InvalidGroupInput)"
            or reconciliation.get("diagnostic_apply_probe", {}).get("diagnostic_artifact_sha256")
               not in diagnostic_repr
            or diagnostic_json.get("nativeState") != "22023"
            or diagnostic_json.get("code") != "invalid_input"
            or diagnostic_json.get("outcome") != "failed"):
        raise StopRun("R15 disposable diagnostic artifact/forced rollback proof changed")
    if (reconciliation.get("consumer_state", {}).get("count") != 1
            or reconciliation.get("consumer_state", {}).get("checkpoint_start") != "genesis"
            or reconciliation.get("consumer_state", {}).get("checkpoint_sha256") != scope["resume_source"]["r15_consumer_checkpoint_sha256"]
            or reconciliation.get("post_r15_database_counts", {}).get("object") != 0
            or reconciliation.get("post_r15_database_counts", {}).get("edge") != 0
            or reconciliation.get("post_r15_database_counts", {}).get("journal") != 0
            or reconciliation.get("post_r15_database_counts", {}).get("runtime_object_attempt") != 0
            or reconciliation.get("post_r15_database_counts", {}).get("request_receipt") != 0
            or reconciliation.get("post_r15_database_counts", {}).get("runtime_feed_ack_receipt") != 0):
        raise StopRun("R15 consumer-genesis/empty-seed reconciliation changed")
    r15_failure_path = Path("/private/tmp/ashlar-r15-truss051-current-cohort-20261101/receipts/failure.json")
    r15_supervisor_path = Path("/private/tmp/ashlar-r15-truss051-current-cohort-20261101-supervisor/supervisor-receipt.json")
    if (sha(r15_failure_path.read_bytes()) != source_hash["r15_failure_sha256"]
            or sha(r15_supervisor_path.read_bytes()) != source_hash["r15_supervisor_sha256"]):
        raise StopRun("R15 native failure/supervisor receipts changed")
    failure = read_json(r15_failure_path)
    if (failure.get("completed_stages", [])[-1:] != ["consumer_registered_at_genesis"]
            or failure.get("failure") != "public Truss group did not confirm seven results"
            or (Path("/private/tmp/ashlar-r15-truss051-current-cohort-20261101/receipts/seed-committed.json")).exists()):
        raise StopRun("R15 seed is not a known rolled-back, no-replay continuation")

    output_root = Path(scope["paths"]["output_root"])
    clone = Path(scope["paths"]["migration_clone_pgdata"])
    receipt_root = Path(scope["paths"]["receipt_root"])
    supervisor_root = Path(scope["paths"]["supervisor_root"])
    if preflight:
        if any(path.exists() or path.is_symlink() for path in map(Path, scope["paths"].values())):
            raise StopRun("R16b effect-free preflight requires all R16b paths absent")
        required_free = source_tree[0] + scope["limits"]["attempt_root_bytes"]
        free = shutil.disk_usage(output_root.parent).free
        if free < required_free:
            raise StopRun(f"insufficient free disk before R16b clone: {free} < {required_free}")
        print(json.dumps({"status":"passed_effect_free_worker_preflight","source_tree_sha256":source_tree[1],
            "source_bytes":source_tree[0],"source_files":source_tree[2],"process_custody":custody,
            "required_free_bytes":required_free,"free_bytes":free,"output_root_created":False,
            "postgres_started":False,"database_contacted":False,"migration_replayed":False,
            "consumer_enrollment_replayed":False,"seed_applied":False,"acknowledgement":False,
            "dispatch_enabled":False,"request_context_pure_check":"passed",
            "runtime_api_imports":"passed","group_result_receipt_shapes":"passed"},sort_keys=True))
        return

    output_root.mkdir(mode=0o700); os.chmod(output_root,0o700)
    receipt_root.mkdir(mode=0o700); os.chmod(receipt_root,0o700)
    source_signature = r15.root_directory_signature(source)
    tree_before = source_tree
    shutil.copytree(source, clone, symlinks=False, copy_function=shutil.copy2)
    os.chmod(clone,0o700)
    if r15.pg18_preflight(clone) != tree_before or r15.root_directory_signature(source) != source_signature:
        raise StopRun("R15 state copy mismatch or source metadata changed")
    write_new(receipt_root / "r15-genesis-source-copied.json", {
        "source":str(source),"source_tree_sha256":tree_before[1],"bytes":tree_before[0],"files":tree_before[2],
        "consumer_enrollment_replayed":False,"migration_replayed":False,"r15_source_unchanged":True})

    import psycopg
    from truss import (LocalPostgres, CatalogRuntimeConfiguration, CatalogDocument,
        adopt_psycopg_host, create_reference_assembly, Ok, RegistrarStatus, AcceptedDocument,
        PreparedGroup, FeedPage)
    from truss.contracts import MapEntry, MapValue, NullValue, TextValue
    from truss.groups import (AliasObjectReference, AuthoredValue, CreateEdge, CreateObject,
        CurrentCatalog, GroupSemanticInput, RootlessOwnership, TextOrderKey)
    from truss.groups import GroupSuccess
    expected = scope["source"]["catalog_accepted"]
    reader = scope["consumer_and_model"]["reader_login_role"]
    registrar = scope["consumer_and_model"]["registrar_login_role"]
    completed = []
    database_effects_may_remain = False
    started = time.monotonic()
    try:
        with LocalPostgres(clone) as pg:
            if pg.info.server_version != "18.6" or Path(pg.info.data_directory) != clone:
                raise StopRun("LocalPostgres did not start exact R16b PG18.6 continuation clone")
            database_effects_may_remain = True
            write_new(receipt_root / "postgres-started.json", {"data_directory":str(clone),"server_version":pg.info.server_version,
                "truss_version":"0.0.51","r15_source_untouched":True})
            params = psycopg.conninfo.conninfo_to_dict(pg.info.connection_uri)
            admin_params = dict(params); admin_params["user"]="postgres"
            # Baseline: the committed consumer is still at genesis and no seed/group effects exist.
            with psycopg.connect(**admin_params,autocommit=True,connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'");conn.execute("SET TRANSACTION READ ONLY")
                    baseline=conn.execute("""SELECT c.consumer_id,c.registration_id::text,c.reader_oid=r.oid,
                      c.checkpoint_bytes,c.last_prior,c.last_target,c.last_payload_sha256,
                      (SELECT count(*) FROM truss.runtime_feed_consumer),
                      (SELECT count(*) FROM truss.object),(SELECT count(*) FROM truss.edge),
                      (SELECT count(*) FROM truss.journal),(SELECT count(*) FROM truss.runtime_object_attempt),
                      (SELECT count(*) FROM truss.runtime_owned_operation),(SELECT count(*) FROM truss.request_receipt),
                      (SELECT count(*) FROM truss.request_receipt_route_guard),(SELECT count(*) FROM truss.runtime_feed_ack_receipt),
                      pg_catalog.has_function_privilege(r.oid,'truss.read_feed(text,integer,integer,text,text)'::regprocedure,'EXECUTE'),
                      pg_catalog.has_function_privilege(r.oid,'truss.feed_reached(text,bytea,integer,text,text)'::regprocedure,'EXECUTE'),
                      current_user=session_user AND current_user='postgres',current_setting('role')='none',current_setting('transaction_read_only')='on'
                      FROM truss.runtime_feed_consumer c JOIN pg_catalog.pg_roles r ON r.rolname=%s WHERE c.consumer_id=%s""",
                      (reader,scope["consumer_and_model"]["consumer_id"])).fetchone()
                    if baseline is None: raise StopRun("R15 consumer row absent on copied R16b source")
                    checkpoint=baseline[3]
                    checkpoint_json=json.loads(checkpoint)
                    expected_baseline=(scope["consumer_and_model"]["consumer_id"],
                        "fdc09c3f-be99-444a-8031-a48ae4f527ec",True,None,None,None,1,0,0,0,0,0,0,0,0,True,True,True,True,True)
                    if tuple(baseline[:3])+tuple(baseline[4:]) != expected_baseline:
                        raise StopRun(f"R15 consumer/genesis/empty-seed baseline mismatch: {baseline!r}")
                    if (sha(checkpoint)!=scope["resume_source"]["r15_consumer_checkpoint_sha256"]
                            or checkpoint_json.get("nextXid")!="0"
                            or checkpoint_json.get("consumerId")!=scope["consumer_and_model"]["consumer_id"]):
                        raise StopRun("R15 consumer checkpoint is not the exact genesis checkpoint")
                    write_new(receipt_root / "consumer-genesis-and-empty-seed-readonly-proof.json",{
                        "consumer_id":baseline[0],"registration_id":baseline[1],"reader_identity_exact":baseline[2],
                        "checkpoint_sha256":sha(checkpoint),"checkpoint_bytes":len(checkpoint),"checkpoint":checkpoint_json,
                        "object_rows":baseline[8],"edge_rows":baseline[9],"journal_rows":baseline[10],
                        "runtime_object_attempt_rows":baseline[11],"runtime_owned_operation_rows":baseline[12],
                        "request_receipt_rows":baseline[13],"request_receipt_route_guard_rows":baseline[14],
                        "ack_rows":baseline[15],"feed_acl_read":baseline[16],"feed_acl_ack_related":baseline[17],
                        "transaction_read_only":True,"r15_source_untouched":True})
            completed.append("r15_consumer_genesis_and_empty_seed_state_proved")

            # Reconfirm exact accepted bytes using the public reader API before the one group call.
            read_params=dict(params);read_params["user"]=reader
            with psycopg.connect(**read_params,autocommit=True,connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'");conn.execute("SET TRANSACTION READ ONLY")
                    host=adopt_psycopg_host(conn,statement_timeout_ms=5000,operation_timeout_ms=30000);assembly=None
                    try:
                        tx=r15.require_ok(host.adopt_transaction(access_mode="read_only"),"reader source transaction")
                        assembly=r15.require_ok(create_reference_assembly(CatalogRuntimeConfiguration(),host),"public .51 assembly")
                        status=r15.require_value_type(r15.require_ok(assembly.registrar_status_in_transaction(tx),"public registrar status"),RegistrarStatus,"registrar status")
                        document=r15.require_value_type(r15.require_ok(assembly.accepted_document_in_transaction(tx,
                            document_id=expected["document_id"],maximum_bytes=1048576),"public accepted document"),AcceptedDocument,"accepted document")
                        if (status.can_register is not False or document.document_id!=expected["document_id"]
                                or document.document_revision!=expected["document_revision"]
                                or document.registration_id!=expected["registration_id"]
                                or document.catalog_revision!=expected["catalog_revision"]
                                or sha(document.content)!=expected["document_sha256"]):
                            raise StopRun("R16b public accepted-document identity mismatch")
                        model=document.content
                        write_new(receipt_root / "accepted-document-readback.json",{
                            "document_id":document.document_id,"document_revision":document.document_revision,
                            "registration_id":document.registration_id,"catalog_revision":document.catalog_revision,
                            "document_sha256":sha(document.content),"read_only":True,"public_api":"Truss .51 accepted_document_in_transaction"})
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach(): raise StopRun("Truss reader host detach refused")
            completed.append("public_51_accepted_document_reverified")

            # Single atomic group; RequestIdentity is sourced from PreparedGroup.context.
            document = CatalogDocument(expected["document_id"],expected["document_revision"],model)
            registrar_params=dict(params);registrar_params["user"]=registrar
            apply_result=None
            with psycopg.connect(**registrar_params,autocommit=True,connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'")
                    identity=conn.execute("SELECT session_user,current_user,current_setting('role')").fetchone()
                    if identity!=(registrar,registrar,"none"):
                        raise StopRun(f"unsafe R16b registrar identity: {identity!r}")
                    host=adopt_psycopg_host(conn,statement_timeout_ms=5000,operation_timeout_ms=30000);assembly=None
                    try:
                        tx=r15.require_ok(host.adopt_transaction(),"R16b group transaction")
                        assembly=r15.require_ok(create_reference_assembly(CatalogRuntimeConfiguration(),host),"public .51 assembly")
                        prepared_result=assembly.prepare_group_in_transaction(tx,
                            records=((expected["document_id"],"m","Person"),(expected["document_id"],"m","Place")),
                            relationships=((expected["document_id"],"m","knows"),(expected["document_id"],"m","located")))
                        prepared=r15.require_value_type(r15.require_ok(prepared_result,"public group preparation"),PreparedGroup,"prepared group")
                        type_pins={ref.authored_identity:ref.definition_pin for ref in prepared.types if ref is not None}
                        rel_pins={ref.authored_identity:ref.definition_pin for ref in prepared.relationships if ref is not None}
                        operations=[]
                        for item in seed["operations"]:
                            if item["op"]=="create_object":
                                values=tuple(AuthoredValue(name=name,value=(NullValue() if val["kind"]=="null" else TextValue(kind=val["kind"],text=val["text"])) ) for name,val in item["values"].items())
                                operations.append(CreateObject(type_definition_pin=type_pins[item["type"]],values=values,ownership=RootlessOwnership(),alias=item["alias"]))
                            elif item["op"]=="create_edge":
                                operations.append(CreateEdge(relationship_definition_pin=rel_pins[item["relationship"]],
                                    source=AliasObjectReference(alias=item["source"]),target=AliasObjectReference(alias=item["target"]),
                                    values=(),order_key=TextOrderKey(text=item["order_key"]),alias=item["alias"]))
                            else: raise StopRun("unsupported R16b seed operation")
                        selection=assembly.group_selection
                        asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind="string",text=v)) for k,v in sorted(seed["origin"].items())))
                        semantic=GroupSemanticInput(layout_profile=selection.layout_profile,mutation_profile=selection.capability_profile,
                            value_profile=selection.value_profile,catalog=CurrentCatalog(),operations=tuple(operations),asserted_origin=asserted)
                        request=prepared.context.request(seed["request_key"])
                        if request.identity.authorized_scope_identity!=prepared.context.authorized_scope_identity:
                            raise StopRun("Truss-derived request identity mismatch")
                        apply_result=assembly.apply_in_transaction(tx,semantic,request,documents=(document,))
                        observed=result_receipt(apply_result)
                        observed.update({"request_key":seed["request_key"],"authorized_scope_identity":request.identity.authorized_scope_identity,
                            "seed_sha256":sha(seed_raw),"operation_input_count":len(operations),"transaction_commit_confirmed":False})
                        write_new(receipt_root / "group-result-before-commit.json",observed)
                        value=r15.require_ok(apply_result,"public R16b group apply")
                        group_success=r15.require_value_type(value,GroupSuccess,"public R16b group result")
                        if (len(group_success.response.semantic.results)!=7
                                or type(group_success.response).__name__!="AppliedPending"):
                            raise StopRun("R16b public group success did not match seven pending results")
                    except StopRun:
                        raise
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach(): raise StopRun("R16b registrar host detach refused")
            database_effects_may_remain=True
            completed.append("seven_operation_group_committed")
            write_new(receipt_root / "group-commit-confirmed.json",{
                "result_type":type(group_success).__name__,"response_type":type(group_success.response).__name__,
                "result_count":len(group_success.response.semantic.results),"request_key":seed["request_key"],
                "seed_sha256":sha(seed_raw),"commit_confirmed_after_transaction_context":True})

            # First feed page only; no ACK API is called.
            reader_params=dict(params);reader_params["user"]=reader
            with psycopg.connect(**reader_params,autocommit=True,connect_timeout=5) as conn:
                with conn.transaction():
                    conn.execute("SET LOCAL statement_timeout='5000ms'");conn.execute("SET TRANSACTION READ ONLY")
                    host=adopt_psycopg_host(conn,statement_timeout_ms=5000,operation_timeout_ms=30000);assembly=None
                    try:
                        tx=r15.require_ok(host.adopt_transaction(access_mode="read_only"),"R16b first-page transaction")
                        assembly=r15.require_ok(create_reference_assembly(CatalogRuntimeConfiguration(),host),"public .51 assembly")
                        page=r15.require_value_type(r15.require_ok(assembly.read_feed_in_transaction(tx,
                            consumer_id=scope["consumer_and_model"]["consumer_id"],maximum_facts=scope["consumer_and_model"]["feed_maximum_facts"],
                            maximum_bytes=scope["consumer_and_model"]["feed_maximum_bytes"]),"public .51 first feed page"),FeedPage,"first feed page")
                        if page.outcome!="transaction":raise StopRun(f"R16b first page is not complete transaction: {page.outcome!r}")
                        write_new(receipt_root / "first-feed-page.json",{"outcome":page.outcome,"sha256":page.sha256,
                            "bytes":len(page.content),"writer_xid":page.writer_xid,"registration_id":page.registration_id,
                            "acknowledgement":False,"page_semantics":"first genesis page; do not infer seed page visibility"})
                    finally:
                        if assembly is not None: assembly.dispose()
                        if not host.detach():raise StopRun("R16b reader host detach refused")
            completed.append("first_feed_page_read_without_ack")
        write_new(receipt_root / "completed.json",{"status":"completed_local_development_only","completed_stages":completed,
            "migration_replayed":False,"consumer_enrollment_replayed":False,"acknowledgement":False,"publication":False,
            "cloud":False,"production_authority":False,"elapsed_seconds":round(time.monotonic()-started,3)})
    except BaseException as error:
        try:
            write_new(receipt_root / "failure.json",{"status":"failed_or_interrupted","completed_stages":completed,
                "effects_may_remain":database_effects_may_remain,"failure_type":type(error).__name__,"failure":str(error)[:1200],
                "migration_replayed":False,"consumer_enrollment_replayed":False,"seed_committed_stage": "seven_operation_group_committed" in completed,
                "acknowledgement":False,"publication":False,"cloud":False,"r15_source_untouched":True})
        except BaseException:
            pass
        raise


if __name__ == "__main__":
    main()
