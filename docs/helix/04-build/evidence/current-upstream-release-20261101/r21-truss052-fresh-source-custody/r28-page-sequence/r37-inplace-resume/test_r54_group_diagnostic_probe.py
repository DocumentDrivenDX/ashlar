"""Focused stub test for exact GroupFailed capture after forced transaction rollback."""
from __future__ import annotations

import base64
import hashlib
import importlib.util
import json
import sys
import tempfile
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("r54_probe", HERE / "r54-group-diagnostic-probe.py")
PROBE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(PROBE)
WORKER_SPEC = importlib.util.spec_from_file_location("r54_worker", HERE / "r54-diagnostic-probe-worker.py")
WORKER = importlib.util.module_from_spec(WORKER_SPEC)
WORKER_SPEC.loader.exec_module(WORKER)


class Ok:
    def __init__(self, value): self.value = value
class PreparedGroup: pass
class CatalogDocument:
    def __init__(self, *args): pass
class GroupFailed:
    def __init__(self, failure): self.failure = failure
class GroupOperationFailure:
    code = "group_failed"
    operation_index = "1"
    operation_code = "key_conflict"
    phase = "final_state"
    def __init__(self, artifact):
        self.error_profile = types.SimpleNamespace(identity="diagnostic", version="0.1", sha256="a" * 64)
        self.inner_error = artifact


class Value:
    def __init__(self, **values): self.__dict__.update(values)


class FeedPage:
    pass


class Connection:
    def __enter__(self): return self
    def __exit__(self, *args): return False


class Assembly:
    group_selection = types.SimpleNamespace(layout_profile="l", capability_profile="c", value_profile="v")
    def __init__(self, failure): self.failure = failure
    def prepare_group_in_transaction(self, *args, **kwargs):
        item = types.SimpleNamespace(authored_identity="Person", definition_pin="pin")
        prepared = PreparedGroup()
        prepared.types=(item,); prepared.relationships=()
        prepared.context=types.SimpleNamespace(authorized_scope_identity="scope", request=lambda key:
            types.SimpleNamespace(identity=types.SimpleNamespace(authorized_scope_identity="scope")))
        return Ok(prepared)
    def apply_in_transaction(self, *args, **kwargs): return Ok(GroupFailed(self.failure))


def main():
    prior = bytes.fromhex("506d67a8dc1f8af88beb73bcd2b899afda8e8bebc62604561b4246a934e88c94")
    empty = FeedPage()
    empty.outcome = "empty_interval"
    empty.consumer_id = "consumer"
    empty.prior = Value(content=prior)
    empty.next = Value(content=b"different next cursor is valid for an empty interval")
    empty.writer_xid = None
    checkpoint = hashlib.sha256(prior).hexdigest()
    assert WORKER.valid_empty_page_from_prior(empty, FeedPage, "consumer", checkpoint)
    empty.writer_xid = "42"
    assert not WORKER.valid_empty_page_from_prior(empty, FeedPage, "consumer", checkpoint)
    empty.writer_xid = None
    empty.prior = Value(content=b"wrong prior")
    assert not WORKER.valid_empty_page_from_prior(empty, FeedPage, "consumer", checkpoint)

    raw=b'{"code":"key_conflict","operationIndex":"1"}'
    digest=hashlib.sha256(raw).hexdigest()
    artifact=types.SimpleNamespace(identity=digest, sha256=digest,
        bytes_base64=base64.b64encode(raw).decode("ascii"))
    failure=GroupOperationFailure(artifact)
    truss=types.ModuleType("truss"); truss.CatalogDocument=CatalogDocument; truss.PreparedGroup=PreparedGroup
    contracts=types.ModuleType("truss.contracts")
    for name in ("MapEntry","MapValue","NullValue","TextValue"): setattr(contracts,name,Value)
    groups=types.ModuleType("truss.groups")
    for name in ("AliasObjectReference","AuthoredValue","CreateEdge","CreateObject","CurrentCatalog",
                 "GroupSemanticInput","RootlessOwnership","TextOrderKey"): setattr(groups,name,Value)
    prior={name:sys.modules.get(name) for name in ("truss","truss.contracts","truss.groups","psycopg")}
    sys.modules.update({"truss":truss,"truss.contracts":contracts,"truss.groups":groups})
    state={"rolled_back":False,"committed":False}
    class FakePsycopg(types.ModuleType):
        @staticmethod
        def connect(**kwargs): return Connection()
    sys.modules["psycopg"]=FakePsycopg("psycopg")
    class StopRun(RuntimeError): pass
    def value(result,label):
        if not isinstance(result,Ok): raise StopRun(label)
        return result.value
    def transaction(conn, callback, **kwargs):
        try:
            return callback(Assembly(failure), object())
        except PROBE.RollbackOutcome:
            state["rolled_back"]=True
            raise
    base=types.SimpleNamespace(_value=value,_transaction=transaction,StopRun=StopRun)
    original=PROBE._write_exclusive
    def write_after_rollback(path, record):
        assert state["rolled_back"] and not state["committed"]
        original(path,record)
    PROBE._write_exclusive=write_after_rollback
    try:
        with tempfile.TemporaryDirectory() as temp:
            receipt=Path(temp)/"diagnostic.json"
            record=PROBE.probe_group(base,lambda role:{"user":role},model={},document_id="doc",
                document_revision="1",request_key="r54-test",origin={"source":"test"},
                operations=({"op":"create_object","type":"Person","alias":"p"},),
                bun=Path("/bin/true"),receipt_path=receipt)
            saved=json.loads(receipt.read_text())
            got=saved["diagnostic_artifact"]["bytes_base64"]
            assert record["transaction_rolled_back"] is True
            assert saved["failure_variant"] == "GroupOperationFailure"
            assert saved["operation_index"] == "1" and saved["operation_code"] == "key_conflict"
            assert saved["diagnostic_artifact"]["sha256"] == digest
            assert base64.b64decode(got,validate=True) == raw
            assert saved["transaction_outcome"] == "forced_rollback"
    finally:
        PROBE._write_exclusive=original
        for name,old in prior.items():
            if old is None: sys.modules.pop(name,None)
            else: sys.modules[name]=old
    print("R54 GroupFailed artifact capture and forced rollback passed")


if __name__ == "__main__": main()
