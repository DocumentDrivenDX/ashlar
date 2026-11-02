"""Effect-free transaction-boundary test for the R52 public group result guard."""
from __future__ import annotations

import importlib.util
import sys
import tempfile
import types
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("r52_group_helper", HERE / "r52-group-helper.py")
HELPER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(HELPER)


class Ok:
    def __init__(self, value): self.value = value


class PreparedGroup: pass
class CatalogDocument:
    def __init__(self, *args): pass
class AppliedPending:
    def __init__(self, count): self.semantic = types.SimpleNamespace(results=[object()] * count)
class GroupSuccess:
    def __init__(self, response): self.response = response
class StrangeCode:
    def __str__(self): return "x" * 500
class ErrorWrapper:
    def __init__(self):
        self.error = types.SimpleNamespace(code=StrangeCode(), catalog_code={"unknown":1},
            sql_state=StrangeCode(), retry_scope=StrangeCode())


def install_public_stubs(count):
    truss = types.ModuleType("truss"); truss.CatalogDocument = CatalogDocument; truss.PreparedGroup = PreparedGroup
    contracts = types.ModuleType("truss.contracts")
    class Value:
        def __init__(self, **kw): self.__dict__.update(kw)
    for name in ("MapEntry", "MapValue", "NullValue", "TextValue"): setattr(contracts, name, Value)
    groups = types.ModuleType("truss.groups")
    for name in ("AliasObjectReference", "AuthoredValue", "CreateEdge", "CreateObject",
                 "CurrentCatalog", "GroupSemanticInput", "RootlessOwnership", "TextOrderKey"):
        setattr(groups, name, Value)
    groups.GroupSuccess = GroupSuccess
    old = {name:sys.modules.get(name) for name in ("truss", "truss.contracts", "truss.groups")}
    sys.modules.update({"truss":truss,"truss.contracts":contracts,"truss.groups":groups})
    return old


class FakeConnection:
    def __init__(self): self.committed = False
    def __enter__(self): return self
    def __exit__(self, *args): return False
    def commit(self): self.committed = True


class FakeAssembly:
    group_selection = types.SimpleNamespace(layout_profile="l", capability_profile="c", value_profile="v")
    def __init__(self, count): self.count = count
    def prepare_group_in_transaction(self, *args, **kwargs):
        item = types.SimpleNamespace(authored_identity="Person", definition_pin="pin")
        return Ok(PreparedGroupWithContext(item))
    def apply_in_transaction(self, *args, **kwargs):
        return ErrorWrapper() if self.count == -1 else Ok(GroupSuccess(AppliedPending(self.count)))


class PreparedGroupWithContext(PreparedGroup):
    def __init__(self, item):
        self.types=(item,); self.relationships=(); self.context=types.SimpleNamespace(
            authorized_scope_identity="scope", request=lambda key: types.SimpleNamespace(
                identity=types.SimpleNamespace(authorized_scope_identity="scope")))


def base_for(count, state):
    class StopRun(RuntimeError): pass
    def value(result, label):
        if not isinstance(result, Ok): raise StopRun(label)
        return result.value
    def transaction(conn, operation, **kwargs):
        try:
            result=operation(FakeAssembly(count), object())
        except BaseException:
            state["rolled_back"] = True
            raise
        state["committed"] = True
        return result
    return types.SimpleNamespace(_value=value, StopRun=StopRun, _transaction=transaction)


def main():
    for count, expect_success in ((1, False), (2, True), (-1, False)):
        old = install_public_stubs(count)
        try:
            connection = FakeConnection(); state = {"rolled_back":False,"committed":False}
            psycopg = types.ModuleType("psycopg")
            psycopg.connect = lambda **kw: connection
            sys.modules["psycopg"] = psycopg
            base = base_for(count, state)
            original_writer = HELPER._write_exclusive
            def write_after_transaction(path, value):
                if value.get("transaction_rolled_back"):
                    assert state["rolled_back"], "failure diagnostic must be durable only after rollback"
                else:
                    assert state["committed"], "success receipt follows transaction commit"
                original_writer(path, value)
            HELPER._write_exclusive = write_after_transaction
            with tempfile.TemporaryDirectory() as temp:
                receipt = Path(temp) / "group-result.json"
                args = dict(model={}, document_id="doc", document_revision="1", request_key="r52-test",
                    origin={"a":"b"}, operations=({"op":"create_object","type":"Person","alias":"p"},),
                    bun=Path("/bin/true"), receipt_path=receipt, expected_count=2)
                if expect_success:
                    result=HELPER.apply_group(base, lambda role:{"user":role}, **args)
                    assert isinstance(result, GroupSuccess) and connection.committed and state["committed"]
                    assert not state["rolled_back"]
                    assert __import__("json").loads(receipt.read_text())["result_count"] == 2
                else:
                    try:
                        HELPER.apply_group(base, lambda role:{"user":role}, **args)
                    except base.StopRun:
                        pass
                    else:
                        raise AssertionError("mismatched result must refuse before commit")
                    diagnostic=__import__("json").loads(receipt.read_text())
                    assert state["rolled_back"] and not connection.committed
                    assert diagnostic["transaction_rolled_back"] is True
                    assert diagnostic["diagnostic_written_after_rollback"] is True
                    if count == -1:
                        assert diagnostic["public_result_wrapper"] == "ErrorWrapper"
                        assert diagnostic["group_variant"] is None
                        assert len(diagnostic["error"]["code"]) <= 160
                        assert diagnostic["error"]["catalog_code"] == "{'unknown': 1}"
                    else:
                        assert diagnostic["group_variant"] == "GroupSuccess"
                        assert diagnostic["response_variant"] == "AppliedPending"
                        assert diagnostic["result_count"] == 1
        finally:
            HELPER._write_exclusive = original_writer
            for name, prior in old.items():
                if prior is None: sys.modules.pop(name, None)
                else: sys.modules[name] = prior
            sys.modules.pop("psycopg", None)
    print("R52 group result rollback/commit contract passed")


if __name__ == "__main__": main()
