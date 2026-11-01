#!/usr/bin/env python3
"""Effect-free validation of the proposed continuation on accepted r6 PGDATA."""
from __future__ import annotations

import hashlib
import inspect
import json
from pathlib import Path

from truss import (CatalogDocument, CatalogReferenceAssembly,
                   ModuleAccessGrant, prepare_acceptance)
from truss._installation_privileges import _name

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[5]
SCOPE_PATH = HERE / "fresh-local-trial-scope.json"
SEED_PATH = HERE / "fresh-local-seed-request.json"
CONTINUATION_PATH = HERE / "r6-continuation-inputs.json"
SOURCE_MODEL = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261031/g03-sdk010-txn1-weft056-packet/r4-sdk010-initialize-only/inputs/typed-graph-model.umf.json"
EXPECTED_SOURCE_SHA = "19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b"
EXPECTED_MODEL_SHA = "2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880"
EXPECTED_OLD = b'"id": "typed-graph-example"'
EXPECTED_NEW = b'"id": "ashlar-local-dev-typed-graph-r6-20261031"'

class DuplicateKey(ValueError):
    pass


def unique_pairs(items):
    result = {}
    for key, value in items:
        if key in result:
            raise DuplicateKey(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def strict_read(path):
    return json.loads(path.read_text(), object_pairs_hook=unique_pairs)


try:
    strict_read(SCOPE_PATH)
except DuplicateKey as error:
    if str(error) != "duplicate JSON key: ordinary_reader_role":
        raise SystemExit(f"unexpected legacy scope duplicate: {error}")
else:
    raise SystemExit("legacy r6 scope duplicate was not detected")
seed = strict_read(SEED_PATH)
continuation = strict_read(CONTINUATION_PATH)
if continuation["status"] != "design_only_pending_sol_review":
    raise SystemExit("continuation is no longer marked design-only")
if continuation["acceptance"]["replay_acceptance"] is not False:
    raise SystemExit("continuation must never replay catalog acceptance")

source = SOURCE_MODEL.read_bytes()
if hashlib.sha256(source).hexdigest() != EXPECTED_SOURCE_SHA or source.count(EXPECTED_OLD) != 1:
    raise SystemExit("frozen source model bytes changed")
model = source.replace(EXPECTED_OLD, EXPECTED_NEW)
model_sha = hashlib.sha256(model).hexdigest()
if model_sha != EXPECTED_MODEL_SHA:
    raise SystemExit("r6 derived model bytes changed")
model_document = json.loads(model)
module_ids = {module["id"] for module in model_document["modules"]}
module = next((module for module in model_document["modules"] if module["id"] == "m"), None)
if module is None or module_ids != {"m"}:
    raise SystemExit("r6 document module identity changed")
record_ids = {element["id"] for element in module["elements"] if element.get("kind") == "record"}
relationship_ids = {relationship["id"] for relationship in module["relationships"]}
if not {"Person", "Place"} <= record_ids or not {"knows", "located"} <= relationship_ids:
    raise SystemExit("r6 model no longer contains the seed record/relationship types")
doc = CatalogDocument(
    document_id=continuation["acceptance"]["document_id"],
    document_revision=continuation["acceptance"]["document_revision"],
    content=model,
)
prepared = prepare_acceptance((doc,))
if prepared is None or doc.document_id not in prepared.input_bytes.decode("utf8"):
    raise SystemExit("pure acceptance preparation did not retain r6 identity")

consumer = continuation["consumer"]["consumer_id"]
reader = continuation["consumer"]["reader_role"]
module_reader = continuation["consumer"]["module_read_role"]
module_writer = continuation["consumer"]["module_writer_role"]
if consumer != "ashlar-local-dev-typed-graph-consumer-r6-20261031":
    raise SystemExit("continuation consumer identity mismatch")
if not (1 <= len(reader.encode("utf8")) <= 63) or not _name(reader):
    raise SystemExit("reader role candidate is not a bounded PostgreSQL role name")
if not _name(module_reader) or not _name(module_writer) or module_reader == module_writer:
    raise SystemExit("module access roles are not distinct bounded PostgreSQL role names")
ModuleAccessGrant(doc.document_id, "m", module_reader, module_writer)
if len(continuation["seed"]["request_key"].encode()) > 1024 or "\0" in continuation["seed"]["request_key"]:
    raise SystemExit("seed request key outside Truss bound")
if len(seed["operations"]) != 7 or sum(x["op"] == "create_object" for x in seed["operations"]) != 4 or sum(x["op"] == "create_edge" for x in seed["operations"]) != 3:
    raise SystemExit("seed operation shape mismatch")
aliases = {item["alias"] for item in seed["operations"]}
if any(item.get(side) not in aliases for item in seed["operations"] if item["op"] == "create_edge" for side in ("source", "target")):
    raise SystemExit("seed edge references unknown alias")
if seed["document_id"] != doc.document_id or seed["request_key"] != continuation["seed"]["request_key"]:
    raise SystemExit("seed/model continuation identity mismatch")

expected_keywords = {
    "status_in_transaction": set(),
    "accepted_document_in_transaction": {"document_id", "maximum_bytes"},
    "register_feed_consumer_in_transaction": {"consumer_id", "reader_role"},
    "configure_module_access_in_transaction": {"grants"},
    "prepare_group_in_transaction": {"records", "relationships"},
    "apply_in_transaction": {"input", "request", "documents"},
    "read_feed_in_transaction": {"consumer_id", "maximum_facts", "maximum_bytes"},
}
for method_name, required in expected_keywords.items():
    params = inspect.signature(getattr(CatalogReferenceAssembly, method_name)).parameters
    if not required <= set(params):
        raise SystemExit(f"unexpected public API signature: {method_name}")

result = {
    "status": "passed_effect_free_continuation_input_validation",
    "database_started": False,
    "database_connected": False,
    "acceptance_replayed": False,
    "source_sha256": EXPECTED_SOURCE_SHA,
    "derived_model_sha256": model_sha,
    "pure_prepare_acceptance": "passed",
    "legacy_scope_duplicate": "rejected by duplicate-key parser; fresh role is explicit in continuation inputs",
    "consumer_id": consumer,
    "reader_role": reader,
    "reader_role_bytes": len(reader.encode()),
    "reader_role_name_contract": "passed Truss bounded-role validator",
    "module_access_grant": {"document_id": doc.document_id, "module_id": "m", "reader_role": module_reader, "writer_role": module_writer},
    "model_seed_definitions": {"module": "m", "records": ["Person", "Place"], "relationships": ["knows", "located"]},
    "seed_operation_count": len(seed["operations"]),
    "seed_object_count": 4,
    "seed_edge_count": 3,
    "seed_request_sha256": hashlib.sha256(SEED_PATH.read_bytes()).hexdigest(),
    "public_api_shapes": "passed installed-signature checks",
    "preflight_methods": ["status_in_transaction", "accepted_document_in_transaction"],
    "feed_read_maximum_facts": continuation["feed_read"]["maximum_facts"],
    "feed_read_maximum_bytes": continuation["feed_read"]["maximum_bytes"],
    "acknowledgement": False,
    "no_native_call": True,
}
print(json.dumps(result, sort_keys=True))
