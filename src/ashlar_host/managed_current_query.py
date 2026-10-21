# SPDX-License-Identifier: MIT OR Apache-2.0
"""Independent source-oracle admission for the managed current functional run.

The oracle is assembled by the external Truss holder from its retained source
snapshot. It is deliberately separate from the managed Delta destination and
is used only to compare the bounded Weft results returned by the task.
"""
from dataclasses import dataclass
import base64
import hashlib
import json
import re
import threading
import time
import uuid
from contextlib import contextmanager
from datetime import timedelta

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_PROFILE = "ashlar-managed-current-source-oracle/0.1"
_MAX_ROWS = 1000
_CASE_PROFILE = "weft-selected-native-case/0.1"


def _require(value, reason="Managed current source oracle refused"):
    if not value:
        raise ValueError(reason)


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _text(value, maximum=4096):
    return type(value) is str and 0 < len(value.encode("utf-8")) <= maximum and "\0" not in value


def _presence(value):
    _require(type(value) is dict and value.get("state") in ("absent", "null", "value"))
    if value["state"] in ("absent", "null"):
        _require(set(value) == {"state"})
    else:
        _require(set(value) == {"state", "value"})
        item = value["value"]
        _require(type(item) in (str, bool, int)
                 and (type(item) is not int or -(2**63) <= item < 2**63)
                 and (type(item) is not str or "\0" not in item))


@dataclass(frozen=True)
class ManagedCurrentSourceOracle:
    """Detached digest-verified view of one admitted current source snapshot."""

    sha256: str
    schedule_sha256: str
    schedule_step: int
    captured_at: str
    authority_receipt_sha256: str
    catalog_head_sha256: str
    model_sha256: str
    feed: str
    epoch: str
    cursor_sha256: str
    persons: tuple
    places: tuple
    edges: tuple
    truss_version: str
    catalog_evidence_profile: str
    runtime_wheel_sha256: str | None = None
    runtime_manifest_sha256: str | None = None


def _managed_source_identity(source_bundle, *, schedule_sha256, schedule_step):
    """Derive oracle identity only from the retained, digest-bound Truss bundle."""
    _require(type(source_bundle) is dict
             and source_bundle.get("status") == "preinstallation-source-bundle")
    retained = dict(source_bundle)
    supplied = retained.pop("bindingSha256", None)
    _require(type(supplied) is str and _SHA.fullmatch(supplied)
             and hashlib.sha256(_canonical(retained).encode("utf-8")).hexdigest() == supplied,
             "Digest-bound retained Truss source bundle required")
    source = source_bundle.get("source")
    from .truss_current_catalog import (CURRENT_TRUSS_VERSION, CURRENT_TRUSS_045_VERSION,
        CURRENT_EVIDENCE_PROFILE, CURRENT_045_EVIDENCE_PROFILE,
        CURRENT_TRUSS_045_WHEEL_SHA256, CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256)
    version = source.get("trussVersion") if type(source) is dict else None
    destination = source_bundle.get("destination")
    physical_profile = (destination.get("physicalProfile", "ashlar-delta/0.3")
                        if type(destination) is dict else "ashlar-delta/0.3")
    profile = ("ashlar-managed-current-core-binding/0.3" if physical_profile == "ashlar-delta/0.4-xxhash64"
               else "ashlar-managed-current-core-binding/0.1" if version == CURRENT_TRUSS_VERSION
               else "ashlar-managed-current-core-binding/0.2" if version == CURRENT_TRUSS_045_VERSION else None)
    if physical_profile == "ashlar-delta/0.4-xxhash64":
        _require(type(destination) is dict and destination.get("lookupHashProfile")
                 == "spark-xxhash64-typed-key/0.1",
                 "Exact versioned Spark xxhash64 key interpretation required")
    elif type(destination) is dict:
        _require("lookupHashProfile" not in destination,
                 "Legacy carrier bundle cannot inherit the new lookup hash semantics")
    expected_catalog_profile = (CURRENT_EVIDENCE_PROFILE if version == CURRENT_TRUSS_VERSION
                                else CURRENT_045_EVIDENCE_PROFILE if version == CURRENT_TRUSS_045_VERSION else None)
    _require(source_bundle.get("profile") == profile
             and type(source) is dict and source.get("umfVersion") == "0.10.0"
             and physical_profile in ("ashlar-delta/0.3", "ashlar-delta/0.4-xxhash64")
             and type(source.get("feed")) is str and source["feed"]
             and type(source.get("epoch")) is str and source["epoch"])
    runtime = source.get("runtimeProvenance")
    if version == CURRENT_TRUSS_045_VERSION:
        _require(source.get("catalogEvidenceProfile") == expected_catalog_profile
                 and runtime == {"trussWheelSha256": CURRENT_TRUSS_045_WHEEL_SHA256,
                             "runtimeManifestSha256": CURRENT_TRUSS_045_RUNTIME_MANIFEST_SHA256},
                 "Truss 0.0.45 installed wheel/runtime provenance required")
    else:
        _require("runtimeProvenance" not in source and "catalogEvidenceProfile" not in source)
    try:
        page = base64.b64decode(source["nativePageBase64"], validate=True)
        before = base64.b64decode(source["cursorBeforeBase64"], validate=True)
        after = base64.b64decode(source["cursorAfterBase64"], validate=True)
        page_value, before_value, after_value = (json.loads(item) for item in (page, before, after))
    except (KeyError, ValueError, TypeError, json.JSONDecodeError):
        _require(False, "Original Truss page and opaque cursor pair required")
    _require(hashlib.sha256(page).hexdigest() == source.get("transactionSha256")
             and type(page_value) is dict and type(page_value.get("writerXid")) is str
             and type(before_value) is dict and type(after_value) is dict
             and before_value.get("profile") == after_value.get("profile")
                == "truss-caller-asserted-feed/0.1"
             and before_value.get("feed") in (None, source["feed"])
             and before_value.get("sourceEpoch") == after_value.get("sourceEpoch") == source["epoch"]
             and before_value.get("installationId") == after_value.get("installationId")
             and before_value.get("consumerId") == after_value.get("consumerId")
             and before_value.get("registrationId") == after_value.get("registrationId"),
             "Retained Truss page, epoch, and cursor identity differ")
    cursor_sha = hashlib.sha256(after).hexdigest()
    model_hashes = {key: item.get("modelSha256") for key, item in
                    source.get("acceptedDocuments", {}).items()}
    catalog_hashes = source.get("catalogViewSha256")
    _require(type(model_hashes) is dict and bool(model_hashes)
             and all(type(item) is str and _SHA.fullmatch(item) for item in model_hashes.values())
             and type(catalog_hashes) is dict and bool(catalog_hashes)
             and all(type(item) is str and _SHA.fullmatch(item) for item in catalog_hashes.values()))
    authority_receipt_sha = hashlib.sha256(_canonical({
        "profile": "ashlar-managed-current-source-authority/0.1",
        "transactionSha256": source["transactionSha256"],
        "feed": source["feed"], "epoch": source["epoch"],
        "cursorSha256": cursor_sha,
    }).encode("utf-8")).hexdigest()
    _require(type(schedule_sha256) is str and _SHA.fullmatch(schedule_sha256)
             and type(schedule_step) is int and schedule_step >= 1)
    return {
        "trussVersion": source["trussVersion"],
        "umfVersion": source["umfVersion"],
        "catalogEvidenceProfile": expected_catalog_profile,
        "runtimeProvenance": runtime,
        "feed": source["feed"], "epoch": source["epoch"],
        "cursorSha256": cursor_sha,
        "authorityReceiptSha256": authority_receipt_sha,
        "catalogHeadSha256": hashlib.sha256(_canonical(catalog_hashes).encode("utf-8")).hexdigest(),
        "modelSha256": hashlib.sha256(_canonical(model_hashes).encode("utf-8")).hexdigest(),
        "scheduleSha256": schedule_sha256, "scheduleStep": schedule_step,
        "manifestProgress": {
            "profile": "truss-native-feed/0.1", "feed": source["feed"],
            "epoch": source["epoch"], "previous": source["cursorBeforeBase64"],
            "position": source["cursorAfterBase64"],
            "payload_digest": source["transactionSha256"],
            "batch_id": page_value["writerXid"],
        },
    }


def admit_managed_current_source_oracle(value, *, source_bundle, schedule_sha256, schedule_step):
    """Check a source-holder snapshot and bind it to the frozen trial step.

    The closed shape includes every Person's exact key/name/quantity state,
    Place identities, and edge identities/endpoints. Edge lists preserve
    multiplicity; IDs must be unique while equal endpoint pairs remain allowed.
    """
    _require(type(value) is dict and type(value.get("trussVersion")) is str)
    optional = {"catalogEvidenceProfile", "runtimeProvenance"} if value["trussVersion"] == "0.0.45" else set()
    _require(set(value) == {
        "profile", "authority", "trussVersion", "umfVersion", "scheduleSha256",
        "scheduleStep", "capturedAt", "authorityReceiptSha256", "catalogHeadSha256",
        "modelSha256", "feed", "epoch", "cursorSha256", "persons", "places",
        "edges", "oracleSha256",
    } | optional)
    supplied = value["oracleSha256"]
    unsigned = dict(value)
    unsigned.pop("oracleSha256")
    _require(type(supplied) is str and _SHA.fullmatch(supplied)
             and hashlib.sha256(_canonical(unsigned).encode("utf-8")).hexdigest() == supplied,
             "Digest-bound original external Truss source snapshot required")
    identity = _managed_source_identity(source_bundle, schedule_sha256=schedule_sha256,
                                        schedule_step=schedule_step)
    _require(value["profile"] == _PROFILE and value["authority"] == "external-real-truss-holder"
             and value["trussVersion"] in ("0.0.44", "0.0.45")
             and value["umfVersion"] == "0.10.0"
             and all(value.get(key) == identity[key] for key in (
                 "trussVersion", "umfVersion", "scheduleSha256", "scheduleStep",
                 "authorityReceiptSha256", "catalogHeadSha256", "modelSha256",
                 "feed", "epoch", "cursorSha256")),
             "Source oracle authority, source bundle, manifest progress, or frozen schedule differs")
    _require(value.get("catalogEvidenceProfile", identity["catalogEvidenceProfile"])
             == identity["catalogEvidenceProfile"]
             and value.get("runtimeProvenance", identity["runtimeProvenance"])
             == identity["runtimeProvenance"],
             "Source oracle Truss runtime version/provenance differs from its retained bundle")
    _require(_text(value["capturedAt"]) and all(type(value[key]) is str and _SHA.fullmatch(value[key])
             for key in ("authorityReceiptSha256", "catalogHeadSha256", "modelSha256", "cursorSha256"))
             and _text(value["feed"]) and _text(value["epoch"]),
             "Complete original source authority/fence custody required")
    persons = value["persons"]
    places = value["places"]
    edges = value["edges"]
    _require(type(persons) is list and 1 <= len(persons) <= _MAX_ROWS
             and type(places) is list and len(places) <= _MAX_ROWS
             and type(edges) is list and len(edges) <= _MAX_ROWS,
             "Explicit finite source snapshot bounds required")
    person_keys = set()
    for person in persons:
        _require(type(person) is dict and set(person) == {"key", "name", "quantity"}
                 and _text(person["key"]) and _text(person["name"])
                 and person["key"] not in person_keys,
                 "Exact unique Person source identity and required fields required")
        _presence(person["quantity"])
        person_keys.add(person["key"])
    place_keys = set()
    for place in places:
        _require(type(place) is dict and set(place) == {"key", "name"}
                 and _text(place["key"]) and _text(place["name"])
                 and place["key"] not in place_keys,
                 "Exact unique Place source identity required")
        place_keys.add(place["key"])
    edge_ids = set()
    for edge in edges:
        _require(type(edge) is dict and set(edge) == {
            "id", "relationship", "sourceType", "sourceKey", "targetType", "targetKey",
        } and _text(edge["id"]) and _text(edge["relationship"])
                 and edge["id"] not in edge_ids and _text(edge["sourceType"])
                 and _text(edge["sourceKey"]) and _text(edge["targetType"])
                 and _text(edge["targetKey"]),
                 "Exact unique independent source edge identity and endpoints required")
        if edge["relationship"] == "knows":
            _require(edge["sourceType"] == edge["targetType"] == "Person"
                     and edge["sourceKey"] in person_keys and edge["targetKey"] in person_keys,
                     "Directed knows edge must retain exact Person endpoints")
        elif edge["relationship"] == "located":
            _require(edge["sourceType"] == "Person" and edge["sourceKey"] in person_keys
                     and edge["targetType"] == "Place" and edge["targetKey"] in place_keys,
                     "Directed located edge must retain exact Person-to-Place endpoints")
        else:
            _require(False, "Only the trial's authored knows/located path fixture is admitted")
        edge_ids.add(edge["id"])
    _require(len(_canonical(value).encode("utf-8")) <= 4 * 1024 * 1024,
             "Source oracle exceeds its retained byte bound")
    return ManagedCurrentSourceOracle(
        supplied, schedule_sha256, schedule_step, value["capturedAt"],
        value["authorityReceiptSha256"], value["catalogHeadSha256"], value["modelSha256"],
        value["feed"], value["epoch"], value["cursorSha256"],
        tuple(dict(item) for item in persons), tuple(dict(item) for item in places),
        tuple(dict(item) for item in edges), value["trussVersion"],
        value.get("catalogEvidenceProfile", identity["catalogEvidenceProfile"]),
        identity["runtimeProvenance"].get("trussWheelSha256") if identity["runtimeProvenance"] else None,
        identity["runtimeProvenance"].get("runtimeManifestSha256") if identity["runtimeProvenance"] else None,
    )


def managed_current_grouped_paths(oracle, *, threshold):
    """Independent bag oracle for Person-knows-then-located path grouping.

    Each directed `knows` edge is an occurrence, including parallel edges.
    A qualifying terminal Place is distinct by its source key. Results group by
    the starting Person key as the selected Weft case does.
    """
    _require(type(oracle) is ManagedCurrentSourceOracle and type(threshold) is int
             and 0 <= threshold < 2**63)
    persons = {person["key"] for person in oracle.persons}
    knows = [edge for edge in oracle.edges if edge["relationship"] == "knows"]
    located = [edge for edge in oracle.edges if edge["relationship"] == "located"]
    by_intermediate = {}
    for edge in located:
        by_intermediate.setdefault(edge["sourceKey"], set()).add(edge["targetKey"])
    result = []
    for person_key in sorted(persons, key=lambda text: text.encode("utf-8")):
        paths = [edge for edge in knows if edge["sourceKey"] == person_key
                 and edge["targetKey"] in by_intermediate]
        count = len(paths)
        terminals = {terminal for edge in paths for terminal in by_intermediate[edge["targetKey"]]}
        if count > threshold:
            result.append([person_key, str(count), str(len(terminals))])
    return result


def managed_current_person_page(oracle, *, after=None, page_size=2):
    """Independent complete STRING page oracle with one-row lookahead semantics."""
    _require(type(oracle) is ManagedCurrentSourceOracle
             and type(page_size) is int and 1 <= page_size <= 100)
    _require(after is None or type(after) is str and "\0" not in after)
    ordered = sorted(oracle.persons, key=lambda row: row["key"].encode("utf-8"))
    if after is not None:
        ordered = [row for row in ordered if row["key"].encode("utf-8") > after.encode("utf-8")]
    lookahead = ordered[:page_size + 1]
    rows = [[row["key"], row["name"]] for row in lookahead[:page_size]]
    next_after = lookahead[page_size - 1]["key"] if len(lookahead) > page_size else None
    return {"rows": rows, "nextAfter": next_after, "hasMore": len(lookahead) > page_size}


def managed_current_oracle_receipt(oracle, expected_rows):
    """Return only the authority/schedule identity needed in public case proof.

    The canonical source snapshot remains private; its digest binds the exact
    Persons/Places/edges and presence states used to compute expected rows.
    """
    _require(type(oracle) is ManagedCurrentSourceOracle)
    receipt = {
        "profile": _PROFILE,
        "sourceOracleSha256": oracle.sha256,
        "sourceAuthority": "external-real-truss-holder",
        "trussVersion": oracle.truss_version,
        "catalogEvidenceProfile": oracle.catalog_evidence_profile,
        "capturedAt": oracle.captured_at,
        "scheduleSha256": oracle.schedule_sha256,
        "scheduleStep": oracle.schedule_step,
        "authorityReceiptSha256": oracle.authority_receipt_sha256,
        "catalogHeadSha256": oracle.catalog_head_sha256,
        "modelSha256": oracle.model_sha256,
        "cursorSha256": oracle.cursor_sha256,
        "expectedRowsSha256": hashlib.sha256(_canonical(expected_rows).encode("utf-8")).hexdigest(),
        "feed": oracle.feed,
        "epoch": oracle.epoch,
    }
    if oracle.truss_version == "0.0.45":
        receipt["runtimeProvenance"] = {
            "trussWheelSha256": oracle.runtime_wheel_sha256,
            "runtimeManifestSha256": oracle.runtime_manifest_sha256,
        }
    receipt["sha256"] = hashlib.sha256(_canonical(receipt).encode("utf-8")).hexdigest()
    return receipt


def _parameter_sha256(artifact):
    """Digest every exact compiler slot object, in original order."""
    slots = artifact.get("parameters") if type(artifact) is dict else None
    _require(type(slots) is list and len(slots) <= 128)
    ordered = []
    for position, slot in enumerate(slots, 1):
        _require(type(slot) is dict and slot.get("position") == position
                 and type(slot.get("value")) is str and "\0" not in slot["value"])
        ordered.append(dict(slot))
    return hashlib.sha256(_canonical(ordered).encode("utf-8")).hexdigest()


def managed_current_query_case(*, case, source_commit, linux_wheel_sha256,
                               linux_native_module_sha256, linux_record_sha256, runtime_identity,
                               publication_receipt, request, artifact, oracle,
                               expected_rows, actual_rows, run_id, task_receipt_id,
                               page_size=None,
                               continuation=None, incoming_continuation_sha256=None):
    """Build a content-addressed case record after exact result comparison.

    For a Backend02 page, the continuation's opaque owner cursor is hashed and
    discarded; its contents never enter evidence. Call once for page one and
    once for the continuation. This function is a recorder, not an execution
    authority: callers must obtain rows through the held AshlarTarget route.
    """
    _require(case in ("backend03-grouped-having", "backend03-negative-having", "backend02-page1",
                      "backend02-continuation"))
    _require(type(source_commit) is str and _COMMIT.fullmatch(source_commit))
    for value in (linux_wheel_sha256, linux_native_module_sha256, linux_record_sha256):
        _require(type(value) is str and _SHA.fullmatch(value))
    _require(type(runtime_identity) is dict and type(publication_receipt) is dict
             and publication_receipt.get("profile") == "ashlar-managed-current-destination-receipt/0.1"
             and type(request) is dict and type(artifact) is dict
             and type(oracle) is ManagedCurrentSourceOracle
             and type(expected_rows) is list and type(actual_rows) is list
             and expected_rows == actual_rows)
    _require(type(runtime_identity.get("opening")) is dict
             and runtime_identity["opening"] == runtime_identity.get("closing")
             and (runtime_identity["opening"].get("sparkBuildText")
                  or runtime_identity["opening"].get("runtimeTag")),
             "An observed public Spark build string or runtime tag is required")
    if case == "backend03-grouped-having":
        source_expected = managed_current_grouped_paths(oracle, threshold=0)
    elif case == "backend03-negative-having":
        source_expected = managed_current_grouped_paths(oracle, threshold=2)
    elif case == "backend02-page1":
        source_expected = managed_current_person_page(oracle, page_size=2)
        _require(source_expected["hasMore"] is True)
        source_expected = source_expected["rows"]
    else:
        first_expected = managed_current_person_page(oracle, page_size=2)
        _require(first_expected["hasMore"] is True)
        source_expected = managed_current_person_page(
            oracle, after=first_expected["nextAfter"], page_size=2)
        _require(source_expected["hasMore"] is False)
        source_expected = source_expected["rows"]
    _require(expected_rows == source_expected,
             "Case expected rows must be independently derived from the bound source oracle")
    if case.startswith("backend03-"):
        sql = request.get("sql") if type(request) is dict else None
        threshold = 0 if case == "backend03-grouped-having" else 2
        normalized_sql = re.sub(r"\s+", " ", sql).upper() if type(sql) is str else ""
        _require("EXPAND_PATHS(P.KNOWS, LOCATED)" in normalized_sql
                 and "COUNT_DISTINCT_PATH_TARGETS(X)" in normalized_sql
                 and re.search(r"HAVING COUNT\(\*\) > " + str(threshold) + r"(?:\D|$)",
                               normalized_sql) is not None,
                 "Grouped path case must execute its exact approved original HAVING query")
    _require(type(run_id) in (int, str) and (type(run_id) is int and run_id > 0
             or type(run_id) is str and _text(run_id, 128))
             and type(task_receipt_id) is str and _text(task_receipt_id, 256),
             "Actual bounded Databricks Jobs run and task-run identifiers required")
    result = {
        "case": case,
        "status": "passed",
        "artifactStatus": artifact.get("status"),
        "runId": run_id,
        "taskReceiptId": task_receipt_id,
        "sourceCommit": source_commit,
        "linuxWheelSha256": linux_wheel_sha256,
        "linuxNativeModuleSha256": linux_native_module_sha256,
        "linuxRecordSha256": linux_record_sha256,
        "runtimeIdentity": runtime_identity,
        "publicationReceipt": publication_receipt,
        "requestJson": _canonical(request),
        "artifactJson": _canonical(artifact),
        "requestSha256": hashlib.sha256(_canonical(request).encode("utf-8")).hexdigest(),
        "artifactSha256": hashlib.sha256(_canonical(artifact).encode("utf-8")).hexdigest(),
        "sqlSha256": hashlib.sha256(request["sql"].encode("utf-8")).hexdigest(),
        "compilerSqlSha256": hashlib.sha256(artifact["sql"].encode("utf-8")).hexdigest(),
        "orderedParameterSha256": _parameter_sha256(artifact),
        "oracle": managed_current_oracle_receipt(oracle, expected_rows),
        "expectedRows": expected_rows,
        "expectedRowsSha256": hashlib.sha256(_canonical(expected_rows).encode("utf-8")).hexdigest(),
        "actualRows": actual_rows,
    }
    _require(result["artifactStatus"] == "compiled")
    if case.startswith("backend02-"):
        _require(type(page_size) is int and page_size == 2)
        if case == "backend02-page1":
            _require(continuation is not None and incoming_continuation_sha256 is None)
            cursor = getattr(continuation, "owner_cursor", None)
            _require(type(cursor) is bytes and 1 <= len(cursor) <= 65536)
            result.update({
                "pageSize": page_size,
                "hasContinuation": True,
                "continuationSha256": hashlib.sha256(cursor).hexdigest(),
            })
        else:
            _require(continuation is None and type(incoming_continuation_sha256) is str
                     and _SHA.fullmatch(incoming_continuation_sha256))
            result.update({
                "pageSize": page_size,
                "hasContinuation": False,
                "incomingContinuationSha256": incoming_continuation_sha256,
            })
    else:
        _require(page_size is None and continuation is None
                 and incoming_continuation_sha256 is None)
    return result


def managed_current_case_bytes(case_record):
    """Canonical retained bytes for one immutable selected qualification case."""
    _require(type(case_record) is dict and case_record.get("status") == "passed")
    return _canonical(case_record).encode("utf-8")


class ManagedCurrentReadOwner:
    """Injected-Spark read owner for original managed Weft artifacts.

    This owner supplies the resolver backend, in-task pin hold, exact SQL
    executor, runtime/publication renewal and independent oracle comparison
    required by ``read_weft``/``AshlarTarget``. It never starts Spark or reads
    an unpinned/latest graph snapshot.
    """

    def __init__(self, spark, *, target_owner, receipt, runtime_identity,
                 expected_environment, source_bundle, oracle, expected_rows,
                 schedule_sha256, schedule_step, maximum_rows=1000):
        from .managed_current_core import ROLES
        from .managed_current_publication import (
            ManagedCurrentTargetOwner, observe_managed_current_environment,
        )
        _require(spark is not None and type(target_owner) is ManagedCurrentTargetOwner
                 and type(receipt) is dict
                 and receipt.get("profile") == "ashlar-managed-current-destination-receipt/0.1"
                 and type(runtime_identity) is dict and set(runtime_identity) == {"opening", "closing", "privateDatabricksBuildIdentity"}
                 and runtime_identity["opening"] == runtime_identity["closing"]
                 and runtime_identity["privateDatabricksBuildIdentity"] == "unknown"
                 and (runtime_identity["opening"].get("sparkBuildText")
                      or runtime_identity["opening"].get("runtimeTag"))
                 and type(expected_environment) is dict
                 and type(source_bundle) is dict
                 and type(oracle) is ManagedCurrentSourceOracle
                 and type(expected_rows) is list
                 and type(schedule_sha256) is str and _SHA.fullmatch(schedule_sha256)
                 and type(schedule_step) is int and schedule_step >= 1
                 and type(maximum_rows) is int and 1 <= maximum_rows <= 1000)
        identity = _managed_source_identity(source_bundle, schedule_sha256=schedule_sha256,
                                            schedule_step=schedule_step)
        _require((oracle.truss_version, oracle.catalog_evidence_profile,
                  oracle.runtime_wheel_sha256, oracle.runtime_manifest_sha256,
                  oracle.schedule_sha256, oracle.schedule_step,
                  oracle.authority_receipt_sha256, oracle.catalog_head_sha256,
                  oracle.model_sha256, oracle.feed, oracle.epoch, oracle.cursor_sha256)
                 == (identity["trussVersion"], identity["catalogEvidenceProfile"],
                     (identity["runtimeProvenance"] or {}).get("trussWheelSha256"),
                     (identity["runtimeProvenance"] or {}).get("runtimeManifestSha256"),
                     schedule_sha256, schedule_step, identity["authorityReceiptSha256"],
                     identity["catalogHeadSha256"], identity["modelSha256"],
                     identity["feed"], identity["epoch"], identity["cursorSha256"]),
                 "Managed query oracle must bind the retained source page, schedule, model, and catalog")
        from .managed_current_core import ManagedCurrentCoreConfig, restore_managed_current_admission
        target_config = source_bundle.get("destination", {})
        carrier_profile = target_config.get("physicalProfile", "ashlar-delta/0.3")
        target_declarations = source_bundle.get("weftTargets", {})
        def target(surface):
            item = target_declarations.get(surface)
            _require(type(item) is dict)
            return {"surface": surface, "backendId": item["backendId"],
                    "backendVersion": item["backendVersion"],
                    "targetProfile": item["targetProfile"],
                    "weftInterface": item["weftInterface"], "weftDialect": item["weftDialect"]}
        admission_config = ManagedCurrentCoreConfig(
            catalog=target_config.get("catalog"), schema=target_config.get("schema"),
            backend03=target("Backend03"), backend02=target("Backend02"),
            preferred_cost_usd=10, hard_cost_usd=100,
            carrier_profile=carrier_profile)
        admission = restore_managed_current_admission(source_bundle, admission_config)
        _require(admission.batch.feed == identity["feed"] and admission.batch.epoch == identity["epoch"],
                 "The retained source bundle must reconstruct the exact admitted Truss page")
        self.spark, self.targets, self.receipt = spark, target_owner, receipt
        from .managed_current_core import CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64
        from .managed_current_core import LOOKUP_HASH_PROFILE_XXHASH64
        _require(carrier_profile in (CARRIER_PROFILE_LEGACY, CARRIER_PROFILE_XXHASH64)
                 and getattr(target_owner.carriers, "physical_profile", CARRIER_PROFILE_LEGACY)
                    == carrier_profile,
                 "Read owner, retained source bundle, and native carrier profile must match")
        if carrier_profile == CARRIER_PROFILE_XXHASH64:
            _require(target_owner.carriers.lookup_hash_profile == LOOKUP_HASH_PROFILE_XXHASH64
                     and target_config.get("lookupHashProfile") == LOOKUP_HASH_PROFILE_XXHASH64,
                     "Read owner and source bundle must bind the exact native lookup hash semantics")
        self.carrier_profile = carrier_profile
        self.runtime_identity = runtime_identity
        self.expected_environment = dict(expected_environment)
        self.source_bundle = json.loads(_canonical(source_bundle))
        self.source_identity = identity
        self.oracle, self.expected_rows, self.maximum_rows = oracle, expected_rows, maximum_rows
        self.context = object()
        self.active = False
        self.page_token = None
        self.current_request = None
        self.current_artifact = None
        self.opening = self.targets.open_existing()
        _require(observe_managed_current_environment(spark, self.targets.profile) == self.expected_environment
                 and self.expected_environment == runtime_identity["closing"],
                 "Managed query task runtime/principal differs from publication receipt")
        self._validate_receipt()
        self.vector = self._pin_vector()
        self.backend = self._native_backend()

    def _validate_receipt(self):
        from ashlar.native import quote_table_identifier
        from .managed_current_core import ROLES
        receipt = self.receipt
        _require(set(receipt) == {"profile", "publicationId", "requestDigest", "manifest",
                    "manifestSha256", "dataRoleVector", "weftBindingInputs",
                    "installationInventory", "sourceAuthority"}
                 and receipt["sourceAuthority"] ==
                    f"development Truss {self.oracle.truss_version} page on managed Unity Catalog; external source ACK required")
        inventory = receipt["installationInventory"]
        _require(type(inventory) is dict and set(inventory) == set(ROLES))
        for role in ROLES:
            item = inventory[role]
            observed = self.opening[role]
            _require(type(item) is dict and item.get("uuid") == observed.uuid
                     and tuple(item.get("name", ())) == self.targets.names[role]
                     and item.get("version") == observed.version
                     and tuple(tuple(x) for x in item.get("schema", ())) == observed.schema
                     and item.get("protocol") == observed.protocol)
            owners = self.targets.transport.query(
                "SELECT table_owner FROM system.information_schema.tables "
                "WHERE table_catalog=:catalog AND table_schema=:schema AND table_name=:name",
                {"catalog": self.targets.catalog, "schema": self.targets.schema, "name": role}).rows
            _require(len(owners) == 1
                     and owners[0].get("table_owner") == self.expected_environment["principal"],
                     "Exact selected principal must own every managed installation role")
        manifest = receipt["manifest"]
        _require(type(manifest) is dict and manifest.get("publication_id") == receipt["publicationId"]
                 and manifest.get("profile_version") == self.carrier_profile
                 and type(receipt["manifestSha256"]) is str
                 and hashlib.sha256(_canonical(manifest).encode("utf-8")).hexdigest() == receipt["manifestSha256"])
        role_names = {".".join(self.targets.names[role]): role for role in
                      ("object_current", "edge_current", "tombstone", "whole_source_history")}
        vector = receipt["dataRoleVector"]
        _require(type(vector) is dict and set(vector) == set(role_names))
        parsed = json.loads(manifest["table_versions_json"])
        _require(parsed == {table: vector[table]["version"] for table in vector})
        for table, role in role_names.items():
            _require(vector[table] == {"uuid": self.opening[role].uuid,
                                       "version": self.opening[role].version})
        progress = json.loads(manifest["source_progress_json"])
        _require(type(progress) is dict
                 and progress.get(self.source_identity["feed"]) == self.source_identity["manifestProgress"],
                 "Durable publication source progress must equal the retained admitted Truss page")
        binding = receipt["weftBindingInputs"]
        _require(type(binding) is dict and set(binding) == {"manifest", "manifestUuid", "dataRoles"}
                 and binding["manifest"] == manifest
                 and binding["manifestUuid"] == inventory["publication_manifest"]["uuid"]
                 and set(binding["dataRoles"]) == set(role_names))
        for table, role in role_names.items():
            role_item = binding["dataRoles"][role]
            _require(type(role_item) is dict and set(role_item) == {"name", "uuid", "version"}
                     and tuple(role_item["name"]) == self.targets.names[role]
                     and role_item["uuid"] == self.opening[role].uuid
                     and role_item["version"] == self.opening[role].version)
        actual = self.targets.transport.query(
            "SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
            "schema_revisions_json,validation_report_json,cast(unix_micros(recorded_at) AS STRING) AS recorded_at FROM "
            + quote_table_identifier(".".join(self.targets.names["publication_manifest"]))
            + " WHERE publication_id=:id", {"id": receipt["publicationId"]}).rows
        _require(len(actual) == 1 and dict(actual[0]) == manifest,
                 "Exact receipt manifest must be independently reopened before query execution")

    def _pin_vector(self):
        from ashlar.manifest import manifest_pin_vector
        names = {".".join(self.targets.names[role]): self.opening[role].uuid for role in
                 ("object_current", "edge_current", "tombstone", "whole_source_history")}
        return manifest_pin_vector(self.receipt["manifest"], names,
                                   authority="managed-current-job:" + self.expected_environment["principal"])

    def _native_backend(self):
        from ashlar.native import NativeBackend
        names = {role: ".".join(self.targets.names[role]) for role in self.targets.names}
        native = {"STRING": "string", "BIGINT": "long", "BOOLEAN": "boolean", "TIMESTAMP": "timestamp"}
        schemas = {names[role]: tuple((column, native[kind])
                    for column, kind, _nullable in self.targets.carriers.ordered_fields(role))
                   for role in ("object_current", "edge_current", "tombstone", "whole_source_history")}
        self.manifest_table = names["publication_manifest"]
        self.manifest_uuid = self.opening["publication_manifest"].uuid
        return NativeBackend(self.targets.transport, self, self.manifest_table,
                             self.manifest_uuid, schemas)

    def _renew(self):
        from .managed_current_publication import observe_managed_current_environment
        _require(self.active and observe_managed_current_environment(self.spark, self.targets.profile)
                 == self.expected_environment)
        current = self.targets.observe()
        _require(set(current) == set(self.opening))
        for role in current:
            before, after = self.opening[role], current[role]
            _require((before.uuid, before.version, before.schema, before.protocol)
                     == (after.uuid, after.version, after.schema, after.protocol),
                     "Managed query table identity, version, schema, or protocol changed")
            owners = self.targets.transport.query(
                "SELECT table_owner FROM system.information_schema.tables "
                "WHERE table_catalog=:catalog AND table_schema=:schema AND table_name=:name",
                {"catalog": self.targets.catalog, "schema": self.targets.schema, "name": role}).rows
            _require(len(owners) == 1
                     and owners[0].get("table_owner") == self.expected_environment["principal"],
                     "Managed query table ownership changed during read")
        return None

    @contextmanager
    def hold(self, vector, *, context):
        _require(context is self.context and vector == self.vector and not self.active)
        self.active = True
        primary = None
        try:
            self._renew()
            yield vector
        except BaseException as error:
            primary = error
        try:
            self._renew()
        except BaseException as error:
            if primary is None:
                primary = error
        self.active = False
        if primary is not None:
            raise primary

    hold_page = hold

    # NativeBackend authorization and descriptor/pin correspondence.
    def authorize(self, context, publication_id, tables):
        self._renew()
        expected = set(self.vector.targets)
        _require(context is self.context and publication_id == self.receipt["publicationId"]
                 and set(tables) == expected)

    def validate_descriptor(self, descriptor, context):
        self._renew()
        _require(context is self.context and dict(descriptor.raw) == self.receipt["manifest"])

    def validate_snapshot(self, table, uuid, version, columns):
        self._renew()
        pair = self.vector.targets.get(table)
        _require(pair is not None and pair == (uuid, version))
        role = table.rsplit(".", 1)[-1]
        expected = self.backend.schemas[table]
        native = {"STRING": "string", "BIGINT": "long", "BOOLEAN": "boolean", "TIMESTAMP": "timestamp"}
        declared = tuple((name, native[kind], nullable) for name, kind, nullable in
                         self.targets.carriers.ordered_fields(role))
        _require(tuple(columns) == expected and self.opening[role].schema == declared)

    # Weft admission, SQL execution, exact decoding and result release.
    def bind_descriptor(self, descriptor, vector, context):
        from ashlar.manifest import bind_manifest_pins
        _require(context is self.context and vector == self.vector
                 and dict(descriptor.raw) == self.receipt["manifest"])
        uuids = {table: pair[0] for table, pair in self.vector.targets.items()}
        bind_manifest_pins(descriptor, vector, uuids, authority=vector.authority)
        self._renew()

    def admit_artifact(self, request, artifact, descriptor, context):
        from .managed_current_weft_binding import (
            BACKEND_ID, BACKEND02_VERSION, BACKEND03_VERSION, PROFILE,
            XXHASH64_BACKEND_ID, XXHASH64_BACKEND02_VERSION,
            XXHASH64_BACKEND03_VERSION, XXHASH64_TARGET_PROFILE,
            managed_current_count_star_request, managed_current_sdk_pages_request,
        )
        self.bind_descriptor(descriptor, self.vector, context)
        target = request.get("target") if type(request) is dict else None
        if self.carrier_profile == "ashlar-delta/0.4-xxhash64":
            backend_id=XXHASH64_BACKEND_ID
            target_profile=XXHASH64_TARGET_PROFILE
            backend02_version=XXHASH64_BACKEND02_VERSION
            backend03_version=XXHASH64_BACKEND03_VERSION
        else:
            backend_id=BACKEND_ID
            target_profile=PROFILE
            backend02_version=BACKEND02_VERSION
            backend03_version=BACKEND03_VERSION
        _require(type(target) is dict and target.get("backendId") == backend_id
                 and target.get("targetProfile") == target_profile
                 and target.get("backendVersion") in (backend02_version, backend03_version)
                 and artifact.get("bindingSha256") == target.get("bindingSha256")
                 and artifact.get("status") == "compiled")
        if target["backendVersion"] == backend03_version:
            rebuilt = managed_current_count_star_request(
                self.source_bundle, self.receipt["weftBindingInputs"], request.get("sql"))
        else:
            rebuilt = managed_current_sdk_pages_request(
                self.source_bundle, self.receipt["weftBindingInputs"],
                request.get("sql"), request.get("parameters"))
        _require(rebuilt == request, "Managed Weft request must rebuild from the exact accepted source and receipt")
        self.current_request, self.current_artifact = json.loads(_canonical(request)), json.loads(_canonical(artifact))
        self._renew()

    def verify_native_profile(self, profile, context):
        self._renew()
        _require(context is self.context and type(profile) is dict
                 and profile.get("targetProfile") == (
                     "ashlar-managed-current-core/0.2-xxhash64" if self.carrier_profile == "ashlar-delta/0.4-xxhash64"
                     else "ashlar-managed-current-core/0.1")
                 and type(profile.get("requirements")) is list
                 and "verify the selected Databricks serverless Spark Jobs runtime/build, authenticated principal, Unity Catalog catalog/schema/table UUIDs and Delta protocol before SQL" in profile["requirements"]
                 and any("recheck" in item and "before releasing" in item for item in profile["requirements"]
                         if type(item) is str))
        self._renew()

    def authorize_query(self, request, descriptor, context):
        self.bind_descriptor(descriptor, self.vector, context)
        _require(self.current_request is not None and request == self.current_request
                 and self.current_artifact is not None)

    def admit_record_integrity(self, check, artifact, context):
        self._renew()
        _require(context is self.context and artifact == self.current_artifact
                 and any(check in obligation.get("parameters", {}).get("checks", [])
                         for obligation in artifact.get("obligations", [])))

    def admit_page_integrity(self, guard, artifact, context):
        self._renew()
        _require(context is self.context and artifact == self.current_artifact
                 and guard.get("key") == artifact.get("logicalPlan", {}).get("pageKey")
                 and any(guard == obligation.get("parameters")
                         for obligation in artifact.get("obligations", [])
                         if obligation.get("id") == "ashlar.candidate.keyIntegrity"))

    def page_context(self, descriptor, vector, context):
        self.bind_descriptor(descriptor, vector, context)
        if self.page_token is None:
            value = {"publicationId": descriptor.publication_id,
                     "vector": {table: list(pair) for table, pair in vector.targets.items()},
                     "oracleSha256": self.oracle.sha256,
                     "runtime": self.expected_environment}
            self.page_token = _canonical(value).encode("utf-8")
        return bytes(self.page_token)

    def decode_result(self, column, value, request, context):
        from ashlar.weft_decode import ExactScalar, decode_exact_scalar
        _require(context is self.context and request == self.current_request
                 and self.current_artifact is not None and type(value) is str)
        _require(column in self.current_artifact["columns"])
        representation = column["representation"]
        logical = representation.get("logicalType", {})
        count = logical.get("family") == "integer" and logical.get("facets") == {}
        return decode_exact_scalar(representation, value, count=count)

    def authorize_result(self, rows, descriptor, context):
        from ashlar.weft_decode import ExactScalar
        self.bind_descriptor(descriptor, self.vector, context)
        actual = [[item.original if type(item) is ExactScalar else item for item in row]
                  for row in rows]
        _require(actual == self.expected_rows,
                 "Managed Weft result differs from independently retained source oracle")
        self._renew()

    def query(self, sql, parameters):
        from ashlar.native import SQLResult
        _require(self.active and self.current_artifact is not None
                 and type(sql) is str and type(parameters) is dict)
        artifact = self.current_artifact
        slots = {"p" + str(slot["position"]): slot["value"] for slot in artifact.get("parameters", [])}
        allowed = [artifact.get("sql")]
        for obligation in artifact.get("obligations", []):
            allowed.extend(check.get("sql") for check in obligation.get("parameters", {}).get("checks", [])
                           if type(check) is dict)
        _require(sql in allowed and parameters == slots)
        frame = self.spark.sql(sql, args=parameters)
        rows = frame.collect()
        _require(len(rows) <= self.maximum_rows)
        type_names = {"string": "STRING", "long": "BIGINT", "integer": "INT",
                      "boolean": "BOOLEAN", "timestamp": "TIMESTAMP", "double": "DOUBLE",
                      "decimal": "DECIMAL"}
        columns = tuple((field.name, type_names.get(field.dataType.simpleString(),
                                                    field.dataType.simpleString().upper()))
                        for field in frame.schema.fields)
        return SQLResult([row.asDict(recursive=True) for row in rows], columns)

    @contextmanager
    def deadline(self, budget):
        _require(type(budget) is timedelta and 0 < budget.total_seconds() <= 120)
        sc = self.spark.sparkContext
        group = "managed-current-read-" + uuid.uuid4().hex
        keys = ("spark.jobGroup.id", "spark.job.description", "spark.job.interruptOnCancel")
        previous = {key: sc.getLocalProperty(key) for key in keys}
        timer = threading.Timer(budget.total_seconds(), lambda: sc.cancelJobGroup(group))
        timer.daemon = True
        try:
            sc.setJobGroup(group, "bounded managed current Weft read", interruptOnCancel=True)
            timer.start()
            yield
        finally:
            timer.cancel()
            for key, value in previous.items():
                sc.setLocalProperty(key, value)

    def run_backend03(self, compiler, request, *, timeout_seconds=90):
        """Compile and execute one original Backend03 query under this owner."""
        from ashlar.weft_query import read_weft
        _require(callable(getattr(compiler, "compile_json", None))
                 and type(timeout_seconds) is int and 1 <= timeout_seconds <= 90
                 and type(request) is dict)
        raw = compiler.compile_json(_canonical(request))
        _require(type(raw) is str and len(raw.encode("utf-8")) <= 4 * 1024 * 1024)
        compiled = json.loads(raw)
        _require(type(compiled) is dict and compiled.get("status") == "compiled"
                 and type(compiled.get("artifact")) is dict)
        artifact = compiled["artifact"]
        expected_backend = ({
            "backendId": "ashlar.databricks.managed-uc-spark-xxhash64",
            "backendVersion": "0.2.0-managed-current-core-xxhash64-candidate",
            "interfaceVersion": "weft-backend/0.3.0",
            "targetProfile": "ashlar-managed-current-core/0.2-xxhash64",
        } if self.carrier_profile == "ashlar-delta/0.4-xxhash64" else {
            "backendId": "ashlar.databricks.managed-uc-spark",
            "backendVersion": "0.1.0-managed-current-core-candidate",
            "interfaceVersion": "weft-backend/0.3.0",
            "targetProfile": "ashlar-managed-current-core/0.1",
        })
        _require(artifact.get("backend") == expected_backend)
        revisions = json.loads(self.receipt["manifest"]["schema_revisions_json"])
        with self.deadline(timedelta(seconds=timeout_seconds)):
            result = read_weft(self, self.backend, self, self.vector, self,
                request=request, artifact=artifact, context=self.context,
                supported_profiles=[self.carrier_profile],
                supported_revisions={name: [revision] for name, revision in revisions.items()})
        return {"request": request, "artifact": artifact,
                "rows": [list(row) for row in result],
                "runtimeIdentity": self.runtime_identity,
                "publicationReceipt": self.receipt}

    def run_backend03_case(self, compiler, request, *, case, expected_rows,
                           source_commit, linux_wheel_sha256,
                           linux_native_module_sha256, linux_record_sha256,
                           oracle, run_id, task_receipt_id, timeout_seconds=90):
        """Run one independently compiled grouped/HAVING program and record it."""
        _require(case in ("backend03-grouped-having", "backend03-negative-having"))
        self.expect_rows(expected_rows)
        outcome = self.run_backend03(compiler, request, timeout_seconds=timeout_seconds)
        return managed_current_query_case(
            case=case, source_commit=source_commit,
            linux_wheel_sha256=linux_wheel_sha256,
            linux_native_module_sha256=linux_native_module_sha256,
            linux_record_sha256=linux_record_sha256,
            runtime_identity=outcome["runtimeIdentity"],
            publication_receipt=outcome["publicationReceipt"],
            request=outcome["request"], artifact=outcome["artifact"], oracle=oracle,
            expected_rows=expected_rows, actual_rows=outcome["rows"],
            run_id=run_id, task_receipt_id=task_receipt_id)

    def expect_rows(self, rows):
        """Set one independently derived page oracle before its held read."""
        _require(not self.active and type(rows) is list and len(rows) <= self.maximum_rows)
        self.expected_rows = [list(row) for row in rows]

    def backend02_target(self, compiler, entry, *, timeout_seconds=90):
        """Construct AshlarTarget for the original managed typed-page catalog."""
        from ashlar.access import AshlarTarget
        _require(type(timeout_seconds) is int and 1 <= timeout_seconds <= 90
                 and type(entry) is not type(None))
        request = entry.artifact()["request"]
        target = request.get("target")
        expected_target = ({
            "backendId": "ashlar.databricks.managed-uc-spark-xxhash64",
            "backendVersion": "0.2.0-managed-current-core-xxhash64-pages-candidate",
            "targetProfile": "ashlar-managed-current-core/0.2-xxhash64",
        } if self.carrier_profile == "ashlar-delta/0.4-xxhash64" else {
            "backendId": "ashlar.databricks.managed-uc-spark",
            "backendVersion": "0.1.0-managed-current-core-pages-candidate",
            "targetProfile": "ashlar-managed-current-core/0.1",
        })
        _require(type(target) is dict and all(target.get(name)==value for name,value in expected_target.items()))
        return AshlarTarget(
            compiler=compiler, compiler_method="ashlar-spark4-pages",
            executor=self, backend=self.backend, pins=self, vector=self.vector,
            policy=self, context=self.context,
            supported_profiles=[self.carrier_profile],
            supported_revisions={name: [revision] for name, revision in
                json.loads(self.receipt["manifest"]["schema_revisions_json"]).items()},
            binding_sha256=target["bindingSha256"],
            platform=(("spark", self.expected_environment["sparkVersion"]),),
            default_timeout=timedelta(seconds=timeout_seconds), deadline=self.deadline,
            catalog_entries=(entry,),
        )

    def run_backend02_page(self, target, entry, parameters, *, expected_rows,
                           after=None, timeout_seconds=90):
        """Execute one original 2-row typed SDK page and retain no cursor bytes."""
        _require(type(expected_rows) is list and type(timeout_seconds) is int
                 and 1 <= timeout_seconds <= 90)
        self.expect_rows(expected_rows)
        page = target.page(entry, parameters, after=after,
                           timeout=timedelta(seconds=timeout_seconds))
        names = tuple(entry.result.model_fields)
        rows = [[item.model_dump(mode="python")[name] for name in names]
                for item in page.items]
        _require(rows == expected_rows)
        return {"rows": rows, "next": page.next_after,
                "request": self.current_request, "artifact": self.current_artifact,
                "runtimeIdentity": self.runtime_identity,
                "publicationReceipt": self.receipt}

    def run_backend02_two_pages(self, target, entry, parameters, *, oracle,
                                 expected_first, expected_second, source_commit,
                                 linux_wheel_sha256, linux_native_module_sha256,
                                 linux_record_sha256, run_id, task_receipt_id,
                                 timeout_seconds=90):
        """Run and record the bounded original SDK 2+1 page continuation.

        The opaque continuation stays in memory. Only its SHA-256 is placed in
        the two immutable case records; no continuation payload is returned.
        """
        first = self.run_backend02_page(target, entry, parameters,
                                        expected_rows=expected_first,
                                        timeout_seconds=timeout_seconds)
        continuation = first["next"]
        _require(continuation is not None and type(continuation.owner_cursor) is bytes
                 and 1 <= len(continuation.owner_cursor) <= 65536)
        continuation_sha = hashlib.sha256(continuation.owner_cursor).hexdigest()
        second = self.run_backend02_page(target, entry, parameters,
                                         expected_rows=expected_second,
                                         after=continuation,
                                         timeout_seconds=timeout_seconds)
        _require(second["next"] is None)
        first_case = managed_current_query_case(
            case="backend02-page1", source_commit=source_commit,
            linux_wheel_sha256=linux_wheel_sha256,
            linux_native_module_sha256=linux_native_module_sha256,
            linux_record_sha256=linux_record_sha256,
            runtime_identity=self.runtime_identity,
            publication_receipt=self.receipt,
            request=first["request"], artifact=first["artifact"], oracle=oracle,
            expected_rows=expected_first, actual_rows=first["rows"],
            run_id=run_id, task_receipt_id=task_receipt_id,
            page_size=2, continuation=continuation)
        second_case = managed_current_query_case(
            case="backend02-continuation", source_commit=source_commit,
            linux_wheel_sha256=linux_wheel_sha256,
            linux_native_module_sha256=linux_native_module_sha256,
            linux_record_sha256=linux_record_sha256,
            runtime_identity=self.runtime_identity,
            publication_receipt=self.receipt,
            request=second["request"], artifact=second["artifact"], oracle=oracle,
            expected_rows=expected_second, actual_rows=second["rows"],
            run_id=run_id, task_receipt_id=task_receipt_id,
            page_size=2, incoming_continuation_sha256=continuation_sha)
        return {"page1": first_case, "continuation": second_case}

