# SPDX-License-Identifier: MIT OR Apache-2.0
"""Versioned Weft 0.4.56 admission for the current `.4-xxhash64` Backend03 read.

This composes the public pinned compiler with the existing publication-held
consumer. It retains the exact compiler response and generated SQL digest for
review; the consumer passes the compiler artifact through unchanged.
"""
import hashlib
import json

from ashlar.weft_056_distribution import Weft056Distribution
from .managed_current_core import CARRIER_PROFILE_XXHASH64
from .managed_current_weft_binding import (
    XXHASH64_BACKEND_ID, XXHASH64_BACKEND03_VERSION, XXHASH64_TARGET_PROFILE,
    BACKEND03_INTERFACE, BACKEND03_DIALECT,
)
from .managed_current_query import ManagedCurrentReadOwner

PROFILE = "ashlar-managed-current-weft-056-admission/0.1"


def _require(value, reason="Weft 0.4.56 current consumer admission refused"):
    if not value:
        raise ValueError(reason)


def _pairs(items):
    result = {}
    for key, value in items:
        _require(key not in result)
        result[key] = value
    return result


def _decode(raw):
    try:
        return json.loads(raw.decode("utf-8"), object_pairs_hook=_pairs)
    except Exception:
        raise ValueError("Weft 0.4.56 current consumer admission refused") from None


def _admit_request(request):
    _require(type(request) is dict and request.get("interfaceVersion") == BACKEND03_INTERFACE
             and request.get("dialect") == BACKEND03_DIALECT
             and type(request.get("sql")) is str and 0 < len(request["sql"].encode("utf-8")) <= 65536
             and type(request.get("target")) is dict)
    target = request["target"]
    _require(target.get("backendId") == XXHASH64_BACKEND_ID
             and target.get("backendVersion") == XXHASH64_BACKEND03_VERSION
             and target.get("targetProfile") == XXHASH64_TARGET_PROFILE
             and type(target.get("bindingJson")) is str
             and type(target.get("bindingSha256")) is str
             and hashlib.sha256(target["bindingJson"].encode("utf-8")).hexdigest() == target["bindingSha256"])
    binding = _decode(target["bindingJson"].encode("utf-8"))
    publication = binding.get("publication")
    tables = publication.get("tables") if type(publication) is dict else None
    _require(binding.get("layoutRevision") == CARRIER_PROFILE_XXHASH64
             and binding.get("lookupHashProfile") == "spark-xxhash64-typed-key/0.1"
             and type(tables) is list and len(tables) == 4
             and all(type(item) is dict and type(item.get("name")) is list
                     and len(item["name"]) == 3
                     and all(type(part) is str and part for part in item["name"])
                     for item in tables)
             and {item["name"][-1] for item in tables}
                 == {"object_current", "edge_current", "tombstone", "whole_source_history"}
             and all(type(item.get("uuid")) is str and type(item.get("version")) is int
                     and item["version"] >= 0 for item in tables))


def run_managed_current_weft_056_backend03(owner, compiler, request, *,
        reviewed_request_sha256, reviewed_response_sha256, timeout_seconds=90):
    """Run one `.4` Backend03 query through the current publication-held owner.

    Caller-supplied request and response digests bind an independently reviewed
    compile step. Both are required and the exact response digest is checked
    before any native invocation. No reviewed `.4` response digest is currently
    qualified by the repository evidence.
    """
    _require(type(owner) is ManagedCurrentReadOwner
             and owner.carrier_profile == CARRIER_PROFILE_XXHASH64
             and type(compiler) is Weft056Distribution and compiler._verify())
    _admit_request(request)
    request_bytes = json.dumps(request, ensure_ascii=False, sort_keys=True,
                               separators=(",", ":"), allow_nan=False).encode("utf-8")
    _require(type(reviewed_request_sha256) is str
             and len(reviewed_request_sha256) == 64
             and all(c in "0123456789abcdef" for c in reviewed_request_sha256)
             and hashlib.sha256(request_bytes).hexdigest() == reviewed_request_sha256
             and type(reviewed_response_sha256) is str
             and len(reviewed_response_sha256) == 64
             and all(c in "0123456789abcdef" for c in reviewed_response_sha256))

    expected_backend = {"backendId": XXHASH64_BACKEND_ID,
        "backendVersion": XXHASH64_BACKEND03_VERSION,
        "interfaceVersion": "weft-backend/0.3.0",
        "targetProfile": XXHASH64_TARGET_PROFILE}

    def admit_compiled(raw):
        _require(type(raw) is bytes and 0 < len(raw) <= 4 * 1024 * 1024)
        _require(hashlib.sha256(raw).hexdigest() == reviewed_response_sha256,
                 "Weft 0.4.56 response differs from independently reviewed response")
        response = _decode(raw)
        artifact = response.get("artifact") if type(response) is dict else None
        _require(response.get("status") == "compiled" and type(artifact) is dict
                 and artifact.get("backend") == expected_backend
                 and type(artifact.get("sql")) is str and bool(artifact["sql"]))

    outcome = owner.run_backend03(compiler, request, timeout_seconds=timeout_seconds,
                                  retain_response=True, admit_compiled=admit_compiled)
    raw = outcome.get("compilerResponseBytes") if type(outcome) is dict else None
    digest = outcome.get("compilerResponseSha256") if type(outcome) is dict else None
    _require(type(raw) is bytes and 0 < len(raw) <= 4 * 1024 * 1024
             and hashlib.sha256(raw).hexdigest() == digest
             and outcome.get("request") == request)
    response = _decode(raw)
    artifact = response.get("artifact") if type(response) is dict else None
    _require(response.get("status") == "compiled" and type(artifact) is dict
             and artifact.get("backend") == expected_backend
             and type(artifact.get("sql")) is str and artifact["sql"]
             and outcome.get("artifact") == artifact)
    return {"profile": PROFILE, "compilerVersion": "0.4.56",
        "compilerSourceCommit": "2f62e1148d1b2030558550e4f6f2ffe518bac62d",
        "request": request, "requestSql": request["sql"],
        "requestSqlSha256": hashlib.sha256(request["sql"].encode("utf-8")).hexdigest(),
        "compilerResponseBytes": raw, "compilerResponseSha256": digest,
        "artifact": artifact, "compilerSqlSha256": outcome["compilerSqlSha256"],
        "rows": outcome["rows"], "runtimeIdentity": outcome["runtimeIdentity"],
        "publicationReceipt": outcome["publicationReceipt"],
        "qualification": "Versioned `.4-xxhash64` local/managed consumer route; native result authority is limited to the enclosing publication, pin, source-oracle and closing checks."}
