# SPDX-License-Identifier: MIT OR Apache-2.0
"""Effect-free recorder for selected current Weft local-program parity.

The compiler invocation is supplied by the caller after the exact unpublished
0.4.50 wheel has been frozen. This module only compares those compiler objects
with the retained .48 local results; it never loads a wheel or starts a runtime.
"""
import gzip
import hashlib
import json
import re
from pathlib import Path

_SHA = re.compile(r"[0-9a-f]{64}\Z")
_COMMIT = re.compile(r"(?:[0-9a-f]{40}|[0-9a-f]{64})\Z")
_CASES = (
    "backend03-grouped-having",
    "backend03-negative-having",
    "backend02-page1",
    "backend02-continuation",
)
_FILES = {
    "backend03-grouped-having": "positive_grouped_graph_query.json.gz",
    "backend03-negative-having": "negative_having_query.json.gz",
    "backend02-page1": "sdk_pages_2_plus_1.json.gz",
    "backend02-continuation": "sdk_pages_2_plus_1.json.gz",
}
_PACKET = "current-native-query-and-sdk-pages-20261021.json"
_PACKET_KEYS = {
    "backend03-grouped-having": "positive_grouped_graph_query",
    "backend03-negative-having": "negative_having_query",
    "backend02-page1": "sdk_pages_2_plus_1",
    "backend02-continuation": "sdk_pages_2_plus_1",
}


def _canonical(value):
    return json.dumps(value, ensure_ascii=False, sort_keys=True,
                      separators=(",", ":"), allow_nan=False)


def _require(value, reason="Managed current local parity refused"):
    if not value:
        raise ValueError(reason)


def _sha(data):
    return hashlib.sha256(data).hexdigest()


def _prior_case(case, root):
    filename = _FILES[case]
    path = Path(root) / filename
    compressed = path.read_bytes()
    raw = gzip.decompress(compressed)
    packet_path = Path(root) / _PACKET
    packet_raw = packet_path.read_bytes()
    packet = json.loads(packet_raw)
    packet_entry = packet.get("files", {}).get(_PACKET_KEYS[case])
    _require(type(packet_entry) is dict and packet_entry.get("file") == filename
             and packet_entry.get("sha256") == _sha(raw)
             and packet_entry.get("bytes") == len(raw),
             "Retained gzip evidence must match the exact hashed baseline packet JSON")
    document = json.loads(raw)
    query = document["query"]
    if case.startswith("backend03-"):
        request = json.loads(query["protocol"]["request"])
        artifact = json.loads(query["protocol"]["recompiled"])
    else:
        index = 0 if case == "backend02-page1" else 1
        program = query["evidence"][index]["program"]
        request = program["request"]
        artifact = program["artifact"]
    _require(type(request) is dict and type(artifact) is dict
             and artifact.get("status") == "compiled"
             and type(artifact.get("sql")) is str
             and type(artifact.get("parameters")) is list)
    return {
        "asset": "local-current/" + filename,
        "sha256": _sha(compressed),
        "baselineReportSha256": _sha(raw),
        "baselinePacketAsset": "local-current/" + _PACKET,
        "baselinePacketSha256": _sha(packet_raw),
        "request": request,
        "artifact": artifact,
    }


def _prior_semantics(artifact):
    return {key: artifact.get(key) for key in
            ("sql", "parameters", "backend", "bindingSha256")}


def build_managed_current_local_parity(*, evidence_root, local_wheel,
                                       source_commit, compiled_cases):
    """Record four exact .50 local compiler outputs against retained .48 data.

    ``compiled_cases`` maps the four case IDs to the exact original request and
    artifact objects returned by compiling those requests with the selected
    .50 Mac wheel. No comparison is inferred from SQL text or a boolean claim.
    The emitted asset embeds canonical objects so the release verifier can hash
    the compiler request/artifact itself and independently check all slots.
    """
    _require(type(source_commit) is str and _COMMIT.fullmatch(source_commit))
    _require(type(local_wheel) is dict and set(local_wheel) == {
        "sha256", "nativeModuleSha256", "recordSha256",
    } and all(type(value) is str and _SHA.fullmatch(value)
              for value in local_wheel.values()))
    _require(type(compiled_cases) is dict and set(compiled_cases) == set(_CASES))
    result = {
        "schema": "weft-local-program-parity/0.2",
        "status": "passed",
        "sourceCommit": source_commit,
        "localWheel": dict(local_wheel),
        "baselinePacket": {
            "asset": "local-current/" + _PACKET,
            "sha256": _sha((Path(evidence_root) / _PACKET).read_bytes()),
        },
        "cases": {},
    }
    for case in _CASES:
        prior = _prior_case(case, evidence_root)
        observed = compiled_cases[case]
        _require(type(observed) is dict and set(observed) == {"request", "artifact"}
                 and type(observed["request"]) is dict
                 and type(observed["artifact"]) is dict)
        request, artifact = observed["request"], observed["artifact"]
        _require(request == prior["request"],
                 "The .50 local compile must use the exact retained .48 request")
        _require(artifact.get("status") == "compiled"
                 and _prior_semantics(artifact) == _prior_semantics(prior["artifact"]),
                 "The .50 local artifact must preserve prior SQL, ordered slots, backend and binding")
        request_json, artifact_json = _canonical(request), _canonical(artifact)
        result["cases"][case] = {
            "priorEvidence": dict(result["baselinePacket"]),
            "gzipEvidence": {"asset": Path(prior["asset"]).name,
                             "compressedSha256": prior["sha256"],
                             "reportSha256": prior["baselineReportSha256"]},
            "local50": {
                "requestJson": request_json,
                "artifactJson": artifact_json,
                "requestSha256": _sha(request_json.encode("utf-8")),
                "artifactSha256": _sha(artifact_json.encode("utf-8")),
                "sqlSha256": _sha(artifact["sql"].encode("utf-8")),
                "orderedParameterSha256": _sha(_canonical(artifact["parameters"]).encode("utf-8")),
            },
        }
    return result


def managed_current_parity_bytes(parity):
    _require(type(parity) is dict and parity.get("schema") == "weft-local-program-parity/0.2"
             and parity.get("status") == "passed" and set(parity.get("cases", {})) == set(_CASES))
    return _canonical(parity).encode("utf-8")
