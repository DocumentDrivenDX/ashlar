"""Effect-free asset admission for the SDK 0.5.8 G03 local packet."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys

PACKET = Path(__file__).resolve().parent
PROFILE = "ashlar-g03-local-delta4-transaction1-sdk058-packet/0.1"
REPO = PACKET.parents[6]
sys.path.insert(0, str(REPO / "src"))

INIT_PACKET = (REPO / "docs/helix/04-build/evidence/current-upstream-release-20261024/"
               "configured-local-delta4-init-20261024/one-shot-packet")
_SPEC = importlib.util.spec_from_file_location("ashlar_sdk058_g03_init_common",
                                               INIT_PACKET / "packet_common.py")
_INIT = importlib.util.module_from_spec(_SPEC)
_SPEC.loader.exec_module(_INIT)
_INIT.PACKET = PACKET
_INIT.MANIFEST = PACKET / "packet-manifest.json"
_INIT.PACKET_PROFILE = PROFILE


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8") + b"\n"


def sha_bytes(raw):
    return hashlib.sha256(raw).hexdigest()


def sha_file(path):
    return _INIT.sha_file(Path(path))


def manifest():
    value = _INIT.load_manifest()
    if type(value) is not dict or value.get("profile") != PROFILE:
        raise ValueError("SDK058 G03 packet manifest refused")
    return value


def packet_digest():
    return _INIT.packet_tree_digest()


def _sdk_config(selected):
    from ashlar_host.typed_graph_sdk058_admission import TypedGraphSdk058AdmissionConfig
    values = selected["sdk058Admission"]
    return TypedGraphSdk058AdmissionConfig(
        node=Path(values["node"]), sdk_package_root=Path(values["sdkPackageRoot"]),
        sdk_archive=Path(values["sdkArchive"]), producer_closure=Path(values["producerClosure"]),
        producer_receipt=Path(values["producerReceipt"]), install_custody=Path(values["installCustody"]),
        source_admission_manifest=Path(values["sourceAdmissionManifest"]))


def verify_sdk058_source(input_root):
    """Run the pinned release checker without Spark and bind its output."""
    from ashlar_host.typed_graph_sdk058_admission import admit_typed_graph_sdk058_policy
    from ashlar.typed_graph_source_config import load_typed_graph_jsonl_configuration
    selected = manifest()
    config_path = Path(input_root) / "configured-typed-graph-source.json"
    configuration = load_typed_graph_jsonl_configuration(config_path)
    policy = admit_typed_graph_sdk058_policy(
        _sdk_config(selected), configuration, configuration.model_source)
    if policy.producer_revision != "umf-sdk-0.5.8/071544fcc83c5abcf1e280890e1516613f679a58cf245b31a7bf16f0b6dd24fd":
        raise ValueError("SDK 0.5.8 source policy identity differs")
    receipt = policy.receipt
    return {"producerRevision": policy.producer_revision,
            "receiptSha256": sha_bytes(receipt), "receiptBytes": len(receipt),
            "sourceAdmissionManifestSha256": selected["sdk058Admission"]["sourceAdmissionManifestSha256"],
            "sparkStarted": False}


def verify_packet(input_root=None, *, runtime=True):
    """Verify source, runtime, full release source admission and six-role bridge."""
    selected = manifest()
    git = selected["umfProducer"]["git"]
    current = subprocess.run([git, "-C", str(REPO), "rev-parse", "HEAD"],
                             check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    ancestor = subprocess.run([git, "-C", str(REPO), "merge-base", "--is-ancestor",
                               selected["sourceCommit"], current], capture_output=True, timeout=10)
    if ancestor.returncode:
        raise ValueError("reviewed SDK058 source commit is not an ancestor of current HEAD")
    _INIT.verify_source_tree(selected)
    root = Path(input_root) if input_root is not None else PACKET / "inputs"
    _INIT.verify_inputs(root, selected)
    oracle_path = root / "source-oracle.json"
    if sha_file(oracle_path) != selected["inputs"]["source-oracle.json"]:
        raise ValueError("G03 source oracle digest differs")
    oracle = json.loads(oracle_path.read_bytes())
    if (oracle.get("inputs", {}).get("publicationEventCount") != 8
            or oracle.get("inputs", {}).get("publicationTransaction") != "typed-graph-1"
            or oracle.get("queryExpected", {}).get("singletonPerson1", {}).get(
                "quantityJsonText") != "9007199254740993"):
        raise ValueError("Exact frozen G03 transaction-1 source oracle required")

    source_admission = verify_sdk058_source(root)
    bridge = verify_umf_bridge(Path("/virtual/ashlar-g03-umf058"), compare_frozen=True)
    runtime_receipt = _INIT.verify_packet_assets(root) if runtime else None
    return {"profile": PROFILE, "sourceCommit": selected["sourceCommit"],
            "currentCommit": current, "packetTreeSha256": packet_digest(),
            "sourceOracleSha256": selected["inputs"]["source-oracle.json"],
            "sourceAdmission": source_admission, "bridge": bridge,
            "runtime": runtime_receipt}


def verify_umf_bridge(installation_path, *, compare_frozen=False):
    """Verify exact SDK058 `/0.2` IIFE proposals; never opens Spark."""
    from ashlar_host.local_current_delta4_umf_sdk058 import (
        EXPECTED_ROLES, LocalUmfDelta4Sdk058Bridge, pinned_sdk058_resources)
    from ashlar_host.schema_rows import LOCAL_CURRENT_ROLE_TO_UMF

    selected = manifest()["umfDelta4Bridge"]
    realization, _bundle, sources, _semantics = pinned_sdk058_resources()
    if (realization["profile"] != selected["profile"]
            or realization["bridge"]["assetSha256"] != selected["bridgeSha256"]
            or realization["bridge"]["buildInputsSha256"] != selected["buildInputsSha256"]
            or set(LOCAL_CURRENT_ROLE_TO_UMF.values()) != set(EXPECTED_ROLES)):
        raise ValueError("SDK 0.5.8 Delta 4 `/0.2` bridge identity differs")
    reference_path = (REPO / "docs/helix/04-build/evidence/current-upstream-release-20261029/"
                      "g03-sdk058-delta4-realization-candidate-20261030/outputs/carrier-proposals.json")
    if sha_file(reference_path) != selected["releaseProposalsSha256"]:
        raise ValueError("reviewed six-role SDK 0.5.8 proposal receipt changed")
    frozen = json.loads(reference_path.read_bytes())
    proposals = {}
    with LocalUmfDelta4Sdk058Bridge.open() as bridge:
        if bridge.manifest["profile"] != selected["profile"]:
            raise ValueError("SDK058 runtime bridge profile differs")
        for item in frozen["roles"]:
            role = item["role"]
            local_role = next((name for name, carrier in LOCAL_CURRENT_ROLE_TO_UMF.items()
                               if carrier == role), None)
            if local_role is None:
                raise ValueError("SDK058 role alias mapping is not closed")
            target = installation_path / (role if compare_frozen else local_role)
            realized = bridge.realize(sources[role + ".umf.json"], str(target), role)
            if compare_frozen:
                if realized["proposal"] != item["release"]:
                    raise ValueError("SDK058 six-role frozen proposal differs: " + role)
            elif (realized["proposal"].get("sourceSha256") != item["release"]["sourceSha256"]
                  or realized["proposal"].get("schemaSha256") != item["release"]["schemaSha256"]
                  or realized["proposal"].get("target", {}).get("path") != str(target)
                  or realized["proposal"].get("target", {}).get("kind") != "delta-path"):
                raise ValueError("SDK058 six-role installation proposal identity differs: " + role)
            proposals[role] = {"localAlias": local_role,
                               "sourceSha256": realized["proposal"]["sourceSha256"],
                               "schemaSha256": realized["proposal"]["schemaSha256"],
                               "proposalSha256": realized["proposal_sha256"]}
    return {"profile": selected["profile"], "bridgeSha256": selected["bridgeSha256"],
            "roles": proposals, "sparkStarted": False}


def write_new(path, raw):
    import os
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, raw) != len(raw):
            raise ValueError("short bounded packet write")
        os.fsync(fd)
    finally:
        os.close(fd)
