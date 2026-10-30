"""Small G03 packet boundary reusing the reviewed `.4` init attestations."""
from __future__ import annotations

import hashlib
import importlib.util
import json
from pathlib import Path
import subprocess
import sys


PACKET = Path(__file__).resolve().parent
PROFILE = "ashlar-g03-local-delta4-transaction1-packet/0.2"
REPO = PACKET.parents[6]
sys.path.insert(0, str(REPO / "src"))
INIT_PACKET = (REPO / "docs/helix/04-build/evidence/current-upstream-release-20261024/"
               "configured-local-delta4-init-20261024/one-shot-packet")
_SPEC = importlib.util.spec_from_file_location("ashlar_delta4_init_packet_common",
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
        raise ValueError("G03 transaction-1 packet manifest refused")
    return value


def packet_digest():
    return _INIT.packet_tree_digest()


def verify_packet(input_root=None, *, runtime=True):
    """Verify frozen source/oracle, current code and selected local runtime."""
    selected = manifest()
    current_commit = subprocess.run(
        [selected["umfProducer"]["git"], "-C", str(REPO), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True, timeout=10).stdout.strip()
    ancestor = subprocess.run(
        [selected["umfProducer"]["git"], "-C", str(REPO), "merge-base", "--is-ancestor",
         selected["sourceCommit"], current_commit], capture_output=True, timeout=10)
    if ancestor.returncode != 0:
        raise ValueError("reviewed code commit is not an ancestor of current HEAD")
    _INIT.verify_source_tree(selected)
    root = Path(input_root) if input_root is not None else PACKET / "inputs"
    _INIT.verify_inputs(root, selected)
    oracle_path = root / "source-oracle.json"
    if sha_file(oracle_path) != selected["inputs"].get("source-oracle.json"):
        raise ValueError("G03 source oracle digest differs")
    oracle = json.loads(oracle_path.read_bytes())
    if (oracle.get("profile") != "ashlar-g03-delta4-synthetic-source-oracle-candidate/0.1"
            or oracle.get("inputs", {}).get("publicationEventCount") != 8
            or oracle.get("inputs", {}).get("publicationTransaction") != "typed-graph-1"
            or oracle.get("queryExpected", {}).get("singletonPerson1", {}).get(
                "quantityJsonText") != "9007199254740993"):
        raise ValueError("Exact frozen G03 transaction-1 source oracle required")
    runtime_receipt = None
    if runtime:
        runtime_receipt = _INIT.verify_packet_assets(root)
    return {"profile": PROFILE, "sourceCommit": selected["sourceCommit"],
            "currentCommit": current_commit,
            "packetTreeSha256": packet_digest(),
            "sourceOracleSha256": selected["inputs"]["source-oracle.json"],
            "runtime": runtime_receipt}


def verify_umf_bridge(installation_path):
    return _INIT.verify_umf_bridge(Path(installation_path), manifest())


def write_new(path, raw):
    import os
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, raw) != len(raw):
            raise ValueError("short bounded packet write")
        os.fsync(fd)
    finally:
        os.close(fd)
