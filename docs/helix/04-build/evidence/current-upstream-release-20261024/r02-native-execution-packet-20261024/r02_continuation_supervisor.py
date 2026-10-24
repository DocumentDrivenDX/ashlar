#!/usr/bin/env python3
"""Prepare and supervise one reviewed R02 live-alias .53 continuation.

Preparation is effect-free. Execution is one private worker process, held in
the existing ordinal-1 publication scope for both live review stages.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
from pathlib import Path
import signal
import stat
import subprocess
import sys
import time
import uuid

PACKET = Path(__file__).resolve().parent
ROOT = PACKET.parents[5]
PRIMARY_ROOT = Path("/Users/erik/Projects/ashlar")
PACKET_RELATIVE = "docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024"
PREPUBLICATION_RESULT = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-prepublication-20261024/result.json"
PREPUBLICATION_ROOT = Path("/private/tmp/ashlar-r02-attempts/ashlar-r02-0b4774f5fb2c43bbb026da651aecd155")
ATTEMPT_PARENT = Path("/private/tmp/ashlar-r02-attempts")
RUNTIME_SOURCE_REVISION = "842551f96491d361040d6312da20750b65b3571b"
INDEX = Path("/private/tmp/ashlar-r02-weft053-index/index2.json")
PYTHON = Path("/private/tmp/ashlar-truss050-current-cohort-9c745278-r13/runtime/venv/bin/python3.12")
JAVA_HOME = Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home").resolve(strict=True)
LIMIT_SECONDS = 300
LIMIT_PRIVATE = 512 * 1024 * 1024
LIMIT_RECEIPTS = 16 * 1024 * 1024
LIMIT_OUTPUT = 1024 * 1024
LIMIT_FILE = 1024 * 1024
ORACLE_SHA256 = "f04f74668ac9adc46ac9928b01a4155ccad3aab6421cd297d018b7e45348aab0"
INDEX_SHA256 = "7dfa7adcf0fbd0215f64346dc14465626bd9c2426d4e1a95a861b0e99a146ea8"
COHORT_RECEIPT = ROOT / "docs/helix/04-build/evidence/current-upstream-release-20261024/truss050-r14-selected-source-lifecycle-20261024/r13-historical-preparation.json"
COHORT_RECEIPT_ID = "fe48796cead974bc06821bd296428f47b389e1016a7b43ff186ec15cb03f72e5"
JARS = {
    "/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-core_2.13-4.2.0.jar": "8e0a03b66338a4a38c9089d9f798ab6a6c6583aefe93f910311704a5ec90ffc4",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-sql-api_2.13-4.2.0.jar": "d789409a56db600c5160300fcd2c9651bf6f32dd8db1035a9bda17b67543b372",
    "/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars/spark-sql_2.13-4.2.0.jar": "ec0a0d699c6c57300ee47ba5d54b4d1f71d3e5401fb90a2bcdcd83b942084dbf",
}
SOURCE_FILES = (
    "src/ashlar_host/configured_local_runtime.py",
    "src/ashlar_host/configured_sdk_pages.py",
    "src/ashlar_host/configured_query_aliases.py",
    "src/ashlar_host/configured_local_read.py",
    "src/ashlar_host/configured_weft_query.py",
    "src/ashlar_host/configured_weft_compile.py",
    "src/ashlar_host/count_star_execution.py",
    "src/ashlar_host/configured_typed_graph_weft_projection.py",
    "src/ashlar_host/configured_sdk_page_oracle.py",
    "src/ashlar/weft_053_distribution.py",
)


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def digest(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical(value: object) -> bytes:
    return (json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False) + "\n").encode()


def write_new(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        if os.write(fd, raw) != len(raw):
            raise OSError("short write")
        os.fsync(fd)
    finally:
        os.close(fd)


def replace_state(path: Path, value: dict) -> None:
    temporary = path.with_name(path.name + ".next")
    write_new(temporary, canonical(value))
    os.replace(temporary, path)
    fd = os.open(path.parent, os.O_RDONLY)
    try: os.fsync(fd)
    finally: os.close(fd)


def _private_dir(path: Path) -> None:
    if path.is_symlink() or not path.is_dir():
        raise ValueError("R02 directory is absent or symlinked")
    st = path.lstat()
    if st.st_uid != os.getuid() or stat.S_IMODE(st.st_mode) != 0o700:
        raise ValueError("R02 directory must be user-owned mode 0700")


def _source_pins() -> dict:
    result = {}
    for relative in SOURCE_FILES:
        path = ROOT / relative
        digest_value = digest(path)
        blob = subprocess.check_output(["git", "-C", str(ROOT), "show", f"{RUNTIME_SOURCE_REVISION}:{relative}"])
        if sha(blob) != digest_value:
            raise ValueError(f"runtime source differs from reviewed source revision: {relative}")
        result[relative] = digest_value
    return result


def _prior_publication() -> dict:
    result_raw = PREPUBLICATION_RESULT.read_bytes()
    result = json.loads(result_raw)
    if (result.get("attemptId") != "0b4774f5fb2c43bbb026da651aecd155"
            or result.get("status") != "complete-held-after-publication"
            or result.get("sourceRevision") != "5298af9dd82c84e4929c0d7a3e286206f5037612"):
        raise ValueError("the reviewed R02 prepublication receipt is not the expected completed attempt")
    _private_dir(ATTEMPT_PARENT)
    _private_dir(PREPUBLICATION_ROOT)
    state_path = PREPUBLICATION_ROOT / "one-shot-state.json"
    vector_path = PREPUBLICATION_ROOT / "publication-vector.json"
    manifest_path = PREPUBLICATION_ROOT / "supervisor-manifest.json"
    review_path = PREPUBLICATION_ROOT / "sol-review.json"
    base_invocation = PREPUBLICATION_ROOT / "invocations" / "final-batch-1.json"
    for path in (state_path, vector_path, manifest_path, review_path, base_invocation):
        if path.is_symlink() or not path.is_file():
            raise ValueError("prepublication state/vector/review evidence is missing")
    state_raw, vector_raw = state_path.read_bytes(), vector_path.read_bytes()
    state, vector = json.loads(state_raw), json.loads(vector_raw)
    review = json.loads(review_path.read_bytes())
    normalized_vector = json.dumps(vector, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()
    if (sha(state_raw) != result.get("stateSha256") or sha(manifest_path.read_bytes()) != result.get("manifestSha256")
            or sha(normalized_vector) != result["publicationVector"]["sha256"]
            or state.get("status") != "complete-held-after-publication"
            or state.get("manifestSha256") != result.get("manifestSha256")
            or state.get("publicationVectorSha256") != sha(normalized_vector)
            or state.get("sourceRevision") != result.get("sourceRevision")
            or state.get("publicationVector") != vector
            or review.get("decision") != "execute" or review.get("reviewer") != "Sol"
            or review.get("manifestSha256") != result.get("manifestSha256")
            or sha(review_path.read_bytes()) != result["review"]["receiptSha256"]):
        raise ValueError("prepublication state, vector, manifest or review receipt does not match the durable result")
    roles = {tuple(table["name"])[-1] for table in vector.get("tables", []) if isinstance(table, dict)}
    if roles != {"edge_current", "object_current", "tombstone", "whole_source_history"} or len(vector["tables"]) != 4:
        raise ValueError("prepublication vector must have exactly the four graph roles")
    return {"resultSha256": sha(result_raw), "attemptId": state["attemptId"],
        "attemptRoot": str(PREPUBLICATION_ROOT), "sourceRevision": state["sourceRevision"],
        "statePath": str(state_path), "stateSha256": sha(state_raw),
        "vectorPath": str(vector_path), "vectorFileSha256": sha(vector_raw),
        "baseInvocationPath": str(base_invocation), "baseInvocationSha256": sha(base_invocation.read_bytes()),
        "vector": vector, "vectorSha256": sha(normalized_vector),
        "manifestSha256": state["manifestSha256"], "reviewSha256": sha(review_path.read_bytes())}


def _runtime_pins(attempt_root: Path) -> dict:
    if digest(INDEX) != INDEX_SHA256:
        raise ValueError("selected Weft 0.4.53 index2 digest changed")
    index = json.loads(INDEX.read_bytes())
    if index.get("version") != "0.4.53" or index.get("sourceCommit") != "726a840e956e25217d6aeea2674fdfcf3f342a24":
        raise ValueError("selected Weft index identity changed")
    python_version = subprocess.check_output([str(PYTHON), "-I", "-c",
        "import importlib.metadata as m,sys;print('.'.join(map(str,sys.version_info[:3])));print(m.version('weft-sql'));print(m.version('pyspark'));print(m.version('delta-spark'))"], text=True).splitlines()
    if python_version != ["3.12.14", "0.4.53", "4.2.0", "4.4.1"]:
        raise ValueError("selected Weft/Spark/Delta Python cohort changed")
    for raw_path, expected in JARS.items():
        if digest(Path(raw_path)) != expected:
            raise ValueError("selected Spark/Delta JAR digest changed")
    schema_dir = PRIMARY_ROOT / PACKET_RELATIVE / "weft053-schemas"
    for name, record in index["schemas"].items():
        schema = schema_dir / name
        if digest(schema) != record["sha256"]:
            raise ValueError("primary-checkout Weft schema bytes differ from selected .53 index")
        record["path"] = str(schema)
    index_raw = canonical(index)
    index_path = attempt_root / "weft053-index.json"
    if index_path.exists():
        if index_path.is_symlink() or index_path.read_bytes() != index_raw:
            raise ValueError("staged primary-checkout Weft index differs")
    else:
        write_new(index_path, index_raw)
    index_sha = sha(index_raw)
    probe = ("import sys;sys.path.insert(0," + repr(str(PRIMARY_ROOT / "src")) + ");"
        "from pathlib import Path;from ashlar.weft_053_distribution import Weft053Distribution,Weft053Paths;"
        "c=Weft053Distribution(Weft053Paths(Path(" + repr(str(index_path)) + ")," + repr(index_sha) + "));"
        "assert c._verify() and c.module.__version__=='0.4.53'")
    subprocess.run([str(PYTHON), "-I", "-B", "-c", probe], check=True,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    cohort = json.loads(COHORT_RECEIPT.read_bytes())
    if cohort.get("runtime", {}).get("receipt_sha256") != COHORT_RECEIPT_ID:
        raise ValueError("selected Spark/Delta cohort receipt changed")
    java = JAVA_HOME / "bin/java"
    return {"weftIndex": str(index_path), "weftIndexSha256": index_sha,
        "weftIndexSource": str(INDEX), "weftIndexSourceSha256": INDEX_SHA256,
        "weftRoot": index["root"], "weftModule": index["module"],
        "weftWheel": index["wheel"]["readPath"], "weftWheelSha256": index["wheel"]["sha256"],
        "weftVersion": "0.4.53", "weftSourceCommit": index["sourceCommit"],
        "weftSchemas": {name: record["sha256"] for name, record in index["schemas"].items()},
        "cohortReceipt": str(COHORT_RECEIPT), "cohortReceiptFileSha256": digest(COHORT_RECEIPT),
        "cohortReceiptSha256": COHORT_RECEIPT_ID,
        "python": str(PYTHON), "pythonSha256": digest(PYTHON.resolve(strict=True)),
        "pythonVersion": "3.12.14", "sparkVersion": "4.2.0", "deltaVersion": "4.4.1",
        "jdkHome": str(JAVA_HOME), "jdkExecutable": str(java.resolve(strict=True)),
        "jdkExecutableSha256": digest(java.resolve(strict=True)), "jdkVersion": "21",
        "jars": {Path(p).name: d for p, d in JARS.items()},
        "jarSha256": {Path(p).name: d for p, d in JARS.items()}}


def build_manifest(attempt_root: Path, attempt_id: str) -> dict:
    _private_dir(attempt_root.parent)
    _private_dir(attempt_root)
    prior = _prior_publication()
    runtime = _runtime_pins(attempt_root)
    packet = PRIMARY_ROOT / PACKET_RELATIVE
    oracle = packet / "source-oracle.json"
    gate = packet / "r02_live_alias_gate.py"
    worker = packet / "r02_live_alias_worker.py"
    packet_files = {p.name: digest(PACKET / p.name) for p in (oracle, gate, worker, Path(__file__))}
    packet_files["prepublication-result.json"] = prior["resultSha256"]
    packet_files["prepublication-manifest.json"] = prior["manifestSha256"]
    code = _source_pins()
    return {"profile": "ashlar-r02-live-alias-continuation/0.1", "phase": "continuation",
        "attemptId": attempt_id, "attemptRoot": str(attempt_root), "attemptParent": str(attempt_root.parent),
        "sourceRevision": RUNTIME_SOURCE_REVISION, "runtimeSourceFiles": code,
        "packetFiles": packet_files, "sourceOracleSha256": ORACLE_SHA256,
        "priorPublication": prior, "runtime": runtime,
        "invocation": {"ordinal": 1, "scopeCount": 1, "aliasScopeCount": 1,
            "worker": str(worker), "workerSha256": packet_files[worker.name],
            "python": str(PYTHON), "argv": [str(PYTHON), "-I", "-B", str(worker),
                "--manifest", str(attempt_root / "continuation-manifest.json"), "--repo", str(PRIMARY_ROOT)]},
        "reviewStages": ["pre-sql", "post-page1"],
        "limits": {"wholeProcessSeconds": LIMIT_SECONDS, "privateBytes": LIMIT_PRIVATE,
            "retainedReceiptBytes": LIMIT_RECEIPTS, "commandOutputBytes": LIMIT_OUTPUT,
            "singleArtifactBytes": LIMIT_FILE},
        "state": "held-for-supervisor-review"}


def prepare(parent: Path = ATTEMPT_PARENT) -> tuple[Path, str]:
    parent.mkdir(mode=0o700, parents=True, exist_ok=True)
    _private_dir(parent)
    attempt_id = uuid.uuid4().hex
    attempt = parent / f"ashlar-r02-continuation-{attempt_id}"
    attempt.mkdir(mode=0o700)
    _private_dir(attempt)
    manifest = build_manifest(attempt, attempt_id)
    raw = canonical(manifest)
    if len(raw) > LIMIT_FILE:
        raise ValueError("continuation manifest exceeds single-artifact cap")
    path = attempt / "continuation-manifest.json"
    write_new(path, raw)
    return path, sha(raw)


def verify_manifest(path: Path) -> dict:
    path = path.resolve(strict=True)
    manifest = json.loads(path.read_bytes())
    if (manifest.get("profile") != "ashlar-r02-live-alias-continuation/0.1"
            or manifest.get("phase") != "continuation" or manifest.get("reviewStages") != ["pre-sql", "post-page1"]
            or manifest.get("limits") != {"wholeProcessSeconds": LIMIT_SECONDS, "privateBytes": LIMIT_PRIVATE,
                "retainedReceiptBytes": LIMIT_RECEIPTS, "commandOutputBytes": LIMIT_OUTPUT, "singleArtifactBytes": LIMIT_FILE}):
        raise ValueError("R02 continuation profile, stages or ceilings differ")
    _private_dir(path.parent); _private_dir(path.parent.parent)
    if (path.name != "continuation-manifest.json" or manifest.get("attemptRoot") != str(path.parent)
            or path.parent.name != f"ashlar-r02-continuation-{manifest.get('attemptId')}"):
        raise ValueError("continuation manifest is not in its owned attempt root")
    expected = build_manifest(path.parent, manifest.get("attemptId", ""))
    if manifest != expected:
        raise ValueError("continuation manifest is stale or does not match live source/runtime/prepublication pins")
    return manifest


def review_receipt(path: Path, manifest_path: Path, manifest: dict) -> None:
    if path.name != "supervisor-review.json" or path.parent.resolve(strict=True) != manifest_path.parent:
        raise PermissionError("supervisor review must be retained in the exact private attempt")
    if path.is_symlink() or not path.is_file() or path.stat().st_size > 65536:
        raise PermissionError("supervisor review file is not a bounded regular receipt")
    receipt = json.loads(path.read_bytes())
    expected = {"profile": "ashlar-r02-continuation-supervisor-review/0.1", "decision": "execute",
        "reviewer": "Sol", "attemptId": manifest["attemptId"],
        "manifestSha256": digest(manifest_path), "sourceRevision": manifest["sourceRevision"],
        "priorStateSha256": manifest["priorPublication"]["stateSha256"],
        "publicationVectorSha256": manifest["priorPublication"]["vectorSha256"],
        "runtimeSha256": sha(canonical(manifest["runtime"])),
        "packetFilesSha256": sha(canonical(manifest["packetFiles"]))}
    if receipt != expected:
        raise PermissionError("Sol review does not authorize these exact R02 continuation packet bytes")


def stop_group(process: subprocess.Popen) -> None:
    try:
        os.killpg(process.pid, signal.SIGTERM)
    except ProcessLookupError:
        return
    deadline = time.monotonic() + 1
    while time.monotonic() < deadline:
        try: os.killpg(process.pid, 0)
        except ProcessLookupError: return
        time.sleep(.05)
    try: os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError: return
    deadline = time.monotonic() + 2
    while time.monotonic() < deadline:
        try: os.killpg(process.pid, 0)
        except ProcessLookupError: return
        time.sleep(.05)
    raise RuntimeError("R02 continuation process group did not close")


def tree_bytes(root: Path) -> int:
    total = 0
    resolved = root.resolve(strict=True)
    for path in root.rglob("*"):
        if path.is_symlink() or resolved not in path.resolve(strict=True).parents:
            raise ValueError("continuation attempt contains a symlink or escaped path")
        if path.is_file():
            total += path.stat().st_size
    return total


def process_inventory(attempt: Path) -> tuple[Path, str]:
    raw = subprocess.check_output(["ps", "-axo", "pid=,comm="])
    active = []
    for line in raw.decode("utf-8", errors="strict").splitlines():
        parts = line.strip().split(None, 1)
        if len(parts) != 2 or parts[0] == str(os.getpid()):
            continue
        name = Path(parts[1]).name.lower()
        if name in {"java", "spark-submit", "spark-class"}:
            active.append(parts[0] + " " + name)
    if active:
        raise RuntimeError("another Java/Spark process is active; R02 continuation stopped")
    path = attempt / "process-inventory.txt"
    write_new(path, raw)
    return path, sha(raw)


def execute(path: Path, review: Path) -> None:
    manifest = verify_manifest(path)
    if path.parent.parent.resolve(strict=True) != ATTEMPT_PARENT.resolve(strict=True):
        raise ValueError("native continuation must use the private R02 attempt parent")
    review_receipt(review, path, manifest)
    attempt = path.parent
    state_path = attempt / "supervisor-state.json"
    if state_path.exists() or state_path.is_symlink():
        raise ValueError("this continuation attempt has already been consumed")
    inventory_path, inventory_sha = process_inventory(attempt)
    ledger = attempt.parent / f"ashlar-r02-continuation-consumed-{manifest['attemptId']}.json"
    write_new(ledger, canonical({"attemptId": manifest["attemptId"], "manifestSha256": digest(path), "status": "consumed"}))
    parent_fd = os.open(ledger.parent, os.O_RDONLY)
    try: os.fsync(parent_fd)
    finally: os.close(parent_fd)
    log = attempt / "worker.log"
    started = time.monotonic(); deadline = started + LIMIT_SECONDS
    write_new(state_path, canonical({"attemptId":manifest["attemptId"],"manifestSha256":digest(path),"status":"running"}))
    with log.open("xb") as stream:
        process = subprocess.Popen(manifest["invocation"]["argv"], cwd=ROOT,
            stdin=subprocess.DEVNULL, stdout=stream, stderr=subprocess.STDOUT,
            env={"PATH":"/usr/bin:/bin","HOME":os.environ.get("HOME","/nonexistent"),
                "LANG":"C.UTF-8","LC_ALL":"C.UTF-8","JAVA_HOME":manifest["runtime"]["jdkHome"],
                "PYSPARK_PYTHON":str(PYTHON),"PYSPARK_DRIVER_PYTHON":str(PYTHON),
                "PYTHONDONTWRITEBYTECODE":"1","SPARK_LOCAL_IP":"127.0.0.1","TMPDIR":str(attempt)},
            start_new_session=True)
        while process.poll() is None:
            if time.monotonic() >= deadline or log.stat().st_size > LIMIT_OUTPUT or tree_bytes(attempt) > LIMIT_PRIVATE:
                os.killpg(process.pid, signal.SIGKILL); process.wait(); stop_group(process)
                raise TimeoutError("R02 continuation exceeded time, output or private storage cap")
            time.sleep(1.0)
        process.wait(); stop_group(process)
    if process.returncode != 0 or log.stat().st_size > LIMIT_OUTPUT:
        raise RuntimeError("R02 live-alias worker failed or exceeded output cap")
    result_path = attempt / "continuation-result.json"
    result = json.loads(result_path.read_bytes())
    stage1 = attempt / "pre-sql-manifest.json"
    review1 = attempt / "pre-sql-review.json"
    stage2 = attempt / "post-page1-manifest.json"
    review2 = attempt / "post-page1-review.json"
    if not all(p.is_file() and not p.is_symlink() for p in (result_path, stage1, review1, stage2, review2)):
        raise ValueError("both reviewed gate stages and closing result are required")
    first, second = json.loads(stage1.read_bytes()), json.loads(stage2.read_bytes())
    prior_vector = manifest["priorPublication"]["vector"]
    expected_gate_vector = {"publicationId":prior_vector["id"],"manifestUuid":prior_vector["manifestUuid"],
        "tables":{item["name"][-1]:{"uuid":item["uuid"],"version":item["version"]}
            for item in prior_vector["tables"]}}
    def check_gate_review(stage_manifest, receipt_path, expected_stage):
        receipt = json.loads(receipt_path.read_bytes())
        return (stage_manifest.get("stage") == expected_stage
            and receipt.get("profile") == "ashlar-r02-live-alias-review-receipt/0.1"
            and receipt.get("decision") == "approve" and receipt.get("reviewer") == "Sol"
            and receipt.get("attemptId") == manifest["attemptId"]
            and receipt.get("stage") == expected_stage
            and receipt.get("manifestSha256") == digest(stage1 if expected_stage == "pre-sql" else stage2)
            and stage_manifest.get("sourceRevision") == manifest["sourceRevision"]
            and stage_manifest.get("publicationVector") == expected_gate_vector
            and stage_manifest.get("sparkApplicationId") == first.get("sparkApplicationId")
            and stage_manifest.get("aliases") == first.get("aliases")
            and stage_manifest.get("runtime", {}).get("weftIndexSha256") == manifest["runtime"]["weftIndexSha256"]
            and stage_manifest.get("runtime", {}).get("continuationRunnerSha256") == manifest["runtimeSourceFiles"]["src/ashlar_host/configured_sdk_pages.py"]
            and stage_manifest.get("runtime", {}).get("liveAliasGateSha256") == manifest["packetFiles"]["r02_live_alias_gate.py"]
            and stage_manifest.get("runtime", {}).get("priorPublicationSourceRevision") == manifest["priorPublication"]["sourceRevision"]
            and receipt.get("publicationVectorSha256") == stage_manifest.get("publicationVectorSha256")
            and receipt.get("aliasMapSha256") == stage_manifest.get("aliasMapSha256")
            and receipt.get("runtimeSha256") == stage_manifest.get("runtimeSha256"))
    if (first.get("state") != "held-for-review" or second.get("state") != "held-for-review"
            or [item.get("name") for item in first.get("artifacts",[])] != ["singleton","backend03-original","backend03-positive","backend02-page1"]
            or [item.get("name") for item in second.get("artifacts",[])] != ["backend02-continuation"]
            or second.get("priorStageManifestSha256") != digest(stage1)
            or second.get("priorStageReviewSha256") != digest(review1)
            or not check_gate_review(first,review1,"pre-sql")
            or not check_gate_review(second,review2,"post-page1")
            or result.get("profile") != "ashlar-r02-weft053-continuation-result/0.1"
            or result.get("status") != "complete" or result.get("scopeClosed") is not True
            or result.get("scopeOrdinal") != 1 or result.get("scopeCount") != 1
            or result.get("aliasScopeCount") != 1 or result.get("reviewStages") != ["pre-sql","post-page1"]
            or result.get("protectedAck") is not False
            or result.get("publicationVectorSha256") != manifest["priorPublication"]["vectorSha256"]):
        raise ValueError("continuation result does not close both reviewed stages against the exact prior vector")
    receipt_bytes = sum(p.stat().st_size for p in attempt.iterdir() if p.is_file())
    if tree_bytes(attempt) > LIMIT_PRIVATE or receipt_bytes > LIMIT_RECEIPTS:
        raise ValueError("R02 continuation exceeded private or retained receipt cap")
    replace_state(state_path, {"profile":"ashlar-r02-continuation-supervisor-state/0.1",
        "attemptId":manifest["attemptId"],"manifestSha256":digest(path),"status":"complete",
        "elapsedSeconds":round(time.monotonic()-started,3),"workerExit":process.returncode,
        "resultSha256":digest(result_path),"preSqlManifestSha256":digest(stage1),
        "preSqlReviewSha256":digest(review1),"postPage1ManifestSha256":digest(stage2),
        "postPage1ReviewSha256":digest(review2),"processGroupClosed":True,
        "processInventorySha256":inventory_sha,
        "privateBytes":tree_bytes(attempt),"receiptBytes":receipt_bytes})


def main() -> int:
    parser = argparse.ArgumentParser()
    commands = parser.add_subparsers(dest="command", required=True)
    commands.add_parser("prepare")
    run = commands.add_parser("run"); run.add_argument("--manifest", type=Path, required=True); run.add_argument("--review", type=Path, required=True)
    args = parser.parse_args()
    try:
        if args.command == "prepare":
            path, digest_value = prepare(); print(json.dumps({"manifest":str(path),"sha256":digest_value}, sort_keys=True))
        else:
            execute(args.manifest,args.review)
    except Exception as exc:
        print(f"R02 continuation supervisor refused: {exc}", file=sys.stderr); return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
