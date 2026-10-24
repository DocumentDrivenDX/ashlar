#!/usr/bin/env python3
"""Build a private, effect-free R02 prepublication packet from pinned inputs."""
from __future__ import annotations

import argparse
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import shutil
import stat
import subprocess
import sys
import tempfile
import uuid


PACKET = Path(__file__).resolve().parent
ROOT = PACKET.parents[5]
if str(PACKET) not in sys.path:
    sys.path.insert(0, str(PACKET))
ATTEMPT_PARENT = Path("/private/tmp/ashlar-r02-attempts")
UMF_ROOT = Path("/Users/erik/Projects/umf")
UMF_REVISION = "2ede4c879bea5db13794a6fa30e67c821306c17f"
PYTHON = Path("/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python3.12")
JAVA_HOME = Path("/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home").resolve(strict=True)
BUN = Path("/opt/homebrew/Cellar/bun/1.4.3/bin/bun")
GIT = Path("/opt/homebrew/Cellar/git/2.56.0_1/bin/git")
JAR_ROOT = Path("/private/tmp/ashlar-release-20261021-m3rde5p2/jars")
SPARK_JARS = Path("/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pyspark/jars")
UMF_VALIDATION_PYTHON = Path("/private/tmp/umf056-public-wheel-verify/venv/bin/python3.12")
WEFT_PYTHON = Path("/private/tmp/ashlar-weft053-adoption-20261023/venv/bin/python3.12")
LIMITS = {"wallSeconds": 300, "privateBytes": 512 * 1024 * 1024,
          "receiptBytes": 16 * 1024 * 1024, "operationOutputBytes": 1024 * 1024}
MAX_STAGED_BYTES = 384 * 1024 * 1024  # reserve at least 128 MiB for native output and logs
RUNTIME_JARS = {
    "delta-spark_4.2_2.13-4.4.1.jar": "2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be",
    "delta-storage-4.4.1.jar": "f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f",
    "spark-core_2.13-4.2.0.jar": "8e0a03b66338a4a38c9089d9f798ab6a6c6583aefe93f910311704a5ec90ffc4",
    "spark-sql-api_2.13-4.2.0.jar": "d789409a56db600c5160300fcd2c9651bf6f32dd8db1035a9bda17b67543b372",
    "spark-sql_2.13-4.2.0.jar": "ec0a0d699c6c57300ee47ba5d54b4d1f71d3e5401fb90a2bcdcd83b942084dbf",
}
SOURCE_HASHES = {
    "examples/end-to-end/typed-graph-model.umf.json": "19ab3cc44efd769346b88fb7c5dc6792b59fd0057ed8846e4a0eec744f8e716b",
    "examples/end-to-end/configured-typed-graph-source.json": "040545b8cc96701a158b8936750c89f97ce991df9652ceeacf6c66fb9d9fc168",
    "examples/end-to-end/typed-graph-source.jsonl": "9edb7566f82324fadb1e7cf1ba0eaf22691096a26209e938c83730d096d4882b",
    "examples/end-to-end/typed-graph-binding.json": "1ab1416d5a36a082c4d5be38dc061377bdae22bb5395fb8cfbec8cee80910863",
}
PACKET_HASHED = ("r02_packet_generator.py", "r02_supervisor.py", "r02_invocation_worker.py",
                 "source-oracle.json", "umf-validation.json", "positive-backend03-request.json",
                 "positive-backend03-response.json", "positive-backend03-compiler-receipt.json",
                 "source-admission-preflight.json",
                 "compile-positive-backend03.py", "examples/configured-local/configuration.template.json",
                 "attempt-refusal-23732236f7e94550b2fa24995c603b75.json",
                 "attempt-refusal-03969306f68446f0802e9a6e039bcb32.json",
                 "attempt-refusal-7d7ecbde56c44dcc9b2f21acc249e8fd.json")


def sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def canonical(value: object) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode("utf-8") + b"\n"


def checked_hashes() -> dict[str, str]:
    for name, expected in SOURCE_HASHES.items():
        if sha256(ROOT / name) != expected:
            raise ValueError(f"source bytes changed: {name}")
    return {name: sha256(ROOT / name) for name in SOURCE_HASHES}


def export_umf(attempt: Path) -> tuple[Path, str, str]:
    """Materialize the committed source/spec closure into a private sparse worktree."""
    destination = attempt / "umf-producer"
    subprocess.run([str(GIT), "clone", "--quiet", "--shared", "--no-checkout", str(UMF_ROOT), str(destination)], check=True)
    def git(*args: str) -> str:
        return subprocess.check_output([str(GIT), "-C", str(destination), "-c", "core.fsmonitor=false",
                                        "-c", "core.hooksPath=/dev/null", *args], text=True).strip()
    revision = git("rev-parse", f"{UMF_REVISION}^{{commit}}")
    if revision != UMF_REVISION:
        raise ValueError("pinned UMF commit is unavailable")
    tree = git("rev-parse", f"{revision}^{{tree}}")
    git("sparse-checkout", "init", "--cone")
    git("sparse-checkout", "set", "src", "spec", "native")
    git("checkout", "--quiet", "--detach", revision)
    if git("rev-parse", "HEAD") != UMF_REVISION or git("status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("sparse UMF checkout is not clean at the pinned commit")
    # Bun resolves the source package's committed imports through this ignored
    # dependency tree. Keep its bytes inside the private attempt instead of
    # relying on a second source checkout or package manager during execution.
    shutil.copytree(UMF_ROOT / "node_modules", destination / "node_modules", symlinks=True)
    for link in (destination / "node_modules").rglob("*"):
        if link.is_symlink() and destination.resolve() not in link.resolve(strict=True).parents:
            raise ValueError("UMF dependency symlink escapes the private attempt root")
    if git("status", "--porcelain", "--untracked-files=normal"):
        raise ValueError("private UMF producer checkout is not clean after dependency staging")
    inventory = git("ls-tree", "-r", "-l", revision, "--", "src", "spec", "native")
    files = {}
    for row in inventory.splitlines():
        metadata, name = row.split("\t", 1)
        mode, kind, _object, _size = metadata.split()
        if kind != "blob" or mode == "120000":
            continue
        files[name] = sha256(destination / name)
    actual = {str(path.relative_to(destination)): sha256(path)
              for base in (destination / "src", destination / "spec", destination / "native")
              for path in base.rglob("*") if path.is_file() and not path.is_symlink()}
    if actual != files:
        raise ValueError("exported UMF source tree does not match the pinned Git object inventory")
    return destination, revision, tree


def runtime_versions() -> dict[str, object]:
    code = "import importlib.metadata as m,sys; print(m.version('pyspark')); print(m.version('delta-spark')); print(sys.version_info.major,sys.version_info.minor,sys.version_info.micro)"
    raw = subprocess.check_output([str(PYTHON), "-I", "-c", code], text=True).splitlines()
    if len(raw) != 3 or raw[:2] != ["4.2.0", "4.4.1"] or raw[2].split()[:2] != ["3", "12"]:
        raise ValueError("selected R05 local runtime versions differ from the packet")
    java_release = (JAVA_HOME / "release").read_text(encoding="utf-8")
    if 'JAVA_VERSION="21' not in java_release:
        raise ValueError("selected JDK is not Java 21")
    umf_core = subprocess.check_output([str(UMF_VALIDATION_PYTHON), "-I", "-c", "import importlib.metadata as m; print(m.version('umf-core'))"], text=True).strip()
    weft = subprocess.check_output([str(WEFT_PYTHON), "-I", "-c", "import importlib.metadata as m; print(m.version('weft-sql'))"], text=True).strip()
    r05_weft = subprocess.check_output([str(PYTHON), "-I", "-c", "import importlib.metadata as m; print(m.version('weft-sql'))"], text=True).strip()
    if umf_core != "0.10.1" or weft != "0.4.53":
        raise ValueError("selected public UMF or Weft verification environment has drifted")
    from r02_supervisor import HOST_SOURCES
    modules = {name.removeprefix("src/").removesuffix(".py").replace("/", "."): expected
               for name, expected in HOST_SOURCES.items()}
    probe = ("import hashlib,importlib,json,pathlib; names=" + repr(modules) + "; out={}; "
             "exec('for n in names:\\n m=importlib.import_module(n); p=pathlib.Path(m.__file__).resolve(); out[n]=[str(p),hashlib.sha256(p.read_bytes()).hexdigest()]'); print(json.dumps(out,sort_keys=True))")
    installed = json.loads(subprocess.check_output([str(PYTHON), "-I", "-c", probe], text=True))
    for module, expected in modules.items():
        path, observed = installed[module]
        if observed != expected or "/site-packages/ashlar_host/" not in path:
            raise ValueError(f"selected R05 Ashlar module differs from current pinned source: {module}")
    python_binary = PYTHON.resolve(strict=True)
    java_binary = (JAVA_HOME / "bin/java").resolve(strict=True)
    return {"nativePython": str(PYTHON), "nativePythonVersion": raw[2],
            "nativePythonExecutableSha256": sha256(python_binary), "pyspark": raw[0],
            "deltaSpark": raw[1], "r05WeftSql": r05_weft, "javaHome": str(JAVA_HOME),
            "javaExecutableSha256": sha256(java_binary),
            "javaReleaseSha256": hashlib.sha256(java_release.encode()).hexdigest(),
            "umfCoreValidationPython": str(UMF_VALIDATION_PYTHON), "umfCore": umf_core,
            "weftCompilerPython": str(WEFT_PYTHON), "weftSql": weft,
            "installedHostModules": installed,
            "weftUse": "continuation only; no Weft request is executed in prepublication"}


def create_packet(output_root: Path | None = None) -> Path:
    hashes = checked_hashes()
    runtime = runtime_versions()
    for name, expected in RUNTIME_JARS.items():
        source = (JAR_ROOT / name) if name.startswith("delta-") else (SPARK_JARS / name)
        if sha256(source) != expected:
            raise ValueError(f"runtime JAR hash mismatch: {name}")
    if not all(path.is_file() for path in (PYTHON, JAVA_HOME / "bin/java", BUN, GIT)):
        raise ValueError("required selected runtime executable is missing")
    ATTEMPT_PARENT.mkdir(mode=0o700, parents=True, exist_ok=True)
    parent_stat = ATTEMPT_PARENT.stat()
    if parent_stat.st_uid != os.getuid() or stat.S_IMODE(parent_stat.st_mode) != 0o700:
        raise ValueError("attempt parent must be mode 0700 and user-owned")
    attempt_id = uuid.uuid4().hex
    attempt = (output_root or ATTEMPT_PARENT) / f"ashlar-r02-{attempt_id}"
    if attempt.parent != ATTEMPT_PARENT:
        raise ValueError("attempt root must be directly beneath the fixed private parent")
    attempt.mkdir(mode=0o700)
    try:
        make_private_layout(attempt)
        umf_source, umf_commit, umf_tree = export_umf(attempt)
        source_admission = validate_staged_source(attempt, umf_source)
        invocation_templates = make_invocations(attempt, attempt_id, umf_source)
        parser_validation = validate_cli_invocations(invocation_templates)
        packet_files = {}
        for filename in PACKET_HASHED:
            source = ROOT / filename if filename.startswith("examples/") else PACKET / filename
            packet_files[filename] = sha256(source)
        shutil.copyfile(PACKET / "r02_invocation_worker.py", attempt / "r02_invocation_worker.py")
        operations = []
        for case, kind, template in invocation_templates:
            template_hash = sha256(template)
            operations.append({"kind": kind, "case": case,
                               "templateSha256": template_hash,
                               "argv": [str(PYTHON), "-I", "-B", str(attempt / "r02_invocation_worker.py"),
                                        "--attempt", str(attempt), "--case", case,
                                        "--template", str(template), "--python", str(PYTHON),
                                        "--java-home", str(JAVA_HOME), "--template-sha256", template_hash]})
        git_head = subprocess.check_output([str(GIT), "-C", str(ROOT), "rev-parse", "HEAD"], text=True).strip()
        manifest = {
            "profile": "ashlar-r02-one-shot/0.1", "phase": "prepublication", "attemptId": attempt_id,
            "attemptParent": str(ATTEMPT_PARENT), "attemptRoot": str(attempt),
            "sourceRevision": git_head, "sourceFiles": hashes,
            "hostSourceFiles": {name: sha256(ROOT / name) for name in __import__("r02_supervisor").HOST_SOURCES},
            "packetFiles": packet_files, "runtimeFiles": {name: value for name, value in RUNTIME_JARS.items()},
            "runtimeIdentity": runtime | {"jars": RUNTIME_JARS, "umfProducerCommit": umf_commit,
                                          "umfProducerTree": umf_tree, "umfProducerPath": str(umf_source)},
            "limits": LIMITS,
            "invocations": [str(path) for _case, _kind, path in invocation_templates],
            "invocationTemplates": [{"case": case, "kind": kind, "path": str(path), "sha256": sha256(path)}
                                    for case, kind, path in invocation_templates],
            "selectedCliParserValidation": parser_validation,
            "selectedCliSourceAdmission": source_admission,
            "attemptFiles": {
                "input/configured-typed-graph-source.json": sha256(attempt / "input/configured-typed-graph-source.json"),
                "input/typed-graph-model.umf.json": sha256(attempt / "input/typed-graph-model.umf.json"),
                "input/typed-graph-source.jsonl": sha256(attempt / "input/typed-graph-source.jsonl"),
                "input/typed-graph-binding.json": sha256(attempt / "input/typed-graph-binding.json"),
                "jars/delta-spark_4.2_2.13-4.4.1.jar": RUNTIME_JARS["delta-spark_4.2_2.13-4.4.1.jar"],
                "jars/delta-storage-4.4.1.jar": RUNTIME_JARS["delta-storage-4.4.1.jar"],
                "jars/inventory.json": sha256(attempt / "jars/inventory.json"),
            },
            "operations": operations,
            "publicationVectorPath": "publication-vector.json",
            "extractorProfile": "ashlar-r02-publication-vector/0.1",
        }
        manifest_path = attempt / "supervisor-manifest.json"
        manifest_path.write_bytes(canonical(manifest))
        os.chmod(manifest_path, 0o600)
        staged_bytes = sum(path.stat().st_size for path in attempt.rglob("*")
                           if path.is_file() and not path.is_symlink())
        if staged_bytes > MAX_STAGED_BYTES:
            raise ValueError("effect-free packet staging exceeds 384 MiB; at least 128 MiB must remain for execution")
        return attempt
    except BaseException:
        # Keep failed roots for diagnosis and to prevent accidental reuse.
        raise


def make_private_layout(attempt: Path) -> None:
    for name in ("input", "jars", "outputs", "invocations", "receipts", "tmp"):
        (attempt / name).mkdir(mode=0o700)
    for rel in SOURCE_HASHES:
        source = ROOT / rel
        if rel.endswith("configured-typed-graph-source.json"):
            target = attempt / "input" / "configured-typed-graph-source.json"
        else:
            target = attempt / "input" / Path(rel).name
        shutil.copyfile(source, target)
    # The source configuration uses relative paths; put its three referenced files together.
    cfg = attempt / "input" / "configured-typed-graph-source.json"
    value = json.loads(cfg.read_text(encoding="utf-8"))
    for field in ("source", "schema", "bindings"):
        name = Path(value[field]["path"]).name
        target = attempt / "input" / name
        if value[field]["path"] != name or not target.is_file() or sha256(target) != value[field]["sha256"]:
            raise ValueError(f"pinned source configuration does not resolve to staged {field} bytes")
    for name in ("delta-spark_4.2_2.13-4.4.1.jar", "delta-storage-4.4.1.jar"):
        shutil.copyfile(JAR_ROOT / name, attempt / "jars" / name)
    inventory = {name: RUNTIME_JARS[name] for name in ("delta-spark_4.2_2.13-4.4.1.jar", "delta-storage-4.4.1.jar")}
    (attempt / "jars" / "inventory.json").write_bytes(canonical(inventory))


def make_invocations(attempt: Path, attempt_id: str, umf_source: Path) -> list[tuple[str, str, Path]]:
    template = json.loads((ROOT / "examples/configured-local/configuration.template.json").read_text(encoding="utf-8"))
    cases = [("initialize", "initialize", "initialize", 0), ("batch-1", "publish", "publish", 1),
             ("batch-2", "publish", "publish", 2), ("batch-3", "publish", "publish", 3),
             ("batch-3-replay", "replay", "publish", 3)]
    result = []
    for case, kind, operation, ordinal in cases:
        config = dict(template)
        config.update({"operation": operation, "ordinal": ordinal,
                       "python": str(PYTHON), "installation": str(attempt / "installation"),
                       "installation_id": f"r02-{attempt_id}", "installation_sha256": None if operation == "initialize" else "@from-initialization@",
                       "output": str(attempt / "outputs" / case), "jars": str(attempt / "jars"),
                       "java_home": str(JAVA_HOME), "source_configuration": str(attempt / "input" / "configured-typed-graph-source.json"),
                       "stream": f"r02-{attempt_id}",
                       "producer": dict(template["producer"], source=str(umf_source), bun=str(BUN), git=str(GIT)),
                       "bounds": {"whole_seconds": 300, "stream_bytes": 1024 * 1024,
                                  "receipt_bytes": 262144, "maximum_admission_receipt_bytes": 4194304}})
        path = attempt / "invocations" / f"{case}.template.json"
        path.write_bytes(canonical(config))
        result.append((case, kind, path))
    return result


def validate_cli_invocations(values: list[tuple[str, str, Path]]) -> list[dict[str, str]]:
    """Parse generated templates in the selected installed CLI without running them."""
    parser = '''import json,sys
from pathlib import Path
from ashlar_host.configured_local_cli import parse_configured_local
out=[]
for raw in sys.argv[1:]:
 p=Path(raw); v=json.loads(p.read_bytes())
 if v["operation"] != "initialize": v["installation_sha256"]="0"*64
 parsed=parse_configured_local(json.dumps(v,sort_keys=True,separators=(",",":")).encode())
 out.append({"name":p.name,"operation":parsed.operation})
print(json.dumps(out,sort_keys=True))
'''
    output = subprocess.check_output([str(PYTHON), "-I", "-c", parser,
                                      *[str(path) for _case, _kind, path in values]], text=True)
    parsed = json.loads(output)
    expected = [(path.name, "initialize" if case == "initialize" else "publish")
                for case, _kind, path in values]
    if [(item.get("name"), item.get("operation")) for item in parsed] != expected:
        raise ValueError("selected installed configured-local parser did not admit all five operations")
    return parsed


def validate_staged_source(attempt: Path, umf_source: Path) -> dict[str, object]:
    """Run only configured-source admission against these staged, pinned inputs."""
    output = attempt / "outputs" / "source-admission"
    environment = {"HOME": "/nonexistent", "TMPDIR": str(attempt / "tmp"),
                   "GIT_CONFIG_NOSYSTEM": "1", "GIT_CONFIG_GLOBAL": "/dev/null",
                   "GIT_CONFIG_SYSTEM": "/dev/null", "GIT_TERMINAL_PROMPT": "0",
                   "GIT_OPTIONAL_LOCKS": "0", "LANG": "C.UTF-8", "LC_ALL": "C.UTF-8",
                   "PATH": "/usr/bin:/bin"}
    command = [str(PYTHON), "-I", "-m", "ashlar", "validate-configured-source",
               "--configuration", str(attempt / "input" / "configured-typed-graph-source.json"),
               "--producer-source", str(umf_source), "--bun", str(BUN), "--git", str(GIT),
               "--output", str(output), "--producer-timeout-seconds", "30",
               "--producer-maximum-output-bytes", "1048576",
               "--producer-maximum-receipt-bytes", "4194304", "--maximum-receipt-bytes", "4194304"]
    result = subprocess.run(command, cwd=attempt, env=environment, capture_output=True, timeout=90)
    if result.returncode != 0 or result.stderr or len(result.stdout) > LIMITS["operationOutputBytes"]:
        raise ValueError("selected CLI staged configured-source admission failed")
    report = json.loads(result.stdout)
    if report.get("admitted") is not True or report.get("published") is not False or report.get("acknowledged") is not False:
        raise ValueError("selected CLI source-admission report has unexpected authority")
    return {"reportSha256": hashlib.sha256(result.stdout).hexdigest(),
            "reportBytes": len(result.stdout), "profile": report.get("profile"),
            "producerRevision": report.get("producer_revision"), "batches": report.get("batches"),
            "records": report.get("records"), "published": False, "acknowledged": False}


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-root", type=Path, default=ATTEMPT_PARENT)
    args = parser.parse_args()
    try:
        path = create_packet(args.output_root)
    except Exception as exc:
        print(f"R02 packet generator refused: {exc}", file=sys.stderr)
        return 2
    manifest = path / "supervisor-manifest.json"
    print(json.dumps({"attemptRoot": str(path), "manifest": str(manifest),
                      "manifestSha256": sha256(manifest), "attemptId": path.name.removeprefix("ashlar-r02-")},
                     sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
