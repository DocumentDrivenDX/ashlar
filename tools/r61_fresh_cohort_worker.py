#!/usr/bin/env python3
"""One bounded fresh Truss .54 / UMF .10.2 native qualification worker.

The outer R61 supervisor owns the wall, aggregate RSS and output limits. This
worker owns the selected Truss/Spark lifecycles and emits a completion receipt
only after source, destination, Spark and PostgreSQL owners have closed.
"""
from __future__ import annotations

import base64
import hashlib
import importlib.metadata
import json
import os
from pathlib import Path
import subprocess
import sys
import traceback

REPO = Path(__file__).resolve().parents[1]
SCOPE = Path(os.environ["R61_SCOPE"])
ROOT = Path(os.environ["R61_OUTPUT_ROOT"])
COMPLETION = ROOT / "worker-completion.json"
MODEL = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json"
CONSUMER = "ashlar-r61-local-consumer"
FEED = "ashlar-r61-local-feed"
DESTINATION = "ashlar-r61-local-delta-0.4"
PROFILE = "ashlar-r61-fresh-cohort-worker/0.1"
EFFECTS = {"delta_write_state": "not_attempted", "source_ack_attempted": False,
           "source_ack_state": "not_attempted", "spark_started": False,
           "spark_closed": False, "postgres_start_attempted": False,
           "postgres_started": False,
           "postgres_closed": False, "graphframes_started": False,
           "graphframes_closed": False, "custody_sessions": []}


def sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def tree_digest(root: Path) -> str:
    digest = hashlib.sha256()
    for path in sorted(root.rglob("*"), key=lambda item: item.relative_to(root).as_posix()):
        if path.is_symlink():
            raise RuntimeError("symlink in selected Delta root")
        if path.is_file():
            relative = path.relative_to(root).as_posix().encode()
            digest.update(len(relative).to_bytes(4, "big"))
            digest.update(relative)
            digest.update(sha(path.read_bytes()).encode())
    return digest.hexdigest()


def canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def write_private(path: Path, value: dict) -> str:
    raw = canonical(value) + b"\n"
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
    try:
        offset = 0
        while offset < len(raw):
            offset += os.write(fd, raw[offset:])
        os.fsync(fd)
    finally:
        os.close(fd)
    return sha(raw)


def _install_runtime(scope: dict) -> dict:
    artifacts = scope["artifacts"]
    required = {
        "umf-truss": "0.0.54", "umf-core": "0.10.6", "umf-access": "0.1.0.dev8",
        "umf-weft": "0.4.56", "pyspark": "4.2.0", "delta-spark": "4.4.1",
        "pgserver-pg18": "18.6.1",
    }
    observed = {name: importlib.metadata.version(name) for name in required}
    if observed != required:
        raise RuntimeError(f"installed native cohort differs: {observed}")
    if sys.version_info[:2] != (3, 12):
        raise RuntimeError("selected native worker Python 3.12 required")
    candidate = Path(artifacts["ashlar_candidate_wheel"])
    candidate_sha = sha(candidate.read_bytes())
    if candidate_sha != artifacts["ashlar_candidate_wheel_sha256"]:
        raise RuntimeError("exact candidate Ashlar wheel bytes differ")
    ashlar_distribution = importlib.metadata.distribution("umf-ashlar")
    direct_url = json.loads(ashlar_distribution.read_text("direct_url.json") or "{}")
    if direct_url.get("url") != candidate.resolve().as_uri():
        raise RuntimeError("native worker Ashlar distribution does not come from exact candidate wheel")
    import ashlar_host
    package_root = Path(ashlar_host.__file__).resolve().parent
    if not package_root.is_relative_to(Path(sys.prefix).resolve()):
        raise RuntimeError("native worker Ashlar package origin is outside selected environment")
    import zipfile
    with zipfile.ZipFile(candidate) as wheel:
        for relative in ("ashlar_host/truss_graph_export.py",
                         "ashlar_host/truss_source_custody_selection.py",
                         "ashlar_host/truss_current_catalog.py",
                         "ashlar_host/truss_note_publication.py"):
            installed = package_root / Path(relative).name
            member = hashlib.sha256(wheel.read(relative)).hexdigest()
            if not installed.is_file() or sha(installed.read_bytes()) != member:
                raise RuntimeError(f"installed Ashlar module differs from candidate wheel: {relative}")
    from ashlar_host.truss_current_catalog import installed_current_truss_profile
    if installed_current_truss_profile() != artifacts["truss_adapter_profile"]:
        raise RuntimeError("Truss .54 adapter profile origin differs")
    from ashlar_host.configured_local_runtime_profile import attest
    jars_dir = Path(artifacts["runtime_manifest"])
    runtime = json.loads(jars_dir.read_text())
    java_home = Path(runtime["java_home"])
    jar_directory = Path(runtime["jars_directory"])
    inventory = runtime["jar_inventory"]
    from ashlar_host.truss_resume_cli import _CurrentTrussRuntime
    # Use the existing validated current-Truss runtime identity. This worker
    # consumes its Spark/JAR identity only; no configured source fixture or
    # configured-local installation is created for the public Truss feed.
    config = _CurrentTrussRuntime(jar_directory, java_home, inventory,
                                  ROOT / "spark-warehouse")
    _, spark_identity, jars = attest(config)
    os.environ["JAVA_HOME"] = str(java_home)
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ["PYSPARK_PYTHON"] = str(Path(sys.executable).resolve())
    os.environ["PYSPARK_DRIVER_PYTHON"] = str(Path(sys.executable).resolve())
    return {"versions": observed, "spark": spark_identity,
            "jars": [str(path) for path in jars], "java_home": str(java_home),
            "runtime_manifest_sha256": sha(Path(artifacts["runtime_manifest"]).read_bytes())}


def _custody_selector(*, page, request, publication_id, destination_transport,
                      source_sessions, registration, runtime_status, accepted_document,
                      context_holder, state):
    """Open Truss hold before the backend publication and bind after readback."""
    from truss.feed import FeedConsumer
    from ashlar_host.truss_source_custody_selection import (
        select_truss_local_dev_source_custody_anchor,
        read_local_native_delta_publication_observation,
    )
    from ashlar_host.truss052_source_custody import Truss052SourceCustodySession
    from ashlar_host.delta_custody import encoded

    request_raw = encoded(request).encode("utf-8")
    consumer = FeedConsumer(consumer_id=page.consumer_id,
                            registration_id=page.registration_id,
                            checkpoint=page.prior)
    ack_registration = page.registration_id
    anchor = select_truss_local_dev_source_custody_anchor(
        registration=registration, consumer=consumer,
        runtime_status=runtime_status, accepted_document=accepted_document,
        page=page, feed=FEED, source_holder_id="ashlar-r61-fresh-local-source",
        document_id="ashlar-local-dev-typed-graph-r21-20261101",
        destination_id=DESTINATION, ack_registration_id=ack_registration,
        ack_scope_id="ashlar-r61-local-ack-scope", publication_id=publication_id,
        request_bytes=request_raw, predecessor_id=request["predecessor"],
        destination_transport=destination_transport)
    opened = anchor.open_selection()
    selected_context = object()
    params = context_holder["params"]
    connection_factory = context_holder["connection_factory"]
    def source_connection(context):
        if context is not selected_context:
            raise RuntimeError("foreign source-custody connection context refused")
        return connection_factory()
    session = Truss052SourceCustodySession(
        connection_factory=source_connection, selection=opened,
        context=selected_context)
    state.update(anchor=anchor, session=session)
    EFFECTS["custody_sessions"].append(session)
    EFFECTS["source_custody_open_attempted"] = True
    session.open()
    EFFECTS["source_custody_opened"] = True

    def postpublication(**post):
        observation = read_local_native_delta_publication_observation(
            anchor=anchor, spark=post["destination_transport"].spark,
            destination_transport=post["destination_transport"],
            manifest_bytes=post["manifest_bytes"], vector=post["vector"],
            receipt_bytes=post["receipt_bytes"],
            expected_post_state=post["expected_post_state"])
        selection = anchor.bind_local_native_publication(
            manifest_bytes=post["manifest_bytes"], vector=post["vector"],
            receipt_bytes=post["receipt_bytes"],
            expected_post_state=post["expected_post_state"],
            observation=observation)
        session.bind_publication(selection)
        if (len(observation.vector.targets) != 4 or len(observation.roles) != 4
                or len(anchor.destination_installation_bindings) != 6):
            raise RuntimeError("complete four-role publication and six-role installation inventory required")
        state["observation"] = observation
        state["selection"] = selection
        manifest_target = post["destination_transport"].targets["local.trussgraph.manifest"]
        state["manifest_uuid"] = manifest_target.uuid
        state["delta_root"] = str(manifest_target.path.parent)
        state["manifest_version"] = int(post["destination_transport"].spark.sql(
            f"DESCRIBE HISTORY delta.`{manifest_target.path}` LIMIT 1"
        ).select("version").first()["version"])
        return observation
    return postpublication


def _acknowledge(state: dict) -> dict:
    session = state["session"]
    primary = None
    cleanup = []
    result = None
    EFFECTS["source_ack_attempted"] = True
    EFFECTS["source_ack_state"] = "attempting"
    try:
        ack = session.acknowledge()
        EFFECTS["source_ack_state"] = "ack_committed_unreconciled"
        reconciled = session.reconcile()
        if reconciled.receipt_outcome != "matched":
            raise RuntimeError("Truss destination receipt reconciliation is not matched")
        EFFECTS["source_ack_state"] = "reconciled_matched"
        result = {"outcome": ack.outcome, "checkpoint_sha256": sha(ack.checkpoint.content),
                  "receipt_outcome": reconciled.receipt_outcome, "ack_matched": True}
    except BaseException as error:
        primary = error
        if EFFECTS["source_ack_state"] in ("attempting", "ack_committed_unreconciled"):
            EFFECTS["source_ack_state"] = "unresolved"
    try:
        session.close()
        EFFECTS["source_custody_closed"] = True
    except BaseException as error:
        cleanup.append({"close_error": f"{type(error).__name__}: {error}"[:500]})
    try:
        session.dispose()
        EFFECTS["source_custody_disposed"] = True
    except BaseException as error:
        cleanup.append({"dispose_error": f"{type(error).__name__}: {error}"[:500]})
    if session in EFFECTS["custody_sessions"]:
        EFFECTS["custody_sessions"].remove(session)
    if primary is not None:
        setattr(primary, "r61_custody_cleanup", cleanup)
        raise primary
    if cleanup:
        raise RuntimeError(f"Truss custody close/dispose incomplete: {cleanup}")
    EFFECTS["source_ack_state"] = "closed_reconciled"
    return result


def _run_graphframes(scope: dict, release: Path, custody: Path, output: Path) -> dict:
    manifest_path = Path(scope["artifacts"]["graphframes_runtime_manifest"])
    manifest = json.loads(manifest_path.read_text())
    python = Path(manifest.get("python", ""))
    jars = Path(manifest.get("jars_directory", ""))
    java_home = Path(manifest.get("java_home", ""))
    if (not python.is_absolute() or not python.is_file()
            or Path(manifest.get("prefix", "")) in (Path(""), Path("/private/tmp/ashlar-r59-graphframes-70dd/worker-env"))
            or not jars.is_absolute() or not jars.is_dir()
            or not java_home.is_absolute() or not java_home.joinpath("bin/java").is_file()):
        raise RuntimeError("separate reviewed GraphFrames runtime manifest/interpreter required")
    jar_inventory = manifest.get("jar_inventory")
    if type(jar_inventory) is not dict or len(jar_inventory) != 4:
        raise RuntimeError("closed four-JAR GraphFrames runtime inventory required")
    for name, expected in jar_inventory.items():
        jar = jars / name
        if not jar.is_file() or sha(jar.read_bytes()) != expected:
            raise RuntimeError("selected GraphFrames JAR hash differs")
    if output.exists():
        raise RuntimeError("fresh GraphFrames output required")
    candidate_wheel = Path(scope["artifacts"]["ashlar_candidate_wheel"])
    ashlar_probe = (
        "import hashlib,importlib.metadata as m,json,pathlib,sys,zipfile;"
        "d=m.distribution('umf-ashlar');u=json.loads(d.read_text('direct_url.json') or '{}');"
        "p=pathlib.Path(sys.argv[1]).resolve();w=pathlib.Path(sys.argv[2]).resolve();"
        "import ashlar.graph_release as g;module=pathlib.Path(g.__file__).resolve();"
        "member='ashlar/graph_release.py';mh=hashlib.sha256(zipfile.ZipFile(w).read(member)).hexdigest();"
        "wh=hashlib.sha256(w.read_bytes()).hexdigest();"
        "assert u.get('url')==w.as_uri() and u.get('archive_info',{}).get('hashes',{}).get('sha256')==wh;"
        "assert module.is_relative_to(p) and hashlib.sha256(module.read_bytes()).hexdigest()==mh;"
        "print(json.dumps({'distribution_url':u['url'],'wheel_sha256':wh,'module':str(module),'module_sha256':mh}))"
    )
    env = {"PATH": os.pathsep.join((str(java_home / "bin"), str(python.parent), "/usr/bin", "/bin")),
           "HOME": str(ROOT / "home"), "TMPDIR": str(ROOT / "tmp"),
           "JAVA_HOME": str(java_home), "PYTHONNOUSERSITE": "1",
           "PYTHONDONTWRITEBYTECODE": "1", "LC_ALL": "C.UTF-8"}
    probe = subprocess.run([str(python), "-c", ashlar_probe, str(Path(manifest["prefix"]).resolve()),
                            str(candidate_wheel)], cwd=ROOT, env=env, stdin=subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=20, check=False)
    if probe.returncode != 0:
        raise RuntimeError("GraphFrames env does not load exact candidate Ashlar wheel: " +
                           probe.stdout[-2000:].decode("utf-8", "replace"))
    from ashlar_host.private_graph_custody import PROFILE as custody_profile
    command = [str(python), str(REPO / "tools/run_graph_release_graphframes.py"),
        "--release", str(release), "--sha256", sha(release.read_bytes()),
        "--output", str(output), "--jars", str(jars),
        "--custody-profile", custody_profile,
        "--custody", str(custody), "--custody-sha256", sha(custody.read_bytes())]
    completed = subprocess.run(command, cwd=REPO, env=env, stdin=subprocess.DEVNULL,
        stdout=subprocess.PIPE, stderr=subprocess.STDOUT, timeout=300, check=False)
    if completed.returncode != 0:
        raise RuntimeError("GraphFrames local action failed: " + completed.stdout[-3000:].decode("utf-8", "replace"))
    report_path = output / "report.json"
    report = json.loads(report_path.read_text())
    if (report.get("release_sha256") != sha(release.read_bytes())
            or report.get("full_row_parity") is not True
            or report.get("counts") != {"nodes": 6, "edges": 3, "one_hop": 3,
                                         "two_hop": 1, "isolates": 2}):
        raise RuntimeError("GraphFrames output differs from exact R61 release topology")
    return {"report_sha256": sha(report_path.read_bytes()),
            "release_sha256": report["release_sha256"],
            "custody_sha256": sha(custody.read_bytes()),
            "counts": report["counts"], "full_row_parity": True,
            "runtime_manifest_sha256": sha(manifest_path.read_bytes()),
            "action_complete": True}


def _group_config(path: Path, output: Path, *, uri: str, records: list,
                  edges: list, key: str) -> dict:
    value = {"profile": "ashlar-truss-record-group/0.1", "connection_uri": uri,
        "bun": os.environ["R61_BUN_EXECUTABLE"], "document_path": str(MODEL),
        "document_revision": "r1", "document_id": "ashlar-local-dev-typed-graph-r21-20261101",
        "source_version": "0.10.0", "origin_path": str(ROOT / "origin.json"),
        "request_key": key, "records": records, "edges": edges}
    path.write_bytes(canonical(value) + b"\n")
    os.chmod(path, 0o600)
    return value


def _run_weft_reads(spark, state: dict, setup: dict) -> dict:
    """Compile twice with Weft .4.56 and execute two R61-bound native reads."""
    import weft
    template_path = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261101/g03-weft-native-consumer/candidate-r12/grouped.request.json"
    template = json.loads(template_path.read_text())
    model_raw = MODEL.read_bytes()
    model_hash = sha(model_raw)
    report = json.loads(base64.b64decode(setup["report"]["original_base64"], validate=True))
    type_ids = {item["elementId"]: str(item["typeId"]) for item in report["types"]}
    property_ids = {item["elementId"]: str(item["propertyId"]) for item in report["properties"]}
    if set(type_ids) != {"Person", "Place"} or not {"person_key", "person_name", "place_key", "place_name"} <= set(property_ids):
        raise RuntimeError("accepted R21 catalog type/property allocations differ")
    roles = state["observation"].roles
    database = "ashlar_r61_" + state["selection"].publication_id[-12:]
    table_pins = [{"name": ["spark_catalog", database, role],
                   "uuid": roles[role].uuid, "version": roles[role].head_version}
                  for role in ("object_current", "edge_current", "tombstone", "whole_source_history")]
    installation_id = setup["installation"]["installation_id"]
    pub_uuid = state["manifest_uuid"]
    source_type = {"17": type_ids["Person"], "18": type_ids["Place"]}
    source_property = {"21": property_ids["person_key"], "22": property_ids["person_name"],
                       "23": property_ids["quantity"], "24": property_ids["place_key"],
                       "25": property_ids["place_name"]}

    def rewrite(value):
        if isinstance(value, dict):
            for key, item in list(value.items()):
                if key == "documentId" and item == "typed-graph-example":
                    value[key] = "ashlar-local-dev-typed-graph-r21-20261101"
                elif key == "sourceSystem" and item == "typed-graph-source":
                    value[key] = installation_id
                elif key == "typeId" and str(item) in source_type:
                    value[key] = source_type[str(item)]
                elif key == "propertyId" and str(item) in source_property:
                    value[key] = source_property[str(item)]
                else:
                    rewrite(item)
        elif isinstance(value, list):
            for item in value:
                rewrite(item)

    def compile_request(sql: str):
        request = json.loads(json.dumps(template))
        binding = json.loads(request["target"]["bindingJson"])
        for pin in binding["modelPins"]:
            pin.update(documentId="ashlar-local-dev-typed-graph-r21-20261101",
                       revision="1", sha256=model_hash, umfVersion="0.10.0")
        binding["layoutRevision"] = "ashlar-delta/0.4-xxhash64"
        binding["lookupHashProfile"] = "spark-xxhash64-typed-key/0.1"
        binding["publication"] = {"id": state["selection"].publication_id,
            "manifestUuid": pub_uuid, "tables": table_pins}
        rewrite(binding)
        binding_raw = json.dumps(binding, ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        request["target"]["bindingJson"] = binding_raw
        request["target"]["bindingSha256"] = sha(binding_raw.encode())
        request["modules"] = [{"documentJson": model_raw.decode(),
            "pin": dict(binding["modelPins"][0]), "selectedModuleIds": ["m"]}]
        request["sql"] = sql
        request_raw = json.dumps(request, ensure_ascii=False, separators=(",", ":")).encode() + b"\n"
        first = weft.compile_paths_keys_json(request_raw.decode())
        second = weft.compile_paths_keys_json(request_raw.decode())
        if first != second:
            raise RuntimeError("Weft deterministic recompilation differs")
        artifact = json.loads(first)
        if (artifact.get("status") != "compiled"
                or artifact.get("targetContext", {}).get("storageLayoutRevision") != "ashlar-delta/0.4-xxhash64"
                or artifact.get("targetContext", {}).get("lookupHashProfile") != "spark-xxhash64-typed-key/0.1"):
            raise RuntimeError("Weft .4.56 compiled artifact/profile refused")
        return request, artifact, request_raw, first.encode()

    queries = {
        "singleton": "SELECT p.person_key, p.person_name FROM Person p WHERE p.person_key = 'p1'",
        "grouped": template["sql"],
    }
    results = {}
    for name, query in queries.items():
        request, artifact, request_raw, response_raw = compile_request(query)
        sql = artifact["sql"]
        refs = re.findall(r"`spark_catalog`\.`([^`]+)`\.`([^`]+)`\s+VERSION\s+AS\s+OF\s+(\d+)", sql)
        if not refs:
            raise RuntimeError("Weft SQL lacks explicit pinned Delta references")
        for db, role, version in refs:
            if db != database or role not in roles or int(version) != roles[role].head_version:
                raise RuntimeError("Weft SQL does not bind exact R61 role heads")
            source = f"`spark_catalog`.`{database}`.`{role}`"
            sql = sql.replace(source, f"delta.`{Path(state['delta_root']) / role}`")
        if "spark_catalog" in sql:
            raise RuntimeError("Unresolved Weft catalog reference")
        parameters = {f"p{slot['position']}": slot["value"] for slot in artifact.get("parameters", [])}
        rows = [row.asDict(recursive=True) for row in spark.sql(sql, args=parameters).collect()]
        results[name] = {"rows": rows, "request_sha256": sha(request_raw),
                         "response_sha256": sha(response_raw), "sql_sha256": sha(sql.encode())}
    if results["singleton"]["rows"] != [{"person_key": "p1", "person_name": "Ada"}]:
        raise RuntimeError("Native Weft singleton differs from accepted public seed source")
    if results["grouped"]["rows"] != [{"person_key": "p1", "paths": 1, "targets": 1}]:
        raise RuntimeError("Native Weft grouped path query differs from source-derived graph")
    return {"compiler": "umf-weft/0.4.56", "results": results,
            "source_oracle": {"singleton": ["p1", "Ada"],
                "grouped": [{"person_key": "p1", "paths": 1, "targets": 1}]}}


def _seed_records() -> tuple[list, list]:
    objects = [
        {"alias": alias, "module_id": "m", "record_id": "Person",
         "values": {"person_key": {"kind": "string", "text": alias},
                    "person_name": {"kind": "string", "text": name}}}
        for alias, name in (("p1", "Ada"), ("p2", "Bo"), ("p3", "Cy"))
    ] + [{"alias": "l1", "module_id": "m", "record_id": "Place",
          "values": {"place_key": {"kind": "string", "text": "place-1"},
                     "place_name": {"kind": "string", "text": "North"}}}]
    edges = [
        {"alias": "e1", "module_id": "m", "relationship_id": "knows",
         "source": {"state": "alias", "alias": "p1"},
         "target": {"state": "alias", "alias": "p2"}, "values": {}, "order_key": {"state": "null"}},
        {"alias": "e2", "module_id": "m", "relationship_id": "knows",
         "source": {"state": "alias", "alias": "p2"},
         "target": {"state": "alias", "alias": "p3"}, "values": {}, "order_key": {"state": "null"}},
        {"alias": "e3", "module_id": "m", "relationship_id": "located",
         "source": {"state": "alias", "alias": "p2"},
         "target": {"state": "alias", "alias": "l1"}, "values": {}, "order_key": {"state": "null"}},
    ]
    return objects, edges


def run() -> dict:
    scope = json.loads(SCOPE.read_text())
    artifacts = scope["artifacts"]
    runtime_identity = _install_runtime(scope)
    from truss import LocalPostgres
    import psycopg
    from ashlar_host.truss_runtime import require_truss_psycopg_runtime
    from ashlar_host.truss_record_group import run_create_truss_record_group_command
    from ashlar_host.truss_note_publication import publish_graph_pages
    from ashlar_host.configured_local_runtime import open_configured_local_spark
    from ashlar_host.configured_local_runtime_profile import attest, verify_loaded_delta_class
    from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt
    require_truss_psycopg_runtime()
    import importlib.util
    setup_stage_path = REPO / "tools/r61_fresh_truss_setup_stage.py"
    spec = importlib.util.spec_from_file_location("r61_setup_stage", setup_stage_path)
    if spec is None or spec.loader is None:
        raise RuntimeError("R61 fresh Truss setup stage loader unavailable")
    setup_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(setup_module)
    run_fresh_truss_setup_stage = setup_module.run_fresh_truss_setup_stage
    root = ROOT
    EFFECTS["postgres_start_attempted"] = True
    setup = run_fresh_truss_setup_stage(root=root,
        bun=Path(artifacts["bun_executable"]),
        expected_adapter_profile=artifacts["truss_adapter_profile"])
    # The public Truss setup stage has started and cleanly closed its owned PG
    # contexts before returning accepted setup evidence.
    EFFECTS["postgres_started"] = True
    EFFECTS["postgres_closed"] = True
    pgdata = Path(scope["fresh_local_state"]["postgres_data"])
    delta = Path(scope["fresh_local_state"]["delta_root"])
    jars = [Path(item) for item in runtime_identity["jars"]]
    java_home = Path(runtime_identity["java_home"])
    report_raw = base64.b64decode(setup["report"]["original_base64"], validate=True)
    report_doc = json.loads(report_raw)
    from truss.catalog_results import CatalogRegistration
    registration = CatalogRegistration(revision=int(report_doc["revision"]),
        registration_id=report_doc["registrationId"],
        report_bytes=report_raw, replayed=False)
    model_raw = MODEL.read_bytes()
    phase_receipts = []
    runtime = LocalPostgres(pgdata)
    runtime_open = False
    primary = None
    try:
        EFFECTS["postgres_start_attempted"] = True
        EFFECTS["postgres_closed"] = False
        runtime.__enter__()
        runtime_open = True
        EFFECTS["postgres_started"] = True
        params = psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
        admin_uri = runtime.info.connection_uri
        source_uri = psycopg.conninfo.make_conninfo(**dict(params, user=setup["roles"]["consumer"]))
        registrar_uri = source_uri
        source_factory = lambda: psycopg.connect(source_uri, autocommit=False, connect_timeout=5)
        with psycopg.connect(registrar_uri, autocommit=False, connect_timeout=5) as connection:
            connection.execute("SET TRANSACTION ISOLATION LEVEL READ COMMITTED, READ ONLY")
            connection.execute("SET LOCAL statement_timeout = '5000ms'")
            from truss import Ok, CatalogRuntimeConfiguration, create_reference_assembly, adopt_psycopg_host
            host = adopt_psycopg_host(connection)
            assembly = None
            try:
                tx = host.adopt_transaction(access_mode="read_only")
                if not isinstance(tx, Ok): raise RuntimeError("public runtime transaction refused")
                assembly_result = create_reference_assembly(CatalogRuntimeConfiguration(), host)
                if not isinstance(assembly_result, Ok): raise RuntimeError("public runtime assembly refused")
                assembly = assembly_result.value
                status_result = assembly.status_in_transaction(tx.value)
                doc_result = assembly.accepted_document_in_transaction(
                    tx.value, document_id="ashlar-local-dev-typed-graph-r21-20261101", maximum_bytes=1048576)
                if not isinstance(status_result, Ok) or not isinstance(doc_result, Ok):
                    raise RuntimeError("public Truss runtime/accepted document read refused")
                runtime_status, accepted_document = status_result.value, doc_result.value
            finally:
                if assembly is not None: assembly.dispose()
                if not host.detach(): raise RuntimeError("public Truss host detach refused")
            connection.rollback()
        from ashlar_host.truss_note_publication import read_page
        genesis_page = read_page(source_factory, CONSUMER, maximum_bytes=1048576)
        initial_page = setup["initial_source_page"]
        genesis_payload = json.loads(genesis_page.content)
        genesis_counts = {key: len(genesis_payload.get(key, [])) for key in
                          ("catalogPrerequisites", "revisions", "changes", "sources", "reservations")}
        genesis_revision_ids = [str(item.get("revision")) for item in genesis_payload.get("revisions", [])]
        prerequisite_revision_ids = [str(item.get("revision"))
                                     for item in genesis_payload.get("catalogPrerequisites", [])]
        if (genesis_page.outcome != "transaction" or genesis_page.consumer_id != CONSUMER
                or genesis_page.registration_id != initial_page["registration_id"]
                or genesis_page.writer_xid != "758"
                or base64.b64decode(initial_page["prior_base64"], validate=True) != genesis_page.prior.content
                or base64.b64decode(initial_page["next_base64"], validate=True) != genesis_page.next.content):
            raise RuntimeError("The exact accepted-catalog genesis page changed before publication")
        if (genesis_page.writer_xid != "758" or genesis_counts != {
                "catalogPrerequisites": 2, "revisions": 1, "changes": 0,
                "sources": 0, "reservations": 0}
                or genesis_revision_ids != ["1"]
                or prerequisite_revision_ids != ["0", "1"]):
            raise RuntimeError("Accepted-catalog genesis page is not the reviewed metadata-only event")
        runtime_config = type("SelectedRuntime", (), {"java_home": java_home,
            "jars": jars[0].parent,
            "jar_inventory": {item.name: sha(item.read_bytes()) for item in jars}})()
        _, _, attested_jars = attest(runtime_config)
        warehouse = root / "spark-warehouse"
        genesis_state = {}
        def selector_for(state):
            def select_page(**kwargs):
                holder = {"params": params, "connection_factory": source_factory}
                return _custody_selector(page=kwargs["page"], request=kwargs["request"],
                    publication_id=kwargs["publication_id"],
                    destination_transport=kwargs["destination_transport"],
                    source_sessions=kwargs["source_sessions"], registration=registration,
                    runtime_status=runtime_status, accepted_document=accepted_document,
                    context_holder=holder, state=state)
            return select_page

        # Publish and protectively acknowledge the accepted-catalog metadata
        # transaction first. It has no entity rows, but it is a real public page.
        EFFECTS["spark_started"] = True
        spark = open_configured_local_spark(runtime_config, attested_jars, warehouse,
                                            app_name="Ashlar R61 catalog genesis")
        try:
            verify_loaded_delta_class(spark, attested_jars)
            EFFECTS["delta_write_state"] = "possible_catalog_genesis_publication"
            genesis_publication = publish_graph_pages(spark, delta,
                connection_factory=source_factory, catalog_connection_factory=lambda:
                    psycopg.connect(registrar_uri, autocommit=False, connect_timeout=5),
                consumer_id=CONSUMER, feed=FEED, maximum_pages=1,
                maximum_page_bytes=1048576, destination_id=DESTINATION,
                carrier_profile="ashlar-delta/0.4-xxhash64", umf_sdk_version="0.10.2",
                defer_source_acknowledgement=True,
                prepublication_source_custody=selector_for(genesis_state))
        finally:
            spark.stop()
            EFFECTS["spark_closed"] = True
        EFFECTS["delta_write_state"] = "confirmed_catalog_genesis_publication"
        if len(genesis_publication.get("publications", ())) != 1 or "selection" not in genesis_state:
            raise RuntimeError("one accepted-catalog genesis publication and native custody bind required")
        genesis_result = genesis_publication["publications"][0]
        genesis_ack = _acknowledge(genesis_state)
        genesis_cursor = read_page(source_factory, CONSUMER, maximum_bytes=1048576)
        if (genesis_cursor.outcome != "empty_interval"
                or sha(genesis_cursor.prior.content) != genesis_ack["checkpoint_sha256"]):
            raise RuntimeError("metadata-only genesis ACK does not match the saved next checkpoint")
        phase_receipts.append({"phase": "accepted_catalog_metadata_genesis_and_protected_ack",
            "source_writer_xid": genesis_page.writer_xid,
            "source_page_sha256": genesis_page.sha256,
            "source_event_counts": genesis_counts,
            "revision_ids": genesis_revision_ids,
            "catalog_prerequisite_revision_ids": prerequisite_revision_ids,
            "publication_id": genesis_result["manifest"]["publication_id"],
            "publication_vector": {k: list(v) for k, v in genesis_state["observation"].vector.targets.items()},
            "ack": genesis_ack})

        group_records, group_edges = _seed_records()
        seed_config = root / "seed-group.json"
        _group_config(seed_config, root / "seed-group-result.json", uri=source_uri,
                      records=group_records, edges=group_edges, key="ashlar-r61-seed-seven")
        run_create_truss_record_group_command(seed_config, root / "seed-group-result.json", require_fresh=True)
        phase_receipts.append({"phase": "seven_operation_public_seed", "records": 4,
                               "edges": 3, "event_count": 7,
                               "receipt_sha256": sha((root / "seed-group-result.json").read_bytes())})
        page = read_page(source_factory, CONSUMER, maximum_bytes=1048576)
        seed_payload = json.loads(page.content)
        if (page.outcome != "transaction" or page.consumer_id != CONSUMER
                or len(seed_payload.get("changes", [])) != 7):
            raise RuntimeError("public Truss did not expose the exact seven-operation seed page")

        from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
        genesis_intent = genesis_result.get("publication_intent")
        if type(genesis_intent) is not dict or set(genesis_intent) != {"path", "sha256"}:
            raise RuntimeError("Exact metadata-genesis publication intent required")
        def continuation_config_for(path, intent):
            value = {
                "connection_uri": source_uri,
                "catalog_connection_uri": registrar_uri,
                "publication_directory": str(delta),
                "jars": [str(item) for item in jars],
                "publication_intent": intent,
                "destination_id": DESTINATION,
                "carrier_profile": "ashlar-delta/0.4-xxhash64",
                "runtime_profile": {"profile": runtime_identity["spark"]["profile"]
                    if isinstance(runtime_identity["spark"], dict) and "profile" in runtime_identity["spark"]
                    else "ashlar-configured-local-runtime/0.2",
                    "java_home": str(java_home),
                    "jar_inventory": {item.name: sha(item.read_bytes()) for item in jars}},
                "archived_acceptance_files": [],
            }
            path.write_bytes(canonical(value) + b"\n")
            os.chmod(path, 0o600)
            return value
        seed_continuation_path = root / "seed-continuation-config.json"
        continuation_config_for(seed_continuation_path, genesis_intent)
        seed_state = {}
        seed_result_path = root / "seed-continuation-result.json"
        EFFECTS["delta_write_state"] = "possible_seed_continuation_publication"
        continued_seed = run_continue_truss_graph_command(
            seed_continuation_path, seed_result_path, maximum_pages=1,
            wait_seconds=0, maximum_page_bytes=1048576,
            defer_source_acknowledgement=True,
            prepublication_source_custody=selector_for(seed_state), export_graph=False)
        EFFECTS["delta_write_state"] = "confirmed_seed_continuation_publication"
        if (continued_seed.get("source_acknowledgement") != "deferred_not_attempted"
                or len(continued_seed.get("publications", ())) != 1
                or "selection" not in seed_state):
            raise RuntimeError("one seven-op seed publication and native custody bind required")
        seed_result = continued_seed["publications"][0]
        seed_ack = _acknowledge(seed_state)
        seed_cursor = read_page(source_factory, CONSUMER, maximum_bytes=1048576)
        if (seed_cursor.outcome != "empty_interval"
                or sha(seed_cursor.prior.content) != seed_ack["checkpoint_sha256"]):
            raise RuntimeError("seven-op seed ACK does not match the saved next checkpoint")

        EFFECTS["spark_started"] = True
        spark = open_configured_local_spark(runtime_config, attested_jars, warehouse,
                                            app_name="Ashlar R61 native Weft reads")
        try:
            verify_loaded_delta_class(spark, attested_jars)
            weft_receipt = _run_weft_reads(spark, seed_state, setup)
        finally:
            spark.stop()
            EFFECTS["spark_closed"] = True
        phase_receipts.append({"phase": "seven_operation_seed_continuation_and_protected_ack",
            "publication_id": seed_result["manifest"]["publication_id"],
            "publication_vector": {k: list(v) for k, v in seed_state["observation"].vector.targets.items()},
            "ack": seed_ack, "weft": weft_receipt, "owner_export": "pending_final_continuation"})

        # Apply two isolated Persons and read the exact public continuation page.
        two = [{"alias": alias, "module_id": "m", "record_id": "Person",
                "values": {"person_key": {"kind": "string", "text": alias},
                           "person_name": {"kind": "string", "text": name}}}
               for alias, name in (("p4", "Di"), ("p5", "Eve"))]
        cont_config = root / "continuation-group.json"
        _group_config(cont_config, root / "continuation-group-result.json", uri=source_uri,
                      records=two, edges=[], key="ashlar-r61-continuation-two")
        run_create_truss_record_group_command(cont_config, root / "continuation-group-result.json", require_fresh=True)
        phase_receipts.append({"phase": "two_record_continuation", "records": 2,
                               "edges": 0, "event_count": 2,
                               "receipt_sha256": sha((root / "continuation-group-result.json").read_bytes())})
        from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
        intent = seed_result.get("publication_intent")
        if type(intent) is not dict or set(intent) != {"path", "sha256"}:
            raise RuntimeError("Exact seed publication intent required for continuation")
        continuation_config = {
            "connection_uri": source_uri,
            "catalog_connection_uri": registrar_uri,
            "publication_directory": str(delta),
            "jars": [str(path) for path in jars],
            "publication_intent": intent,
            "destination_id": DESTINATION,
            "carrier_profile": "ashlar-delta/0.4-xxhash64",
            "runtime_profile": {"profile": runtime_identity["spark"]["profile"]
                if isinstance(runtime_identity["spark"], dict) and "profile" in runtime_identity["spark"]
                else "ashlar-configured-local-runtime/0.2",
                "java_home": str(java_home),
                "jar_inventory": {path.name: sha(path.read_bytes()) for path in jars}},
            "archived_acceptance_files": [],
        }
        continuation_path = root / "continuation-config.json"
        continuation_path.write_bytes(canonical(continuation_config) + b"\n")
        os.chmod(continuation_path, 0o600)
        continuation_state = {}
        def select_continuation(**kwargs):
            holder = {"params": params, "connection_factory": source_factory}
            return _custody_selector(page=kwargs["page"], request=kwargs["request"],
                publication_id=kwargs["publication_id"],
                destination_transport=kwargs["destination_transport"],
                source_sessions=kwargs["source_sessions"], registration=registration,
                runtime_status=runtime_status, accepted_document=accepted_document,
                context_holder=holder, state=continuation_state)
        continuation_result_path = root / "continuation-result.json"
        EFFECTS["delta_write_state"] = "possible_continuation_publication"
        EFFECTS["spark_started"] = True
        continued = run_continue_truss_graph_command(
            continuation_path, continuation_result_path, maximum_pages=1,
            wait_seconds=0, maximum_page_bytes=1048576,
            defer_source_acknowledgement=True,
            prepublication_source_custody=select_continuation,
            export_graph=True)
        EFFECTS["delta_write_state"] = "confirmed_continuation_publication"
        EFFECTS["spark_closed"] = True
        if (continued.get("source_acknowledgement") != "deferred_not_attempted"
                or len(continued.get("publications", ())) != 1
                or "selection" not in continuation_state):
            raise RuntimeError("one continuation publication and native source-custody bind required")
        continuation_publication = continued["publications"][0]
        continuation_ack = _acknowledge(continuation_state)
        continuation_cursor = read_page(source_factory, CONSUMER, maximum_bytes=1048576)
        if (continuation_cursor.outcome != "empty_interval"
                or sha(continuation_cursor.prior.content) != continuation_ack["checkpoint_sha256"]
                or continuation_cursor.prior.content != continuation_cursor.next.content):
            raise RuntimeError("continuation protected ACK does not match the public next checkpoint")
        graph_export = continued.get("original_graph_export", {})
        release_b64 = graph_export.get("release_base64")
        custody_b64 = graph_export.get("custody_base64")
        if type(release_b64) is not str or type(custody_b64) is not str:
            raise RuntimeError("Owner-closed original graph release/custody pair required")
        release_bytes = base64.b64decode(release_b64, validate=True)
        custody_bytes = base64.b64decode(custody_b64, validate=True)
        if (sha(release_bytes) != graph_export.get("release_sha256")
                or sha(custody_bytes) != graph_export.get("custody_sha256")):
            raise RuntimeError("Owner graph export byte hashes differ")
        evidence = Path(scope["fresh_local_state"]["evidence_root"])
        evidence.mkdir(mode=0o700)
        release_path = evidence / "r61-graph-release.json"
        custody_path = evidence / "r61-graph-custody.json"
        for output, raw in ((release_path, release_bytes), (custody_path, custody_bytes)):
            fd = os.open(output, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
            try:
                os.write(fd, raw)
                os.fsync(fd)
            finally:
                os.close(fd)
        phase_receipts.append({"phase": "continuation_owner_export_and_ack",
            "publication_id": continuation_publication["manifest"]["publication_id"],
            "ack": continuation_ack,
            "graph_release_sha256": sha(release_bytes),
            "graph_release_bytes": len(release_bytes),
            "graph_custody_sha256": sha(custody_bytes),
            "graph_custody_bytes": len(custody_bytes),
            "owner_close_before_bytes_exposed": True})
        runtime.__exit__(None, None, None)
        runtime_open = False
        delta_before_graphframes = tree_digest(delta)
        graphframes_output = evidence / "graphframes"
        EFFECTS["graphframes_started"] = True
        graphframes = _run_graphframes(scope, release_path, custody_path, graphframes_output)
        EFFECTS["graphframes_closed"] = True
        delta_after_graphframes = tree_digest(delta)
        if delta_before_graphframes != delta_after_graphframes:
            raise RuntimeError("GraphFrames action changed the original Delta root")
        return {"profile": PROFILE, "status": "completed",
            "setup": setup, "phases": phase_receipts, "weft": weft_receipt,
            "installed_ashlar_wheel_sha256": sha(Path(artifacts["ashlar_candidate_wheel"]).read_bytes()),
            "graph_release_sha256": sha(release_bytes),
            "graph_custody_sha256": sha(custody_bytes),
            "owner_export_complete": True,
            "graphframes_action_sha256": graphframes["report_sha256"],
            "graphframes_action_complete": graphframes["action_complete"],
            "graphframes": graphframes,
            "source_checkpoint_sha256": continuation_ack["checkpoint_sha256"],
            "source_empty_after_ack": True,
            "postgres_started": True, "postgres_start_attempted": True,
            "four_role_publication_vector": {
                key: list(value) for key, value in continuation_state["observation"].vector.targets.items()},
            "six_role_installation_inventory": sorted(
                continuation_state["anchor"].destination_installation_bindings),
            "delta_tree_sha256_before_graphframes": delta_before_graphframes,
            "delta_tree_sha256_after_graphframes": delta_after_graphframes,
            "source_ack_attempted": True, "delta_written": True,
            "all_owners_closed": True, "source_state_reconciled": True,
            "publication_vector_reconciled": True, "ack_reconciled": True,
            "no_delta_write_after_close": True,
            "spark_closed": True, "postgres_closed": True}
    finally:
        if runtime_open:
            runtime.__exit__(None, None, None)
            runtime_open = False
            EFFECTS["postgres_closed"] = True


def main() -> int:
    result = {"profile": PROFILE, "status": "failed", "effect_free": False,
              "native_dispatched": True, "source_ack_attempted": False,
              "delta_written": False, "cloud": False, "production_authority": False}
    try:
        completed = run()
        if completed.get("status") != "completed" or completed.get("graphframes_action_complete") is not True:
            raise RuntimeError("R61 full success is withheld until all bounded native actions complete")
        result.update(completed)
        result["status"] = "completed"
        result["effect_free"] = False
        result["native_dispatched"] = True
        result["source_ack_attempted"] = True
        result["delta_written"] = True
        result["spark_closed"] = True
        result["postgres_closed"] = True
        result["cloud"] = False
        result["production_authority"] = False
        scope = json.loads(SCOPE.read_text())
        result["installed_ashlar_wheel_sha256"] = sha(Path(os.environ["R61_ASHLAR_CANDIDATE_WHEEL"]).read_bytes())
        result["graphframes_ashlar_wheel_sha256"] = result["installed_ashlar_wheel_sha256"]
        result["truss_wheel_sha256"] = sha(Path(os.environ["R61_TRUSS_WHEEL"]).read_bytes())
        result["runtime_manifest_sha256"] = sha(Path(os.environ["R61_RUNTIME_MANIFEST"]).read_bytes())
        result["artifact_hashes"] = {
            "truss_wheel_sha256": sha(Path(os.environ["R61_TRUSS_WHEEL"]).read_bytes()),
            "truss_release_manifest_sha256": scope["artifacts"]["truss_release_manifest_sha256"],
            "umf_sdk_sha256": sha(Path(os.environ["R61_UMF_SDK_ARTIFACT"]).read_bytes()),
            "umf_core_sha256": sha(Path(os.environ["R61_UMF_CORE_PLATFORM_ARTIFACT"]).read_bytes()),
            "ashlar_candidate_wheel_sha256": sha(Path(os.environ["R61_ASHLAR_CANDIDATE_WHEEL"]).read_bytes()),
            "runtime_manifest_sha256": sha(Path(os.environ["R61_RUNTIME_MANIFEST"]).read_bytes()),
            "graphframes_runtime_manifest_sha256": sha(Path(os.environ["R61_GRAPHFRAMES_RUNTIME_MANIFEST"]).read_bytes()),
            "bun_sha256": sha(Path(os.environ["R61_BUN_EXECUTABLE"]).read_bytes()),
            "postgresql_18_runtime_manifest_sha256": sha(Path(os.environ["R61_POSTGRESQL_18_RUNTIME_MANIFEST"]).read_bytes()),
        }
    except BaseException as error:
        cleanup = []
        for session in list(EFFECTS["custody_sessions"]):
            try:
                session.close()
                cleanup.append({"close": "confirmed"})
            except BaseException as close_error:
                cleanup.append({"close_error": f"{type(close_error).__name__}: {close_error}"[:500]})
            try:
                session.dispose()
                cleanup.append({"dispose": "confirmed"})
            except BaseException as dispose_error:
                cleanup.append({"dispose_error": f"{type(dispose_error).__name__}: {dispose_error}"[:500]})
            EFFECTS["custody_sessions"].remove(session)
        try:
            failure_scope = json.loads(SCOPE.read_text())
            failure_pgdata = Path(failure_scope["fresh_local_state"]["postgres_data"])
            if ((failure_pgdata / "PG_VERSION").is_file()
                    or (failure_pgdata / "postmaster.pid").is_file()):
                EFFECTS["postgres_started"] = True
        except Exception:
            pass
        result.update({"failure_type": type(error).__name__, "failure": str(error)[:1500],
                       "traceback": traceback.format_exc(limit=12)[-8000:],
                       "source_ack_attempted": EFFECTS["source_ack_attempted"],
                       "source_ack_state": EFFECTS["source_ack_state"],
                       "delta_write_state": EFFECTS["delta_write_state"],
                       "delta_written": EFFECTS["delta_write_state"] != "not_attempted",
                       "spark_started": EFFECTS["spark_started"],
                       "spark_closed": EFFECTS["spark_closed"],
                       "postgres_started": EFFECTS["postgres_started"],
                       "postgres_start_attempted": EFFECTS["postgres_start_attempted"],
                       "postgres_closed": EFFECTS["postgres_closed"],
                       "graphframes_started": EFFECTS["graphframes_started"],
                       "graphframes_closed": EFFECTS["graphframes_closed"],
                       "custody_cleanup": cleanup})
        try:
            write_private(ROOT / "worker-failure.json", result)
        except Exception:
            pass
        print(json.dumps(result, sort_keys=True, separators=(",", ":")), file=sys.stderr)
        return 2
    write_private(COMPLETION, result)
    print(json.dumps(result, sort_keys=True, separators=(",", ":")))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
