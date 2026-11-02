"""Execute the four compiled R59 Weft selections read-only on local Delta."""
import hashlib
import importlib.metadata
import json
import re
from pathlib import Path

from pyspark.sql import SparkSession


HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[5]
R59 = Path("/private/tmp/ashlar-r36-truss052-page-sequence-20261101")
DELTA = R59 / "delta"
COMPILED = Path("/private/tmp/ashlar-local-release-native-read/r59-weft-compiled.json")
OUTPUT = HERE / "r59-native-query-result.json"
WEFT_WHEEL = Path(
    "/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/wheels/"
    "umf_weft-0.4.56-cp39-abi3-macosx_11_0_arm64.whl"
)
WEFT_WHEEL_SHA256 = "68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d"
MODEL_SHA256 = "b908c0dd6a0da65c864c0debd0c793d8ab06a933546f34050480909c5c9349f2"
PUBLICATION_ID = (
    "truss-graph-678967ac3deeafa90b514ae8a5a1b05f78b0b32e5f89135f4c67205fede001ea"
)
ALIAS_DATABASE = "ashlar_r59_205fede001ea"
ROLES = ("object_current", "edge_current", "tombstone", "whole_source_history")
JARS = (
    "/private/tmp/ashlar-r16-delta4-native-smoke-20261029/attempt-84c7b6002712486495c5dd66bfa0b8d6/jars/"
    "delta-spark_4.2_2.13-4.4.1.jar,"
    "/private/tmp/ashlar-r16-delta4-native-smoke-20261029/attempt-84c7b6002712486495c5dd66bfa0b8d6/jars/"
    "delta-storage-4.4.1.jar"
)


def sha256(raw):
    return hashlib.sha256(raw).hexdigest()


def canonical_hash(value):
    raw = json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
    return sha256(raw.encode("utf-8"))


def pages_sha256():
    return {
        path.name: sha256(path.read_bytes())
        for path in sorted(DELTA.glob("source-page-*.json"))
    }


def table_heads(spark):
    result = {}
    for role in ROLES:
        path = DELTA / role
        version = spark.sql(
            f"DESCRIBE HISTORY delta.`{path}` LIMIT 1"
        ).select("version").first()["version"]
        result[role] = int(version)
    return result


def selected_binding(value):
    response = value["response"]
    artifact = response.get("artifact", response)
    request = value["request"]
    if not isinstance(request, dict) or "target" not in request:
        request = request.get("request", {}) if isinstance(request, dict) else {}
    if "target" not in request:
        request = response.get("request", {})
    binding = json.loads(request["target"]["bindingJson"])
    return artifact, binding, request["target"]


def validate_binding(name, value, observation):
    artifact, binding, target = selected_binding(value)
    if target.get("bindingSha256") != canonical_hash(binding):
        raise ValueError(f"{name} binding digest differs")
    context = artifact.get("targetContext", value["response"].get("targetContext", {}))
    if context.get("storageLayoutRevision") != "ashlar-delta/0.4-xxhash64":
        raise ValueError(f"{name} compiled for a different carrier profile")
    if context.get("sessionSettings", {}).get("lookupHashProfile") != "spark-xxhash64-typed-key/0.1":
        raise ValueError(f"{name} compiled for a different lookup hash profile")
    publication = binding["publication"]
    if publication["id"] != PUBLICATION_ID:
        raise ValueError(f"{name} compiled for a different publication")
    if publication["manifestUuid"] != observation["publication"]["manifest_uuid"]:
        raise ValueError(f"{name} compiled for a different manifest UUID")
    selected = {
        role: {
            "name": ["spark_catalog", ALIAS_DATABASE, role],
            "uuid": observation["roles"][role]["uuid"],
            "version": observation["roles"][role]["head_version"],
        }
        for role in ROLES
    }
    actual = {entry["name"][2]: entry for entry in publication["tables"]}
    if len(publication["tables"]) != len(ROLES) or set(actual) != set(ROLES):
        raise ValueError(f"{name} compiled with a different publication role set")
    for role, expected in selected.items():
        entry = actual[role]
        if {"name": entry["name"], "uuid": entry["uuid"], "version": entry["version"]} != expected:
            raise ValueError(f"{name} compiled binding differs from retained {role} vector")
    pins = binding.get("modelPins", [])
    if not any(pin.get("sha256") == MODEL_SHA256 for pin in pins):
        raise ValueError(f"{name} compiled model pin differs")
    return artifact


def execute(spark, artifact):
    sql = artifact["sql"]
    refs = re.findall(r"`spark_catalog`\.`([^`]+)`\.`([^`]+)`\s+VERSION\s+AS\s+OF\s+(\d+)", sql)
    if not refs:
        raise ValueError("Compiled SQL has no explicitly versioned publication table reference")
    for database, role, version in refs:
        if database != ALIAS_DATABASE or role not in ROLES:
            raise ValueError("Compiled SQL contains an unexpected publication table name")
        if int(version) != OBSERVATION["roles"][role]["head_version"]:
            raise ValueError(f"{role} SQL version differs from the retained vector")
    for role in ROLES:
        source = f"`spark_catalog`.`{ALIAS_DATABASE}`.`{role}`"
        target = f"delta.`{DELTA / role}`"
        expected_count = sum(1 for _, table, _ in refs if table == role)
        if sql.count(source) != expected_count:
            raise ValueError(f"{role} replacement count differs from exact compiled references")
        sql = sql.replace(source, target)
    if re.search(r"`spark_catalog`\.`[^`]+`\.`[^`]+`", sql):
        raise ValueError("Compiled SQL contains an unreplaced catalog table reference")
    parameters = {
        f"p{slot['position']}": slot["value"]
        for slot in artifact.get("parameters", [])
    }
    rows = [row.asDict(recursive=True) for row in spark.sql(sql, args=parameters).collect()]
    return sql, rows


def observation_version(role):
    return OBSERVATION["roles"][role]["head_version"]


def live_state(spark, observation):
    state = {"roles": {}, "manifest_uuid": None, "manifest": None}
    for role in ROLES:
        path = DELTA / role
        detail = spark.sql(f"DESCRIBE DETAIL delta.`{path}`").first().asDict(recursive=True)
        schema = [
            (field.name, field.dataType.simpleString())
            for field in spark.read.format("delta").load(str(path)).schema.fields
        ]
        version = int(spark.sql(f"DESCRIBE HISTORY delta.`{path}` LIMIT 1").select("version").first()["version"])
        expected = observation["roles"][role]
        if detail["id"] != expected["uuid"] or version != expected["head_version"]:
            raise RuntimeError(f"Live {role} UUID or head differs from retained vector")
        if schema != [tuple(column) for column in expected["schema"]]:
            raise RuntimeError(f"Live {role} schema differs from retained vector")
        state["roles"][role] = {"uuid": detail["id"], "version": version, "schema": schema}
    manifest_path = DELTA / "manifest"
    manifest_detail = spark.sql(f"DESCRIBE DETAIL delta.`{manifest_path}`").first().asDict(recursive=True)
    state["manifest_uuid"] = manifest_detail["id"]
    state["manifest_version"] = int(spark.sql(
        f"DESCRIBE HISTORY delta.`{manifest_path}` LIMIT 1"
    ).select("version").first()["version"])
    publication = observation["publication"]
    if state["manifest_uuid"] != publication["manifest_uuid"]:
        raise RuntimeError("Live manifest UUID differs from retained observation")
    if state["manifest_version"] != publication["manifest_version"]:
        raise RuntimeError("Live manifest head differs from retained observation")
    row = spark.read.format("delta").load(str(manifest_path)).where(
        f"publication_id = '{PUBLICATION_ID}'"
    ).select("publication_id", "profile_version", "table_versions_json", "source_progress_json",
             "schema_revisions_json", "validation_report_json").collect()
    if len(row) != 1:
        raise RuntimeError("Live manifest does not have exactly one selected publication row")
    state["manifest"] = row[0].asDict(recursive=True)
    for key in ("publication_id", "profile_version", "table_versions_json", "source_progress_json",
                "schema_revisions_json", "validation_report_json"):
        if state["manifest"][key] != publication[key]:
            raise RuntimeError(f"Live manifest {key} differs from retained observation")
    return state


def main():
    compiled = json.loads(COMPILED.read_text(encoding="utf-8"))
    observation_path = HERE / "r59-vector-observation.json"
    observation = json.loads(observation_path.read_text(encoding="utf-8"))
    if observation["publication"]["publication_id"] != PUBLICATION_ID:
        raise ValueError("R59 vector observation publication ID differs")
    if observation["publication"]["profile_version"] != "ashlar-delta/0.4-xxhash64":
        raise ValueError("Exact `.4-xxhash64` vector required")
    if sha256(json.dumps({k: v for k, v in observation.items() if k != "observation_sha256"},
                         sort_keys=True, separators=(",", ":"), ensure_ascii=False).encode()) != observation["observation_sha256"]:
        raise ValueError("R59 vector observation self-digest differs")
    global OBSERVATION
    OBSERVATION = observation
    if importlib.metadata.version("umf-weft") != "0.4.56":
        raise ValueError("Weft 0.4.56 required")
    if sha256(WEFT_WHEEL.read_bytes()) != WEFT_WHEEL_SHA256:
        raise ValueError("Selected Weft wheel digest differs")

    spark = (
        SparkSession.builder.master("local[1]")
        .appName("ashlar-r59-weft-readonly")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.jars", JARS)
        .config("spark.sql.warehouse.dir", "/private/tmp/ashlar-local-release-native-read/warehouse-weft")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.decimalOperations.allowPrecisionLoss", "false")
        .config("spark.sql.legacy.parameterSubstitution.constantsOnly", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    application_id = spark.sparkContext.applicationId
    spark_version = spark.version
    opening_state = live_state(spark, observation)
    opening_heads = {role: entry["version"] for role, entry in opening_state["roles"].items()}
    selected_heads = {
        role: observation["roles"][role]["version"] for role in ROLES
    }
    if opening_heads != selected_heads:
        raise RuntimeError("Opening complete R59 heads differ from selected vector")

    executed = {}
    sql_digests = {}
    try:
        for name in ("singleton", "grouped", "page", "continuation"):
            artifact = validate_binding(name, compiled[name], observation)
            sql, rows = execute(spark, artifact)
            executed[name] = rows
            sql_digests[name] = sha256(sql.encode("utf-8"))
        closing_state = live_state(spark, observation)
        closing_heads = {role: entry["version"] for role, entry in closing_state["roles"].items()}
        if closing_state != opening_state:
            raise RuntimeError("Closing full R59 live vector or manifest changed")
    finally:
        spark.stop()

    expected = {
        "singleton": [{"person_key": "local-dana-r21-20261101", "person_name": "R21 Dana"}],
        "grouped": [{"person_key": "local-ada-20261101", "paths": "2", "targets": "1"}],
        "page": [
            {"person_key": "local-ada-20261101", "person_name": "R21 Ada"},
            {"person_key": "local-ben-20261101", "person_name": "R21 Ben"},
            {"person_key": "local-cy-20261101", "person_name": "R21 Cy"},
        ],
        "continuation": [
            {"person_key": "local-cy-20261101", "person_name": "R21 Cy"},
            {"person_key": "local-dana-r21-20261101", "person_name": "R21 Dana"},
        ],
    }
    normalized = {
        "singleton": [
            {k: row[k] for k in ("person_key", "person_name")}
            for row in executed["singleton"]
        ],
        "grouped": executed["grouped"],
        "page": [
            {k: row[k] for k in ("person_key", "person_name")}
            for row in executed["page"]
        ],
        "continuation": [
            {k: row[k] for k in ("person_key", "person_name")}
            for row in executed["continuation"]
        ],
    }
    if normalized != expected:
        raise RuntimeError("Native Weft output differs from page-derived source oracle")

    result = {
        "profile": "ashlar-r59-native-weft-read/0.1",
        "status": "passed",
        "publication_id": PUBLICATION_ID,
        "carrier_profile": "ashlar-delta/0.4-xxhash64",
        "runtime": {
            "spark": spark_version,
            "master": "local[1]",
            "application_id": application_id,
            "delta": "4.4.1",
            "weft": "0.4.56",
            "weft_wheel_sha256": WEFT_WHEEL_SHA256,
            "parameter_substitution_setting": "spark.sql.legacy.parameterSubstitution.constantsOnly=true",
        },
        "vector_observation_sha256": sha256(observation_path.read_bytes()),
        "source_page_sha256": pages_sha256(),
        "compiled_bundle_sha256": sha256(COMPILED.read_bytes()),
        "model_sha256": sha256(
            (
                ROOT
                / "docs/helix/04-build/evidence/current-upstream-release-20261101/"
                "r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json"
            ).read_bytes()
        ),
        "queries": {
            name: {
                "request_sha256": canonical_hash(compiled[name]["request"]),
                "response_sha256": canonical_hash(compiled[name]["response"]),
                "compiled_sql_sha256": sql_digests[name],
                "rows": normalized[name],
            }
            for name in ("singleton", "grouped", "page", "continuation")
        },
        "page": {"page_size": 2, "lookahead_row": "local-cy-20261101", "continuation_after": "local-ben-20261101"},
        "opening_heads": opening_heads,
        "closing_heads": closing_heads,
        "opening_state_sha256": canonical_hash(opening_state),
        "closing_state_sha256": canonical_hash(closing_state),
        "source_oracle": "Decoded exact retained R59 source page event bytes; no old G03 row oracle reused.",
        "setup_attempts": [
            {
                "outcome": "refused before native query results",
                "failure": "UNBOUND_SQL_PARAMETER p1",
                "cause": "First local Spark attempt omitted spark.sql.legacy.parameterSubstitution.constantsOnly=true.",
                "classification": "Runtime setup refusal; no publication data result was produced.",
            },
            {
                "outcome": "refused by opening custody guard before native query results",
                "failure": "Spark SQL schema type labels differed from retained Delta schema labels",
                "cause": "The first strict live-vector check used DataType.typeName() (`long`) instead of Spark SQL simpleString() (`bigint`).",
                "classification": "Evidence-runner representation defect; no publication data result was produced.",
            },
        ],
        "scope": "Local native Weft read only; no source/ACK interaction, Delta mutation, cloud, managed Unity Catalog, or engine execution.",
    }
    OUTPUT.write_text(json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "application_id": application_id, "queries": list(result["queries"]), "output": str(OUTPUT)}, sort_keys=True))


if __name__ == "__main__":
    main()
