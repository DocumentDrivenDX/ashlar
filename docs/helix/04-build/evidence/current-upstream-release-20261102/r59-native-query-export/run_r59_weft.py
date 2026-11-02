"""Execute the four compiled R59 Weft selections read-only on local Delta."""
import hashlib
import importlib.metadata
import json
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


def execute(spark, value):
    artifact = value.get("artifact", value)
    sql = artifact["sql"]
    for role in ROLES:
        sql = sql.replace(
            f"`spark_catalog`.`{ALIAS_DATABASE}`.`{role}`",
            f"delta.`{DELTA / role}`",
        )
    parameters = {
        f"p{slot['position']}": slot["value"]
        for slot in artifact.get("parameters", [])
    }
    rows = [row.asDict(recursive=True) for row in spark.sql(sql, args=parameters).collect()]
    return sql, rows


def main():
    compiled = json.loads(COMPILED.read_text(encoding="utf-8"))
    observation_path = HERE / "r59-vector-observation.json"
    observation = json.loads(observation_path.read_text(encoding="utf-8"))
    if observation["publication"]["publication_id"] != PUBLICATION_ID:
        raise ValueError("R59 vector observation publication ID differs")
    if observation["publication"]["profile_version"] != "ashlar-delta/0.4-xxhash64":
        raise ValueError("Exact `.4-xxhash64` vector required")
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
    opening_heads = table_heads(spark)
    selected_heads = {
        role: observation["roles"][role]["version"] for role in ROLES
    }
    if opening_heads != selected_heads:
        raise RuntimeError("Opening complete R59 heads differ from selected vector")

    executed = {}
    sql_digests = {}
    try:
        for name in ("singleton", "grouped", "page", "continuation"):
            request = compiled[name]["request"]
            response = compiled[name]["response"]
            sql, rows = execute(spark, response)
            executed[name] = rows
            sql_digests[name] = sha256(sql.encode("utf-8"))
        closing_heads = table_heads(spark)
        if closing_heads != opening_heads:
            raise RuntimeError("Closing complete R59 heads changed")
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
        "source_oracle": "Decoded exact retained R59 source page event bytes; no old G03 row oracle reused.",
        "setup_attempts": [
            {
                "outcome": "refused before native query results",
                "failure": "UNBOUND_SQL_PARAMETER p1",
                "cause": "First local Spark attempt omitted spark.sql.legacy.parameterSubstitution.constantsOnly=true.",
                "classification": "Runtime setup refusal; no publication data result was produced.",
            }
        ],
        "scope": "Local native Weft read only; no source/ACK interaction, Delta mutation, cloud, managed Unity Catalog, or engine execution.",
    }
    OUTPUT.write_text(json.dumps(result, sort_keys=True, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(json.dumps({"status": result["status"], "application_id": application_id, "queries": list(result["queries"]), "output": str(OUTPUT)}, sort_keys=True))


if __name__ == "__main__":
    main()
