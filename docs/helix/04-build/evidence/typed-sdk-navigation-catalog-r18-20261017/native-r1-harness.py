"""Owned Spark4/Delta4 + PostgreSQL18 pins -> original Weft -> typed SDK.

Explicit local development carrier IDs are not accepted Truss IDs. Native
publication UUID/version, PostgreSQL roles/locks and retained files are real.
"""

import argparse
import copy
import hashlib
import json
import subprocess
import sys
import threading
import uuid
from contextlib import contextmanager
from datetime import timedelta
from pathlib import Path

import psycopg
import weft
from pyspark.sql import SparkSession
from umf_access.engine_catalog import EngineRead
from umf_access.pages import StaleContinuation, StreamLimitReached
from umf_access.types import StrictModel

from ashlar.access import AshlarTarget
from ashlar.native import NativeBackend, SQLResult
from ashlar.pins import PinVector, PostgresPins
from ashlar.publication import ResolutionError
from ashlar_host.postgres import Session

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--generated", type=Path, required=True)
parser.add_argument("--model-request", type=Path, required=True)
parser.add_argument("--jars", type=Path, required=True)
parser.add_argument("--conformance-root", type=Path)
args = parser.parse_args()
if args.conformance_root is not None:
    sys.path.insert(0, str(args.conformance_root.absolute()))
    import conformance

    if (
        Path(conformance.__file__).resolve().parent
        != (args.conformance_root / "conformance").resolve()
    ):
        raise RuntimeError("Requested local shared conformance tooling was not loaded")
root = Path(__file__).resolve().parents[1]
out = args.output.absolute()
out.mkdir(parents=True, exist_ok=False)
sys.path.insert(0, str(args.generated.absolute()))
from native_related.keys import CustomerKey
from native_related.models import Order


class Parameters(StrictModel):
    status: str


expected = {
    "delta-spark_2.13-4.0.0.jar": "538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e",
    "delta-storage-4.0.0.jar": "9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d",
}
assert all(
    hashlib.sha256((args.jars / n).read_bytes()).hexdigest() == v
    for n, v in expected.items()
)
pgbin = Path("/opt/homebrew/opt/postgresql@18/bin")
pg = out / "postgres"
sock = out / "socket"
sock.mkdir()
started = False
spark = None
checks = []
status = "fail"


def run(*command):
    p = subprocess.run(
        list(map(str, command)), capture_output=True, text=True, check=True
    )
    with (out / "lifecycle.log").open("a") as f:
        f.write(p.stdout + p.stderr)


try:
    run(pgbin / "initdb", "-D", pg, "-A", "trust", "-U", "sdk_test", "--no-sync")
    run(
        pgbin / "pg_ctl",
        "-D",
        pg,
        "-l",
        out / "postgres.log",
        "-o",
        f"-F -k {sock} -c listen_addresses='' -p 55496",
        "-w",
        "start",
    )
    started = True
    dsn = f"host={sock} port=55496 dbname=postgres user=sdk_test"
    with psycopg.connect(dsn, autocommit=True) as admin:
        for name in (
            "01-postgresql.sql",
            "03-read-guard.sql",
            "04-table-guard.sql",
            "05-maintenance-quarantine.sql",
            "06-availability-guard.sql",
        ):
            admin.execute((root / "sql/ashlar-pins" / name).read_text())
        admin.execute(
            "CREATE ROLE sdk_person LOGIN;GRANT ashlar_pin_reader,ashlar_pin_writer TO sdk_person;CREATE ROLE sdk_maintenance LOGIN;GRANT ashlar_pin_maintenance TO sdk_maintenance"
        )
    spark = (
        SparkSession.builder.master("local[1]")
        .appName("Ashlar original typed SDK native E2E")
        .config("spark.ui.enabled", "false")
        .config("spark.driver.memory", "512m")
        .config("spark.sql.shuffle.partitions", "1")
        .config("spark.databricks.delta.snapshotPartitions", "1")
        .config("spark.sql.ansi.enabled", "true")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.jars", ",".join(str(args.jars / n) for n in expected))
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
        .config("spark.sql.warehouse.dir", str(out / "warehouse"))
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    spark.sql(f"CREATE DATABASE sdk_ashlar LOCATION '{out / 'database'}'")
    table = "spark_catalog.sdk_ashlar.object_current"
    edge_table = "spark_catalog.sdk_ashlar.edge_current"
    manifest_table = "spark_catalog.sdk_ashlar.manifest"
    spark.sql(
        f"CREATE TABLE {table} (source_system STRING,type_id BIGINT,id BIGINT,schema_revision STRING,props_json STRING) USING DELTA LOCATION '{out / 'objects'}'"
    )
    model = json.loads(args.model_request.read_bytes())["binding"]["modelJson"]
    source_sha = hashlib.sha256(model.encode()).hexdigest()
    document = json.loads(model)
    spark.sql(
        f"INSERT INTO {table} VALUES (:source,17,1,:revision,:a),(:source,17,2,:revision,:b),(:source,17,3,:revision,:c)",
        args={
            "source": "typed-sdk-local",
            "revision": source_sha,
            **{key: json.dumps({"1": key, "2": "draft"}) for key in ("a", "b", "c")},
        },
    )
    spark.sql(
        f"INSERT INTO {table} VALUES (:source,18,100,:revision,:a),(:source,18,101,:revision,:b),(:source,18,102,:revision,:c)",
        args={
            "source": "typed-sdk-local",
            "revision": source_sha,
            **{
                key: json.dumps({"1": "customer-" + key, "2": "Name " + key})
                for key in ("a", "b", "c")
            },
        },
    )
    spark.sql(
        f"CREATE TABLE {edge_table} (source_system STRING,rel_type_id BIGINT,id BIGINT,source_id BIGINT,source_type BIGINT,target_id BIGINT,target_type BIGINT,schema_revision STRING) USING DELTA LOCATION '{out / 'edges'}'"
    )
    spark.sql(
        f"INSERT INTO {edge_table} VALUES (:source,110,1,1,17,100,18,:revision),(:source,110,2,1,17,100,18,:revision),(:source,110,3,1,17,101,18,:revision),(:source,110,4,2,17,102,18,:revision)",
        args={"source": "typed-sdk-local", "revision": source_sha},
    )
    edge_detail = spark.sql("DESCRIBE DETAIL " + edge_table).first().asDict()
    edge_version = int(spark.sql("DESCRIBE HISTORY " + edge_table).first()["version"])
    detail = spark.sql("DESCRIBE DETAIL " + table).first().asDict()
    version = int(spark.sql("DESCRIBE HISTORY " + table).first()["version"])
    data_files = tuple((out / "objects").glob("*.parquet")) + tuple(
        (out / "edges").glob("*.parquet")
    )
    assert data_files
    originals = {str(p): hashlib.sha256(p.read_bytes()).hexdigest() for p in data_files}
    publication_id = str(uuid.uuid4())
    row = {
        "publication_id": publication_id,
        "profile_version": "ashlar-delta/0.3",
        "table_versions_json": json.dumps({table: version, edge_table: edge_version}),
        "source_progress_json": json.dumps({"typed-sdk-local": {"offset": "1"}}),
        "schema_revisions_json": json.dumps({"typed-sdk-local": source_sha}),
        "validation_report_json": json.dumps(
            {
                "complete": True,
                "sourceSha256": source_sha,
                "scope": "local explicit development carrier mapping; not accepted Truss identities",
            }
        ),
    }
    spark.sql(
        f"CREATE TABLE {manifest_table} (publication_id STRING,profile_version STRING,table_versions_json STRING,source_progress_json STRING,schema_revisions_json STRING,validation_report_json STRING,recorded_at TIMESTAMP) USING DELTA LOCATION '{out / 'manifest'}'"
    )
    spark.sql(
        f"INSERT INTO {manifest_table} VALUES (:publication_id,:profile_version,:table_versions_json,:source_progress_json,:schema_revisions_json,:validation_report_json,current_timestamp())",
        args=row,
    )
    manifest_detail = spark.sql("DESCRIBE DETAIL " + manifest_table).first().asDict()
    raw_descriptor = dict(
        row,
        recorded_at=spark.sql(
            f"SELECT cast(unix_micros(recorded_at) AS STRING) AS value FROM {manifest_table}"
        ).first()["value"],
    )
    custody = hashlib.sha256(
        json.dumps(raw_descriptor, sort_keys=True, separators=(",", ":")).encode()
    ).hexdigest()
    vector = PinVector(
        "typed-sdk-owned-local",
        "manifest",
        publication_id,
        custody,
        {table: (detail["id"], version), edge_table: (edge_detail["id"], edge_version)},
    )
    context = object()
    active = {
        "pins": False,
        "deny": False,
        "policyRevision": 1,
        "pinActor": None,
        "faultSql": None,
        "fault": None,
    }
    native_calls = []

    class PinExecutor:
        @contextmanager
        def transaction(self, supplied):
            assert supplied is context
            params = psycopg.conninfo.conninfo_to_dict(dsn)
            params["user"] = "sdk_person"
            with psycopg.connect(**params) as connection:
                connection.execute("SET LOCAL statement_timeout='10000ms'")
                active["pinActor"] = connection.execute(
                    "SELECT current_user,(SELECT oid::text FROM pg_catalog.pg_roles WHERE rolname=current_user),current_database()"
                ).fetchone()
                session = Session(connection)
                try:
                    yield session
                finally:
                    session.active = False

    class PinPolicy:
        def __init__(self, expected_vector=vector, expected_files=originals):
            self.expected_vector, self.expected_files = expected_vector, expected_files

        def admit_registration(self, original, supplied):
            assert (
                supplied is context
                and original == self.expected_vector
                and all(Path(p).is_file() for p in self.expected_files)
            )

        def authorize_read(self, original, supplied):
            assert supplied is context and original == self.expected_vector
            if active["deny"]:
                raise PermissionError("local reader revoked")

    pins = PostgresPins(PinExecutor(), PinPolicy())
    pins.register(vector, context=context)

    class HeldPins:
        def __init__(self, original=pins):
            self.original = original

        @contextmanager
        def hold(self, original, *, context):
            with self.original.hold(original, context=context):
                active["pins"] = True
                try:
                    yield original
                finally:
                    active["pins"] = False

        @contextmanager
        def hold_page(self, original, *, context):
            with self.original.hold_page(original, context=context):
                active["pins"] = True
                try:
                    yield original
                finally:
                    active["pins"] = False

    class Executor:
        def query(self, sql, parameters):
            assert active["pins"]
            native_calls.append(sql)
            frame = spark.sql(sql, args=parameters)
            schema = tuple(
                (f.name, f.dataType.simpleString().upper()) for f in frame.schema
            )
            rows = [r.asDict(recursive=True) for r in frame.collect()]
            if active["fault"] is not None and sql == active["faultSql"]:
                fault = active["fault"]
                active["fault"] = None
                active["faultAt"] = len(native_calls)
                fault()
            return SQLResult(rows, schema)

    class NativePolicy:
        def __init__(
            self,
            expected_vector=vector,
            expected_descriptor=raw_descriptor,
            expected_files=originals,
            expected_publication=publication_id,
        ):
            (
                self.expected_vector,
                self.expected_descriptor,
                self.expected_files,
                self.expected_publication,
            ) = (
                expected_vector,
                expected_descriptor,
                expected_files,
                expected_publication,
            )

        def authorize(self, supplied, pid, tables):
            if (
                supplied is not context
                or not active["pins"]
                or active["deny"]
                or pid != self.expected_publication
                or set(tables) != set(self.expected_vector.targets)
            ):
                raise PermissionError("original local publication authority refused")

        def validate_descriptor(self, descriptor, supplied):
            if (
                supplied is not context
                or dict(descriptor.raw) != self.expected_descriptor
            ):
                raise PermissionError("original native descriptor differs")

        def validate_snapshot(self, name, identity, retained, columns):
            if (
                not active["pins"]
                or name not in self.expected_vector.targets
                or (identity, retained) != self.expected_vector.targets[name]
            ):
                raise PermissionError("original held native snapshot differs")
            if any(
                not Path(p).is_file()
                or hashlib.sha256(Path(p).read_bytes()).hexdigest() != sha
                for p, sha in self.expected_files.items()
            ):
                raise PermissionError("original retained data files unavailable")

    executor = Executor()
    backend = NativeBackend(
        executor,
        NativePolicy(),
        manifest_table,
        manifest_detail["id"],
        {
            table: (
                ("source_system", "STRING"),
                ("type_id", "BIGINT"),
                ("id", "BIGINT"),
                ("schema_revision", "STRING"),
                ("props_json", "STRING"),
            ),
            edge_table: (
                ("source_system", "STRING"),
                ("rel_type_id", "BIGINT"),
                ("id", "BIGINT"),
                ("source_id", "BIGINT"),
                ("source_type", "BIGINT"),
                ("target_id", "BIGINT"),
                ("target_type", "BIGINT"),
                ("schema_revision", "STRING"),
            ),
        },
    )
    pin = {
        "documentId": document["id"],
        "revision": source_sha,
        "sha256": source_sha,
        "umfVersion": "0.8.0",
    }

    def logical(element):
        return {
            "documentId": document["id"],
            "revision": source_sha,
            "module": "sales",
            "element": element,
        }

    binding = {
        "profile": "ashlar-databricks-candidate/0.1.0",
        "layoutRevision": "ashlar-delta/0.3",
        "layoutSha256": "ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e",
        "modelPins": [pin],
        "publication": {
            "id": publication_id,
            "manifestUuid": manifest_detail["id"],
            "tables": [
                {"name": table.split("."), "uuid": detail["id"], "version": version}
            ],
        },
        "records": [
            {
                "logical": logical("order"),
                "table": 0,
                "kind": "object",
                "sourceSystem": "typed-sdk-local",
                "typeId": "17",
                "schemaRevision": source_sha,
                "properties": [
                    {
                        "logical": logical(field),
                        "home": {"kind": "props", "propertyId": str(number)},
                    }
                    for number, field in ((1, "id"), (2, "status"))
                ],
            }
        ],
    }
    binding["records"].append(
        {
            "logical": logical("customer"),
            "table": 0,
            "kind": "object",
            "sourceSystem": "typed-sdk-local",
            "typeId": "18",
            "schemaRevision": source_sha,
            "properties": [
                {
                    "logical": logical(field),
                    "home": {"kind": "props", "propertyId": str(number)},
                }
                for number, field in ((1, "customerId"), (2, "customerName"))
            ],
        }
    )
    binding["publication"]["tables"].append(
        {
            "name": edge_table.split("."),
            "uuid": edge_detail["id"],
            "version": edge_version,
        }
    )
    definition = document["modules"][0]["relationships"][0]
    binding["relationships"] = [
        {
            "kind": "edge",
            "logical": {
                "documentId": document["id"],
                "revision": source_sha,
                "module": "sales",
                "relationship": "customers",
            },
            "acceptedDefinition": definition,
            "source": logical("order"),
            "target": logical("customer"),
            "sourceSystem": "typed-sdk-local",
            "schemaRevision": source_sha,
            "typeId": "110",
            "table": 1,
        }
    ]
    binding_json = json.dumps(binding, sort_keys=True, separators=(",", ":"))
    binding_sha = hashlib.sha256(binding_json.encode()).hexdigest()
    request = {
        "interfaceVersion": "weft-compile/0.2.0",
        "dialect": "weft-sql/0.2.0",
        "sql": 'SELECT r.id AS "id", r.status AS "status" FROM sales."order" r WHERE r.status = :status ORDER BY r.id ASC LIMIT 10',
        "parameters": {"status": {"family": "string", "value": "draft"}},
        "modules": [
            {"documentJson": model, "pin": pin, "selectedModuleIds": ["sales"]}
        ],
        "target": {
            "backendId": "ashlar.databricks",
            "backendVersion": "0.1.0-local-spark4-candidate",
            "targetProfile": "spark4.0.1-delta4.0.0-candidate",
            "bindingJson": binding_json,
            "bindingSha256": binding_sha,
        },
        "options": {"allowCandidate": True},
    }
    entry = EngineRead.build(
        weft, request, parameters=Parameters, result=Order, method="ashlar-spark4"
    )
    (out / "original-engine-request.json").write_text(json.dumps(request))
    (out / "engine-catalog.json").write_text(entry.artifact_json)

    class ReadPolicy:
        def __init__(
            self,
            expected_vector=vector,
            expected_descriptor=raw_descriptor,
            expected_files=originals,
            expected_publication=publication_id,
            expected_binding=binding,
            expected_binding_sha=binding_sha,
        ):
            self.expected_vector, self.expected_binding, self.expected_binding_sha = (
                expected_vector,
                expected_binding,
                expected_binding_sha,
            )
            self.native = NativePolicy(
                expected_vector,
                expected_descriptor,
                expected_files,
                expected_publication,
            )

        def bind_descriptor(self, descriptor, original, supplied):
            assert active["pins"] and original == self.expected_vector
            self.native.validate_descriptor(descriptor, supplied)

        def admit_artifact(self, current, artifact, descriptor, supplied):
            if artifact.get("logicalPlan", {}).get("navigation") and active.get(
                "withholdSource"
            ):
                raise PermissionError("complete source input withheld")
            assert (
                active["pins"]
                and current["modules"] == request["modules"]
                and artifact["modelPins"] == [pin]
            )
            assert (
                artifact["bindingSha256"] == self.expected_binding_sha
                and json.loads(current["target"]["bindingJson"])
                == self.expected_binding
            )

        def verify_native_profile(self, profile, supplied):
            assert active["pins"] and supplied is context
            if (
                profile["targetProfile"] != "spark4.0.1-delta4.0.0-candidate"
                or profile["engineVersion"] != spark.version
                or profile["deltaVersion"] != "4.0.0"
                or spark.conf.get("spark.sql.ansi.enabled") != "true"
                or spark.conf.get("spark.sql.session.timeZone") != "UTC"
            ):
                raise ResolutionError("original native Spark profile drift")
            assert all(
                hashlib.sha256((args.jars / n).read_bytes()).hexdigest() == v
                for n, v in expected.items()
            )

        def authorize_query(self, current, descriptor, supplied):
            self.native.authorize(
                supplied,
                self.native.expected_publication,
                tuple(self.expected_vector.targets),
            )

        def admit_record_integrity(self, check, artifact, supplied):
            if (
                check.get("record") not in (logical("order"), logical("customer"))
                or supplied is not context
            ):
                raise ResolutionError("unadmitted original record integrity")

        def admit_relationship_integrity(self, guard, artifact, supplied):
            assert active["pins"] and supplied is context
            if active.get("withholdTarget"):
                raise PermissionError("complete target input withheld")
            assert binding["relationships"][0]["acceptedDefinition"] == definition
            assert all(
                check["relationship"]
                == {
                    "documentId": document["id"],
                    "revision": source_sha,
                    "module": "sales",
                    "relationship": "customers",
                }
                for check in guard["checks"]
            )

        def admit_page_integrity(self, guard, artifact, supplied):
            assert (
                active["pins"]
                and supplied is context
                and guard["key"] == artifact["logicalPlan"]["pageKey"]
            )
            owners = [
                e
                for e in document["modules"][0]["elements"]
                if e.get("kind") == "record"
                and any(
                    key.get("primary") is True
                    and [logical(ref["element"]) for ref in key["fields"]]
                    == guard["key"]["fields"]
                    for key in e.get("keys", [])
                )
            ]
            assert len(owners) == 1
            record = owners[0]
            primary = [key for key in record["keys"] if key.get("primary") is True]
            assert len(primary) == 1 and guard["key"]["id"] == primary[0]["id"]
            assert guard["key"]["fields"] == [
                logical(ref["element"]) for ref in primary[0]["fields"]
            ] and all(ref["module"] == "sales" for ref in primary[0]["fields"])
            assert guard["key"]["types"] == [
                {"family": "string", "facets": {}, "nullable": False}
                for _ in primary[0]["fields"]
            ]

        def page_context(self, descriptor, original, supplied):
            self.bind_descriptor(descriptor, original, supplied)
            self.native.authorize(
                supplied, self.native.expected_publication, tuple(original.targets)
            )
            assert active["pinActor"] is not None
            return json.dumps(
                {
                    "publication": original.scope_id,
                    "custody": original.custody_digest,
                    "nativePinActor": active["pinActor"],
                    "nativeSparkUser": spark.sparkContext.sparkUser(),
                    "policyRevision": active["policyRevision"],
                },
                sort_keys=True,
                separators=(",", ":"),
            ).encode()

        def decode_result(self, column, value, current, supplied):
            assert active["pins"] and supplied is context
            representation = column["representation"]
            if representation["kind"] == "relatedKeys":
                from ashlar.weft_related_keys_decode import (
                    RelatedKeysDecodeConfig,
                    decode_related_keys,
                )

                return decode_related_keys(
                    representation,
                    value,
                    model_pins=[pin],
                    config=RelatedKeysDecodeConfig(8 * 1024 * 1024),
                )
            if (
                representation["kind"] == "scalar"
                and representation["logicalType"]
                == {"family": "string", "facets": {}, "nullable": False}
                and type(value) is str
            ):
                return value
            if representation["kind"] == "value":
                item = json.loads(value)
                if (
                    item.get("state") == "value"
                    and set(item) == {"state", "value"}
                    and type(item["value"]) is str
                ):
                    return item
            raise ResolutionError("original source exact string decoder refused")

        def authorize_result(self, rows, descriptor, supplied):
            assert active["pins"]
            self.native.authorize(
                supplied,
                self.native.expected_publication,
                tuple(self.expected_vector.targets),
            )

    @contextmanager
    def deadline(budget):
        group = "typed-sdk-" + str(uuid.uuid4())
        spark.sparkContext.setJobGroup(
            group, "bounded original Ashlar typed read", interruptOnCancel=True
        )
        timer = threading.Timer(
            budget.total_seconds(), lambda: spark.sparkContext.cancelJobGroup(group)
        )
        timer.daemon = True
        timer.start()
        try:
            yield
        finally:
            timer.cancel()
            spark.sparkContext.setLocalProperty("spark.jobGroup.id", None)
            spark.sparkContext.setLocalProperty("spark.job.description", None)
            spark.sparkContext.setLocalProperty("spark.job.interruptOnCancel", None)

    from umf_access.codec_support import make_projection_model
    from umf_access.related_keys import make_related_result_model

    class Empty(StrictModel):
        pass

    request["sql"] = (
        'SELECT r.id AS "id", RELATED_KEYS(r.customers, 2) AS "related" FROM sales."order" r ORDER BY r.id'
    )
    request["parameters"] = {}
    compiled = json.loads(
        weft.compile_engine_read_json(
            json.dumps(
                {
                    "version": "weft.engine-read/0.1",
                    "compiler": "ashlar-spark4",
                    "request": request,
                }
            )
        )
    )
    assert compiled["status"] == "compiled", compiled
    decoder = compiled["outputs"][1]["decoder"]
    row_model = make_projection_model("OriginalOrderId", (("id", Order, "id"),))
    result_model = make_related_result_model(
        "OriginalOrderCustomerKeys", row_model, CustomerKey, "related", decoder, Order
    )
    entry = EngineRead.build(
        weft, request, parameters=Empty, result=result_model, method="ashlar-spark4"
    )
    (out / "original-engine-related.json").write_text(json.dumps(compiled))
    target = AshlarTarget(
        compiler=weft,
        compiler_method="ashlar-spark4",
        executor=executor,
        backend=backend,
        pins=HeldPins(),
        vector=vector,
        policy=ReadPolicy(),
        context=context,
        supported_profiles=["ashlar-delta/0.3"],
        supported_revisions={"typed-sdk-local": [source_sha]},
        binding_sha256=binding_sha,
        platform=(("spark", "4.0.1"), ("delta", "4.0.0"), ("postgresql", "18.6")),
        default_timeout=timedelta(seconds=60),
        deadline=deadline,
        catalog_entries=(entry,),
    )
    # Independent original physical-table oracle. No compiled query or decoder is
    # used to determine edge occurrence order, multiplicity or truncation.
    oracle = {name: [] for name in ("a", "b", "c")}
    native = spark.sql(
        f"SELECT get_json_object(s.props_json,'$.1') AS source_key,get_json_object(t.props_json,'$.1') AS target_key,e.id AS edge_id FROM {edge_table} e JOIN {table} s ON s.id=e.source_id AND s.type_id=e.source_type JOIN {table} t ON t.id=e.target_id AND t.type_id=e.target_type ORDER BY source_key,target_key,edge_id"
    ).collect()
    for row in native:
        oracle[row.source_key].append(row.target_key)
    expected_rows = tuple(
        (key, tuple(values[:2]), len(values) > 2)
        for key, values in sorted(oracle.items())
    )
    actual = target.read(entry, Empty())
    projected = tuple(
        (
            row.id,
            tuple(key.customer_id for key in row.related.items),
            row.related.truncated,
        )
        for row in actual
    )
    assert projected == expected_rows, (projected, expected_rows)
    checks.append(
        "original native relationship guards and generated CustomerKey bags match independent physical edge oracle"
    )
    assert projected[0] == ("a", ("customer-a", "customer-a"), True)
    assert projected[1] == ("b", ("customer-c",), False) and projected[2] == (
        "c",
        (),
        False,
    )
    checks.append(
        "parallel native edges, full-prefix truncation, partial and empty bags retain original meanings"
    )
    assert all(type(key) is CustomerKey for row in actual for key in row.related.items)
    checks.append(
        "all native related Key occurrences use exact current generated Key class"
    )
    before = len(native_calls)
    active["withholdTarget"] = True
    try:
        target.read(entry, Empty())
    except PermissionError:
        pass
    else:
        raise AssertionError("withheld complete target input released related bag")
    assert compiled["artifact"]["sql"] not in native_calls[before:]
    active["withholdTarget"] = False
    checks.append(
        "withheld complete native target authority refuses before business SQL instead of empty bag"
    )
    before = len(native_calls)
    active["deny"] = True
    try:
        target.read(entry, Empty())
    except PermissionError:
        pass
    else:
        raise AssertionError("revoked native publication authority accepted")
    assert compiled["artifact"]["sql"] not in native_calls[before:]
    active["deny"] = False
    checks.append("native publication authority revocation refuses typed related read")
    spark.sql(
        f"INSERT INTO {edge_table} VALUES (:source,110,5,3,17,100,18,:revision)",
        args={"source": "typed-sdk-local", "revision": source_sha},
    )
    unchanged = target.read(entry, Empty())
    assert (
        tuple(
            (
                row.id,
                tuple(key.customer_id for key in row.related.items),
                row.related.truncated,
            )
            for row in unchanged
        )
        == expected_rows
    )
    checks.append(
        "later native edge mutation cannot alter original immutable publication bags"
    )
    # Successor: actual public CLI -> generated WithKeys -> engine target.
    import importlib
    import shutil
    from dataclasses import replace

    from umf_access.capture import capture
    from umf_access.dsl import Node, render_logical
    from umf_access.dsl.render import render_related_logical

    generated = out / "generated"
    shutil.copytree(args.generated, generated)
    declarations = Path(
        "/private/tmp/sdk-engine-related-20261017/related_declarations.py"
    ).read_text()
    declarations += """
from native_related.models import Customer
from native_related.keys import CustomerKey

@query
def navigation_uncapped(source_status: Param[str], target_name: Param[str]):
    return Order.objects.filter(Order.c.status == source_status).related(Order.c.customers).filter(Customer.c.customer_name == target_name).select(Customer.c.customer_id).order_by(Customer.c.customer_id.asc()).limit(10).all()

@query
def navigation_limited():
    return Order.objects.order_by(Order.c.id.asc()).limit(1).related(Order.c.customers).select(Customer.c.customer_id).order_by(Customer.c.customer_id.asc()).limit(10).all()

@query
def navigation_inverse():
    return Customer.objects.filter(Customer.c.customer_id == 'customer-a').related(Order.c.customers.inverse()).select(Order.c.id).order_by(Order.c.id.asc()).limit(10).all()

@query
def navigation_pages(after: Param[CustomerKey | None]):
    return Order.objects.filter(Order.c.status == 'draft').related(Order.c.customers).select(Customer.c.customer_id).page(size=1, after=after)
"""
    (generated / "related_declarations.py").write_text(declarations)
    # Generated source package imports already refer to identical retained bytes.
    sys.path.insert(0, str(generated))
    sys.modules.pop("related_declarations", None)
    authored = importlib.import_module("related_declarations")
    template = json.loads(
        Path(
            "/private/tmp/sdk-engine-related-20261017/generated/related-config.json"
        ).read_text()
    )
    selected = template["targets"][0]
    selected["engineCompiler"] = "ashlar-spark4-pages"
    selected["backendVersion"] = "0.1.0-local-spark4-pages-candidate"
    selected["platform"] = [
        {"name": name, "versions": [version]} for name, version in target.platform
    ]
    owner_requests = {}
    for name in (
        "related_rows",
        "related_pages",
        "navigation_uncapped",
        "navigation_limited",
        "navigation_inverse",
        "navigation_pages",
    ):
        variant = capture(getattr(authored, name)).variants[0]
        is_page = variant.node.op == "page"
        if is_page:
            variant = replace(variant, node=Node("all", variant.node.args[:2]))
        navigation = render_related_logical(
            variant, binding=selected["binding"], owner_bound=is_page
        )
        rendered_logical = (
            navigation.target_sql
            if navigation is not None
            else render_logical(
                variant, binding=selected["binding"], owner_bound=is_page
            )
        )
        rendered_logical = rendered_logical.replace(
            'SELECT "r"."id",', 'SELECT "r"."id" AS "col_0",'
        )
        owned = copy.deepcopy(request)
        if name.startswith("navigation_"):
            import re

            rendered_logical = re.sub(
                r'^(SELECT "r"\."[^"]+")( FROM)', r'\1 AS "col_0"\2', rendered_logical
            )
        owned["sql"] = rendered_logical
        owned["parameters"] = (
            {
                "source_status": {"family": "string", "value": "draft"},
                "target_name": {"family": "string", "value": "Name a"},
            }
            if name == "navigation_uncapped"
            else {}
        )
        owned["interfaceVersion"] = "weft-compile/0.2.2"
        owned["target"]["backendVersion"] = selected["backendVersion"]
        owner_requests[name + ":0"] = owned
    # R14 proves exact authored-Key ordering redundant, while a different Field
    # order must remain outside this original Key-cursor profile.
    explicit_order = copy.deepcopy(owner_requests["related_pages:0"])
    explicit_order["sql"] += ' ORDER BY "r"."id" ASC NULLS LAST'
    before = len(native_calls)
    accepted_order = json.loads(
        weft.compile_engine_page_json(
            json.dumps(
                {
                    "version": "weft.engine-page/0.1",
                    "compiler": "ashlar-spark4-pages",
                    "request": explicit_order,
                    "pageSize": 1,
                }
            )
        )
    )
    assert (
        accepted_order["status"] == "compiled"
        and accepted_order["request"] == explicit_order
    ), accepted_order
    wrong_order = copy.deepcopy(explicit_order)
    wrong_order["sql"] = wrong_order["sql"].replace(
        'ORDER BY "r"."id"', 'ORDER BY "r"."status"'
    )
    refused = json.loads(
        weft.compile_engine_page_json(
            json.dumps(
                {
                    "version": "weft.engine-page/0.1",
                    "compiler": "ashlar-spark4-pages",
                    "request": wrong_order,
                    "pageSize": 1,
                }
            )
        )
    )
    assert refused["status"] == "blocked" and any(
        d["code"] == "WFT-PROFILE" for d in refused["original"]["diagnostics"]
    ), refused
    assert len(native_calls) == before
    (out / "exact-key-order-program.json").write_text(json.dumps(accepted_order))
    (out / "non-key-order-page-refusal.json").write_text(json.dumps(refused))
    checks.append(
        "original authored-Key page preserves exact Key order and refuses non-Key order without native SQL"
    )
    selected["engineRequests"] = owner_requests
    config = generated / "related-native-config.json"
    config.write_text(json.dumps(template))
    for name, extra in (("build", []), ("check", ["--check"])):
        built = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "umf_access.build.catalog",
                "catalog",
                "--root",
                str(generated),
                "--config",
                str(config),
                *extra,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        (out / ("catalog-" + name + ".log")).write_text(built.stdout + built.stderr)
        assert built.returncode == 0, built.stdout + built.stderr
    sys.modules.pop("native_related.queries", None)
    # Source package was loaded before staging; its path is redirected only to
    # the exact copied generated bytes for importing the newly built catalog.
    import native_related

    native_related.__path__ = [str(generated / "native_related")]
    queries = importlib.import_module("native_related.queries")
    entries = tuple(
        variant.entry
        for name, value in vars(queries).items()
        if name.startswith("_Q") and hasattr(value, "variants")
        for variant in value.variants
    )
    catalog_target = AshlarTarget(
        compiler=weft,
        compiler_method="ashlar-spark4-pages",
        executor=executor,
        backend=backend,
        pins=HeldPins(),
        vector=vector,
        policy=ReadPolicy(),
        context=context,
        supported_profiles=["ashlar-delta/0.3"],
        supported_revisions={"typed-sdk-local": [source_sha]},
        binding_sha256=binding_sha,
        platform=target.platform,
        default_timeout=timedelta(seconds=60),
        deadline=deadline,
        catalog_entries=entries,
    )

    def observed(items):
        result = []
        for item in items:
            assert type(item.row) is tuple and len(item.row) == 1
            assert all(type(key) is CustomerKey for key in item.keys.items)
            result.append(
                (
                    item.row[0],
                    tuple(key.customer_id for key in item.keys.items),
                    item.keys.truncated,
                )
            )
        return tuple(result)

    assert observed(queries.related_rows(catalog_target)) == expected_rows
    checks.append(
        "installed public catalog build/check and generated WithKeys rows match independent native edge oracle"
    )
    first = queries.related_pages(catalog_target, after=None)
    second = queries.related_pages(catalog_target, after=first.next_after)
    third = queries.related_pages(catalog_target, after=second.next_after)
    assert observed((*first.items, *second.items, *third.items)) == expected_rows
    assert (
        first.next_after is not None
        and second.next_after is not None
        and third.next_after is None
    )
    checks.append(
        "generated original related-Key pages preserve complete bags and authored root Key continuations"
    )
    assert (
        observed(
            tuple(
                queries.related_pages_stream(
                    catalog_target, after=None, max_rows=10, page_size=1
                )
            )
        )
        == expected_rows
    )
    checks.append(
        "generated related-Key stream naturally exhausts exact original publication"
    )
    stream = queries.related_pages_stream(
        catalog_target, after=None, max_rows=2, page_size=1
    )
    assert observed((next(stream), next(stream))) == expected_rows[:2]
    try:
        next(stream)
    except StreamLimitReached as limit:
        assert limit.max_rows == 2 and limit.truncated is None
    else:
        raise AssertionError(
            "generated related stream silently truncated at exact full page"
        )
    assert not active["pins"]
    stream = queries.related_pages_stream(catalog_target, after=None, max_rows=10)
    assert observed((next(stream),)) == expected_rows[:1]
    stream.close()
    assert not active["pins"]
    checks.append(
        "generated bounded related stream signals truncation and early close releases original pins"
    )
    active["policyRevision"] += 1
    try:
        queries.related_pages(catalog_target, after=first.next_after)
    except StaleContinuation:
        pass
    else:
        raise AssertionError(
            "related page continued across original policy context change"
        )
    active["policyRevision"] -= 1
    checks.append(
        "generated related continuation refuses changed original policy context"
    )
    active["withholdTarget"] = True
    try:
        queries.related_rows(catalog_target)
    except PermissionError:
        pass
    else:
        raise AssertionError("generated WithKeys released withheld target bag")
    active["withholdTarget"] = False
    catalog_target.verify()
    assert not active["pins"]
    checks.append(
        "generated WithKeys preserves full-target policy refusal and actual target verification"
    )
    # Independent native graph oracle over the exact original publication, not
    # compiled SQL, engine IDs supplied by projections, or later mutable heads.
    physical = spark.sql(
        f"SELECT DISTINCT get_json_object(s.props_json,'$.1') AS sid,get_json_object(s.props_json,'$.2') AS status,get_json_object(t.props_json,'$.1') AS tid,get_json_object(t.props_json,'$.2') AS name FROM {edge_table} VERSION AS OF {edge_version} e JOIN {table} VERSION AS OF {version} s ON s.id=e.source_id AND s.type_id=e.source_type JOIN {table} VERSION AS OF {version} t ON t.id=e.target_id AND t.type_id=e.target_type ORDER BY sid,tid"
    ).collect()

    def targets(status, name):
        return tuple(
            (key,)
            for key in sorted(
                {r.tid for r in physical if r.status == status and r.name == name}
            )
        )

    assert tuple(
        queries.navigation_uncapped(
            catalog_target, source_status="draft", target_name="Name a"
        )
    ) == targets("draft", "Name a")
    assert tuple(
        queries.navigation_uncapped(
            catalog_target, source_status="draft", target_name="Name c"
        )
    ) == targets("draft", "Name c")
    assert (
        tuple(
            queries.navigation_uncapped(
                catalog_target, source_status="absent", target_name="Name a"
            )
        )
        == targets("absent", "Name a")
        == ()
    )
    checks.append(
        "generated target-set uncapped source/target-only parameters and empty source match independent pinned native oracle"
    )
    smallest_source = min(r.sid for r in physical)
    limited = tuple(
        (key,) for key in sorted({r.tid for r in physical if r.sid == smallest_source})
    )
    assert tuple(queries.navigation_limited(catalog_target)) == limited
    inverse = tuple(
        (key,) for key in sorted({r.sid for r in physical if r.tid == "customer-a"})
    )
    assert tuple(queries.navigation_inverse(catalog_target)) == inverse
    checks.append(
        "generated forward explicit source LIMIT and inverse authored edge membership deduplicate parallel native edges"
    )
    expected_navigation = tuple(
        (key,) for key in sorted({r.tid for r in physical if r.status == "draft"})
    )
    pages = []
    cursor = None
    for _ in range(4):
        page = queries.navigation_pages(catalog_target, after=cursor)
        pages.extend(page.items)
        cursor = page.next_after
        if cursor is None:
            break
    assert tuple(pages) == expected_navigation and cursor is None
    assert (
        tuple(queries.navigation_pages_stream(catalog_target, after=None, max_rows=10))
        == expected_navigation
    )
    checks.append(
        "generated target-set original target Key pages and stream exactly exhaust independent pinned target membership"
    )
    for flag in ("withholdSource", "withholdTarget"):
        active[flag] = True
        before = len(native_calls)
        try:
            queries.navigation_uncapped(
                catalog_target, source_status="draft", target_name="Name a"
            )
        except PermissionError:
            pass
        else:
            raise AssertionError(
                "withheld original navigation scope released target results"
            )
        assert not active["pins"]
        active[flag] = False
    checks.append(
        "withheld original navigation source or target refuses rather than yielding empty target membership"
    )
    status = "pass"
finally:
    if spark is not None:
        spark.stop()
    if started:
        run(pgbin / "pg_ctl", "-D", pg, "-m", "fast", "-w", "stop")
    (out / "verification.json").write_text(
        json.dumps(
            {
                "status": status,
                "checks": checks,
                "ownedPostgresStopped": not (pg / "postmaster.pid").exists(),
                "sparkStopped": spark is None or spark.sparkContext._jsc is None,
                "temporal": "HELD",
                "scope": "local Spark4/Delta4 generated original target-set navigation and bounded related-Key reads with PostgreSQL18 pins; no managed search, engine writes or performance claim",
            },
            indent=2,
        )
        + "\n"
    )
print(json.dumps({"status": "pass", "checks": len(checks), "output": str(out)}))
