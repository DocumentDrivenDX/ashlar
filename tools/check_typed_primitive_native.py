"""Owned Spark4/Delta4 + PostgreSQL18 pins -> original Weft -> typed SDK.

Explicit local development carrier IDs are not accepted Truss IDs. Native
publication UUID/version, PostgreSQL roles/locks and retained files are real.
"""

import argparse
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
parser.add_argument("--primitive-fixture", type=Path, required=True)
parser.add_argument("--prepare-only", action="store_true")
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


model = json.loads(args.model_request.read_bytes())["binding"]["modelJson"]
source_sha = hashlib.sha256(model.encode()).hexdigest()
document = json.loads(model)
primitive_fixture = json.loads(args.primitive_fixture.read_text())
assert primitive_fixture["sourceSha256"] == source_sha

# Actual original source/instance checks precede fixture publication.
from decimal import Decimal

from ashlar.primitive_field_binding import PrimitiveFieldBinding
from ashlar.schema import SchemaIntake
from ashlar.typed_source_policy import (
    RECORD_CHECK_PIN,
    typed_values,
)

producer = Path("/private/tmp/sdk-a1-umf-c45c72-source")
primitive_bindings = {
    17: {
        "record": ("sales", "order"),
        "fields": {
            "1": ("sales", "id", "string"),
            "2": ("sales", "status", "string"),
            "3": ("sales", "quantity", "integer"),
            "4": ("sales", "amount", "decimal"),
            "5": ("sales", "active", "boolean"),
            "6": ("sales", "unsignedQuantity", "integer"),
            "7": ("sales", "wideAmount", "decimal"),
        },
    },
    18: {
        "record": ("sales", "customer"),
        "fields": {
            "1": ("sales", "customerId", "string"),
            "2": ("sales", "customerName", "string"),
        },
    },
}
source_rows = [
    (17, index, primitive_fixture["orderProps"][key])
    for index, key in enumerate(("a", "b", "c"), 1)
] + [
    (18, 100 + index, json.dumps({"1": "customer-" + key, "2": "Name " + key}))
    for index, key in enumerate(("a", "b", "c"))
]
original_requests = []
for type_id, identity, raw in source_rows:
    digest = hashlib.sha256(raw.encode()).hexdigest()
    original_requests.append(
        {
            "deliveryId": f"owned-{type_id}-{identity}",
            "recordSha256": digest,
            "typeId": type_id,
            "propsSha256": digest,
            "identity": {
                "module": "sales",
                "element": primitive_bindings[type_id]["record"][1],
            },
            "values": typed_values(raw, primitive_bindings[type_id]["fields"]),
        }
    )
source_file = out / "authored-core08.json"
source_file.write_text(model)
values_file = out / "original-value-requests.json"
values_file.write_text(json.dumps({"records": original_requests}))
bun = "/private/tmp/sdk-bun142-recovered/bun"
validated = subprocess.run(
    [
        bun,
        str(root / "tools/check_primitive_fields.ts"),
        str(producer),
        str(source_file),
        str(values_file),
    ],
    capture_output=True,
    check=False,
)
(out / "original-record-producer.stderr").write_bytes(validated.stderr)
assert validated.returncode == 0, validated.stderr.decode()
(out / "original-record-receipt.json").write_bytes(validated.stdout)
for bad_name, bad_value in [
    ("null", "null"),
    ("boolean", "true"),
    ("overflow", "9223372036854775808"),
]:
    raw = primitive_fixture["orderProps"]["a"].replace(
        '"3":9007199254740993', '"3":' + bad_value
    )
    assert raw != primitive_fixture["orderProps"]["a"]
    digest = hashlib.sha256(raw.encode()).hexdigest()
    bad_request = dict(
        original_requests[0],
        deliveryId="invalid-" + bad_name,
        recordSha256=digest,
        propsSha256=digest,
        values=typed_values(raw, primitive_bindings[17]["fields"]),
    )
    bad_file = out / ("original-invalid-" + bad_name + ".json")
    bad_file.write_text(json.dumps({"records": [bad_request]}))
    refused = subprocess.run(
        [
            bun,
            str(root / "tools/check_primitive_fields.ts"),
            str(producer),
            str(source_file),
            str(bad_file),
        ],
        capture_output=True,
        check=False,
    )
    (out / ("original-invalid-" + bad_name + ".log")).write_bytes(
        refused.stdout + refused.stderr
    )
    assert refused.returncode != 0, (
        "original public producer admitted invalid " + bad_name
    )
primitive_policy = PrimitiveFieldBinding(
    model.encode(),
    validated.stdout,
    primitive_bindings,
    source_system="typed-sdk-local",
    schema_revision=source_sha,
    trusted_producer_revision=RECORD_CHECK_PIN,
)
inspected = subprocess.run(
    [
        bun,
        str(root / "tools/inspect_umf.ts"),
        str(producer),
        RECORD_CHECK_PIN,
        str(source_file),
    ],
    capture_output=True,
    check=False,
)
assert inspected.returncode == 0, inspected.stderr.decode()
(out / "original-intake.json").write_bytes(inspected.stdout)
primitive_intake = SchemaIntake.read(
    inspected.stdout, source_sha, trusted_validator_revision=RECORD_CHECK_PIN
)
from pydantic import ValidationError

for _, _, raw in source_rows[:3]:
    props = json.loads(raw, parse_float=Decimal)
    value = Order(
        id=props["1"],
        status=props["2"],
        quantity=props["3"],
        amount=props["4"],
        active=props["5"],
        unsigned_quantity=props["6"],
        wide_amount=props["7"],
    )
    assert (
        value.quantity == props["3"]
        and value.amount.as_tuple() == props["4"].as_tuple()
        and type(value.active) is bool
        and value.unsigned_quantity == props["6"]
        and value.wide_amount.as_tuple() == props["7"].as_tuple()
    )
    for invalid in (None, True, 2**63):
        try:
            Order(
                id=props["1"],
                status=props["2"],
                quantity=invalid,
                amount=props["4"],
                active=props["5"],
                unsigned_quantity=props["6"],
                wide_amount=props["7"],
            )
        except ValidationError as error:
            assert any(item["loc"] == ("quantity",) for item in error.errors()), error
        else:
            raise AssertionError(
                "Actual generated primitive instance admitted null/bool/overflow Integer"
            )
checks.append(
    "actual authored Core08 intake and complete original Field checks precede publication; original incomplete Record observations retained unchanged; generated instances preserve exact primitives and reject null/bool/overflow"
)


if args.prepare_only:
    from ashlar.primitive_binding import (
        decode_primitive_column,
        primitive_compile_request,
    )
    from ashlar.publication import Descriptor

    declaration = Descriptor(
        "compiler-only-not-native",
        "ashlar-delta/0.3",
        {"spark_catalog.compiler_only.object_current": 0},
        {"compiler-only": source_sha},
        {},
        {},
        {},
    )
    for field, token in [
        ("quantity", "9007199254740993"),
        ("amount", "-0.00"),
        ("active", "true"),
        ("unsignedQuantity", "18446744073709551615"),
        ("wideAmount", "99999999999999999999999999999999.123456"),
    ]:
        original_request = primitive_compile_request(
            f'SELECT r.{field} AS value FROM sales."order" r ORDER BY r.id LIMIT 10',
            primitive_intake,
            primitive_policy,
            declaration,
            schema_alias="compiler-only",
            table_uuids={
                "spark_catalog.compiler_only.object_current": "11111111-1111-1111-1111-111111111111"
            },
            manifest_uuid="22222222-2222-2222-2222-222222222222",
            layout_sha256="ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e",
            backend_version="0.1.0-local-spark4-fields-candidate",
            target_profile="spark4.0.1-delta4.0.0-fields-candidate",
        )
        original = json.loads(
            weft.compile_engine_read_json(
                json.dumps(
                    {
                        "version": "weft.engine-read/0.1",
                        "compiler": "ashlar-spark4-fields",
                        "request": original_request,
                    }
                )
            )
        )
        assert original["status"] == "compiled", original
        (out / (field + "-original-program.json")).write_text(
            json.dumps(original, indent=2)
        )
        decoded = decode_primitive_column(
            original["artifact"]["columns"][0],
            token,
            original["artifact"],
            primitive_policy,
            primitive_intake,
        )
        assert decoded.original == token
    (out / "source-preflight.json").write_text(
        json.dumps(
            {
                "status": "pass",
                "nativeExecuted": False,
                "originalFieldPrograms": 5,
                "scope": "Actual source/intake/Field/instance/owner compiler controls with unchanged incomplete Record observations; compiler-only physical declarations are NOT native identities or authority",
                "temporal": "HELD",
            },
            indent=2,
        )
    )
    print(json.dumps({"status": "source-preflight-pass", "nativeExecuted": False}))
    sys.exit(0)

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
    spark.sql(
        f"INSERT INTO {table} VALUES (:source,17,1,:revision,:a),(:source,17,2,:revision,:b),(:source,17,3,:revision,:c)",
        args={
            "source": "typed-sdk-local",
            "revision": source_sha,
            **primitive_fixture["orderProps"],
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
                "fieldChecksComplete": True,
                "originalRecordComplete": False,
                "sourceSha256": source_sha,
                "originalFieldReceiptSha256": primitive_policy.receipt_sha256,
                "originalFieldCheckedInstances": len(original_requests),
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
                    for number, field in (
                        (1, "id"),
                        (2, "status"),
                        (3, "quantity"),
                        (4, "amount"),
                        (5, "active"),
                        (6, "unsignedQuantity"),
                        (7, "wideAmount"),
                    )
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
    binding_json = json.dumps(binding, sort_keys=True, separators=(",", ":"))
    binding_sha = hashlib.sha256(binding_json.encode()).hexdigest()
    request = {
        "interfaceVersion": "weft-compile/0.2.0",
        "dialect": "weft-sql/0.2.0",
        "sql": 'SELECT r.id AS "id", r.status AS "status", r.quantity AS "quantity", r.amount AS "amount", r.active AS "active", r.unsignedQuantity AS "unsigned_quantity", r.wideAmount AS "wide_amount" FROM sales."order" r WHERE r.status = :status ORDER BY r.id ASC LIMIT 10',
        "parameters": {"status": {"family": "string", "value": "draft"}},
        "modules": [
            {"documentJson": model, "pin": pin, "selectedModuleIds": ["sales"]}
        ],
        "target": {
            "backendId": "ashlar.databricks",
            "backendVersion": "0.1.0-local-spark4-fields-candidate",
            "targetProfile": "spark4.0.1-delta4.0.0-fields-candidate",
            "bindingJson": binding_json,
            "bindingSha256": binding_sha,
        },
        "options": {"allowCandidate": True},
    }
    entry = EngineRead.build(
        weft,
        request,
        parameters=Parameters,
        result=Order,
        method="ashlar-spark4-fields",
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
                profile["targetProfile"] != "spark4.0.1-delta4.0.0-fields-candidate"
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

        def admit_relationship_integrity(self, *args):
            raise ResolutionError(
                "Field-only primitive profile has no relationship authority"
            )

        def admit_page_integrity(self, guard, artifact, supplied):
            assert (
                active["pins"]
                and supplied is context
                and guard["key"] == artifact["logicalPlan"]["pageKey"]
            )
            record = next(
                e for e in document["modules"][0]["elements"] if e["id"] == "order"
            )
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
            if (
                representation["kind"] == "scalar"
                and representation["logicalType"]["family"] != "string"
            ):
                from ashlar.primitive_binding import decode_primitive_column

                return decode_primitive_column(
                    column, value, current, primitive_policy, primitive_intake
                )
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

    platform = (("spark", "4.0.1"), ("delta", "4.0.0"), ("postgresql", "18.6"))

    # Genuine generated public catalog and original primitive owner transport.
    import importlib
    import shutil

    import native_related

    from ashlar.primitive_binding import primitive_compile_request
    from ashlar.publication import Descriptor
    from ashlar.weft_decode import ExactScalar

    generated = out / "primitive-generated"
    shutil.copytree(args.generated, generated)
    declarations = args.primitive_fixture.parent / "primitive_declarations.py"
    shutil.copy2(declarations, generated / "primitive_declarations.py")
    descriptor = Descriptor(
        publication_id,
        "ashlar-delta/0.3",
        {table: version, edge_table: edge_version},
        {"typed-sdk-local": source_sha},
        json.loads(raw_descriptor["source_progress_json"]),
        json.loads(raw_descriptor["validation_report_json"]),
        raw_descriptor,
    )
    names = (
        "id",
        "status",
        "quantity",
        "amount",
        "active",
        "unsignedQuantity",
        "wideAmount",
    )
    sql = (
        "SELECT "
        + ", ".join(
            f'"r"."{name}" AS "col_{index}"' for index, name in enumerate(names)
        )
        + ' FROM "sales"."order" AS "r" ORDER BY "r"."id" ASC NULLS LAST LIMIT 10'
    )
    primitive_request = primitive_compile_request(
        sql,
        primitive_intake,
        primitive_policy,
        descriptor,
        schema_alias="typed-sdk-local",
        table_uuids={table: detail["id"], edge_table: edge_detail["id"]},
        manifest_uuid=manifest_detail["id"],
        layout_sha256=binding["layoutSha256"],
        backend_version="0.1.0-local-spark4-fields-candidate",
        target_profile="spark4.0.1-delta4.0.0-fields-candidate",
    )
    primitive_binding = json.loads(primitive_request["target"]["bindingJson"])
    primitive_binding_sha = primitive_request["target"]["bindingSha256"]
    config = json.loads(
        Path(
            "/private/tmp/sdk-engine-related-20261017/generated/related-config.json"
        ).read_text()
    )
    config["modules"] = ["primitive_declarations"]
    selected = config["targets"][0]
    selected["engineCompiler"] = "ashlar-spark4-fields"
    selected["backendVersion"] = "0.1.0-local-spark4-fields-candidate"
    selected["targetProfile"] = "spark4.0.1-delta4.0.0-fields-candidate"
    selected["binding"]["modelJson"] = model
    selected["binding"]["modelSha256"] = source_sha
    for logical_table in selected["binding"]["tables"]:
        if logical_table["logical"] == ["sales", "order"]:
            logical_table["columns"].extend(
                [
                    {
                        "logical": name,
                        "native": name,
                        "carrier": family,
                        "nullable": False,
                    }
                    for name, family in [
                        ("quantity", "integer"),
                        ("amount", "decimal"),
                        ("active", "boolean"),
                        ("unsignedQuantity", "integer"),
                        ("wideAmount", "decimal"),
                    ]
                ]
            )
    selected["engineRequests"] = {"primitive_rows:0": primitive_request}
    selected["platform"] = [
        {"name": name, "versions": [value]} for name, value in platform
    ]
    config_file = generated / "primitive-config.json"
    config_file.write_text(json.dumps(config))
    for step, extra in [("build", []), ("check", ["--check"])]:
        result = subprocess.run(
            [
                sys.executable,
                "-B",
                "-m",
                "umf_access.build.catalog",
                "catalog",
                "--root",
                str(generated),
                "--config",
                str(config_file),
                *extra,
            ],
            capture_output=True,
            text=True,
            check=False,
        )
        (out / f"primitive-public-cli-{step}.log").write_text(
            result.stdout + result.stderr
        )
        assert result.returncode == 0, result.stdout + result.stderr
    native_related.__path__ = [str(generated / "native_related")]
    sys.modules.pop("native_related.queries", None)
    queries = importlib.import_module("native_related.queries")
    entries = tuple(
        variant.entry
        for name, value in vars(queries).items()
        if name.startswith("_Q") and hasattr(value, "variants")
        for variant in value.variants
    )
    assert len(entries) == 1
    closed = {"revoke": False, "originalRows": []}

    class PrimitiveReadPolicy(ReadPolicy):
        def authorize_result(self, rows, descriptor, supplied):
            assert active["pins"]
            assert all(
                len(row) == 7 and all(type(cell) is ExactScalar for cell in row[2:])
                for row in rows
            )
            closed["originalRows"].append(rows)
            super().authorize_result(rows, descriptor, supplied)
            if closed["revoke"]:
                active["deny"] = True

    primitive_target = AshlarTarget(
        compiler=weft,
        compiler_method="ashlar-spark4-fields",
        executor=executor,
        backend=backend,
        pins=HeldPins(),
        vector=vector,
        policy=PrimitiveReadPolicy(
            expected_binding=primitive_binding,
            expected_binding_sha=primitive_binding_sha,
        ),
        context=context,
        supported_profiles=["ashlar-delta/0.3"],
        supported_revisions={"typed-sdk-local": [source_sha]},
        binding_sha256=primitive_binding_sha,
        platform=platform,
        default_timeout=timedelta(seconds=60),
        deadline=deadline,
        catalog_entries=entries,
    )
    # This independent native projection does not copy the emitted engine SQL.
    oracle_sql = f"SELECT get_json_object(props_json,'$.1') AS id,get_json_object(props_json,'$.2') AS status,CAST(get_json_object(props_json,'$.3') AS BIGINT) AS quantity,CAST(CAST(get_json_object(props_json,'$.4') AS DECIMAL(18,2)) AS STRING) AS amount,CAST(get_json_object(props_json,'$.5') AS BOOLEAN) AS active,CAST(CAST(get_json_object(props_json,'$.6') AS DECIMAL(20,0)) AS STRING) AS unsigned_quantity,CAST(CAST(get_json_object(props_json,'$.7') AS DECIMAL(38,6)) AS STRING) AS wide_amount FROM {table} WHERE source_system='typed-sdk-local' AND type_id=17 ORDER BY id"
    assert int(spark.sql("DESCRIBE HISTORY " + table).first()["version"]) == version
    oracle_rows = spark.sql(oracle_sql).collect()
    expected_primitive = tuple(
        (
            row.id,
            row.status,
            int(row.quantity),
            Decimal(row.amount),
            row.active,
            int(row.unsigned_quantity),
            Decimal(row.wide_amount),
        )
        for row in oracle_rows
    )
    (out / "primitive-independent-oracle.json").write_text(
        json.dumps(
            {
                "sql": oracle_sql,
                "rows": [
                    [a, b, str(c), str(d), e, str(f), str(g)]
                    for a, b, c, d, e, f, g in expected_primitive
                ],
            },
            indent=2,
        )
    )
    source_expected = tuple(
        (key, "draft", json.loads(primitive_fixture["orderProps"][key])["3"])
        for key in ("a", "b", "c")
    )
    assert (
        tuple((a, b, c) for a, b, c, _, _, _, _ in expected_primitive)
        == source_expected
    )
    source_wide = tuple(
        (
            json.loads(primitive_fixture["orderProps"][key], parse_float=Decimal)["6"],
            json.loads(primitive_fixture["orderProps"][key], parse_float=Decimal)["7"],
        )
        for key in ("a", "b", "c")
    )
    assert tuple((row[5], row[6]) for row in expected_primitive) == source_wide
    observed = queries.primitive_rows(primitive_target)
    assert tuple(observed) == expected_primitive
    assert all(
        type(row[2]) is int
        and type(row[3]) is Decimal
        and type(row[4]) is bool
        and type(row[5]) is int
        and type(row[6]) is Decimal
        for row in observed
    )
    assert tuple(row[3].as_tuple() for row in observed) == tuple(
        row[3].as_tuple() for row in expected_primitive
    )
    assert tuple(row[6].as_tuple() for row in observed) == tuple(
        row[6].as_tuple() for row in expected_primitive
    )
    assert closed["originalRows"] and all(
        type(cell) is ExactScalar
        for row in closed["originalRows"][-1]
        for cell in row[2:]
    )
    checks.append(
        "actual public CLI build/check and generated primitive query match independent native oracle above2^53/full signed64/UInt64max/Decimal38/Boolean with original carrier custody"
    )
    # Source negative zero bytes remain retained independently of native decimal
    # canonicalization. Never claim a sign unsupported by the native result.
    assert primitive_fixture["orderProps"]["a"].find('"4":-0.00') >= 0
    assert (out / "authored-core08.json").read_text() == model
    checks.append(
        "original signed-zero source is retained; generated decimal tuple matches actual independent native result sign/exponent"
    )
    invalid_latest = primitive_fixture["orderProps"]["a"].replace(
        '"3":9007199254740993', '"3":null'
    )
    spark.sql(
        f"UPDATE {table} SET props_json=:props WHERE type_id=17 AND id=1",
        args={"props": invalid_latest},
    )
    assert int(spark.sql("DESCRIBE HISTORY " + table).first()["version"]) > version
    assert (
        spark.sql(
            f"SELECT get_json_object(props_json,'$.3') AS quantity FROM {table} WHERE type_id=17 AND id=1"
        ).first()["quantity"]
        is None
    )
    assert tuple(queries.primitive_rows(primitive_target)) == expected_primitive
    checks.append(
        "actual later native NULL mutation cannot alter the original accepted primitive snapshot; no latest-table fallback"
    )
    before = len(native_calls)
    active["deny"] = True
    try:
        queries.primitive_rows(primitive_target)
    except PermissionError:
        pass
    else:
        raise AssertionError("withheld primitive view published rows")
    assert entries[0].artifact()["artifact"]["sql"] not in native_calls[before:]
    active["deny"] = False
    checks.append(
        "withheld original complete primitive authority refuses before business SQL"
    )
    closed["revoke"] = True
    try:
        queries.primitive_rows(primitive_target)
    except PermissionError:
        pass
    else:
        raise AssertionError(
            "primitive authority revoked during original result authorization published rows"
        )
    finally:
        closed["revoke"] = False
        active["deny"] = False
    assert not active["pins"]
    checks.append(
        "original authority revocation after exact-carrier authorization refuses final release and closes pins"
    )
    restored = queries.primitive_rows(primitive_target)
    assert tuple(restored) == expected_primitive
    checks.append(
        "fresh explicit primitive call after restored original authority succeeds without retrying revoked call"
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
                "scope": "local Spark4/Delta4 original primitive Field/public source/SDK transport with PostgreSQL18 pins; no managed search, engine writes, temporal or performance claim",
            },
            indent=2,
        )
        + "\n"
    )
print(json.dumps({"status": "pass", "checks": len(checks), "output": str(out)}))
