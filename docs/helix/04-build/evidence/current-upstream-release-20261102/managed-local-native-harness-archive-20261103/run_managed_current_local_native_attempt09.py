#!/usr/bin/env python3
"""One fresh local Spark/Truss run through the managed-current native owners.

This is a local Spark catalog qualification. It does not impersonate Unity
Catalog or Databricks. The only UC metadata query adapted here is the managed
table type query; each value comes from the real Spark catalog table entry.
"""
from __future__ import annotations

import base64
import hashlib
import inspect
import json
import os
from pathlib import Path
import re
import shutil
import stat
import sys
import time
import traceback
from urllib.parse import unquote, urlsplit
import uuid

REPO = Path(__file__).resolve().parents[1]
RUNTIME = Path("/private/tmp/ashlar-r61-runtime-68191905")
ROOT = Path("/private/tmp/managed-current-local-native-20261103-attempt09")
SCHEMA = "managed_current_local_20261103_attempt09"
MODEL = REPO / "docs/helix/04-build/evidence/current-upstream-release-20261101/r21-truss052-fresh-source-custody/r28-page-sequence/r28-model.umf.json"
MAX_WALL_SECONDS = 900
FROZEN_PUBLICATION_SOURCE = REPO / "src/ashlar_host/managed_current_publication.py"
FROZEN_PUBLICATION_SHA256 = "1fe9d88d1b463981e5a12384d3d135f3ef4c687f8d50ab96908e03b9b825f0c4"


def _sha(raw: bytes) -> str:
    return hashlib.sha256(raw).hexdigest()


def _canonical(value) -> bytes:
    return json.dumps(value, sort_keys=True, separators=(",", ":"),
                      ensure_ascii=False, allow_nan=False).encode("utf-8")


def _private(path: Path, raw: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL |
                 getattr(os, "O_NOFOLLOW", 0), 0o600)
    with os.fdopen(fd, "wb") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def _local_table_type_shim(spark):
    """Adapt one UC query from actual local Spark catalog metadata."""
    original_sql = spark.sql
    marker = "SELECT table_name,table_type FROM system.information_schema.tables"
    metrics = {"sql_calls": 0, "catalog_table_type_lookups": 0}

    def sql(statement, *args, **kwargs):
        metrics["sql_calls"] += 1
        compact = " ".join(statement.split())
        if compact.startswith(marker):
            parameters = kwargs.get("args")
            if type(parameters) is not dict or set(parameters) != {
                    "catalog", "schema", "role_0", "role_1", "role_2",
                    "role_3", "role_4", "role_5"}:
                raise RuntimeError("Unexpected UC table metadata query shape")
            catalog, schema = parameters["catalog"], parameters["schema"]
            rows = []
            for index in range(6):
                role = parameters[f"role_{index}"]
                name = f"{catalog}.{schema}.{role}"
                observed = spark.catalog.getTable(name)
                metrics["catalog_table_type_lookups"] += 1
                table_type = getattr(observed, "tableType", None)
                if type(table_type) is not str or not table_type:
                    raise RuntimeError("Local Spark catalog omitted actual tableType")
                rows.append((role, table_type.upper()))
            from pyspark.sql.types import StructType, StructField, StringType
            schema_type = StructType([
                StructField("table_name", StringType(), False),
                StructField("table_type", StringType(), False),
            ])
            return spark.createDataFrame(rows, schema_type)
        return original_sql(statement, *args, **kwargs)

    spark.sql = sql
    return original_sql, metrics


def _verify_native_zstd_footers(spark, table_name, version, expected_rows):
    """Read the literal local Delta snapshot's Parquet footers through Spark's JVM."""
    if type(version) is not int or version < 0 or type(expected_rows) is not int or expected_rows < 0:
        raise RuntimeError("Exact nonnegative Delta version and source row count required")
    frame = spark.read.format("delta").option("versionAsOf", version).table(table_name)
    files = frame.inputFiles()
    if type(files) not in (list, tuple) or len(files) > 4096 or len(set(files)) != len(files):
        raise RuntimeError("Bounded complete pinned Parquet file list required")
    jvm = spark._jvm
    configuration = spark.sparkContext._jsc.hadoopConfiguration()
    reader_type = jvm.org.apache.parquet.hadoop.ParquetFileReader
    input_type = jvm.org.apache.parquet.hadoop.util.HadoopInputFile
    path_type = jvm.org.apache.hadoop.fs.Path
    row_count = 0
    chunks = 0
    path_hashes = []
    for uri in sorted(files):
        parsed = urlsplit(uri)
        if parsed.scheme not in ("", "file") or parsed.netloc not in ("", "localhost"):
            raise RuntimeError("Nonlocal Parquet file in local qualification snapshot")
        local_path = Path(unquote(parsed.path if parsed.scheme else uri)).resolve()
        if not local_path.is_file():
            raise RuntimeError("Pinned local Parquet file is absent")
        reader = reader_type.open(input_type.fromPath(path_type(uri), configuration))
        try:
            blocks = reader.getFooter().getBlocks()
            if blocks is None or blocks.size() > 4096:
                raise RuntimeError("Unbounded Parquet row-group footer")
            for group_index in range(blocks.size()):
                block = blocks.get(group_index)
                columns = block.getColumns()
                if columns is None or columns.size() > 16384:
                    raise RuntimeError("Unbounded Parquet column chunk list")
                row_count += int(block.getRowCount())
                for column_index in range(columns.size()):
                    if str(columns.get(column_index).getCodec().name()) != "ZSTD":
                        raise RuntimeError("Local Parquet column codec is not ZSTD")
                    chunks += 1
        finally:
            reader.close()
        path_hashes.append(_sha(uri.encode("utf-8")))
    if row_count != expected_rows or (expected_rows and chunks == 0):
        raise RuntimeError("Pinned Parquet footer row count differs from source-derived bag")
    return {"version": version, "file_count": len(files), "file_uri_sha256": path_hashes,
        "row_count": row_count, "zstd_column_chunks": chunks}


def _independent_readback(*, spark, target, inventory, published, plan,
        genesis_bytes, held_source_bytes, initial_manifest_bytes, context, phase,
        spark_metrics):
    """Read the full pinned vector and source-derived four-role bags before ACK."""
    from ashlar.manifest import manifest_pin_vector
    from ashlar.protocol import validate_protocol_detail
    from ashlar_host.managed_current_core import (
        CARRIER_PROFILE_XXHASH64, ManagedCurrentCoreConfig,
    )
    from ashlar_host.managed_current_carriers import load_current_managed_carriers
    from ashlar_host.managed_current_spark_source_oracle import (
        build_managed_current_a1_source_oracle,
        compare_complete_managed_current_bags,
        managed_current_a1_lookup_keys,
    )
    from ashlar_host.managed_current_a1_prefix_builder import (
        build_managed_current_a1_prefix_evidence,
    )

    started = time.monotonic()
    sql_before = spark_metrics["sql_calls"]
    transport_before = target.transport.query_generation
    manifest = published["receipt"]["manifest"]
    data_roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
    table_versions = json.loads(manifest["table_versions_json"])
    if type(table_versions) is not dict or set(table_versions) != {
            ".".join(("spark_catalog", SCHEMA, role)) for role in data_roles}:
        raise RuntimeError("Published manifest does not pin the exact four data roles")
    native_uuids = {".".join(("spark_catalog", SCHEMA, role)): inventory[role].uuid
                    for role in data_roles}
    vector = manifest_pin_vector(manifest, native_uuids,
        authority="managed-current-local-native-pre-ack-readback")
    if vector.targets != {name: tuple(pin) for name, pin in published["vector_wire"]["targets"].items()}:
        raise RuntimeError("Independent native vector differs from the publication-bound vector")

    config = ManagedCurrentCoreConfig(catalog="spark_catalog", schema=SCHEMA,
        backend03=None, backend02=None, preferred_cost_usd=0, hard_cost_usd=0,
        carrier_profile=CARRIER_PROFILE_XXHASH64)
    selected_keys = managed_current_a1_lookup_keys(plan, config=config)
    lookup_hashes = {}
    for kind, source, type_id, entity_id in selected_keys:
        result = spark.sql(
            "SELECT xxhash64(CAST(:source AS STRING), CAST(:type AS BIGINT), "
            "CAST(:id AS BIGINT)) AS lookup_hash",
            args={"source": source, "type": str(type_id), "id": str(entity_id)}).collect()
        if len(result) != 1 or type(result[0]["lookup_hash"]) is not int:
            raise RuntimeError("Native source-key hash observation was not exact")
        lookup_hashes[(kind, source, type_id, entity_id)] = result[0]["lookup_hash"]

    prefix_bytes, prefix_sha, selected = build_managed_current_a1_prefix_evidence(
        genesis_bytes=genesis_bytes, source_snapshot_bytes=held_source_bytes,
        initial_manifest_bytes=initial_manifest_bytes,
        request_bytes=published["request_bytes"],
        publication_manifest_bytes=published["manifest_bytes"],
        destination={"catalog": "spark_catalog", "schema": SCHEMA},
        carrier_profile=CARRIER_PROFILE_XXHASH64,
        materialization_clock_us=plan.driver_inputs["materialized_at"],
        phase="a0" if phase == "A0 metadata" else "a1")
    oracle = build_managed_current_a1_source_oracle(plan, config=config,
        prefix_bytes=prefix_bytes, prefix_sha256=prefix_sha, selected=selected,
        manifest=manifest, lookup_hashes=lookup_hashes)
    expected = oracle.expected_rows
    carriers = load_current_managed_carriers(CARRIER_PROFILE_XXHASH64)
    actual = {}
    observed = {}
    expected_inventory = published["receipt"]["installationInventory"]
    manifest_rows = spark.sql(
        f"SELECT publication_id,profile_version,table_versions_json,source_progress_json,"
        f"schema_revisions_json,validation_report_json,"
        f"CAST(unix_micros(recorded_at) AS STRING) AS recorded_at "
        f"FROM `spark_catalog`.`{SCHEMA}`.`publication_manifest` "
        "WHERE publication_id=:publication_id",
        args={"publication_id": manifest["publication_id"]}).collect()
    if len(manifest_rows) != 1 or manifest_rows[0].asDict(recursive=True) != manifest:
        raise RuntimeError("Independent native manifest row differs from publication receipt")
    for role in data_roles:
        name = f"spark_catalog.{SCHEMA}.{role}"
        version = table_versions[name]
        if type(version) is not int or version < 0:
            raise RuntimeError("Manifest data-role version is not a nonnegative integer")
        detail_rows = spark.sql(f"DESCRIBE DETAIL `{name.replace('.', '`.`')}`").collect()
        history_rows = spark.sql(f"DESCRIBE HISTORY `{name.replace('.', '`.`')}` LIMIT 1").collect()
        if len(detail_rows) != 1 or len(history_rows) != 1:
            raise RuntimeError("Native readback omitted Delta detail/history")
        detail = detail_rows[0].asDict(recursive=True)
        history = history_rows[0].asDict(recursive=True)
        expected_uuid = expected_inventory[role]["uuid"]
        if detail.get("format") != "delta" or detail.get("id") != expected_uuid:
            raise RuntimeError(f"Native Delta UUID/format changed for {role}")
        if history.get("version") != version:
            raise RuntimeError(f"Current native head differs from pinned vector for {role}")
        proto = dict(detail)
        proto["minReaderVersion"] = str(detail["minReaderVersion"])
        proto["minWriterVersion"] = str(detail["minWriterVersion"])
        proto["tableFeatures"] = _canonical(list(detail.get("tableFeatures", []))).decode()
        protocol = validate_protocol_detail(proto, uuid=expected_uuid,
            profile=target.profile)
        schema = spark.table(name).schema
        schema_observed = [[field.name, field.dataType.simpleString(), field.nullable]
                           for field in schema.fields]
        spark_types = {"STRING": "string", "BIGINT": "bigint", "BOOLEAN": "boolean",
                       "TIMESTAMP": "timestamp"}
        schema_expected = [[field, spark_types[kind], nullable]
                           for field, kind, nullable in carriers.ordered_fields(role)]
        if schema_observed != schema_expected:
            raise RuntimeError(f"Native schema differs from admitted carrier for {role}")
        projections = []
        for field, kind, _nullable in carriers.ordered_fields(role):
            quoted = "`" + field + "`"
            if kind == "BIGINT":
                projections.append(f"CAST({quoted} AS STRING) AS {quoted}")
            elif kind == "TIMESTAMP":
                projections.append(f"CAST(unix_micros({quoted}) AS STRING) AS {quoted}")
            else:
                projections.append(quoted)
        rows = spark.sql("SELECT " + ",".join(projections) + f" FROM `{name.replace('.', '`.`')}` "
            + f"VERSION AS OF {version} LIMIT 1025").collect()
        actual[role] = [row.asDict(recursive=True) for row in rows]
        observed[role] = {"table": name, "uuid": expected_uuid, "version": version,
            "schema": schema_observed, "protocol": protocol,
            "row_count": len(actual[role]), "rows_sha256": _sha(_canonical(actual[role]))}
        if phase == "A1 seven-operation seed" and expected[role]:
            observed[role]["parquet_footer"] = _verify_native_zstd_footers(
                spark, name, version, len(expected[role]))
    comparison_sha = compare_complete_managed_current_bags(expected, actual,
        carrier_profile=CARRIER_PROFILE_XXHASH64)
    return {"phase": phase, "publication_id": manifest["publication_id"],
        "manifest_sha256": _sha(published["manifest_bytes"]),
        "vector": {name: list(pin) for name, pin in vector.targets.items()},
        "roles": observed, "source_oracle_sha256": _sha(oracle.expected_rows_json.encode("utf-8")),
        "comparison_sha256": comparison_sha,
        "source_expected_rows": {role: len(expected[role]) for role in data_roles},
        "native_rows": {role: len(actual[role]) for role in data_roles},
        "elapsed_seconds": round(time.monotonic() - started, 6),
        "spark_sql_calls": spark_metrics["sql_calls"] - sql_before,
        "transport_query_generation_delta": target.transport.query_generation - transport_before,
        "readback_passed": True}


def _effect_free_preflight():
    """Resolve every code/runtime API used later, before either PG source starts."""
    if _sha(FROZEN_PUBLICATION_SOURCE.read_bytes()) != FROZEN_PUBLICATION_SHA256:
        raise RuntimeError("Managed-current production publication source changed from reviewed candidate")
    manifest_path = RUNTIME / "r61-native-runtime-manifest.json"
    if not manifest_path.is_file() or manifest_path.is_symlink():
        raise RuntimeError("Exact retained native runtime manifest is required")
    manifest = json.loads(manifest_path.read_bytes())
    required = {"java_home", "jars_directory", "jar_inventory"}
    if type(manifest) is not dict or not required <= set(manifest):
        raise RuntimeError("Native runtime manifest omits Java/JAR bindings")
    for raw in (manifest["java_home"], manifest["jars_directory"]):
        path = Path(raw)
        if not path.is_absolute() or not path.exists() or path.is_symlink():
            raise RuntimeError("Pinned native runtime path is absent or unsafe")
    if str(REPO) not in sys.path:
        sys.path.insert(0, str(REPO))
    from ashlar_host.managed_a1_source_bootstrap import (
        bootstrap_from_private_config, canonical_json, example_private_config,
    )
    from ashlar_host.truss053_source_custody import Truss053A1SourceHolder
    from ashlar_host.a1_ack_attempt import DurableA1AckAttemptStore
    from ashlar_host.managed_current_publication import ManagedCurrentTargetOwner
    from ashlar_host.managed_current_carriers import load_current_managed_carriers
    from ashlar_host.managed_current_genesis import ManagedCurrentGenesisManifestOwner
    from ashlar_host.managed_current_spark_source_oracle import (
        build_managed_current_a1_source_oracle, managed_current_a1_lookup_keys,
    )
    from ashlar_host.managed_current_a1_prefix_builder import build_managed_current_a1_prefix_evidence
    from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
    from ashlar_host.truss_resume_cli import _CurrentTrussRuntime
    from ashlar_host.configured_local_runtime_profile import attest
    from ashlar_host.configured_local_runtime import open_configured_local_spark
    package_modules = (
        "ashlar_host.a1_ack_attempt",
        "ashlar_host.truss053_source_custody",
        "ashlar_host.managed_current_publication",
        "ashlar_host.managed_a1_detached_admission",
    )
    installed_module_files = {}
    for module_name in package_modules:
        module = sys.modules[module_name]
        module_path = Path(module.__file__).resolve()
        if not module_path.is_relative_to(Path(sys.prefix).resolve()):
            raise RuntimeError(f"Selected wheel module is not installed in this runtime: {module_name}")
        installed_module_files[module_name] = {"path": str(module_path),
            "sha256": _sha(module_path.read_bytes())}
    if Truss053A1SourceHolder.__module__ != "ashlar_host.truss053_source_custody" \
            or DurableA1AckAttemptStore.__module__ != "ashlar_host.a1_ack_attempt":
        raise RuntimeError("Fresh Truss custody/ACK classes did not resolve from installed Ashlar")
    signatures = {
        "bootstrap": inspect.signature(bootstrap_from_private_config),
        "finish_ack": inspect.signature(Truss053A1SourceHolder.finish_ack),
        "target": inspect.signature(ManagedCurrentTargetOwner),
        "genesis_owner": inspect.signature(ManagedCurrentGenesisManifestOwner),
        "plan": inspect.signature(admit_detached_a1_plan),
        "prefix": inspect.signature(build_managed_current_a1_prefix_evidence),
        "source_oracle": inspect.signature(build_managed_current_a1_source_oracle),
        "lookup_keys": inspect.signature(managed_current_a1_lookup_keys),
        "runtime": inspect.signature(_CurrentTrussRuntime),
        "attest": inspect.signature(attest),
        "spark": inspect.signature(open_configured_local_spark),
    }
    expected_parameters = {
        "target": {"spark", "catalog", "schema", "protocol_profile", "carrier_profile"},
        "plan": {"genesis_snapshot_bytes", "genesis_manifest", "stream", "materialized_at", "context"},
        "source_oracle": {"plan", "config", "prefix_bytes", "prefix_sha256", "selected", "manifest", "lookup_hashes"},
    }
    finish_ack = signatures["finish_ack"]
    if ("settlement" not in finish_ack.parameters
            or finish_ack.parameters["settlement"].kind is not inspect.Parameter.KEYWORD_ONLY):
        raise RuntimeError("Selected Truss holder finish_ack settlement must be keyword-only")
    for name, required_params in expected_parameters.items():
        if not required_params <= set(signatures[name].parameters):
            raise RuntimeError(f"Selected installed API signature changed for {name}")
    carriers = load_current_managed_carriers("ashlar-delta/0.4-xxhash64")
    ddl_property_keys = set()
    for role in ("object_current", "edge_current", "tombstone", "whole_source_history",
                 "publication_manifest", "publication_attempt_phase"):
        ddl = carriers.create_sql(role, ("spark_catalog", SCHEMA, role))
        properties = re.search(r"TBLPROPERTIES\s*\((.*?)\);\s*\Z", ddl, re.S)
        if properties:
            ddl_property_keys.update(re.findall(r"'([^']+)'\s*=", properties.group(1)))
    if ddl_property_keys != {"delta.dataSkippingStatsColumns", "delta.targetFileSize",
                             "delta.parquet.compression.codec"}:
        raise RuntimeError("Authored local DDL property set differs from reviewed exact allowlist")
    runtime_config = _CurrentTrussRuntime(Path(manifest["jars_directory"]),
        Path(manifest["java_home"]), manifest["jar_inventory"], ROOT / "spark-warehouse")
    runtime_profile, runtime_identity, runtime_jars = attest(runtime_config)
    if runtime_profile != "ashlar-configured-local-runtime/0.2":
        raise RuntimeError("Selected local Spark runtime profile changed")
    bun = Path("/opt/homebrew/Cellar/bun/1.4.3/bin/bun")
    private_config = example_private_config(root=ROOT / "sources",
        bun_path=bun, repository_root=REPO)
    config_bytes = canonical_json(private_config)
    if type(config_bytes) is not bytes or not config_bytes:
        raise RuntimeError("Fresh Truss source configuration is not canonical bytes")
    return {"production_publication_sha256": FROZEN_PUBLICATION_SHA256,
        "runtime_manifest_sha256": _sha(manifest_path.read_bytes()),
        "runtime_identity": runtime_identity,
        "runtime_jars": {path.name: _sha(path.read_bytes()) for path in runtime_jars},
        "truss_config_sha256": _sha(config_bytes),
        "api_signatures": {name: str(signature) for name, signature in signatures.items()},
        "authored_ddl_property_keys": sorted(ddl_property_keys),
        "installed_ashlar_module_files": installed_module_files,
        "local_unknown_delta_property": "delta.parquet.compression.codec",
        "effect_free": True}


def _publish_page(*, target, plan, context, phase, destination_id, spark_metrics):
    from ashlar.attempt_store import DeltaAttemptStore
    from ashlar.manifest import manifest_pin_vector
    from ashlar.publisher import publish_batch
    from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64
    from ashlar_host.managed_current_publication import (
        ManagedCurrentIncrementalPublicationDriver,
        ManagedCurrentManifestPort,
    )
    from ashlar_host.truss_backend import TrussPublisherBackend
    from ashlar_host.truss_feed_sessions import encode_truss_destination_receipt

    import time
    from ashlar_host.managed_current_publication import ManagedCurrentTargetOwner
    phase_target = ManagedCurrentTargetOwner(target.spark, catalog=target.catalog,
        schema=target.schema, protocol_profile=target.profile,
        carrier_profile=target.carrier_profile)
    opening_started = time.monotonic()
    opening_sql_before = spark_metrics["sql_calls"]
    opening_transport_before = phase_target.transport.query_generation
    opening_inventory = phase_target.open_existing()
    opening_metrics = {"elapsed_seconds": round(time.monotonic() - opening_started, 6),
        "spark_sql_calls": spark_metrics["sql_calls"] - opening_sql_before,
        "transport_query_generation_delta": phase_target.transport.query_generation - opening_transport_before}
    selection = plan.selection
    revisions = _canonical(selection.schema_revisions).decode("utf-8")
    driver_inputs = plan.driver_inputs
    driver = ManagedCurrentIncrementalPublicationDriver(
        phase_target, selection.admission, stream=driver_inputs["stream"],
        materialized_at=driver_inputs["materialized_at"], context=context,
        managed_genesis_predecessor=driver_inputs["managed_genesis_predecessor"],
        source_owner_admitted_batch=driver_inputs["source_owner_admitted_batch"],
        schema_revisions_json=revisions)
    request = dict(plan.request)
    role = "publication_attempt_phase"
    attempts = DeltaAttemptStore(driver.transport, driver.policy,
        phase_target.tables[role], phase_target.opening[role].uuid, original_request=request)
    backend = TrussPublisherBackend(attempts, driver,
        lambda original, held: ManagedCurrentManifestPort(driver, original, held),
        context=context)
    sql_before = phase_target.transport.query_generation
    started = time.monotonic()
    descriptor = publish_batch(backend, driver.stream, selection.admission.batch,
        predecessor=request["predecessor"], schema_revisions_json=revisions,
        context=context, source_checkpoint_json=request["source_checkpoint_json"])
    receipt = driver.reopen_receipt(descriptor)
    manifest = receipt["manifest"]
    manifest_bytes = _canonical(manifest)
    request_bytes = _canonical(request)
    roles = ("object_current", "edge_current", "tombstone", "whole_source_history")
    inventory = receipt["installationInventory"]
    uuids = {inventory[name]["name"]: inventory[name]["uuid"] for name in roles}
    vector = manifest_pin_vector(manifest, uuids,
        authority="managed-current-spark:local-native-qualification")
    destination_receipt = encode_truss_destination_receipt(
        request_bytes, manifest_bytes, vector)
    vector_wire = {"authority": vector.authority, "scope_kind": vector.scope_kind,
        "scope_id": vector.scope_id, "custody_digest": vector.custody_digest,
        "targets": {name: list(pin) for name, pin in vector.targets.items()}}
    return {"phase": phase, "plan": plan, "driver": driver,
        "receipt": receipt, "request_bytes": request_bytes,
        "manifest_bytes": manifest_bytes, "destination_receipt": destination_receipt,
        "vector_wire": vector_wire, "destination_id": destination_id,
        "target": phase_target, "inventory": opening_inventory,
        "opening_inventory_metrics": opening_metrics,
        "driver_sql_count": phase_target.transport.query_generation - sql_before,
        "driver_elapsed_seconds": round(time.monotonic() - started, 6)}


def _ack_once(holder, published, selection_bytes, attempt_directory, evidence):
    from ashlar_host.a1_ack_attempt import DurableA1AckAttemptStore
    from ashlar_host.truss_detached_source_snapshot_codec import decode_detached_source_snapshot

    detached = decode_detached_source_snapshot(selection_bytes)
    attempt_directory.mkdir(mode=0o700)
    store = DurableA1AckAttemptStore(attempt_directory)
    page = detached.source_page
    store.begin(selection_bytes=selection_bytes,
        request_bytes=published["request_bytes"],
        manifest_bytes=published["manifest_bytes"],
        receipt_bytes=published["destination_receipt"],
        prior_checkpoint_bytes=page.prior_bytes,
        next_checkpoint_bytes=page.next_bytes,
        page_sha256=page.page_fields["sha256"],
        destination_id=published["destination_id"],
        publication_id=page.publication_id)
    proof_path = attempt_directory.parent / ("pre-ack-" + attempt_directory.name + ".json")
    if not proof_path.is_file() or proof_path.is_symlink():
        raise RuntimeError("Independent durable native readback receipt required before ACK")
    proof = json.loads(proof_path.read_bytes())
    roles = {"object_current", "edge_current", "tombstone", "whole_source_history"}
    proof_vector = {name: tuple(pin) for name, pin in proof.get("vector", {}).items()}
    receipt_vector = {name: tuple(pin) for name, pin in published["vector_wire"]["targets"].items()}
    if (proof.get("readback_passed") is not True
            or proof.get("manifest_sha256") != _sha(published["manifest_bytes"])
            or proof.get("publication_id") != published["receipt"]["manifest"]["publication_id"]
            or proof.get("phase") != published["phase"]
            or set(proof.get("roles", {})) != roles
            or proof.get("source_expected_rows") != proof.get("native_rows")
            or proof_vector != receipt_vector):
        raise RuntimeError("Pre-ACK native readback does not bind this publication")
    process_group_id = os.getpgrp()
    generation = store.record_worker_started(process_group_id=process_group_id,
                                             selection_bytes=selection_bytes)
    evidence["effect_state"]["source_ack_attempted"] = True
    evidence["effect_state"]["source_ack_outcome"] = "unknown_after_dispatch"
    store.mark_ack_dispatched(process_group_id=process_group_id,
                              generation=generation)
    settlement = holder.acknowledge_once(
        attempt_directory=attempt_directory, generation=generation)
    if settlement.outcome != "matched":
        raise RuntimeError("Fresh dedicated Truss ACK did not reconcile as matched")
    store.record_settlement(settlement)
    holder.finish_ack(settlement=settlement, attempt_directory=attempt_directory,
                      generation=generation)
    evidence["effect_state"]["source_ack_outcome"] = "matched"
    return {"outcome": settlement.outcome,
        "attempt_sha256": _sha(store.path.read_bytes()),
        "selection_sha256": _sha(selection_bytes),
        "request_sha256": _sha(published["request_bytes"]),
        "manifest_sha256": _sha(published["manifest_bytes"]),
        "destination_receipt_sha256": _sha(published["destination_receipt"]),
        "checkpoint_sha256": _sha(settlement.checkpoint_bytes)}, settlement


def main():
    started = time.monotonic()
    preflight = _effect_free_preflight()
    if ROOT.exists() or ROOT.is_symlink():
        raise RuntimeError("One-shot fresh local qualification root already exists")
    ROOT.mkdir(mode=0o700)
    if stat.S_IMODE(ROOT.stat().st_mode) != 0o700 or ROOT.stat().st_uid != os.getuid():
        raise RuntimeError("Owner-private fresh output root required")
    evidence = {"profile": "ashlar-managed-current-local-spark-native/0.1",
        "scope": {"runtime": "local Spark 4.2 / Delta 4.4.1", "catalog": "spark_catalog",
            "schema": SCHEMA, "UC": False, "databricks": False,
            "source_ack": "fresh dedicated Truss .55 A/B sources only",
            "wall_cap_seconds": MAX_WALL_SECONDS},
        "status": "running", "started_unix_ns": time.time_ns(),
        "delta_effects": [], "source_ack": [],
        "effect_state": {"delta_publication_attempted": False,
            "delta_publication_outcome": "not_started", "source_ack_attempted": False,
            "source_ack_outcome": "not_attempted", "delta_initialization_attempted": False,
            "delta_initialization_outcome": "not_started"}}
    receipt_path = ROOT / "qualification.json"
    context = None
    holder = None
    spark = None
    try:
        evidence["preflight"] = preflight
        evidence["production_source"] = {"path": str(FROZEN_PUBLICATION_SOURCE),
            "sha256": FROZEN_PUBLICATION_SHA256}
        if str(REPO) not in sys.path:
            sys.path.insert(0, str(REPO))
        source_root = ROOT / "sources"
        source_root.mkdir(mode=0o700)
        from ashlar_host.managed_a1_source_bootstrap import (
            example_private_config, canonical_json, bootstrap_from_private_config,
        )
        private_config = example_private_config(root=source_root,
            bun_path=Path("/opt/homebrew/Cellar/bun/1.4.3/bin/bun"), repository_root=REPO)
        config_bytes = canonical_json(private_config)
        holder = bootstrap_from_private_config(config_bytes)
        genesis_bytes = holder.select_genesis()
        selection = holder.genesis
        evidence["source_genesis"] = {"bytes": len(genesis_bytes),
            "sha256": _sha(genesis_bytes), "installations": {
                key: value.installation_id for key, value in selection.source_progress.items()},
            "feeds": {key: value.feed for key, value in selection.source_progress.items()},
            "acknowledged": False}

        runtime_manifest_path = RUNTIME / "r61-native-runtime-manifest.json"
        runtime_manifest_bytes = runtime_manifest_path.read_bytes()
        runtime = json.loads(runtime_manifest_bytes)
        java_home = Path(runtime["java_home"])
        os.environ["JAVA_HOME"] = str(java_home)
        os.environ["PATH"] = str(java_home / "bin") + os.pathsep + os.environ.get("PATH", "")
        os.environ.setdefault("SPARK_LOCAL_IP", "127.0.0.1")
        os.environ["PYSPARK_PYTHON"] = str(Path(sys.executable).resolve())
        os.environ["PYSPARK_DRIVER_PYTHON"] = str(Path(sys.executable).resolve())
        from ashlar_host.truss_resume_cli import _CurrentTrussRuntime
        from ashlar_host.configured_local_runtime_profile import attest
        jars_directory = Path(runtime["jars_directory"])
        warehouse = ROOT / "spark-warehouse"
        local_runtime = _CurrentTrussRuntime(jars_directory, java_home,
            runtime["jar_inventory"], warehouse)
        _, spark_identity, jars = attest(local_runtime)
        from ashlar_host.configured_local_runtime import open_configured_local_spark
        spark = open_configured_local_spark(local_runtime, jars, warehouse,
            app_name="Ashlar managed current local qualification", driver_memory="512m")
        spark.conf.set("spark.sql.parquet.compression.codec", "zstd")
        original_sql, spark_metrics = _local_table_type_shim(spark)
        evidence["spark"] = {"version": spark.version, "java_home": str(java_home),
            "runtime_manifest_sha256": _sha(runtime_manifest_bytes),
            "jar_inventory": runtime["jar_inventory"],
            "engine_profile": spark_identity,
            "catalog_type_source": "spark.catalog.getTable().tableType"}

        from ashlar_host.configured_local_runtime import MANAGED_SERVERLESS_PROFILE
        from ashlar_host.managed_current_core import CARRIER_PROFILE_XXHASH64
        from ashlar_host.managed_current_publication import ManagedCurrentTargetOwner
        from ashlar_host.managed_a1_serverless_tasks import _GenesisOnlyManifestPolicy
        from ashlar.manifest import DeltaManifestStore
        from ashlar_host.managed_current_genesis import ManagedCurrentGenesisManifestOwner
        from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
        from ashlar_host.r04_schedule import canonical

        spark.sql(f"CREATE DATABASE spark_catalog.`{SCHEMA}`")
        target = ManagedCurrentTargetOwner(spark, catalog="spark_catalog", schema=SCHEMA,
            protocol_profile=MANAGED_SERVERLESS_PROFILE,
            carrier_profile=CARRIER_PROFILE_XXHASH64)
        observed_sweeps = []
        real_observe = target.observe
        def measured_observe(*args, **kwargs):
            started_observe = time.monotonic()
            sql_before_observe = spark_metrics["sql_calls"]
            generation_before_observe = target.transport.query_generation
            table_types_before = spark_metrics["catalog_table_type_lookups"]
            value = real_observe(*args, **kwargs)
            observed_sweeps.append({"elapsed_seconds": round(time.monotonic() - started_observe, 6),
                "spark_sql_calls": spark_metrics["sql_calls"] - sql_before_observe,
                "transport_query_generation_delta": target.transport.query_generation - generation_before_observe,
                "catalog_table_type_lookups": spark_metrics["catalog_table_type_lookups"] - table_types_before})
            return value
        target.observe = measured_observe
        property_keys = set()
        for role in ("object_current", "edge_current", "tombstone",
                     "whole_source_history", "publication_manifest",
                     "publication_attempt_phase"):
            ddl = target.carriers.create_sql(role, target.names[role])
            match = re.search(r"TBLPROPERTIES\s*\((.*?)\);\s*\Z", ddl, re.S)
            if match:
                property_keys.update(re.findall(r"'([^']+)'\s*=", match.group(1)))
        if "delta.parquet.compression.codec" not in property_keys:
            raise RuntimeError("Authored Delta carrier lost its exact compression property")
        spark.conf.set("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
        evidence["effect_state"]["delta_initialization_attempted"] = True
        evidence["effect_state"]["delta_initialization_outcome"] = "unknown_after_start"
        initialize_started = time.monotonic()
        init_sql_before = spark_metrics["sql_calls"]
        init_transport_before = target.transport.query_generation
        try:
            inventory = target.initialize()
            evidence["effect_state"]["delta_initialization_outcome"] = "completed"
        finally:
            spark.conf.set("spark.databricks.delta.allowArbitraryProperties.enabled", "false")
        stored_codec = spark.sql(
            f"SHOW TBLPROPERTIES `spark_catalog`.`{SCHEMA}`.`object_current` "
            "('delta.parquet.compression.codec')").collect()
        if len(stored_codec) != 1 or stored_codec[0]["value"] != "zstd":
            raise RuntimeError("Local table did not retain authored compression metadata")
        evidence["local_delta_adapter"] = {
            "allowArbitraryProperties": "temporary during authored local DDL only",
            "authored_property_keys": sorted(property_keys),
            "delta_parquet_compression_codec_enforced_by_local_delta": False,
            "effective_spark_parquet_compression": spark.conf.get("spark.sql.parquet.compression.codec"),
            "compression_scope": "local OSS Spark output; UC/Databricks property behavior not qualified"}
        if evidence["local_delta_adapter"]["effective_spark_parquet_compression"].lower() != "zstd":
            raise RuntimeError("Local Spark session did not retain the ZSTD Parquet codec")
        evidence["native_inventory_sweeps"] = observed_sweeps
        evidence["native_initialization"] = {"elapsed_seconds": round(
            time.monotonic() - initialize_started, 6),
            "spark_sql_calls": spark_metrics["sql_calls"] - init_sql_before,
            "transport_query_generation_delta": target.transport.query_generation - init_transport_before,
            "carrier_ddl_count": 6}
        context = object()
        progress_json = {item.feed: item.document() for item in selection.source_progress.values()}
        policy = _GenesisOnlyManifestPolicy(
            table=target.tables["publication_manifest"],
            table_uuid=inventory["publication_manifest"].uuid,
            context=context, schedule_sha256=selection.schedule_sha256,
            progress_json=progress_json, revisions=selection.schema_revisions,
            target=target, schema=SCHEMA, principal="local-spark-catalog")
        manifest_store = DeltaManifestStore(target.transport, policy,
            target.tables["publication_manifest"], inventory["publication_manifest"].uuid)
        genesis_owner = ManagedCurrentGenesisManifestOwner(target, manifest_store)
        from ashlar_host.managed_current_truss053_genesis_owner import (
            ManagedCurrentTruss053GenesisSourceOwner,
        )
        genesis_args = ManagedCurrentTruss053GenesisSourceOwner.manifest_owner_arguments(
            selection, recorded_at=str(time.time_ns() // 1000), context=context)
        genesis_started = time.monotonic()
        genesis_sql_before = spark_metrics["sql_calls"]
        genesis_transport_before = target.transport.query_generation
        genesis_manifest = genesis_owner.initialize(**genesis_args)
        evidence["native_genesis_operation"] = {"elapsed_seconds": round(
            time.monotonic() - genesis_started, 6),
            "spark_sql_calls": spark_metrics["sql_calls"] - genesis_sql_before,
            "transport_query_generation_delta": target.transport.query_generation - genesis_transport_before}
        native_genesis_manifest_bytes = _canonical(genesis_manifest)
        evidence["native_genesis"] = {"manifest_sha256": _sha(native_genesis_manifest_bytes),
            "source_genesis_sha256": _sha(genesis_bytes),
            "six_role_uuid": {role: inventory[role].uuid for role in sorted(inventory)},
            "six_role_head": {role: inventory[role].version for role in sorted(inventory)},
            "data_rows": {role: len(target.transport.query(
                "SELECT * FROM `spark_catalog`.`" + SCHEMA + "`.`" + role + "`", {}).rows)
                for role in ("object_current", "edge_current", "tombstone", "whole_source_history")}}
        genesis_close_started = time.monotonic()
        genesis_close_sql_before = spark_metrics["sql_calls"]
        genesis_closing = target.closing()
        evidence["native_genesis_close"] = {"elapsed_seconds": round(
            time.monotonic() - genesis_close_started, 6),
            "spark_sql_calls": spark_metrics["sql_calls"] - genesis_close_sql_before,
            "roles": {role: {"uuid": genesis_closing[role].uuid,
                "head": genesis_closing[role].version} for role in sorted(genesis_closing)}}

        stream = "ashlar-managed-current-local-" + uuid.uuid4().hex
        destination_id = "spark_catalog." + SCHEMA
        phase_clock = str(time.time_ns() // 1000)
        plan = admit_detached_a1_plan(genesis_snapshot_bytes=genesis_bytes,
            genesis_manifest=genesis_manifest, stream=stream,
            materialized_at=phase_clock, context=context)
        request_bytes = _canonical(plan.request)
        snapshot_bytes = holder.open_prepublication(request_bytes=request_bytes,
            destination_id=destination_id, publication_id=plan.publication_id)
        evidence["effect_state"]["delta_publication_attempted"] = True
        evidence["effect_state"]["delta_publication_outcome"] = "unknown_after_start"
        published_a0 = _publish_page(target=target, plan=plan, context=context,
            phase="A0 metadata", destination_id=destination_id, spark_metrics=spark_metrics)
        evidence["effect_state"]["delta_publication_outcome"] = "committed_receipt_reopened"
        readback_a0 = _independent_readback(spark=spark, target=published_a0["target"],
            inventory=published_a0["inventory"], published=published_a0, plan=plan,
            genesis_bytes=genesis_bytes, held_source_bytes=snapshot_bytes,
            initial_manifest_bytes=native_genesis_manifest_bytes, context=context,
            phase="A0 metadata", spark_metrics=spark_metrics)
        readback_a0_path = ROOT / "pre-ack-ack-a0.json"
        _private(readback_a0_path, _canonical(readback_a0) + b"\n")
        holder.bind_publication(manifest_bytes=published_a0["manifest_bytes"],
            vector_payload=published_a0["vector_wire"],
            receipt_bytes=published_a0["destination_receipt"])
        ack0, settlement0 = _ack_once(holder, published_a0, snapshot_bytes,
            ROOT / "ack-a0", evidence)
        evidence["source_ack"].append(ack0)
        close_a0_started = time.monotonic()
        close_a0_sql_before = spark_metrics["sql_calls"]
        close_a0 = published_a0["target"].closing()
        evidence["a0_closing_inventory"] = {"elapsed_seconds": round(
            time.monotonic() - close_a0_started, 6),
            "spark_sql_calls": spark_metrics["sql_calls"] - close_a0_sql_before,
            "roles": {role: {"uuid": close_a0[role].uuid, "head": close_a0[role].version}
                for role in sorted(close_a0)}}
        evidence["a0"] = {"publication_id": published_a0["receipt"]["manifest"]["publication_id"],
            "request_sha256": _sha(published_a0["request_bytes"]),
            "manifest_sha256": _sha(published_a0["manifest_bytes"]),
            "destination_receipt_sha256": _sha(published_a0["destination_receipt"]),
            "vector": published_a0["vector_wire"], "ack": ack0,
            "opening_inventory_metrics": published_a0["opening_inventory_metrics"],
            "driver_sql_count": published_a0["driver_sql_count"],
            "driver_elapsed_seconds": published_a0["driver_elapsed_seconds"],
            "independent_readback": readback_a0}
        evidence["delta_effects"].append("A0 metadata publication confirmed by original driver receipt")

        next_genesis_bytes = holder.advance_after_a0_ack(settlement=settlement0)
        a1_context = object()
        a1_manifest = published_a0["receipt"]["manifest"]
        from ashlar_host.managed_a1_detached_admission import admit_detached_a1_plan
        plan_a1 = admit_detached_a1_plan(genesis_snapshot_bytes=next_genesis_bytes,
            genesis_manifest=a1_manifest, stream=stream,
            materialized_at=str(time.time_ns() // 1000), context=a1_context,
            continuation=True)
        request_a1 = _canonical(plan_a1.request)
        snapshot_a1 = holder.open_prepublication(request_bytes=request_a1,
            destination_id=destination_id, publication_id=plan_a1.publication_id)
        evidence["effect_state"]["delta_publication_attempted"] = True
        evidence["effect_state"]["delta_publication_outcome"] = "unknown_after_start"
        published_a1 = _publish_page(target=target, plan=plan_a1, context=a1_context,
            phase="A1 seven-operation seed", destination_id=destination_id,
            spark_metrics=spark_metrics)
        evidence["effect_state"]["delta_publication_outcome"] = "committed_receipt_reopened"
        readback_a1 = _independent_readback(spark=spark, target=published_a1["target"],
            inventory=published_a1["inventory"], published=published_a1, plan=plan_a1,
            genesis_bytes=next_genesis_bytes, held_source_bytes=snapshot_a1,
            initial_manifest_bytes=published_a0["manifest_bytes"], context=a1_context,
            phase="A1 seven-operation seed", spark_metrics=spark_metrics)
        readback_a1_path = ROOT / "pre-ack-ack-a1.json"
        _private(readback_a1_path, _canonical(readback_a1) + b"\n")
        holder.bind_publication(manifest_bytes=published_a1["manifest_bytes"],
            vector_payload=published_a1["vector_wire"],
            receipt_bytes=published_a1["destination_receipt"])
        ack1, settlement1 = _ack_once(holder, published_a1, snapshot_a1,
            ROOT / "ack-a1", evidence)
        evidence["source_ack"].append(ack1)
        close_a1_started = time.monotonic()
        close_a1_sql_before = spark_metrics["sql_calls"]
        close_a1 = published_a1["target"].closing()
        evidence["a1_closing_inventory"] = {"elapsed_seconds": round(
            time.monotonic() - close_a1_started, 6),
            "spark_sql_calls": spark_metrics["sql_calls"] - close_a1_sql_before,
            "roles": {role: {"uuid": close_a1[role].uuid, "head": close_a1[role].version}
                for role in sorted(close_a1)}}
        evidence["a1"] = {"publication_id": published_a1["receipt"]["manifest"]["publication_id"],
            "request_sha256": _sha(published_a1["request_bytes"]),
            "manifest_sha256": _sha(published_a1["manifest_bytes"]),
            "destination_receipt_sha256": _sha(published_a1["destination_receipt"]),
            "vector": published_a1["vector_wire"], "ack": ack1,
            "changes": len(plan_a1.selection.admission.changes),
            "opening_inventory_metrics": published_a1["opening_inventory_metrics"],
            "driver_sql_count": published_a1["driver_sql_count"],
            "driver_elapsed_seconds": published_a1["driver_elapsed_seconds"],
            "independent_readback": readback_a1}
        evidence["delta_effects"].append("A1 seven-operation native publication confirmed by original driver receipt")

        evidence["closing_inventory"] = evidence["a1_closing_inventory"]["roles"]
        evidence["status"] = "passed_local_native_a0_a1"
    except BaseException as error:
        evidence["status"] = "failed"
        evidence["error"] = {"type": type(error).__name__, "message": str(error)[:2000],
            "traceback": traceback.format_exc(limit=8)[-6000:]}
        raise
    finally:
        if holder is not None and not getattr(holder, "_closed", False):
            try:
                holder.close_before_ack()
            except BaseException as error:
                evidence.setdefault("cleanup_errors", []).append(
                    "source holder: " + type(error).__name__ + ": " + str(error)[:500])
        if spark is not None:
            try:
                spark.stop()
                evidence["spark_closed"] = True
            except BaseException as error:
                evidence.setdefault("cleanup_errors", []).append(
                    "Spark: " + type(error).__name__ + ": " + str(error)[:500])
        evidence["elapsed_seconds"] = round(time.monotonic() - started, 3)
        if evidence.get("status") == "passed_local_native_a0_a1" and evidence.get("cleanup_errors"):
            evidence["status"] = "failed_cleanup"
        evidence["closed_before_receipt"] = evidence.get("spark_closed") is True
        _private(receipt_path, _canonical(evidence) + b"\n")
    if time.monotonic() - started > MAX_WALL_SECONDS:
        raise RuntimeError("External 900-second cap exceeded; success claim withheld")
    if evidence.get("status") != "passed_local_native_a0_a1":
        raise RuntimeError("Local managed-current qualification did not pass")
    print(json.dumps({"status": evidence["status"], "receipt": str(receipt_path),
        "elapsed_seconds": evidence["elapsed_seconds"],
        "a0_manifest_sha256": evidence["a0"]["manifest_sha256"],
        "a1_manifest_sha256": evidence["a1"]["manifest_sha256"]},
        separators=(",", ":")))


if __name__ == "__main__":
    main()
