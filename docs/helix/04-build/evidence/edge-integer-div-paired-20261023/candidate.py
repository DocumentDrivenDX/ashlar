"""Single bounded local sample for the reviewed Ashlar Delta 0.4 carrier.

Materializes one deterministic 45M-node/225M-edge sample on local[18], then
does two point lookups per carrier table. Row values retain the same synthetic
pattern as the earlier 1M/5M fixture. This is a bounded physical sample, not a
publication or full-graph qualification.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from urllib.parse import unquote, urlparse

NODES = 45_000_000
EDGES = 225_000_000
PROFILE = "ashlar-delta/0.4-xxhash64"
HASH_PROFILE = "spark-xxhash64-typed-key/0.1"
GENERATED_SHA256 = "cb6c75c3cb3805011245ca460a763f15509a7d9627009301cc815e6983413faa"
REVIEWED_WHEEL_SHA256 = "18401e7052202515da90c5c7a87d775c8c33b82f82b9526f9802d615356b76af"
MAX_SECONDS = 570  # The invoking gtimeout applies the hard 600-second cap.
EXPECTED_DRIVER_MEMORY_BYTES = 24 * 1024**3
EXPECTED_MASTER = "local[18]"
DELTA_JARS = Path("/private/tmp/ashlar-release-20261021-m3rde5p2/jars")
DELTA_CLASSPATH = ",".join(str(DELTA_JARS / name) for name in (
    "delta-spark_4.2_2.13-4.4.1.jar", "delta-storage-4.4.1.jar"))


def append_record(path: Path, record: dict) -> None:
    raw = json.dumps(record, sort_keys=True, separators=(",", ":"), allow_nan=False) + "\n"
    with path.open("a", encoding="utf-8") as stream:
        stream.write(raw)
        stream.flush()
        os.fsync(stream.fileno())


def write_receipt(path: Path, record: dict) -> None:
    with path.open("x", encoding="utf-8") as stream:
        json.dump(record, stream, sort_keys=True, separators=(",", ":"), allow_nan=False)
        stream.write("\n")
        stream.flush()
        os.fsync(stream.fileno())


def runtime_snapshot(spark) -> dict:
    jvm = spark._jvm
    runtime = jvm.java.lang.Runtime.getRuntime()
    gc = {}
    for bean in jvm.java.lang.management.ManagementFactory.getGarbageCollectorMXBeans():
        gc[str(bean.getName())] = {
            "collection_count": int(bean.getCollectionCount()),
            "collection_time_ms": int(bean.getCollectionTime()),
        }
    process_cpu_ns = None
    try:
        process_cpu_ns = int(
            jvm.java.lang.management.ManagementFactory.getOperatingSystemMXBean().getProcessCpuTime()
        )
        if process_cpu_ns < 0:
            process_cpu_ns = None
    except Exception:
        pass
    return {
        "jvm_heap_used_bytes": int(runtime.totalMemory() - runtime.freeMemory()),
        "jvm_heap_committed_bytes": int(runtime.totalMemory()),
        "jvm_heap_max_bytes": int(runtime.maxMemory()),
        "jvm_available_processors": int(runtime.availableProcessors()),
        "jvm_process_cpu_time_ns": process_cpu_ns,
        "gc": gc,
    }


def disk_snapshot(path: Path) -> dict:
    usage = shutil.disk_usage(path)
    return {"total_bytes": usage.total, "used_bytes": usage.used, "free_bytes": usage.free}


def output_tree_bytes(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file())


def resource_delta(before: dict, after: dict) -> dict:
    before_gc, after_gc = before["gc"], after["gc"]
    gc_delta = {}
    for name in sorted(set(before_gc) | set(after_gc)):
        left, right = before_gc.get(name, {}), after_gc.get(name, {})
        gc_delta[name] = {
            "collection_count": right.get("collection_count", 0) - left.get("collection_count", 0),
            "collection_time_ms": right.get("collection_time_ms", 0) - left.get("collection_time_ms", 0),
        }
    cpu_before, cpu_after = before.get("jvm_process_cpu_time_ns"), after.get("jvm_process_cpu_time_ns")
    return {
        "jvm_process_cpu_time_delta_ns": None if cpu_before is None or cpu_after is None else cpu_after - cpu_before,
        "jvm_heap_used_before_bytes": before["jvm_heap_used_bytes"],
        "jvm_heap_used_after_bytes": after["jvm_heap_used_bytes"],
        "jvm_heap_max_bytes": after["jvm_heap_max_bytes"],
        "gc_delta": gc_delta,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repo", required=True, type=Path,
                        help="Reviewed Ashlar checkout containing the .4 carrier resource")
    parser.add_argument("--wheel", required=True, type=Path,
                        help="Exact reviewed Ashlar wheel built from the accepted commit")
    parser.add_argument("--expected-commit", required=True,
                        help="Full reviewed source commit used for the wheel")
    parser.add_argument("--output", required=True, type=Path,
                        help="Fresh private output directory; failures are durable and not reusable")
    args = parser.parse_args()

    repo = args.repo.resolve()
    wheel = args.wheel.resolve()
    out = args.output.resolve()
    submit_args = os.environ.get("PYSPARK_SUBMIT_ARGS", "")
    if "--driver-memory 24g" not in submit_args.lower():
        raise SystemExit("set PYSPARK_SUBMIT_ARGS='--driver-memory 24g pyspark-shell' before launch")
    if not repo.is_dir() or not wheel.is_file():
        raise SystemExit("reviewed repository and wheel must exist")
    wheel_sha256 = __import__("hashlib").sha256(wheel.read_bytes()).hexdigest()
    if wheel_sha256 != REVIEWED_WHEEL_SHA256:
        raise SystemExit("reviewed wheel SHA-256 does not match the authorized build")
    commit = args.expected_commit
    resource_rel = "src/ashlar_host/resources/managed-current-core-010-xxhash64/runtime-carriers.generated.json"
    reviewed_resource = subprocess.run(
        ["git", "-C", str(repo), "show", f"{commit}:{resource_rel}"],
        check=True, capture_output=True).stdout
    if not reviewed_resource:
        raise SystemExit("reviewed commit does not contain the expected carrier resource")
    if not out.is_absolute() or out.exists() or out.is_symlink():
        raise SystemExit("provide a fresh absolute output directory")
    out.mkdir(mode=0o700, parents=True)
    progress = out / "progress.jsonl"
    append_record(progress, {"state": "started", "nodes": NODES, "edges": EDGES,
                            "profile": PROFILE, "hash_profile": HASH_PROFILE,
                            "scope": "one local[18] 45M/225M carrier sample and four exact point lookups"})
    initial_disk = disk_snapshot(out)
    started = time.monotonic()
    def ensure_time():
        if time.monotonic() - started >= MAX_SECONDS:
            raise TimeoutError("sample reached its 570-second in-process deadline")

    current_stage = "runtime-startup"
    spark = None
    stages = []
    tables = []
    lookups = []
    runtime_observations = {}
    write_resource_deltas = {}
    prewrite_table_locations = {}
    try:
        # Import only from the reviewed wheel. The generated UMF DDL and
        # selected profile come through the production carrier loader.
        current_stage = "reviewed-wheel-admission"
        sys.path.insert(0, str(wheel))
        from ashlar_host.managed_current_carriers import load_current_managed_carriers
        carriers = load_current_managed_carriers(PROFILE)
        if (carriers.physical_profile != PROFILE or carriers.lookup_hash_profile != HASH_PROFILE
                or __import__("hashlib").sha256(carriers.generated_bytes).hexdigest() != GENERATED_SHA256):
            raise ValueError("reviewed wheel did not select the required physical/hash profile")
        if reviewed_resource != carriers.generated_bytes:
            raise ValueError("wheel carrier bytes do not match the reviewed checkout resource")
        append_record(progress, {"stage": current_stage,
                                 "source_commit": commit,
                                 "wheel_sha256": wheel_sha256,
                                 "generated_ddl_sha256": GENERATED_SHA256})

        from pyspark.sql import SparkSession

        spark = (SparkSession.builder.master(EXPECTED_MASTER)
            .appName("ashlar-current-carrier-xxhash64-45m18c-sample")
            .config("spark.driver.memory", "24g")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.jars", DELTA_CLASSPATH)
            .config("spark.databricks.delta.allowArbitraryProperties.enabled", "true")
            .config("spark.sql.parquet.compression.codec", "zstd")
            .config("spark.sql.shuffle.partitions", "18")
            .config("spark.default.parallelism", "18")
            .config("spark.sql.session.timeZone", "UTC")
            .config("spark.sql.warehouse.dir", str(out / "warehouse"))
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate())
        stages.append({"stage": current_stage, "seconds": time.monotonic() - started})
        driver_memory_setting = spark.sparkContext.getConf().get("spark.driver.memory", "")
        initial_runtime = runtime_snapshot(spark)
        if (spark.sparkContext.master != EXPECTED_MASTER or "24g" not in driver_memory_setting.lower()
                or initial_runtime["jvm_heap_max_bytes"] < 23 * 1024**3
                or initial_runtime["jvm_heap_max_bytes"] > 25 * 1024**3):
            raise ValueError("actual Spark master or JVM maximum heap does not match local[18]/24g target")
        runtime_observations["after_startup"] = initial_runtime
        append_record(progress, {"stage": current_stage, "spark": spark.version,
                                 "python": sys.version.split()[0], "master": spark.sparkContext.master,
                                 "spark_driver_memory_setting": driver_memory_setting,
                                 "jvm_runtime": initial_runtime})

        for role in ("object_current", "edge_current"):
            ensure_time()
            current_stage = "create-" + role
            begin = time.monotonic()
            spark.sql(carriers.create_sql(role, ("spark_catalog", "default", role)))
            detail = spark.sql(
                f"DESCRIBE DETAIL `spark_catalog`.`default`.`{role}`"
            ).first().asDict()
            location = urlparse(detail.get("location", ""))
            if location.scheme != "file" or location.netloc not in ("", "localhost"):
                raise ValueError(f"local file table location required before write for {role}")
            table_dir = Path(unquote(location.path)).resolve()
            if not table_dir.is_relative_to(out):
                raise ValueError(f"table location escaped the private output directory before write: {role}")
            prewrite_table_locations[role] = str(table_dir)
            stage = {"stage": current_stage, "seconds": time.monotonic() - begin,
                     "validated_table_location": str(table_dir)}
            stages.append(stage)
            append_record(progress, stage)

        if set(prewrite_table_locations) != {"object_current", "edge_current"}:
            raise ValueError("both Delta table locations must be verified before the first insert")

        node = "`spark_catalog`.`default`.`object_current`"
        # Preserve source_position=id and every other fixture value from the
        # 0.3 sample; only the lookup field representation/expression changes.
        current_stage = "write-object-current"
        object_before = runtime_snapshot(spark)
        begin = time.monotonic()
        ensure_time()
        spark.sql(f"""INSERT INTO {node} SELECT source_system, type_id, id, logical_key_json,
          schema_revision, entity_version, props_json, retained_json, root_id, source_feed,
          source_epoch, source_position, published_at,
          xxhash64(CAST(source_system AS STRING), CAST(type_id AS BIGINT), CAST(id AS BIGINT)),
          apply_batch_id, source_cursor_json, source_delivery_id
        FROM (SELECT 'layout-probe' AS source_system, CAST(1 AS BIGINT) AS type_id, id,
          '[]' AS logical_key_json, 'layout-probe-r1' AS schema_revision, CAST(1 AS BIGINT) AS entity_version,
          to_json(named_struct('label', concat('node-', CAST(id AS STRING)), 'seed', 17)) AS props_json,
          '{{}}' AS retained_json, CAST(NULL AS BIGINT) AS root_id, 'direct-layout-fixture' AS source_feed,
          '1' AS source_epoch, id AS source_position, TIMESTAMP '2026-10-15T00:00:00.000Z' AS published_at,
          'layout-probe' AS apply_batch_id, '{{}}' AS source_cursor_json, CAST(id AS STRING) AS source_delivery_id
          FROM range({NODES})) fixture""")
        stage = {"stage": current_stage, "rows_by_range": NODES,
                 "seconds": time.monotonic() - begin}
        stages.append(stage)
        object_after = runtime_snapshot(spark)
        write_resource_deltas[current_stage] = resource_delta(object_before, object_after)
        append_record(progress, {**stage, "resource_delta": write_resource_deltas[current_stage]})

        edge = "`spark_catalog`.`default`.`edge_current`"
        current_stage = "write-edge-current"
        ensure_time()
        edge_before = runtime_snapshot(spark)
        begin = time.monotonic()
        spark.sql(f"""INSERT INTO {edge} SELECT source_system, rel_type_id, id, source_type,
          source_id, target_type, target_id, schema_revision, entity_version, props_json,
          retained_json, order_key, source_feed, source_epoch, source_position, published_at,
          xxhash64(CAST(source_system AS STRING), CAST(rel_type_id AS BIGINT), CAST(id AS BIGINT)),
          apply_batch_id, source_cursor_json, source_delivery_id
        FROM (SELECT 'layout-probe' AS source_system, CAST(1 AS BIGINT) AS rel_type_id, id,
          CAST(1 AS BIGINT) AS source_type, CAST(floor(id/5) AS BIGINT) AS source_id,
          CAST(1 AS BIGINT) AS target_type,
          pmod(CAST(floor(id/5) AS BIGINT) + CAST(pmod(id,5) DIV 2 AS BIGINT), {NODES}) AS target_id,
          'layout-probe-r1' AS schema_revision, CAST(1 AS BIGINT) AS entity_version,
          '{{}}' AS props_json, '{{}}' AS retained_json, CAST(NULL AS STRING) AS order_key,
          'direct-layout-fixture' AS source_feed, '1' AS source_epoch, id AS source_position,
          TIMESTAMP '2026-10-15T00:00:00.000Z' AS published_at, 'layout-probe' AS apply_batch_id,
          '{{}}' AS source_cursor_json, CAST(id AS STRING) AS source_delivery_id
          FROM range({EDGES})) fixture""")
        stage = {"stage": current_stage, "rows_by_range": EDGES,
                 "seconds": time.monotonic() - begin}
        stages.append(stage)
        edge_after = runtime_snapshot(spark)
        write_resource_deltas[current_stage] = resource_delta(edge_before, edge_after)
        append_record(progress, {**stage, "resource_delta": write_resource_deltas[current_stage]})

        for role, expected in (("object_current", NODES), ("edge_current", EDGES)):
            ensure_time()
            current_stage = "inspect-storage-" + role
            begin = time.monotonic()
            table = f"`spark_catalog`.`default`.`{role}`"
            detail = spark.sql(f"DESCRIBE DETAIL {table}").first().asDict()
            location = urlparse(detail.get("location", ""))
            if location.scheme != "file" or location.netloc not in ("", "localhost"):
                raise ValueError(f"local table location required for {role}")
            table_dir = Path(unquote(location.path)).resolve()
            if not table_dir.is_relative_to(out):
                raise ValueError(f"table location escaped the private output directory: {role}")
            parquet_files = [path for path in table_dir.rglob("*.parquet") if path.is_file()]
            if not parquet_files or len(parquet_files) > 1000:
                raise ValueError(f"bounded nonempty Parquet file inventory required for {role}")

            jvm = spark._jvm
            parquet_reader = jvm.org.apache.parquet.hadoop.ParquetFileReader
            hadoop_path = jvm.org.apache.hadoop.fs.Path
            parquet_conf = spark._jsc.hadoopConfiguration()
            codecs = set()
            compressed_by_column = {}
            uncompressed_by_column = {}
            footer_rows = 0
            for parquet_path in parquet_files:
                footer = parquet_reader.readFooter(parquet_conf, hadoop_path(str(parquet_path)))
                for block in footer.getBlocks():
                    footer_rows += int(block.getRowCount())
                    for column in block.getColumns():
                        codecs.add(str(column.getCodec().name()))
                        name = str(column.getPath().toDotString())
                        compressed_by_column[name] = compressed_by_column.get(name, 0) + int(column.getTotalSize())
                        uncompressed_by_column[name] = uncompressed_by_column.get(name, 0) + int(column.getTotalUncompressedSize())
            delta_log = table_dir / "_delta_log"
            item = {
                "role": role,
                "rows_by_range": expected,
                "rows_from_parquet_footers": footer_rows,
                "sizeInBytes": detail.get("sizeInBytes"),
                "numFiles": detail.get("numFiles"),
                "parquet_file_bytes": sum(path.stat().st_size for path in parquet_files),
                "parquet_files": len(parquet_files),
                "parquet_codecs": sorted(codecs),
                "compressed_column_chunk_bytes": dict(sorted(compressed_by_column.items())),
                "uncompressed_column_chunk_bytes": dict(sorted(uncompressed_by_column.items())),
                "delta_log_bytes": sum(path.stat().st_size for path in delta_log.rglob("*") if path.is_file()),
                "format": detail.get("format"),
                "clusteringColumns_metadata": detail.get("clusteringColumns"),
                "declared_delta_codec_property": detail.get("properties", {}).get("delta.parquet.compression.codec"),
                "enforced_spark_codec_setting": spark.conf.get("spark.sql.parquet.compression.codec"),
            }
            tables.append(item)
            stage = {"stage": current_stage, "seconds": time.monotonic() - begin,
                     "parquet_file_bytes": item["parquet_file_bytes"],
                     "parquet_files": item["parquet_files"], "parquet_codecs": item["parquet_codecs"]}
            stages.append(stage)
            append_record(progress, stage)
            if footer_rows != expected:
                raise ValueError(f"Parquet footer row count mismatch for {role}: {footer_rows} != {expected}")
            if item["format"] != "delta" or codecs != {"ZSTD"}:
                raise ValueError(f"Delta format or observed Parquet ZSTD mismatch for {role}")

        # Two point lookups per table. Each WHERE contains both the exact
        # profile hash expression and every full identity component. The plan
        # and one-row result are captured; no distinct/collision search runs.
        for role, typed, ids in (("object_current", "type_id", (0, NODES - 1)),
                                 ("edge_current", "rel_type_id", (0, EDGES - 1))):
            for identity in ids:
                ensure_time()
                current_stage = f"point-lookup-{role}-{identity}"
                begin = time.monotonic()
                table = f"`spark_catalog`.`default`.`{role}`"
                fingerprint = ("xxhash64(CAST('layout-probe' AS STRING), CAST(1 AS BIGINT), "
                               f"CAST({identity} AS BIGINT))")
                sql = (f"SELECT source_system, {typed}, id, lookup_hash, "
                       f"xxhash64(CAST(source_system AS STRING), CAST({typed} AS BIGINT), "
                       f"CAST(id AS BIGINT)) AS computed_lookup_hash FROM {table} "
                       f"WHERE lookup_hash={fingerprint} AND source_system='layout-probe' "
                       f"AND {typed}=CAST(1 AS BIGINT) AND id=CAST({identity} AS BIGINT)")
                frame = spark.sql(sql)
                rows = [row.asDict(recursive=True) for row in frame.collect()]
                physical_plan = frame._jdf.queryExecution().executedPlan().toString()
                if len(rows) != 1:
                    raise ValueError(f"point lookup returned {len(rows)} rows for {role}/{identity}")
                row = rows[0]
                if (row["source_system"], int(row[typed]), int(row["id"])) != ("layout-probe", 1, identity):
                    raise ValueError(f"full key mismatch for point lookup {role}/{identity}")
                if int(row["lookup_hash"]) != int(row["computed_lookup_hash"]):
                    raise ValueError(f"stored hash differs from native Spark expression for {role}/{identity}")
                lookup = {"role": role, "identity": identity, "sql": sql,
                          "row_count": len(rows), "row_hash": int(row["lookup_hash"]),
                          "plan": physical_plan[:16000], "seconds": time.monotonic() - begin}
                lookups.append(lookup)
                append_record(progress, {"stage": current_stage, **lookup})

        elapsed = time.monotonic() - started
        result = {
            "profile": "ashlar-current-carrier-xxhash64-45m18c-sample/0.1",
            "scope": "Local direct 45M/225M carrier write and four exact point lookups only; no publication, ACK, resolver, full scan, graph-engine, or full-scale claim",
            "carrier_profile": PROFILE,
            "lookup_hash_profile": HASH_PROFILE,
            "source_commit": commit,
            "wheel_sha256": wheel_sha256,
            "spark": spark.version,
            "python": sys.version.split()[0],
            "master": spark.sparkContext.master,
            "codec_setting": spark.conf.get("spark.sql.parquet.compression.codec"),
            "nodes": NODES,
            "edges": EDGES,
            "source_fixture": "same deterministic value pattern as the 1M/5M fixture scaled to 45M/225M; source_position equals id",
            "stages": stages,
            "tables": tables,
            "prewrite_table_locations": prewrite_table_locations,
            "point_lookups": lookups,
            "elapsed_seconds": elapsed,
            "driver_memory_setting": spark.sparkContext.getConf().get("spark.driver.memory", ""),
            "runtime_observations": {"after_startup": initial_runtime,
                                     "after_sample": runtime_snapshot(spark)},
            "write_resource_deltas": write_resource_deltas,
            "host_cpu_count": os.cpu_count(),
            "disk_usage_before": initial_disk,
            "disk_usage_after": disk_snapshot(out),
            "private_output_file_bytes": output_tree_bytes(out),
            "submit_driver_memory_argument_present": "--driver-memory 24g" in submit_args.lower(),
        }
        target_node_rows, target_edge_rows = 1_000_000_000, 5_000_000_000
        node_write = next(s for s in stages if s["stage"] == "write-object-current")
        edge_write = next(s for s in stages if s["stage"] == "write-edge-current")
        node_rate, edge_rate = NODES / node_write["seconds"], EDGES / edge_write["seconds"]
        node_80_seconds = target_node_rows / (node_rate * 0.8)
        edge_80_seconds = target_edge_rows / (edge_rate * 0.8)
        active_bytes = sum(table["parquet_file_bytes"] for table in tables)
        result["full_scale_algorithmic_projection"] = {
            "basis": "Measured local[18] 45M/225M write rates with 80% throughput target; no scale-out or CPU-count multiplier.",
            "node_write_seconds_at_80_percent": node_80_seconds,
            "edge_write_seconds_at_80_percent": edge_80_seconds,
            "write_seconds_total_at_80_percent": node_80_seconds + edge_80_seconds,
            "write_minutes_total_at_80_percent": (node_80_seconds + edge_80_seconds) / 60,
            "active_parquet_bytes_per_row": active_bytes / (NODES + EDGES),
            "active_parquet_GB_linear_arithmetic_for_6B_rows": active_bytes / (NODES + EDGES) * (target_node_rows + target_edge_rows) / 1e9,
            "limitations": "Synthetic fixture arithmetic only; writes only, excluding runtime startup, creation, validation, graph engine, publication, history, retries, and production payloads. Not a full-scale qualification."
        }
        write_receipt(out / "result.json", result)
        append_record(progress, {"state": "completed", "elapsed_seconds": elapsed})
        print(json.dumps(result, sort_keys=True, separators=(",", ":"), allow_nan=False))
    except BaseException as error:
        failure = {"state": "failed", "stage": current_stage,
                   "elapsed_seconds": time.monotonic() - started,
                   "runtime_observations": ({"at_failure": runtime_snapshot(spark)} if spark is not None else {}),
                   "disk_usage": disk_snapshot(out),
                   "completed_stages": stages, "completed_tables": tables,
                   "completed_point_lookups": lookups,
                   "error_type": type(error).__name__, "error": str(error)[:1000]}
        write_receipt(out / "partial-result.json", failure)
        append_record(progress, failure)
        raise
    finally:
        if spark is not None:
            spark.stop()


if __name__ == "__main__":
    main()
