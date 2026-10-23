import json, os, pathlib, sys, time
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
root = pathlib.Path('/private/tmp/ashlar-umf056-delta4-one-table-20261023')
table = root / 'edge_current'
limit = 64 * 1024 * 1024
builder = (SparkSession.builder.master('local[1]').appName('umf-0.5.6-delta4-one-table')
    .config('spark.driver.memory', '1g')
    .config('spark.ui.enabled', 'false')
    .config('spark.sql.shuffle.partitions', '1')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.sql.warehouse.dir', str(root / 'warehouse'))
    .config('spark.local.dir', str(root / 'spark-local'))
    .config('spark.jars.ivy', str(root / 'ivy'))
    .config('spark.driver.host', '127.0.0.1')
    .config('spark.driver.bindAddress', '127.0.0.1'))
builder = configure_spark_with_delta_pip(builder, extra_packages=[])
spark = builder.getOrCreate()
try:
    spark.conf.set('spark.sql.parquet.compression.codec', 'zstd')
    ddl = f"""CREATE TABLE delta.`{table}` (
  `id` BIGINT NOT NULL,
  `caption` STRING
) USING DELTA CLUSTER BY (`id`)
TBLPROPERTIES ('delta.dataSkippingStatsColumns'='id,caption',
'delta.deletedFileRetentionDuration'='interval 14 days')"""
    spark.sql(ddl)
    spark.sql(f"INSERT INTO delta.`{table}` VALUES (1, 'one-row-acceptance')")
    props = {r.key: r.value for r in spark.sql(f"SHOW TBLPROPERTIES delta.`{table}`").collect()}
    detail = spark.sql(f"DESCRIBE DETAIL delta.`{table}`").first().asDict(recursive=True)
    rows = [r.asDict(recursive=True) for r in spark.sql(f"SELECT id, caption FROM delta.`{table}`").collect()]
    total = sum(p.stat().st_size for p in root.rglob('*') if p.is_file())
    if total > limit: raise RuntimeError(f'temp bytes {total} exceed {limit}')
    codec = spark.conf.get('spark.sql.parquet.compression.codec')
    result = {
      'status': 'passed', 'sparkVersion': spark.version,
      'deltaPackage': 'delta-spark 4.4.1 (resolved from isolated R4 venv)',
      'tablePath': str(table), 'ddl': ddl,
      'writerSetting': {'key':'spark.sql.parquet.compression.codec','value':codec},
      'observedTableProperties': {k:props.get(k) for k in ('delta.dataSkippingStatsColumns','delta.deletedFileRetentionDuration')},
      'observedDetail': {k:detail.get(k) for k in ('format','location','numFiles','sizeInBytes','properties','clusteringColumns')},
      'rows': rows, 'tempBytesAtCapture': total, 'tempByteLimit': limit
    }
    if codec != 'zstd': raise RuntimeError(f'writer setting mismatch: {codec}')
    if props.get('delta.dataSkippingStatsColumns') != 'id,caption': raise RuntimeError(f'property mismatch: {props}')
    if props.get('delta.deletedFileRetentionDuration') != 'interval 14 days': raise RuntimeError(f'property mismatch: {props}')
    if rows != [{'id':1,'caption':'one-row-acceptance'}]: raise RuntimeError(f'row mismatch: {rows}')
    (root / 'native-result.json').write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    print(json.dumps(result, sort_keys=True))
finally:
    spark.stop()
