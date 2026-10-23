import json, pathlib
from pyspark.sql import SparkSession
root = pathlib.Path('/private/tmp/ashlar-umf056-delta4-one-table-20261023-attempt-04')
table = root / 'edge_current'
jars = [
    '/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-spark_4.2_2.13-4.4.1.jar',
    '/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-storage-4.4.1.jar',
]
if SparkSession.getActiveSession() is not None:
    raise RuntimeError('unexpected pre-existing SparkSession')
b = (SparkSession.builder.master('local[1]').appName('umf-0.5.6-delta4-one-table-attempt-04')
    .config('spark.driver.memory', '1g')
    .config('spark.ui.enabled', 'false')
    .config('spark.sql.shuffle.partitions', '1')
    .config('spark.databricks.delta.snapshotPartitions', '1')
    .config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension')
    .config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog')
    .config('spark.sql.parquet.compression.codec', 'zstd')
    .config('spark.sql.warehouse.dir', str(root / 'warehouse'))
    .config('spark.local.dir', str(root / 'spark-local'))
    .config('spark.jars', ','.join(jars))
    .config('spark.driver.host', '127.0.0.1')
    .config('spark.driver.bindAddress', '127.0.0.1'))
spark = b.getOrCreate()
try:
    package_request = spark.sparkContext.getConf().get('spark.jars.packages', '')
    if package_request:
        raise RuntimeError(f'unexpected Maven package request: {package_request}')
    configured_jars = spark.sparkContext.getConf().get('spark.jars', '')
    codec = spark.conf.get('spark.sql.parquet.compression.codec')
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
    result = {
      'status': 'passed', 'sparkVersion': spark.version,
      'deltaPythonPackage': '4.4.1', 'tablePath': str(table), 'ddl': ddl,
      'sparkJars': configured_jars.split(','), 'sparkJarsPackages': package_request,
      'writerSetting': {'key':'spark.sql.parquet.compression.codec','value':codec},
      'observedTableProperties': {k:props.get(k) for k in ('delta.dataSkippingStatsColumns','delta.deletedFileRetentionDuration')},
      'observedDetail': {k:detail.get(k) for k in ('format','location','numFiles','sizeInBytes','properties','clusteringColumns')},
      'rows': rows,
    }
    mismatches = []
    if codec != 'zstd': mismatches.append(f'writer codec observed {codec!r}')
    if props.get('delta.dataSkippingStatsColumns') != 'id,caption': mismatches.append('stats property mismatch')
    if props.get('delta.deletedFileRetentionDuration') != 'interval 14 days': mismatches.append('retention property mismatch')
    if rows != [{'id': 1, 'caption': 'one-row-acceptance'}]: mismatches.append('row mismatch')
    result['mismatches'] = mismatches
    if mismatches: result['status'] = 'failed_assertion'
    (root / 'native-result.json').write_text(json.dumps(result, sort_keys=True, indent=2) + '\n')
    print(json.dumps(result, sort_keys=True))
    if mismatches: raise RuntimeError('; '.join(mismatches))
finally:
    spark.stop()
