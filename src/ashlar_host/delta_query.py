"""Selected indexed query lifecycle; mandatory reader and Spark closure before report."""
import hashlib,json
from pathlib import Path
from ashlar.weft_distribution import DistributionPaths
from .indexed_query import run_indexed_queries
from .relationship_query import run_indexed_relationship
from .publication_reader import open_commerce_reader
from .source import recompute_dataset,read_bounded
from .config import HostError
from .lifecycle import owned_context,finish
from .commerce import runtime_paths
def query(config):
    index=config.index; installation=config.installation; publication=config.publication; source=config.producer.source; jars=config.jars; output=config.output
    paths = runtime_paths(config, False)
    if output.exists():
        raise ValueError('Fresh output required')
    output.mkdir(parents=False)
    receipt = output / 'fresh-public-dataset.json'
    recompute_dataset(config.producer, config.model, config.graph, receipt)
    if read_bounded(receipt, config.producer.maximum_receipt_bytes) != read_bounded(publication / 'public-dataset.json', config.producer.maximum_receipt_bytes):
        raise HostError('public-dataset-correspondence-refused')
    original_report_bytes = read_bounded(publication / 'report.json', 4*1024*1024)
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.master('local[1]').appName('Indexed original commerce query').config('spark.driver.memory', '512m').config('spark.ui.enabled', 'false').config('spark.sql.shuffle.partitions', '1').config('spark.databricks.delta.snapshotPartitions', '1').config('spark.sql.session.timeZone', 'UTC').config('spark.sql.ansi.enabled', 'true').config('spark.jars', ','.join((str(p) for p in paths))).config('spark.sql.extensions', 'io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog', 'org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir', str(output / 'warehouse')).getOrCreate()
    result = None
    primary = None
    try:
        with owned_context(open_commerce_reader(spark, publication)) as opened:
            if opened.original_report != json.loads(original_report_bytes):
                raise ValueError('Opening publication report differs')
            with owned_context(opened.provider.interval(opened.context)):
                for (table, alias) in opened.aliases.items():
                    if not all((part.replace('_', '').isalnum() for part in alias.split('.'))):
                        raise ValueError('Unsafe catalog alias')
                    database = alias.split('.')[-2]
                    spark.sql('CREATE DATABASE IF NOT EXISTS ' + database).collect()
                    native = opened.driver.transport.targets[table].path
                    spark.sql('CREATE TABLE ' + alias + " USING DELTA LOCATION '" + str(native).replace("'", "''") + "'").collect()
                if opened.native_files() != opened.original_native_files:
                    raise ValueError('Alias registration changed originals')
            alias_custody = opened.provider.closed_interval_custody(opened.context)
            opening_native = dict(opened.original_native_files)
            paths_config = DistributionPaths(index, installation)
            result = run_indexed_queries(opened, paths_config)
            result['relationship'] = run_indexed_relationship(opened, paths_config)
            closing_native = opened.native_files()
            if closing_native != opening_native:
                raise ValueError('Closing complete native vector differs')
            result['alias_interval'] = alias_custody
            result['original_publication_report_sha256'] = hashlib.sha256(original_report_bytes).hexdigest()
            result['original_native_manifest'] = opened.manifest
            result['opening_native_files'] = opening_native
            result['closing_native_files'] = closing_native
            result['fresh_public_dataset_sha256'] = hashlib.sha256(receipt.read_bytes()).hexdigest()
            result['source_publication'] = str(publication)
            result['runtime'] = {'spark': '4.0.1', 'delta': '4.0.0', 'jars': [{'path': str(p), 'sha256': hashlib.sha256(p.read_bytes()).hexdigest()} for p in paths]}
        if (publication / 'report.json').read_bytes() != original_report_bytes:
            raise ValueError('Closing publication report differs')
    except BaseException as error:
        result = None
        primary = error
    finish(primary, [spark.stop])
    runtime_paths(config, False, require_fresh=False)
    payload = json.dumps(result, ensure_ascii=False, separators=(',', ':')) + '\n'
    with (output / 'report.json').open('x') as stream:
        stream.write(payload)
    return result
