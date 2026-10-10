"""Fresh read-only indexed String/count queries over original commerce publication.

Qualified local composition: Spark4.0.1/Delta4.0.0, UTC/ANSI, one worker/512MiB.
Original setup uses run_commerce_outbox_publication.py with Spark3.5.3/Delta3.2.1
and clean UMFc7c95e1c4ea5b72541f47fa0350ca467ff02f395. Operator supplies all
runtime resources; this script installs/downloads nothing. The explicit private
PG profile is the previously qualified labeled development container, not Truss.
"""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess

from ashlar.weft_distribution import DistributionPaths
from indexed_weft_commerce import run_indexed_queries
from run_commerce_relationship_weft import run_indexed_relationship
from run_commerce_publication_weft import open_commerce_reader

UMF_PIN = 'c7c95e1c4ea5b72541f47fa0350ca467ff02f395'
ACK_PROFILE = ('ashlar-e2e-truss-pg17', '127.0.0.1', 15432, 'truss_e2e')
DELTA4_JARS = {'delta-spark_2.13-4.0.0.jar':'538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e', 'delta-storage-4.0.0.jar':'9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d'}


def preflight(source: Path, publication: Path, jars: Path, ack: tuple):
    if ack != ACK_PROFILE: raise ValueError('Unsupported explicit private ACK connection profile')
    head = subprocess.check_output(['git','-C',str(source),'rev-parse','HEAD'],text=True).strip()
    dirty = subprocess.check_output(['git','-C',str(source),'status','--porcelain','--untracked-files=no'],text=True)
    if head != UMF_PIN or dirty: raise ValueError('Exact clean original public UMF producer required')
    if not (publication/'report.json').is_file(): raise ValueError('Complete original publication required')
    paths = [jars/name for name in sorted(DELTA4_JARS)]
    if set(p.name for p in jars.glob('*.jar'))!=set(DELTA4_JARS) or any(not p.is_file() or p.is_symlink() or hashlib.sha256(p.read_bytes()).hexdigest()!=DELTA4_JARS[p.name] for p in paths):
        raise ValueError('Exact two-JAR Delta4 runtime required')
    if importlib.metadata.version('pyspark')!='4.0.1' or importlib.metadata.version('delta-spark')!='4.0.0':
        raise ValueError('Exact Spark4/Delta4 runtime required')
    return paths


def finalize(spark, result, output: Path):
    # Stop is mandatory before report publication, including on query failure.
    spark.stop()
    if result is not None:
        (output/'report.json').write_text(json.dumps(result,ensure_ascii=False,separators=(',',':'))+'\n')


def run(*, index: Path, installation: Path, publication: Path, source: Path,
        jars: Path, output: Path, ack: tuple):
    paths = preflight(source,publication,jars,ack)
    if output.exists(): raise ValueError('Fresh output required')
    output.mkdir(parents=False)
    root=Path(__file__).resolve().parents[1]
    receipt=output/'fresh-public-dataset.json'
    call=subprocess.run(['bun',str(root/'tools/check_commerce_dataset.ts'),str(source),str(receipt)],capture_output=True,timeout=60)
    if call.returncode or not receipt.is_file() or receipt.read_bytes()!=(publication/'public-dataset.json').read_bytes():
        raise ValueError('Fresh original public dataset admission differs')
    original_report_bytes=(publication/'report.json').read_bytes()
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Indexed original commerce query')
           .config('spark.driver.memory','512m').config('spark.ui.enabled','false')
           .config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1')
           .config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true')
           .config('spark.jars',','.join(str(p) for p in paths))
           .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
           .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
           .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    result=None
    try:
        with open_commerce_reader(spark,publication) as opened:
            if opened.original_report!=json.loads(original_report_bytes):raise ValueError('Opening publication report differs')
            with opened.provider.interval(opened.context):
                # Metadata aliases preserve original native data/UUID/version.
                for table,alias in opened.aliases.items():
                    if not all(part.replace('_','').isalnum() for part in alias.split('.')):raise ValueError('Unsafe catalog alias')
                    database=alias.split('.')[-2]
                    spark.sql('CREATE DATABASE IF NOT EXISTS '+database).collect()
                    native=opened.driver.transport.targets[table].path
                    spark.sql('CREATE TABLE '+alias+" USING DELTA LOCATION '"+str(native).replace("'","''")+"'").collect()
                if opened.native_files()!=opened.original_native_files:raise ValueError('Alias registration changed originals')
            alias_custody=opened.provider.closed_interval_custody(opened.context)
            opening_native=dict(opened.original_native_files)
            paths_config=DistributionPaths(index,installation)
            result=run_indexed_queries(opened,paths_config)
            result['relationship']=run_indexed_relationship(opened,paths_config)
            closing_native=opened.native_files()
            if closing_native!=opening_native:raise ValueError('Closing complete native vector differs')
            result['alias_interval']=alias_custody
            result['original_publication_report_sha256']=hashlib.sha256(original_report_bytes).hexdigest()
            result['original_native_manifest']=opened.manifest
            result['opening_native_files']=opening_native
            result['closing_native_files']=closing_native
            result['fresh_public_dataset_sha256']=hashlib.sha256(receipt.read_bytes()).hexdigest()
            result['source_publication']=str(publication)
            result['runtime']={'spark':'4.0.1','delta':'4.0.0','jars':[{'path':str(p),'sha256':hashlib.sha256(p.read_bytes()).hexdigest()}for p in paths]}
        if (publication/'report.json').read_bytes()!=original_report_bytes:raise ValueError('Closing publication report differs')
    except BaseException:
        result=None
        raise
    finally:
        finalize(spark,result,output)
    return result


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for name in ('index','installation','publication','umf-source','jars','output'):
        parser.add_argument('--'+name,type=Path,required=True)
    parser.add_argument('--ack-container',required=True);parser.add_argument('--ack-host',required=True)
    parser.add_argument('--ack-port',type=int,required=True);parser.add_argument('--ack-database',required=True)
    a=parser.parse_args()
    run(index=a.index,installation=a.installation,publication=a.publication,source=a.umf_source,jars=a.jars,output=a.output,
        ack=(a.ack_container,a.ack_host,a.ack_port,a.ack_database))


if __name__=='__main__':main()
