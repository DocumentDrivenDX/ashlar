"""Original ecology DISTINCT on held private publication; no broader engine claim.

Execute unchanged public compiler SQL/checks, retain exact original graph oracle,
source/full UUID vector and ordinary ACK custody. Never deduplicate native rows.
"""
import hashlib,importlib.metadata,json,time
from pathlib import Path
from local_delta_custody import encoded
from prepare_pack_query_cases import prepare
from run_pack_publication_weft import compiler_request,open_pack_reader
from run_commerce_arithmetic_weft import compile_original,execute_guarded,admit_public_source,decode_rows,persist_after_stop
COMPILER_SHA='8015e54b4f35752325ce5f63aba55c7b83d1ba026a876e8d7edd923c2410f758'


def run(publication,output,jars,compiler,source_guard,umf):
    from pyspark.sql import SparkSession
    from run_local_weft_typed_spark4 import JAR_SHA
    output=Path(output);jars=Path(jars);compiler=Path(compiler)
    if output.exists() or hashlib.sha256(compiler.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Fresh output and immutable reviewed compiler required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Exact existing Spark4.0.1 required')
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    case=next(c for c in prepare('ecology')['cases']if c['original_scenario']['id']=='comparability')
    output.mkdir(mode=0o700);started=time.monotonic()
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original ecology DISTINCT')
      .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true')
      .config('spark.sql.session.timeZone','UTC').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
      .config('spark.jars',','.join(map(str,paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
      .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');completed=False;pending={}
    try:
        with open_pack_reader(spark,publication,'ecology')as opened:
            provider=opened.provider;context=opened.context
            with provider.interval(context):
                spark.sql('CREATE DATABASE ecology').collect()
                for table,alias in opened.aliases.items():
                    target=opened.driver.transport.targets[table]
                    spark.sql('CREATE TABLE '+alias+' USING DELTA LOCATION '+"'"+str(target.path)+"'").collect()
                if opened.native_files()!=opened.original_native_files:raise ValueError('Alias registration changed original native bytes')
            alias_custody=provider.closed_interval_custody(context)
            request=compiler_request('ecology',case['original_scenario']['sql'],opened.model,opened.graph,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
            provider.expected_binding=request['target']['bindingJson'];artifact=compile_original(compiler,request,COMPILER_SHA)
            result=execute_guarded(provider,request,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c),distinct=True)
            result['closed_interval_custody']=provider.closed_interval_custody(context)
            rows=decode_rows(artifact,result['rows']);expected=case['original_graph_expected']
            if sorted(map(encoded,rows))!=sorted(map(encoded,expected)):raise ValueError('Complete independent original graph tuple bag differs')
            with provider.interval(context):
                resolved=provider.resolve(context);provider.admit_binding(json.loads(request['target']['bindingJson']),resolved,context);provider.runtime(context)
            closing_custody=provider.closed_interval_custody(context)
            if opened.native_files()!=opened.original_native_files:raise ValueError('DISTINCT read changed original bytes')
            report={'format':'ashlar-original-ecology-weft-distinct/0.1','qualification':__doc__,
                'compiler_sha256':COMPILER_SHA,'official_weft_revision':'e9d606d3bb0216264b7351388875a0df384dc02c',
                'case':case,'native_result':result,'exact_rows':rows,'independent_expected':expected,
                'original_manifest':opened.manifest,'original_table_registry':opened.original_report['table_registry'],
                'original_native_files':opened.original_native_files,'alias_closed_interval_custody':alias_custody,
                'final_closed_interval_custody':closing_custody,'elapsed_before_cleanup_seconds':time.monotonic()-started}
            pending={'original-request.json':encoded(request)+'\n','original-artifact.json':encoded(artifact)+'\n','report.json':encoded(report)+'\n'}
        completed=True
    finally:
        if not completed:spark.stop()
    if not completed:raise ValueError('Suppressed reader failure')
    persist_after_stop(spark,pending,output);return report

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('ecology','output','jars','compiler','source-guard','umf'):p.add_argument('--'+name,required=True)
    a=p.parse_args();run(a.ecology,a.output,a.jars,a.compiler,a.source_guard,a.umf)
