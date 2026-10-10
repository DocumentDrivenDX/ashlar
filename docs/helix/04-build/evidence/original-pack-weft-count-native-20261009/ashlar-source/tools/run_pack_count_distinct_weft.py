"""Original ecology required-String COUNT DISTINCT and IN on held private publication; no broader engine claim.

Execute unchanged public compiler SQL/checks, retain exact original graph oracle,
source/full UUID vector and ordinary ACK custody. Never deduplicate native rows. Logical counts remain mathematical integers; native signed64 capacity is separately guarded.
"""
import hashlib,importlib.metadata,json,time
from pathlib import Path
from local_delta_custody import encoded
from prepare_pack_query_cases import prepare
from run_pack_publication_weft import compiler_request,open_pack_reader
from run_commerce_arithmetic_weft import compile_original,execute_guarded,admit_public_source,decode_rows,persist_after_stop,COUNT_BACKEND
COMPILER_SHA='8e2ce68b9482bc0c2bb7782e25879859f8d1d03843d5378a038d804f3e4f603b'


def admit_ordered_rows(rows,expected):
    if list(map(encoded,rows))!=list(map(encoded,expected)):
        raise ValueError('Complete independently ordered original graph tuple bag differs')
    return rows

def control_oracles(graph_bytes):
    """Derive finite controls from retained original bytes, never native result rows."""
    if type(graph_bytes) is not bytes:raise ValueError('Original retained graph bytes required')
    graph=json.loads(graph_bytes)
    names=[o['values']['observed_properties.name'] for o in graph['objects'] if o['type']['element']=='observed_properties']
    if any(type(n) is not str for n in names) or 'no-original-record' in names:raise ValueError('Explicit String control domain required')
    return names,[('global','SELECT COUNT(DISTINCT p.name) AS n FROM observed_properties p',[[str(len(set(names)))]]),
        ('global-empty',"SELECT COUNT(DISTINCT p.name) AS n FROM observed_properties p WHERE p.name IN ('no-original-record')",[['0']]),
        ('grouped-empty',"SELECT p.name,COUNT(DISTINCT p.name) AS n FROM observed_properties p WHERE p.name IN ('no-original-record') GROUP BY p.name ORDER BY p.name",[]),
        ('literal-duplicates',"SELECT p.name FROM observed_properties p WHERE p.name IN ('temperature','temperature','dissolved-oxygen') ORDER BY p.name",[[n]for n in sorted(names) if n in ('temperature','dissolved-oxygen')])]

def run(publication,output,jars,compiler,source_guard,umf):
    from pyspark.sql import SparkSession
    from run_local_weft_typed_spark4 import JAR_SHA
    output=Path(output);jars=Path(jars);compiler=Path(compiler)
    if output.exists() or hashlib.sha256(compiler.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Fresh output and immutable reviewed compiler required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Exact existing Spark4.0.1 required')
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    case=next(c for c in prepare('ecology')['cases']if c['original_scenario']['id']=='connected-measurements')
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
            request['target']={**request['target'],**COUNT_BACKEND}
            provider.expected_binding=request['target']['bindingJson'];artifact=compile_original(compiler,request,COMPILER_SHA)
            result=execute_guarded(provider,request,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c),count_distinct=True)
            result['closed_interval_custody']=provider.closed_interval_custody(context)
            rows=decode_rows(artifact,result['rows']);original_expected=case['original_graph_expected']
            expected=[[cell if type(cell)is str else str(cell) for cell in row]for row in original_expected]
            if any(type(row[-1]) is not str or not row[-1].isdigit() for row in original_expected):raise ValueError('Independent original count oracle requires exact Integer text')
            admit_ordered_rows(rows,expected)
            names,controls=control_oracles(opened.graph)
            control_results=[]
            for name,sql,control_expected in controls:
                control_request=compiler_request('ecology',sql,opened.model,opened.graph,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
                control_request['target']={**control_request['target'],**COUNT_BACKEND}
                provider.expected_binding=control_request['target']['bindingJson']
                control_artifact=compile_original(compiler,control_request,COMPILER_SHA)
                control_result=execute_guarded(provider,control_request,control_artifact,context=context,count_distinct=True)
                control_result['closed_interval_custody']=provider.closed_interval_custody(context)
                control_rows=decode_rows(control_artifact,control_result['rows'])
                if control_rows!=control_expected:raise ValueError('Exact independently derived control bag/order differs: '+name)
                control_results.append({'id':name,'sql':sql,'native_result':control_result,'exact_rows':control_rows,'independent_expected':control_expected})
                pending[name+'-request.json']=encoded(control_request)+'\n'
                pending[name+'-artifact.json']=encoded(control_artifact)+'\n'
            with provider.interval(context):
                resolved=provider.resolve(context);provider.admit_binding(json.loads(request['target']['bindingJson']),resolved,context);provider.runtime(context)
            closing_custody=provider.closed_interval_custody(context)
            if opened.native_files()!=opened.original_native_files:raise ValueError('Count/IN read changed original bytes')
            report={'format':'ashlar-original-ecology-weft-count-distinct/0.1','qualification':__doc__,
                'compiler_sha256':COMPILER_SHA,'official_weft_revision':'320a7a20d508000583c133badce9bfc151f3f72e',
                'controls':control_results,'control_original_names':names,'case':case,'native_result':result,'exact_rows':rows,'independent_expected':expected,'original_independent_expected':original_expected,
                'original_manifest':opened.manifest,'original_table_registry':opened.original_report['table_registry'],
                'original_native_files':opened.original_native_files,'alias_closed_interval_custody':alias_custody,
                'final_closed_interval_custody':closing_custody,'elapsed_before_cleanup_seconds':time.monotonic()-started}
            pending.update({'original-request.json':encoded(request)+'\n','original-artifact.json':encoded(artifact)+'\n','report.json':encoded(report)+'\n'})
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
