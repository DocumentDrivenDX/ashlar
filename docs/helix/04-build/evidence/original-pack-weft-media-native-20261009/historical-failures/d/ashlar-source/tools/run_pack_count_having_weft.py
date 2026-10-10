"""Original archaeology optional-String COUNT DISTINCT and narrow HAVING on held private publication; no broader engine claim.

Execute unchanged public compiler SQL/checks, retain exact original graph oracle,
source/full UUID vector and ordinary ACK custody. Never deduplicate native rows. Logical counts remain mathematical integers; native signed64 capacity is separately guarded.
"""
import hashlib,importlib.metadata,json,time
from pathlib import Path
from local_delta_custody import encoded
from prepare_pack_query_cases import prepare
from run_pack_publication_weft import compiler_request,open_pack_reader
from run_commerce_arithmetic_weft import compile_original,execute_guarded,admit_public_source,decode_rows,persist_after_stop,HAVING_BACKEND
COMPILER_SHA='4914d593b706d2740dbcaa08a441d1dec3677b4b2d79e6fa03f6c532e0b1f70d'


def admit_ordered_rows(rows,expected):
    if list(map(encoded,rows))!=list(map(encoded,expected)):
        raise ValueError('Complete independently ordered original graph tuple bag differs')
    return rows

def control_oracles(graph_bytes):
    """Exact optional counts from retained original graph values; no native inference."""
    if type(graph_bytes) is not bytes:raise ValueError('Original retained graph bytes required')
    graph=json.loads(graph_bytes);groups={};null_groups={};nonnull=set()
    for obj in graph['objects']:
        if obj['type']['element']!='asset_subjects':continue
        values=obj['values'];asset=values['asset_subjects.asset_id']
        if type(asset) is not str or 'asset_subjects.context_id' not in values:raise ValueError('Complete original source cells required')
        context=values['asset_subjects.context_id']
        if context is not None and type(context) is not str:raise ValueError('Original optional String/null required')
        groups.setdefault(asset,set())
        if context is None:null_groups.setdefault(asset,set())
        else:groups[asset].add(context);nonnull.add(context)
    ordered=[[asset,str(len(contexts))]for asset,contexts in sorted(groups.items())]
    return ordered,[
      ('global','SELECT COUNT(DISTINCT context_id) AS n FROM asset_subjects',[[str(len(nonnull))]]),
      ('global-empty',"SELECT COUNT(DISTINCT context_id) AS n FROM asset_subjects WHERE asset_id IN ('no-original-record')",[['0']]),
      ('all-null-groups','SELECT asset_id,COUNT(DISTINCT context_id) AS n FROM asset_subjects WHERE context_id IS NULL GROUP BY asset_id ORDER BY asset_id',[[asset,'0']for asset in sorted(null_groups)]),
      ('having-excludes-all','SELECT asset_id,COUNT(DISTINCT context_id) AS n FROM asset_subjects GROUP BY asset_id HAVING COUNT(DISTINCT context_id)>9223372036854775807 ORDER BY asset_id',[]),
      ('global-having-empty','SELECT COUNT(DISTINCT context_id) AS n FROM asset_subjects HAVING COUNT(DISTINCT context_id)>9223372036854775807',[])]


def opt_in(request):
    """Authenticate only the selected original optional context Field's new home encoding."""
    import copy
    request=copy.deepcopy(request);binding=json.loads(request['target']['bindingJson'])
    props=[p for r in binding['records'] for p in r['properties'] if p['logical']['element']=='asset_subjects.context_id']
    if len(props)!=1 or props[0]['home'].get('kind')!='props':raise ValueError('Exact original optional property home required')
    props[0]['home']['encoding']='ashlar-weft-json-native-null/0.1-candidate'
    raw=encoded(binding);request['target']={**HAVING_BACKEND,'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()}
    return request

def run(publication,output,jars,compiler,source_guard,umf):
    from pyspark.sql import SparkSession
    from run_local_weft_typed_spark4 import JAR_SHA
    output=Path(output);jars=Path(jars);compiler=Path(compiler)
    if output.exists() or hashlib.sha256(compiler.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Fresh output and immutable reviewed compiler required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Exact existing Spark4.0.1 required')
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    case=next(c for c in prepare('archaeology')['cases']if c['original_scenario']['id']=='media')
    output.mkdir(mode=0o700);started=time.monotonic()
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original archaeology optional counts')
      .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true')
      .config('spark.sql.session.timeZone','UTC').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
      .config('spark.jars',','.join(map(str,paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
      .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');completed=False;pending={}
    try:
        with open_pack_reader(spark,publication,'archaeology')as opened:
            provider=opened.provider;context=opened.context
            with provider.interval(context):
                spark.sql('CREATE DATABASE archaeology').collect()
                for table,alias in opened.aliases.items():
                    target=opened.driver.transport.targets[table]
                    spark.sql('CREATE TABLE '+alias+' USING DELTA LOCATION '+"'"+str(target.path)+"'").collect()
                if opened.native_files()!=opened.original_native_files:raise ValueError('Alias registration changed original native bytes')
            alias_custody=provider.closed_interval_custody(context)
            request=compiler_request('archaeology',case['original_scenario']['sql'],opened.model,opened.graph,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
            request=opt_in(request)
            provider.expected_binding=request['target']['bindingJson'];artifact=compile_original(compiler,request,COMPILER_SHA)
            result=execute_guarded(provider,request,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c),count_having=True)
            result['closed_interval_custody']=provider.closed_interval_custody(context)
            rows=decode_rows(artifact,result['rows']);original_expected=case['original_graph_expected']
            expected=[[cell if type(cell)is str else str(cell) for cell in row]for row in original_expected]
            if any(type(row[-1]) is not str or not row[-1].isdigit() for row in original_expected):raise ValueError('Independent original count oracle requires exact Integer text')
            admit_ordered_rows(rows,expected)
            all_group_counts,controls=control_oracles(opened.graph)
            control_results=[]
            for name,sql,control_expected in controls:
                control_request=compiler_request('archaeology',sql,opened.model,opened.graph,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
                control_request=opt_in(control_request)
                provider.expected_binding=control_request['target']['bindingJson']
                control_artifact=compile_original(compiler,control_request,COMPILER_SHA)
                control_result=execute_guarded(provider,control_request,control_artifact,context=context,count_having=True)
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
            report={'format':'ashlar-original-archaeology-weft-count-having/0.1','qualification':__doc__,
                'compiler_sha256':COMPILER_SHA,'official_weft_revision':'d2d63879a80c83f604179ade6c169b10676f6de4',
                'controls':control_results,'independent_all_group_counts':all_group_counts,'case':case,'native_result':result,'exact_rows':rows,'independent_expected':expected,'original_independent_expected':original_expected,
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
    for name in ('archaeology','output','jars','compiler','source-guard','umf'):p.add_argument('--'+name,required=True)
    a=p.parse_args();run(a.archaeology,a.output,a.jars,a.compiler,a.source_guard,a.umf)
