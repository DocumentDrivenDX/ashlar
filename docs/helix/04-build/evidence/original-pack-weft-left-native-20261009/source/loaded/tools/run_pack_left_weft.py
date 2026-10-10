"""Original archaeology LEFT query on a held private publication.

Unchanged compiler SQL/guards and exact original full ordered graph oracle.
Physical match schema and source non-null proofs stay separate from authored
optional null; no production authority or broader LEFT subset claim.
"""
import hashlib,importlib.metadata,json,time
from pathlib import Path
from local_delta_custody import encoded
from prepare_pack_query_cases import prepare
from run_pack_publication_weft import compiler_request,open_pack_reader
from run_commerce_arithmetic_weft import compile_original,execute_guarded,admit_public_source,decode_rows,persist_after_stop
from weft_left_plan import BACKEND,ENCODING
COMPILER_SHA='a8bade61ddaf8062de21103e0f290d88669606c1f3466f78cb45c3a24f6942d0'


def admit_ordered_rows(rows,expected):
    if list(map(encoded,rows))!=list(map(encoded,expected)):
        raise ValueError('Complete independently ordered original graph LEFT tuple bag differs')
    return rows


def tagged_oracle(original):
    """This original query's independent graph oracle uses None only for unmatched joins.

    The authored pack supplies no matched native null for these two projected
    fields; that witness must remain separate from the generic tagged decoder.
    """
    expected=[]
    for row in original:
        if type(row)is not list or len(row)!=3 or type(row[0])is not str:raise ValueError('Original complete LEFT oracle required')
        if any(v is not None and type(v)is not str for v in row[1:]):raise ValueError('Original String/unmatched oracle required')
        expected.append([row[0],*[{'state':'absent'} if v is None else {'state':'value','value':v} for v in row[1:]]])
    return expected


def admit_matched_source_strings(graph_bytes):
    """Prove this source fixture's matched payloads are Strings, even unused rows."""
    if type(graph_bytes)is not bytes:raise ValueError('Original retained graph bytes required')
    graph=json.loads(graph_bytes);witness=[]
    for obj in graph['objects']:
        field={'pottery_results':'pottery_results.form','fauna_results':'fauna_results.taxon'}.get(obj['type']['element'])
        if field is not None:
            if field not in obj['values'] or type(obj['values'][field])is not str:raise ValueError('Matched original source null/absent cannot be inferred as relational absence')
            witness.append({'key':obj['key'],'type':obj['type'],'field':field,'value':obj['values'][field]})
    if not witness:raise ValueError('Original matched source witness required')
    return witness


def opt_in(request):
    import copy
    request=copy.deepcopy(request);binding=json.loads(request['target']['bindingJson'])
    for field in ('interpretation_evidence.pottery_result_id','interpretation_evidence.fauna_result_id'):
        props=[p for r in binding['records'] for p in r['properties'] if p['logical']['element']==field]
        if len(props)!=1 or props[0]['home'].get('kind')!='props':raise ValueError('Exact original optional ON property required')
        props[0]['home']['encoding']=ENCODING
    raw=encoded(binding);request['target']={**BACKEND,'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()}
    return request

def prepare_original_case():
    cases=[c for c in prepare('archaeology')['cases']if c['original_scenario']['id']=='evidence-links']
    if len(cases)!=1:raise ValueError('Exact single original evidence-links scenario required')
    return cases[0]


def run(publication,output,jars,compiler,source_guard,umf):
    from pyspark.sql import SparkSession
    from run_local_weft_typed_spark4 import JAR_SHA
    output=Path(output);jars=Path(jars);compiler=Path(compiler)
    if output.exists() or hashlib.sha256(compiler.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Fresh output and immutable reviewed compiler required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Exact existing Spark4.0.1 required')
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    case=prepare_original_case()
    output.mkdir(mode=0o700);started=time.monotonic()
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original archaeology LEFT evidence links')
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
            source_witness=admit_matched_source_strings(opened.graph)
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
            result=execute_guarded(provider,request,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c),left_join=True)
            result['closed_interval_custody']=provider.closed_interval_custody(context)
            rows=decode_rows(artifact,result['rows'],native_schema=result['native_schema'],ordered_rows=result['ordered_rows'],left_join=True);original_expected=case['original_graph_expected']
            expected=tagged_oracle(original_expected)
            admit_ordered_rows(rows,expected)
            with provider.interval(context):
                resolved=provider.resolve(context);provider.admit_binding(json.loads(request['target']['bindingJson']),resolved,context);provider.runtime(context)
            closing_custody=provider.closed_interval_custody(context)
            if opened.native_files()!=opened.original_native_files:raise ValueError('LEFT read changed original bytes')
            report={'format':'ashlar-original-archaeology-weft-left/0.1','qualification':__doc__,
                'compiler_sha256':COMPILER_SHA,'compiler_realization_base':'ee90571a5aa67b6d2e6069220f6e1cc0eec3822c', 'compiler_source_manifest_sha256':'8b60ac82f3f730471adde8f2c54a0e42c9210d19fe7ea94a4948f2020235e0dd',
                'case':case,'native_result':result,'exact_rows':rows,'independent_expected':expected,'original_independent_expected':original_expected,
                'matched_source_string_witness':source_witness,'original_manifest':opened.manifest,'original_table_registry':opened.original_report['table_registry'],
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
