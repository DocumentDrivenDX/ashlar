"""Execute original compiled null-aware field queries on held local pack publications.

Private Spark4/Delta4 fixture read profile; no UC, accepted Truss, remote source,
scale or whole-language qualification. SQL is freshly emitted by the exact pinned
Weft executable and executed unchanged. Every original model/Field, scalar check,
whole publication vector and ordinary PostgreSQL ACK interval remains required.
"""
import hashlib,importlib.metadata,json,time
from pathlib import Path
from local_delta_custody import encoded
from prepare_pack_query_cases import prepare
from run_pack_publication_weft import PACKS,compiler_request,open_pack_reader
from run_commerce_arithmetic_weft import (compile_original,execute_guarded,
    admit_public_source,decode_rows,persist_after_stop,NativeGuardRefusal)

COMPILER_SHA='22148789dd5b7c20fdccf9c841311cfed268a1fa62d5b8314f0e927b3fcf5a3a'
CASES={'archaeology':('sample',), 'ecology':('censor','effort','zero')}
NULL_FIELDS={'archaeology-sample':{'samples.parent_id'},'ecology-censor':{'observations.value','observations.threshold'},'ecology-effort':{'effort.amount'},'ecology-zero':{'effort.amount'}}


def opt_in(request,case):
    binding=json.loads(request['target']['bindingJson']);seen=set()
    for record in binding['records']:
        for prop in record['properties']:
            field=prop['logical']['element']
            if field in NULL_FIELDS[case]:
                if prop['home'].get('kind') != 'props':raise ValueError('Original Props home required')
                prop['home']['encoding']='ashlar-weft-json-native-null/0.1-candidate';seen.add(field)
    if seen != NULL_FIELDS[case]:raise ValueError('Exact selected original null Field inventory required')
    request['target']['bindingJson']=encoded(binding)
    request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest()
    return request


def tagged_expected(rows,artifact):
    result=exact_expected(rows)
    for row in result:
        for position,column in enumerate(artifact['columns']):
            if column['representation']['kind']=='value':
                row[position]={'state':'null'} if row[position] is None else {'state':'value','value':row[position]}
    return result



def exact_expected(rows):
    result=[]
    for row in rows:
        cells=[]
        for value in row:
            if type(value)is str:cells.append(value)
            elif type(value)is int:cells.append(str(value))
            elif value is None:cells.append(None)
            else:raise ValueError('Unsupported independent expected carrier')
        result.append(cells)
    return result


def run(publications,output,jars,compiler,source_guard,umf):
    from run_local_weft_typed_spark4 import JAR_SHA
    from pyspark.sql import SparkSession
    output=Path(output);jars=Path(jars);compiler=Path(compiler);source_guard=Path(source_guard);umf=Path(umf)
    if output.exists()or set(publications)!=set(PACKS):raise ValueError('Fresh output and both original publications required')
    if hashlib.sha256(compiler.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Exact reviewed compiler required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Existing Spark4.0.1 required')
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(not p.is_file()or hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    output.mkdir(mode=0o700);started=time.monotonic()
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar original pack Weft field reads')
      .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
      .config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false')
      .config('spark.jars',','.join(str(p)for p in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
      .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
      .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');pending={};reports=[];completed=False
    try:
        for pack in PACKS:
            with open_pack_reader(spark,publications[pack],pack)as opened:
                provider=opened.provider;context=opened.context
                with provider.interval(context):
                    spark.sql('CREATE DATABASE '+pack).collect()
                    for table,alias in opened.aliases.items():
                        target=opened.driver.transport.targets[table]
                        spark.sql('CREATE TABLE '+alias+' USING DELTA LOCATION '+"'"+str(target.path)+"'").collect()
                    if opened.native_files()!=opened.original_native_files:raise ValueError('Alias registration changed original bytes')
                alias_custody=provider.closed_interval_custody(context)
                queries=[];controls=[]
                for case in prepare(pack)['cases']:
                    original=case['original_scenario']
                    if original['id']not in CASES[pack]:continue
                    request=compiler_request(pack,original['sql'],opened.model,opened.graph,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
                    request=opt_in(request,pack+'-'+original['id'])
                    provider.expected_binding=request['target']['bindingJson']
                    artifact=compile_original(compiler,request,COMPILER_SHA)
                    positioned=any(o['id']=='weft.output.positioned'for o in artifact['obligations'])
                    result=execute_guarded(provider,request,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c),positioned_outputs=positioned,native_null=True)
                    result['closed_interval_custody']=provider.closed_interval_custody(context)
                    rows=decode_rows(artifact,result['rows'],positioned=positioned,native_schema=result.get('native_schema'),ordered_rows=result.get('ordered_rows'),native_null=True);expected=tagged_expected(case['original_graph_expected'],artifact)
                    if sorted(rows,key=encoded)!=sorted(expected,key=encoded):raise ValueError('Independent original graph bag differs: '+encoded({'case':original['id'],'actualTaggedRows':rows,'expectedOriginalGraphRows':expected,'originalColumns':artifact['columns'],'originalTypeGraph':artifact['logicalPlan']['typeGraph']}))
                    queries.append({'case':case,'result':result,'exact_text_rows':rows,'independent_expected_text':expected})
                    prefix=pack+'-'+original['id'];pending[prefix+'-request.json']=encoded(request)+'\n';pending[prefix+'-artifact.json']=encoded(artifact)+'\n'
                if {q['case']['original_scenario']['id']for q in queries}!=set(CASES[pack]):raise ValueError('Complete named native field corpus required')
                # Separately authored control: incorrect schema selection must
                # refuse on the original source rows before executing user SQL.
                request=compiler_request(pack,queries[0]['case']['original_scenario']['sql'],opened.model,opened.graph,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases)
                request=opt_in(request,pack+'-'+CASES[pack][0])
                binding=json.loads(request['target']['bindingJson'])
                for record in binding['records']:record['schemaRevision']='wrong-original-schema-revision'
                request['target']['bindingJson']=encoded(binding);request['target']['bindingSha256']=hashlib.sha256(encoded(binding).encode()).hexdigest()
                provider.expected_binding=request['target']['bindingJson'];artifact=compile_original(compiler,request,COMPILER_SHA)
                try:execute_guarded(provider,request,artifact,context=context,public_source=lambda r,a,c:admit_public_source(source_guard,umf,r,a,c),positioned_outputs=any(o['id']=='weft.output.positioned'for o in artifact['obligations']),native_null=True)
                except NativeGuardRefusal as refused:
                    if refused.evidence['obligation']!='ashlar.candidate.scalarIntegrity':raise
                    controls.append({'name':'wrong-schema-revision','actual':refused.evidence})
                else:raise ValueError('Wrong schema revision failed to refuse')
                pending[pack+'-wrong-schema-request.json']=encoded(request)+'\n';pending[pack+'-wrong-schema-artifact.json']=encoded(artifact)+'\n'
                correct=json.loads(pending[pack+'-'+CASES[pack][0]+'-request.json'])['target']['bindingJson']
                provider.expected_binding=correct
                with provider.interval(context):
                    resolved=provider.resolve(context);provider.admit_binding(json.loads(correct),resolved,context);provider.runtime(context)
                final_custody=provider.closed_interval_custody(context)
                if opened.native_files()!=opened.original_native_files:raise ValueError('Read-only Weft consumer changed native files')
                reports.append({'pack':pack,'manifest':opened.manifest,'table_registry':opened.original_report['table_registry'],
                    'source_publication_path':str(Path(publications[pack])),
                    'source_publication_report_sha256':hashlib.sha256((Path(publications[pack])/'report.json').read_bytes()).hexdigest(),
                    'alias_closed_interval_custody':alias_custody,
                    'final_closed_interval_custody':final_custody,
                    'original_native_files':opened.original_native_files,'queries':queries,'controls':controls})
        completed=True
    finally:
        if not completed:spark.stop()
    if not completed:raise ValueError('Suppressed reader failure')
    report={'format':'ashlar-original-pack-weft-null-reads/0.1','qualification':__doc__,
        'compiler_sha256':COMPILER_SHA,'compiler_base_revision':'1a1c0ad2c26d0cdd7aa6fb7f415abbaa44ab2842','official_weft_revision':'1a8a3445302aa44a93167ef340dbcfb306805254','compiler_scope':'Exact reviewed nullable source landed upstream; immutable original221 compiler bytes retained',
        'runtime':'Spark4.0.1/Delta4.0.0','elapsed_before_cleanup_seconds':time.monotonic()-started,
        'reports':reports,'slice_original_queries':sum(len(v)for v in CASES.values())}
    pending['report.json']=encoded(report)+'\n';persist_after_stop(spark,pending,output)
    return report


if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('archaeology','ecology','output','jars','compiler','source-guard','umf'):p.add_argument('--'+name,required=True)
    a=p.parse_args();r=run({'archaeology':a.archaeology,'ecology':a.ecology},a.output,a.jars,a.compiler,a.source_guard,a.umf)
    print('Completed original Weft field queries:',sum(len(x['queries'])for x in r['reports']))
