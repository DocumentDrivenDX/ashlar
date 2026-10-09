"""Tiny unpublished Delta/compiler kernel controls, never publication qualification.

All table UUIDs/versions come from newly authored private Delta tables. Compiler
SQL and checks execute unchanged; no protected ACK, Truss acceptance, canonical
manifest authority or source publication is claimed for these kernel controls.
"""
import hashlib,importlib.metadata,json,subprocess,time
from pathlib import Path
from prepare_native_null_controls import fixture_model,controls,request
from local_delta_custody import encoded
from run_commerce_arithmetic_weft import compile_original,decode_rows,persist_after_stop
from run_pack_null_weft import COMPILER_SHA


def run(output,jars,compiler,umf):
    from pyspark.sql import SparkSession
    from run_local_weft_typed_spark4 import JAR_SHA
    output=Path(output);jars=Path(jars);compiler=Path(compiler)
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Exact existing Spark4.0.1 runtime required')
    if output.exists()or hashlib.sha256(compiler.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Fresh output and exact immutable compiler required')
    public_request=encoded({'model':fixture_model(),'controls':controls()})+'\n'
    # Actual public semantics precede Spark acquisition; preserve source-invalid
    # controls as invalid rather than silently relabeling them representation loss.
    import tempfile
    script=Path(__file__).with_name('check_native_null_control_source.ts')
    with tempfile.TemporaryDirectory(prefix='ashlar-null-public-')as tmp:
        tmp=Path(tmp);(tmp/'request.json').write_text(public_request)
        subprocess.run(['bun',str(script),str(umf),str(tmp/'request.json'),str(tmp/'receipt.json')],check=True,capture_output=True,text=True)
        public_receipt=(tmp/'receipt.json').read_text()
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    output.mkdir(mode=0o700)
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar unpublished native null controls').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true').config('spark.sql.decimalOperations.allowPrecisionLoss','false').config('spark.sql.session.timeZone','UTC').config('spark.jars',','.join(map(str,paths))).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');pending={'public-request.json':public_request,'public-receipt.json':public_receipt};results=[];completed=False
    try:
        spark.sql('CREATE DATABASE null_kernel').collect()
        def materialize(name,rows):
            path=output/'native'/name
            frame=spark.createDataFrame(rows,'source_system STRING,type_id LONG,schema_revision STRING,props_json STRING')
            frame.write.format('delta').mode('error').save(str(path))
            alias='spark_catalog.null_kernel.'+name
            spark.sql('CREATE TABLE '+alias+' USING DELTA LOCATION '+"'"+str(path)+"'").collect()
            detail=spark.sql('DESCRIBE DETAIL '+alias).collect()[0];history=spark.sql('DESCRIBE HISTORY '+alias+' LIMIT 1').collect()[0]
            return {'name':alias.split('.'),'uuid':detail['id'],'version':history['version']}
        empty=[materialize(name,[])for name in ('edge_current','tombstone','whole_source_history','manifest')]
        def native_files():
            return {str(p.relative_to(output/'native')):hashlib.sha256(p.read_bytes()).hexdigest() for p in sorted((output/'native').rglob('*')) if p.is_file()}
        model=fixture_model();sha=hashlib.sha256(encoded(model).encode()).hexdigest()
        fields=[e for e in model['modules'][0]['elements']if e['kind']=='field']
        for case in controls():
            props=json.loads(case['originalPropsJson']);physical={str(index):props[field['id']]for index,field in enumerate(fields,1)if field['id']in props}
            table=materialize(case['name'].replace('-','_'),[('private-null-kernel',1,sha,encoded(physical))])
            opening_files=native_files()
            req=request(model,case['sql'],[table,*empty[:3]],empty[3]['uuid']);artifact=compile_original(compiler,req,COMPILER_SHA)
            params={'p'+str(p['position']):p['value']for p in artifact['parameters']};checks=[];refused=None
            for obligation in artifact['obligations']:
                if obligation['id']=='ashlar.candidate.publication':continue
                if obligation['id']not in ('ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact'):raise ValueError('Unknown kernel obligation')
                for check in obligation['parameters']['checks']:
                    if check.get('publicSourceOnly')is True:raise ValueError('Unbounded source guard not in finite kernel fixture')
                    rows=[r.asDict()for r in spark.sql(check['sql'],args=params).collect()]
                    checks.append({'obligation':obligation['id'],'check':check,'rows':rows})
                    if rows!=[{'violations':'0'}]:
                        if len(rows)!=1 or set(rows[0])!={'violations'}or not rows[0]['violations'].isdigit()or int(rows[0]['violations'])<=0:raise ValueError('Malformed kernel count')
                        refused='capability'if check.get('representabilityOnly')is True else'source-invalid';break
                if refused:break
            expected=case['expectedDisposition'];rows=None;decoded=None
            if expected in ('capability','source-invalid'):
                if refused!=expected:raise ValueError('Native failure classification differs: '+case['name'])
            else:
                if refused:raise ValueError('Valid control unexpectedly refused')
                rows=[r.asDict()for r in spark.sql(artifact['sql'],args=params).collect()];decoded=decode_rows(artifact,rows,native_null=True)
                if expected=='pass-empty'and rows:raise ValueError('Native null/null equality matched')
                if case['name']=='zero-false-empty'and decoded!=[['original-control',{'state':'value','value':''},{'state':'value','value':'0.00'},{'state':'value','value':False},'0']]:raise ValueError('Exact zero/false/empty states differ')
                if case['name']=='explicit-null'and decoded!=[['original-control',{'state':'null'},{'state':'null'},{'state':'null'}]]:raise ValueError('Explicit native null states differ')
            closing_files=native_files()
            if closing_files != opening_files:raise ValueError('Kernel queries changed sealed native Delta files')
            for pinned in [table,*empty]:
                alias='.'.join(pinned['name']);detail=spark.sql('DESCRIBE DETAIL '+alias).collect()[0];history=spark.sql('DESCRIBE HISTORY '+alias+' LIMIT 1').collect()[0]
                if detail['id']!=pinned['uuid']or history['version']!=pinned['version']:raise ValueError('Kernel native snapshot identity changed')
            results.append({'openingNativeFiles':opening_files,'closingNativeFiles':closing_files,'case':case,'actualFixtureTableVector':[table,*empty[:3]],'actualFixtureManifestTable':empty[3],'originalPhysicalPropsJson':encoded(physical),'checks':checks,'actualDisposition':refused or expected,'rows':rows,'decoded':decoded,'userSqlExecuted':rows is not None,'qualification':'Unpublished native kernel only; no authority obligation fulfilled or result publication claim'})
            pending[case['name']+'-request.json']=encoded(req)+'\n';pending[case['name']+'-artifact.json']=encoded(artifact)+'\n'
        completed=True
    finally:
        if not completed:spark.stop()
    if not completed:raise ValueError('Suppressed native kernel failure')
    report={'format':'ashlar-unpublished-native-null-controls/0.1','qualification':__doc__,'compiler_sha256':COMPILER_SHA,'runtime':'Spark4.0.1/Delta4.0.0','controls':results}
    pending['report.json']=encoded(report)+'\n';persist_after_stop(spark,pending,output);return report

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('output','jars','compiler','umf'):p.add_argument('--'+name,required=True)
    a=p.parse_args();run(a.output,a.jars,a.compiler,a.umf)
