"""Unpublished exact String DISTINCT kernel; publication obligation unfulfilled.

Uses separately authored original String rows and newly observed native UUIDs.
No original pack, canonical publication, manifest authority, ACK or result release
qualification is borrowed. Native compiler SQL/checks execute unchanged.
"""
import hashlib,importlib.metadata,json,subprocess,tempfile
from pathlib import Path
from local_delta_custody import encoded
from prepare_native_distinct_controls import fixture_model,rows,controls,request
# Corrected DISTINCT projected-order compiler; canonical historical runner keeps its pin.
COMPILER_SHA='b91bfc411a7124916de8f256a67828d70cd2cc2f655b11aafd9024aa221b738c'
from run_commerce_arithmetic_weft import compile_original,decode_rows,persist_after_stop
from weft_field_plan import admit_field_plan


def decode_native_tuple(artifact,schema,ordered):
    """Exact native positions and String types; dictionaries only for unique names."""
    columns=artifact['columns'];positioned=any(o['id']=='weft.output.positioned'for o in artifact['obligations'])
    expected=[[c.get('carrierName',c['outputName']),'STRING']for c in columns]
    if schema!=expected or len({name for name,_ in schema})!=len(schema):raise ValueError('Exact unique native String carrier schema required')
    if any(type(row)is not list or len(row)!=len(columns)for row in ordered):raise ValueError('Exact complete native tuple rows required')
    if positioned:return decode_rows(artifact,ordered,positioned=True,native_schema=schema,ordered_rows=ordered)
    return decode_rows(artifact,[dict(zip((name for name,_ in schema),row))for row in ordered])


def run(output,jars,compiler,umf):
    from pyspark.sql import SparkSession
    from run_local_weft_typed_spark4 import JAR_SHA
    output=Path(output);jars=Path(jars);compiler=Path(compiler)
    if output.exists()or hashlib.sha256(compiler.read_bytes()).hexdigest()!=COMPILER_SHA:raise ValueError('Fresh output and immutable compiler required')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Exact existing Spark4.0.1 required')
    public_request=encoded({'model':fixture_model(),'rows':rows()})+'\n'
    with tempfile.TemporaryDirectory(prefix='ashlar-distinct-public-')as tmp:
        tmp=Path(tmp);(tmp/'request.json').write_text(public_request)
        subprocess.run(['bun',str(Path(__file__).with_name('check_native_distinct_control_source.ts')),str(umf),str(tmp/'request.json'),str(tmp/'receipt.json')],check=True,capture_output=True,text=True)
        public_receipt=(tmp/'receipt.json').read_text()
    paths=[jars/n for n in ('delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar')]
    if any(hashlib.sha256(p.read_bytes()).hexdigest()!=JAR_SHA[p.name]for p in paths):raise ValueError('Exact existing Delta4 jars required')
    output.mkdir(mode=0o700)
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar unpublished DISTINCT controls')
      .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
      .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true').config('spark.sql.session.timeZone','UTC')
      .config('spark.sql.decimalOperations.allowPrecisionLoss','false').config('spark.jars',','.join(map(str,paths)))
      .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
      .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR');pending={'public-request.json':public_request,'public-receipt.json':public_receipt};results=[];completed=False
    try:
        spark.sql('CREATE DATABASE distinct_kernel').collect()
        model=fixture_model();sha=hashlib.sha256(encoded(model).encode()).hexdigest()
        def materialize(name,source_rows):
            path=output/'native'/name
            spark.createDataFrame(source_rows,'source_system STRING,type_id LONG,schema_revision STRING,props_json STRING').write.format('delta').mode('error').save(str(path))
            alias='spark_catalog.distinct_kernel.'+name;spark.sql('CREATE TABLE '+alias+' USING DELTA LOCATION '+"'"+str(path)+"'").collect()
            detail=spark.sql('DESCRIBE DETAIL '+alias).collect()[0];history=spark.sql('DESCRIBE HISTORY '+alias+' LIMIT 1').collect()[0]
            return {'name':alias.split('.'),'uuid':detail['id'],'version':history['version']}
        physical=[('private-distinct-kernel',1,sha,encoded({'1':r['id'],'2':r['value']}))for r in rows()]
        table=materialize('object_current',physical);empty=[materialize(n,[])for n in ('edge_current','tombstone','whole_source_history','manifest')]
        vector=[table,*empty[:3]]
        def files():return {str(p.relative_to(output/'native')):hashlib.sha256(p.read_bytes()).hexdigest()for p in sorted((output/'native').rglob('*'))if p.is_file()}
        opening=files()
        for case in controls():
            req=request(case['sql'],vector,empty[3]['uuid']);artifact=compile_original(compiler,req,COMPILER_SHA)
            admit_field_plan(artifact,json.loads(req['target']['bindingJson']),req['modules'],distinct=True)
            params={'p'+str(p['position']):p['value']for p in artifact['parameters']};checks=[]
            obligations={o['id']:o for o in artifact['obligations']}
            expected={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact'}|({'weft.output.positioned'}if any('carrierName'in c for c in artifact['columns'])else set())
            if len(obligations)!=len(artifact['obligations'])or set(obligations)!=expected:raise ValueError('Unknown kernel obligations')
            for oid in ('ashlar.candidate.scalarIntegrity','ashlar.arithmetic.exact'):
                for check in obligations[oid]['parameters']['checks']:
                    if check.get('publicSourceOnly')is True:raise ValueError('No unbounded scalar fixture')
                    actual=[r.asDict()for r in spark.sql(check['sql'],args=params).collect()]
                    if actual!=[{'violations':'0'}]:raise ValueError('Original source/native guard refused')
                    checks.append({'obligation':oid,'check':check,'rows':actual})
            frame=spark.sql(artifact['sql'],args=params);ordered=[list(r)for r in frame.collect()];schema=[[f.name,f.dataType.simpleString().upper()]for f in frame.schema.fields]
            decoded=decode_native_tuple(artifact,schema,ordered)
            if sorted(map(encoded,decoded))!=sorted(map(encoded,case['expected']))or(case.get('ordered')and decoded!=case['expected']):raise ValueError('Independent original String tuple/order differs')
            if files()!=opening:raise ValueError('Kernel queries changed native files')
            for pinned in [*vector,empty[3]]:
                alias='.'.join(pinned['name']);detail=spark.sql('DESCRIBE DETAIL '+alias).collect()[0];history=spark.sql('DESCRIBE HISTORY '+alias+' LIMIT 1').collect()[0]
                if detail['id']!=pinned['uuid']or history['version']!=pinned['version']:raise ValueError('Actual native pin changed')
            results.append({'case':case,'checks':checks,'native_schema':schema,'native_ordered_rows':ordered,'exact_decoded':decoded,'actualTableVector':vector,'actualManifestTable':empty[3],'publicationObligationFulfilled':False})
            pending[case['name']+'-request.json']=encoded(req)+'\n';pending[case['name']+'-artifact.json']=encoded(artifact)+'\n'
        report={'format':'ashlar-unpublished-distinct-kernel/0.1','qualification':__doc__,'compiler_sha256':COMPILER_SHA,'runtime':'Spark4.0.1/Delta4.0.0','originalRows':rows(),'physicalRows':physical,'openingNativeFiles':opening,'closingNativeFiles':files(),'controls':results}
        completed=True
    finally:
        if not completed:spark.stop()
    if not completed:raise ValueError('Suppressed kernel failure')
    pending['report.json']=encoded(report)+'\n';persist_after_stop(spark,pending,output);return report

if __name__=='__main__':
    import argparse
    p=argparse.ArgumentParser(description=__doc__)
    for name in ('output','jars','compiler','umf'):p.add_argument('--'+name,required=True)
    a=p.parse_args();run(a.output,a.jars,a.compiler,a.umf)
