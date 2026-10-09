"""Bounded unchanged Weft typed SQL compatibility pass on local Spark4/Delta4.

Independent experimental execution profile. Qualified Databricks native-profile
and immutable-publication obligations remain refused, so no result publication.
"""
import argparse
import hashlib
import importlib.metadata
import json
from pathlib import Path
import sys
from run_local_weft_typed_probe import FIXTURE,UMF_PIN,WEFT_PIN,EXTENSION_SHA,_pin,_sha,_save,_public_records,compile_request

JAR_SHA={'delta-spark_2.13-4.0.0.jar':'538511702aae0ef6973a6a70af3d4543c9009f8edbed786a00737e2d3cd7f04e','delta-storage-4.0.0.jar':'9bdb9fb450f1e119eba53feb427f331b0d09072d26485b8273883ad72c9a2e1d'}

EXPECTED={
 'select':[{'quantity':'2','amount':'40.00','active':'false','label':'second'},{'quantity':'9007199254740993','amount':'12.50','active':'true','label':'雪'}],
 'filter':[{'quantity':'9007199254740993','amount':'12.50'}],
 'group-count':[{'active':'false','n':'1'},{'active':'true','n':'1'}],
 'join':[{'left_quantity':'2','right_quantity':'2'},{'left_quantity':'9007199254740993','right_quantity':'9007199254740993'}],
 'count':[{'n':'2'}],
}

def run(umf,weft_source,wheel,jars,output):
    if output.exists():raise ValueError('Fresh local output required')
    _pin(umf,UMF_PIN);_pin(weft_source,WEFT_PIN)
    if _sha((wheel/'weft/weft.abi3.so').read_bytes())!=EXTENSION_SHA:raise ValueError('Pinned compiler bytes differ')
    if importlib.metadata.version('pyspark')!='4.0.1':raise ValueError('Explicit Spark4.0.1 compatibility profile required')
    paths=[jars/n for n in ['delta-spark_2.13-4.0.0.jar','delta-storage-4.0.0.jar']]
    if not all(p.is_file() and _sha(p.read_bytes())==JAR_SHA[p.name] for p in paths):raise ValueError('Exact matching local Delta4 jar bytes required')
    output.mkdir(parents=True)
    jar_sha={p.name:_sha(p.read_bytes()) for p in paths}
    model=(FIXTURE/'model.umf.json').read_bytes();rows=json.loads((FIXTURE/'rows.json').read_bytes())
    receipt=_public_records(umf,output,model,rows)
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar local typed Weft Spark4 screening')
        .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
        .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true')
        .config('spark.jars',','.join(str(p) for p in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        name='spark_catalog.default.object_current'
        spark.sql(f"CREATE TABLE {name} (source_system STRING, type_id BIGINT, id BIGINT, schema_revision STRING, props_json STRING) USING DELTA LOCATION '{(output/'object_current').as_posix()}'")
        # One bounded whole-table load; IDs are explicit local development IDs.
        data=[('independent-weft-typed',27,int(r['id']),'local-1',r['props_json']) for r in rows]
        spark.createDataFrame(data,'source_system string,type_id long,id long,schema_revision string,props_json string').write.format('delta').mode('append').saveAsTable(name)
        detail=spark.sql('DESCRIBE DETAIL '+name).first().asDict();version=int(spark.sql('DESCRIBE HISTORY '+name).first()['version'])
        table={'name':name.split('.'),'uuid':detail['id'],'version':version}
        _save(output/'local-table.json',{'table':table,'location':detail['location'],'manifest_authority':False,'manifest_locator_kind':'actual-local-object-table-UUID reused only for compiler-shape probe'})
        observed=[{'id':str(r['id']),'props_json':r['props_json']} for r in spark.sql('SELECT id,props_json FROM '+name+' ORDER BY id').collect()]
        if observed!=rows:raise ValueError('Original local source props differ')
        sys.path.insert(0,str(wheel));import weft
        probes=[]
        for query in json.loads((FIXTURE/'queries.json').read_bytes()):
            request=compile_request(query['sql'],model,table);artifact=json.loads(weft.compile_json(json.dumps(request)))
            _save(output/(query['name']+'-request.json'),request);_save(output/(query['name']+'-artifact.json'),artifact)
            if artifact['status']!='compiled' or artifact['bindingSha256']!=request['target']['bindingSha256'] or artifact['modelPins']!=[request['modules'][0]['pin']]:raise ValueError('Original typed compiler artifact refused/differs')
            ids={o['id'] for o in artifact['obligations']}
            known={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.nativeProfile'}
            if ids!=known:raise ValueError('Exact known public obligation inventory required')
            params={'p'+str(p['position']):p['value'] for p in artifact['parameters']}
            checks=[]
            for obligation in artifact['obligations']:
                if obligation['id']!='ashlar.candidate.scalarIntegrity':continue
                for check in obligation['parameters']['checks']:
                    actual=[r.asDict() for r in spark.sql(check['sql'],args=params).collect()]
                    if actual!=[{'violations':'0'}]:raise ValueError('Original compiler scalar-integrity check failed')
                    checks.append({'check':check,'rows':actual})
            actual=[r.asDict() for r in spark.sql(artifact['sql'],args=params).collect()]
            if actual!=EXPECTED[query['name']]:raise ValueError('Actual unchanged typed SQL differs from independently authored expected values: '+repr(actual))
            current=spark.sql('DESCRIBE DETAIL '+name).first().asDict()
            current_version=int(spark.sql('DESCRIBE HISTORY '+name).first()['version'])
            if current['id']!=table['uuid'] or current_version!=table['version']:raise ValueError('Local context changed during checks/query')
            probes.append({'name':query['name'],'state':'passed-unchanged-local-sql','integrity':checks,'rows':actual,'profile_admitted':False,'publication_guard':'refused: local table has no immutable native manifest/authorization/pin authority','native_profile_guard':'refused: independent Spark4.0.1 is not dbsql2026.39-qualified','published':False})
        refusals=[]
        for field,corrupt in [('quantity','{"31":9223372036854775808,"32":12.50,"33":true,"34":"雪"}'),
                              ('amount','{"31":1,"32":12.501,"33":true,"34":"雪"}'),
                              ('active','{"31":1,"32":12.50,"33":"true","34":"雪"}')]:
            spark.sql('UPDATE '+name+' SET props_json=:props WHERE id=1',args={'props':corrupt})
            corrupt_table=dict(table,version=int(spark.sql('DESCRIBE HISTORY '+name).first()['version']))
            request=compile_request('SELECT i.quantity, i.amount, i.active, i.label FROM Item i',model,corrupt_table)
            artifact=json.loads(weft.compile_json(json.dumps(request)))
            if artifact['status']!='compiled':raise ValueError('Original invalid-data control did not compile')
            params={'p'+str(p['position']):p['value'] for p in artifact['parameters']}
            observations=[]
            for obligation in artifact['obligations']:
                if obligation['id']=='ashlar.candidate.scalarIntegrity':
                    for check in obligation['parameters']['checks']:
                        result=[r.asDict() for r in spark.sql(check['sql'],args=params).collect()]
                        expected='1' if check['field']['element']==field else '0'
                        if result!=[{'violations':expected}]:raise ValueError('Actual original integrity control differs')
                        observations.append({'check':check,'rows':result})
            if len(observations)!=4:raise ValueError('Every original typed field control required')
            _save(output/('refusal-'+field+'-request.json'),request)
            _save(output/('refusal-'+field+'-artifact.json'),artifact)
            refusals.append({'field':field,'props_json':corrupt,'table':corrupt_table,'checks':observations,'user_sql_executed':False})
        # All successful queries consumed the original exact local version;
        # later negative controls do not alter that retained Delta snapshot.
        retained=[{'id':str(r['id']),'props_json':r['props_json']} for r in spark.sql('SELECT id,props_json FROM '+name+' VERSION AS OF '+str(table['version'])+' ORDER BY id').collect()]
        if retained!=rows:raise ValueError('Original local snapshot changed after refusal controls')
        _pin(umf,UMF_PIN);_pin(weft_source,WEFT_PIN)
        report={'format':'ashlar-local-weft-typed-spark4/0.1','state':'passed-local-unchanged-typed-sql','engine_profile':'experimental-spark4.0.1-delta4.0.0','engine_version':spark.version,'jar_sha256':jar_sha,'compiler_revision':WEFT_PIN,'extension_sha256':EXTENSION_SHA,'umf_revision':UMF_PIN,'source_sha256':_sha(model),'table':table,'public_record_passes':len(receipt['records']),'public_field_passes':sum(len(r['result']['fields']) for r in receipt['records']),'original_props_roundtrip':True,'retained_snapshot_after_refusals':True,'refusals':refusals,'probes':probes,'published':False,'acknowledged':False,'qualification':'Actual original Weft typed SQL and every emitted scalar-integrity statement on two local Delta rows, original public UMF upgrade/field checks and exact values. Independent experimental engine screening only; no Databricks native-profile or publication/runtime authorization claim.'}
        _save(output/'summary.json',report);return report
    finally:spark.stop()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for flag in ['umf-source','weft-source','wheel','jars','output']:p.add_argument('--'+flag,type=Path,required=True)
    a=p.parse_args();print(json.dumps(run(a.umf_source,a.weft_source,a.wheel,a.jars,a.output),ensure_ascii=False,indent=2))
