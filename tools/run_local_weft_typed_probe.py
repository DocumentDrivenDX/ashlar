"""Actual public typed compiler/UMF pass plus local Delta compatibility screening.

Compiler artifacts remain Databricks-specific. Screening never authorizes a
publication: the native-profile obligation is deliberately unavailable locally.
"""
import argparse
import base64
import hashlib
import importlib.metadata
import json
from pathlib import Path
import subprocess
import sys

ROOT=Path(__file__).resolve().parents[1]
FIXTURE=ROOT/'examples/end-to-end/weft-typed'
WEFT_PIN='2744531735c2a771fbe7ed24a7f67e3afc851b25'
UMF_PIN='c45c72a2a8a3c4fba61c40c5927dd9091acf8cc3'
EXTENSION_SHA='4eaadc7b9512c904665b9f554296c2a0ef03ad08d2587748b1e6292ae9a34004'
LAYOUT_SHA='ad4a264508c971aefcd94e3ae90f8f74dcf119b7d767f6060c638f4abde3284e'
FIELDS={'31':('typed','quantity','integer'),'32':('typed','amount','decimal'),'33':('typed','active','boolean'),'34':('typed','label','string')}
JAR_SHA={'io.delta_delta-spark_2.12-3.2.1.jar':'088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499','io.delta_delta-storage-3.2.1.jar':'4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7'}
JARS=tuple(JAR_SHA)

def _sha(data):return hashlib.sha256(data).hexdigest()
def _save(path,value):path.write_text(json.dumps(value,ensure_ascii=False,indent=2)+'\n')
def _pin(root,pin):
    for args,expected in [(['rev-parse','HEAD'],pin),(['status','--porcelain'],'')]:
        if subprocess.check_output(['git','-C',str(root),*args],text=True).strip()!=expected:raise ValueError('Exact clean public source required')

def compile_request(sql,model_bytes,table):
    """Compiler-only binding using original local Delta identities; no authority."""
    model=json.loads(model_bytes);pin={'documentId':model['id'],'revision':'local-1','umfVersion':'0.7.0','sha256':_sha(model_bytes)}
    logical=lambda element:{'documentId':model['id'],'revision':'local-1','module':'typed','element':element}
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3','layoutSha256':LAYOUT_SHA,'modelPins':[pin],
        'publication':{'id':'local-compatibility-screening-only','manifestUuid':table['uuid'],'tables':[table]},
        'records':[{'logical':logical('item'),'table':0,'kind':'object','sourceSystem':'independent-weft-typed','typeId':'27','schemaRevision':'local-1',
                    'properties':[{'logical':logical(ref[1]),'home':{'kind':'props','propertyId':pid}} for pid,ref in FIELDS.items()]}]}
    # The compiler requires a UUID-shaped manifest locator. This is explicitly
    # the observed local object-table UUID, not a fabricated publication UUID.
    text=json.dumps(binding,ensure_ascii=False,separators=(',',':'),sort_keys=True)
    return {'interfaceVersion':'weft-compile/0.2.0','dialect':'weft-sql/0.2.0','sql':sql,
        'modules':[{'documentJson':model_bytes.decode(),'pin':pin,'selectedModuleIds':['typed']}],
        'target':{'backendId':'ashlar.databricks','backendVersion':'0.1.0-qualified','targetProfile':'dbsql2026.39-qualified','bindingJson':text,'bindingSha256':_sha(text.encode())},'options':{'allowCandidate':False}}

def _public_records(umf,output,model_bytes,rows):
    from ashlar.typed_source_policy import typed_values
    request={'identity':{'module':'typed','element':'item'},'records':[{'deliveryId':'local-'+r['id'],'recordSha256':_sha(r['props_json'].encode()),'values':typed_values(r['props_json'],FIELDS)} for r in rows]}
    _save(output/'umf-request.json',request)
    process=subprocess.run(['bun',str(ROOT/'tools/check_umf_record_values.ts'),str(umf),UMF_PIN,str(FIXTURE/'model.umf.json'),str(output/'umf-request.json')],capture_output=True,check=True)
    (output/'umf-receipt.json').write_bytes(process.stdout)
    receipt=json.loads(process.stdout)
    if base64.b64decode(receipt['sourceBase64'],validate=True)!=model_bytes or receipt['sourceSha256']!=_sha(model_bytes) or receipt['requestSha256']!=_sha((output/'umf-request.json').read_bytes()):raise ValueError('Original UMF custody differs')
    if receipt['upgrade']['source']!=json.loads(model_bytes) or receipt['upgrade']['source']['umf']!='0.7.0' or receipt['upgrade']['target']['umf']!='0.8.0' or receipt['upgrade']['residuals']:raise ValueError('Exact public upgrade required without field meaning loss')
    if len(receipt['records'])!=len(rows):raise ValueError('Complete original public checks required')
    members=receipt['upgrade']['target']['modules'][0]['elements'][0]['members']
    for wanted,actual in zip(request['records'],receipt['records']):
        result=actual['result'];validation=result['validation']
        if actual['deliveryId']!=wanted['deliveryId'] or actual['recordSha256']!=wanted['recordSha256'] or result['values']!=wanted['values'] or result['source']!=receipt['upgrade']['target']:raise ValueError('Original record receipt differs')
        if validation['valid'] is not True or validation['complete'] is not True or validation['diagnostics']!=[]:raise ValueError('Public typed Record refusal')
        if len(result['fields'])!=len(members):raise ValueError('Incomplete member checks')
        for member,field in zip(members,result['fields']):
            if field['field']!=member or field['state']!='present' or field['validation']['valid'] is not True or field['validation']['complete'] is not True or field['validation']['diagnostics']!=[]:raise ValueError('Original public field check required')
    return receipt

def run(umf,weft_source,wheel,jars,output):
    if output.exists():raise ValueError('Fresh local output required')
    _pin(umf,UMF_PIN);_pin(weft_source,WEFT_PIN)
    extension=wheel/'weft/weft.abi3.so'
    if _sha(extension.read_bytes())!=EXTENSION_SHA:raise ValueError('Pinned compiler extension differs')
    if importlib.metadata.version('pyspark')!='3.5.3' or importlib.metadata.version('delta-spark')!='3.2.1':raise ValueError('Explicit Spark3.5.3/Delta3.2.1 screening profile required')
    paths=[jars/n for n in JARS]
    if not all(p.is_file() and _sha(p.read_bytes())==JAR_SHA[p.name] for p in paths):raise ValueError('Exact existing local Delta jar bytes required')
    output.mkdir(parents=True);model=(FIXTURE/'model.umf.json').read_bytes();rows=json.loads((FIXTURE/'rows.json').read_bytes())
    receipt=_public_records(umf,output,model,rows)
    from pyspark.sql import SparkSession
    spark=(SparkSession.builder.master('local[1]').appName('Ashlar bounded typed Weft probe')
        .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
        .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.ansi.enabled','true')
        .config('spark.jars',','.join(str(p) for p in paths)).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
        .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
        .config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    probes=[]
    try:
        name='spark_catalog.default.object_current'
        spark.sql(f"CREATE TABLE {name} (source_system STRING, type_id BIGINT, id BIGINT, schema_revision STRING, props_json STRING) USING DELTA LOCATION '{(output/'object_current').as_posix()}'")
        for row in rows:
            spark.sql(f'INSERT INTO {name} VALUES (:source, 27, :id, :revision, :props)',args={'source':'independent-weft-typed','id':int(row['id']),'revision':'local-1','props':row['props_json']})
        detail=spark.sql('DESCRIBE DETAIL '+name).first().asDict()
        version=int(spark.sql('DESCRIBE HISTORY '+name).first()['version'])
        table={'name':name.split('.'),'uuid':detail['id'],'version':version}
        _save(output/'local-table.json',{'table':table,'location':detail['location'],'manifest_authority':False,'manifest_locator_kind':'actual-local-object-table-UUID reused only for compiler-shape probe'})
        observed=[{'id':str(r['id']),'props_json':r['props_json']} for r in spark.sql('SELECT id,props_json FROM '+name+' ORDER BY id').collect()]
        if observed!=rows:raise ValueError('Original native local props roundtrip differs')
        sys.path.insert(0,str(wheel));import weft
        for query in json.loads((FIXTURE/'queries.json').read_bytes()):
            request=compile_request(query['sql'],model,table);artifact=json.loads(weft.compile_json(json.dumps(request)))
            _save(output/(query['name']+'-request.json'),request);_save(output/(query['name']+'-artifact.json'),artifact)
            item={'name':query['name'],'status':artifact['status'],'published':False,'profile_admitted':False}
            if artifact['status']=='compiled':
                if artifact['bindingSha256']!=request['target']['bindingSha256'] or artifact['modelPins']!=[request['modules'][0]['pin']]:raise ValueError('Original compiler binding/model custody differs')
                ids={o['id'] for o in artifact['obligations']}
                known={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity','ashlar.nativeProfile'}
                if not ids.issubset(known):raise ValueError('Unknown public obligation: '+str(ids-known))
                item['obligations']=sorted(ids)
                item['publication_guard']='refused: no immutable publication/authorization/pin custody; local table UUID is not a manifest UUID'
                item['native_profile_guard']='refused: Spark3.5.3 is not dbsql2026.39-qualified'
                params={'p'+str(p['position']):p['value'] for p in artifact['parameters']}
                statements=[c['sql'] for o in artifact['obligations'] if o['id']=='ashlar.candidate.scalarIntegrity' for c in o['parameters']['checks']]
                # Deliberate compatibility experiment: never production host
                # admission. Original SQL is executed unchanged, no shim/rewrite.
                screening=[]
                for sql in [*statements,artifact['sql']]:
                    try:result=spark.sql(sql,args=params).collect();screening.append({'state':'executed','rows':[r.asDict() for r in result]})
                    except Exception as error:
                        if type(error).__name__ not in ('ParseException','AnalysisException'):raise
                        screening.append({'state':'unsupported-original-sql','exception':type(error).__name__,'message':str(error)[:8000]})
                item['screening']=screening
            else:item['diagnostics']=artifact['diagnostics']
            probes.append(item)
        _pin(umf,UMF_PIN);_pin(weft_source,WEFT_PIN)
        report={'format':'ashlar-local-weft-typed-probe/0.1','state':'public-typed-pass-local-compatibility-screened','engine_profile':'experimental-local-spark3.5.3-delta3.2.1','jar_sha256':JAR_SHA,'compiler_revision':WEFT_PIN,'extension_sha256':EXTENSION_SHA,'umf_revision':UMF_PIN,'source_sha256':_sha(model),'rows':len(rows),'public_record_passes':len(receipt['records']),'public_field_passes':sum(len(r['result']['fields']) for r in receipt['records']),'local_original_props_roundtrip':True,'table':table,'probes':probes,'published':False,'acknowledged':False,'qualification':'Separately authored core0.7 typed model, public explicit core0.8 upgrade and field/Record checks, actual pinned Weft artifacts, unchanged emitted SQL screening on independent local Spark. No dbsql profile, native publication, accepted IDs or query runtime qualification.'}
        _save(output/'summary.json',report);return report
    finally:spark.stop()

if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__)
    for flag in ['umf-source','weft-source','wheel','jars','output']:p.add_argument('--'+flag,type=Path,required=True)
    a=p.parse_args();print(json.dumps(run(a.umf_source,a.weft_source,a.wheel,a.jars,a.output),ensure_ascii=False,indent=2))
