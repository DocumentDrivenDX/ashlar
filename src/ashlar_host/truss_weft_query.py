"""Original Truss STRING bindings and publication-pinned public Weft queries.

Compiler receipts supply no native/source authority; the live owner supplies it.
"""
import hashlib
import json
import re
import uuid
from contextlib import contextmanager
from ashlar.manifest import manifest_pin_vector,validate_manifest_row
from ashlar.publication import resolve_publication
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar.weft_distribution import DistributionPaths,compile_distribution
from .delta_custody import encoded
from .driver import NativeDriver
from .indexed_query import decode_compile_artifact
from .lifecycle import owned_context, finish
from .truss_feed_sessions import TrussFeedSessions
from .truss_graph_admission import TrussGraphAdmission

BACKEND={'backendId':'ashlar.databricks','backendVersion':'0.1.0-candidate',
    'interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql-candidate'}


def truss_string_compile_request(admission,manifest,table_uuids,manifest_uuid,sql,*,revision,document_id,aliases=None):
    """Bind one selected original accepted document/revision without rewriting it."""
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:
        raise ValueError('Actual selected accepted Truss graph revision required')
    if type(sql)is not str or not sql or len(sql.encode())>65536:
        raise ValueError('Bounded authored Weft SQL required')
    validate_manifest_row(manifest)
    if json.loads(manifest['validation_report_json'])['source_admission']!=admission.metadata():
        raise ValueError('Original published Truss semantic admission differs')
    installation=json.loads(admission.batch.begin)['prior']['installationId']
    if json.loads(manifest['schema_revisions_json']).get(installation)!=revision:
        raise ValueError('Selected physical catalog revision differs from publication')
    selected=admission.revisions[revision]
    if document_id not in selected['models']:raise ValueError('Original accepted document required')
    model,original=selected['models'][document_id]
    source_hash=hashlib.sha256(original).hexdigest()
    pin={'documentId':document_id,'revision':source_hash,'sha256':source_hash,'umfVersion':model['umf']}
    versions=json.loads(manifest['table_versions_json']);names=sorted(versions)
    if set(table_uuids)!=set(names)or len(names)!=4 or {name.split('.')[-1]for name in names}!={'object_current','edge_current','tombstone','whole_source_history'}:
        raise ValueError('Complete original four-role Truss publication required')
    uuid=lambda value:type(value)is str and re.fullmatch(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}',value)is not None
    if not uuid(manifest_uuid)or any(not uuid(value)for value in table_uuids.values())or len(set(table_uuids.values()))!=len(table_uuids):
        raise ValueError('Complete distinct canonical original native UUIDs required')
    if any(len(name.split('.'))!=3 or any(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',part)is None for part in name.split('.'))for name in names):
        raise ValueError('Canonical qualified original table names required')
    aliases={name:name for name in names}if aliases is None else dict(aliases)
    if set(aliases)!=set(names)or len(set(aliases.values()))!=len(names)or any(type(alias)is not str or len(alias.split('.'))!=3 or any(re.fullmatch(r'[A-Za-z_][A-Za-z0-9_]*',part)is None for part in alias.split('.'))for alias in aliases.values()):
        raise ValueError('Complete injective original table alias inventory required')
    tables=[{'name':aliases[name].split('.'),'uuid':table_uuids[name],'version':versions[name]}for name in names]
    objects=next(index for index,name in enumerate(names)if name.split('.')[-1]=='object_current')
    records=[]
    report=json.loads(selected['report'])
    logical=lambda module,element:{'documentId':document_id,'revision':source_hash,'module':module,'element':element}
    for type_id,record in sorted(selected['types'].items()):
        owner=record['owner']
        if owner[0]!=document_id:continue
        properties=[]
        for field in report['properties']:
            if field['typeId']!=type_id:continue
            if field['propertyId']not in record['fields']:raise ValueError('Actual accepted STRING property required')
            properties.append({'logical':logical(field['moduleId'],field['elementId']),
                'home':{'kind':'props','propertyId':field['propertyId']}})
        records.append({'logical':logical(owner[1],owner[2]),'kind':'object','table':objects,
            'sourceSystem':installation,'typeId':type_id,'schemaRevision':revision,'properties':properties})
    if not records:raise ValueError('Selected original STRING record bindings required')
    binding={'profile':'ashlar-databricks-candidate/0.1.0','layoutRevision':'ashlar-delta/0.3',
        'layoutSha256':LAYOUT_SHA256,'modelPins':[pin],
        'publication':{'id':manifest['publication_id'],'manifestUuid':manifest_uuid,'tables':tables},'records':records}
    raw=encoded(binding)
    return {'interfaceVersion':'weft-compile/0.2.0','dialect':'weft-sql/0.2.0','sql':sql,
        'modules':[{'documentJson':original.decode(),'pin':pin,'selectedModuleIds':sorted({record['owner'][1]for record in selected['types'].values()if record['owner'][0]==document_id})}],
        'target':{**{key:BACKEND[key]for key in ('backendId','backendVersion','targetProfile')},
            'bindingJson':raw,'bindingSha256':hashlib.sha256(raw.encode()).hexdigest()},'options':{'allowCandidate':True}}


def _truss_string_query(driver,paths,sql,*,revision,document_id,maximum_rows=None):
    """Compile exact authored singleton/list SQL under the actual publication hold.

    Returns original request/response bytes after closing native pins and writer.
    No generated SQL is executed and outer resources remain caller owned.
    """
    if (type(driver)is not NativeDriver or type(paths)is not DistributionPaths
            or type(driver.source_sessions)is not TrussFeedSessions or driver.ack_sessions is not driver.source_sessions):
        raise PermissionError('Actual Truss publication/compiler owners required')
    if maximum_rows is not None and (type(maximum_rows)is not int or not 1<=maximum_rows<=10000):
        raise ValueError('Explicit bounded query row budget required')
    rows=None;checks=None
    with owned_context(driver.writer(driver.policy.active['request']['stream'],driver.context)):
        manifest=dict(driver.policy.active['manifest'])
        uuids={table:driver.transport.targets[table].uuid for table in json.loads(manifest['table_versions_json'])}
        vector=manifest_pin_vector(manifest,uuids,authority='private-local-process')
        with owned_context(driver.hold(vector,context=driver.context)):
            resolved=resolve_publication(driver,manifest['publication_id'],uuids,context=driver.context,
                supported_profiles=['ashlar-delta/0.3'],supported_revisions={key:[value]for key,value in json.loads(manifest['schema_revisions_json']).items()})
            if dict(resolved.descriptor.raw)!=manifest:raise PermissionError('Original pinned Truss manifest differs')
            with owned_context(_query_aliases(driver,manifest,enabled=maximum_rows is not None))as aliases:
                request=truss_string_compile_request(driver.source_admission,manifest,uuids,
                    driver.transport.targets[driver.tables['manifest']].uuid,sql,revision=revision,document_id=document_id,aliases=aliases)
                request_bytes=(encoded(request)+'\n').encode()
                response=compile_distribution(paths,request_bytes)
                artifact=decode_compile_artifact(response)
                if (artifact.get('status')!='compiled' or artifact.get('backend')!=BACKEND or artifact.get('bindingSha256')!=request['target']['bindingSha256']
                        or artifact.get('modelPins')!=json.loads(request['target']['bindingJson'])['modelPins']):
                    raise ValueError('Original public Weft Truss compiler correspondence differs')
                binding=json.loads(request['target']['bindingJson'])
                obligations=artifact.get('obligations')
                if (type(obligations)is not list or len(obligations)!=2
                        or {item.get('id')for item in obligations}!={'ashlar.candidate.publication','ashlar.candidate.scalarIntegrity'}):
                    raise ValueError('Exact candidate Truss compiler obligation inventory required')
                if any(type(item)is not dict or set(item)!={'id','owner','parameters','failureCode'} or item['owner']!='host' or type(item['failureCode'])is not str or not item['failureCode']or type(item['parameters'])is not dict for item in obligations):
                    raise ValueError('Exact immutable host compiler obligations required')
                publication=next(item['parameters']for item in obligations if item['id']=='ashlar.candidate.publication')
                if any(publication.get(key)!=binding[key]for key in ('publication','modelPins','layoutRevision','layoutSha256')):
                    raise ValueError('Original compiled Truss publication obligation differs')
                integrity=next(item['parameters']for item in obligations if item['id']=='ashlar.candidate.scalarIntegrity')
                if (integrity.get('phase')!='before-user-query' or integrity.get('samePublicationRequired')is not True
                        or integrity.get('noPartialPublication')is not True or type(integrity.get('checks'))is not list):
                    raise ValueError('Original candidate scalar integrity lifecycle differs')
                driver.validate_manifest(manifest,driver.context)
                if maximum_rows is not None:
                    count=False
                    if len(artifact.get('columns',[]))==1 and artifact['columns'][0].get('representation',{}).get('logicalType',{}).get('family')=='integer':
                        admit_truss_global_count(driver.source_admission,artifact,binding,revision=revision,document_id=document_id)
                        count=True
                    rows,checks=_execute_string_artifact(driver.transport.spark,artifact,maximum_rows,count=count)
                    driver.validate_manifest(manifest,driver.context)
    result={'request_bytes':request_bytes,'response_bytes':response,
        'request_sha256':hashlib.sha256(request_bytes).hexdigest(),'response_sha256':hashlib.sha256(response).hexdigest(),
        'publication_id':manifest['publication_id'],'qualification':'Original accepted UMF0.7 STRING record mapping for the selected physical catalog revision; public candidate Weft compilation under native publication pins. No SQL execution or scalar-integrity discharge.'}
    if maximum_rows is not None:
        result.update(rows=rows,integrity=checks,qualification='Original accepted UMF0.7 STRING scalar/global COUNT query through public candidate Weft compiler SQL after all exact integrity checks, under complete native publication pins; outer source/Spark lifetime remains caller owned.')
    return result


@contextmanager
def _query_aliases(driver,manifest,*,enabled):
    if not enabled:
        yield None
        return
    spark=driver.transport.spark
    namespace='ashlar_truss_query_'+uuid.uuid4().hex
    schema='`spark_catalog`.`'+namespace+'`'
    created=[];owned=False;primary=None
    aliases={table:'spark_catalog.'+namespace+'.'+table.split('.')[-1]for table in json.loads(manifest['table_versions_json'])}
    try:
        spark.sql('CREATE DATABASE '+schema).collect();owned=True
        for table,alias in aliases.items():
            name='`'+'`.`'.join(alias.split('.'))+'`'
            target=driver.transport.targets[table]
            location=str(target.path).replace("'","''")
            spark.sql('CREATE TABLE '+name+" USING DELTA LOCATION '"+location+"'").collect();created.append(name)
            detail=spark.sql('DESCRIBE DETAIL '+name).first()
            if detail['id']!=target.uuid:raise PermissionError('Original external alias native UUID differs')
        yield aliases
    except BaseException as error:primary=error
    finish(primary,[*(lambda name=name:spark.sql('DROP TABLE '+name).collect()for name in reversed(created)),
        *([lambda:spark.sql('DROP DATABASE '+schema).collect()]if owned else [])])


def _execute_string_artifact(spark,artifact,maximum_rows,*,count=False):
    from .path_capture import PathCaptureConfig,capture_string_frame
    from ashlar.weft_decode import decode_exact_scalar
    columns=artifact.get('columns')
    if type(columns)is not list or not columns or len(columns)>64:
        raise ValueError('Bounded original compiler output inventory required')
    names=[]
    for position,column in enumerate(columns,1):
        representation=column.get('representation')
        expected={'kind':'scalar','logicalType':{'family':'integer'if count else'string','facets':{},'nullable':False},'carrier':'text','decoder':'exact-integer'if count else'text'}
        if column.get('position')!=position or column.get('nullable')is not False or representation!=expected or type(column.get('outputName'))is not str or not column['outputName']:
            raise ValueError('Only admitted non-null STRING scalar outputs supported')
        names.append(column['outputName'])
    if len(set(names))!=len(names):raise ValueError('Distinct original result output names required')
    params={}
    slots=artifact.get('parameters')
    if type(slots)is not list or len(slots)>256:raise ValueError('Bounded original parameter inventory required')
    for position,slot in enumerate(slots,1):
        if type(slot)is not dict or slot.get('position')!=position or type(slot.get('value'))is not str:
            raise ValueError('Exact emitted ordered STRING parameter slots required')
        params['p'+str(position)]=slot['value']
    integrity=next(item['parameters']for item in artifact['obligations']if item['id']=='ashlar.candidate.scalarIntegrity')
    if len(integrity['checks'])>128:raise ValueError('Bounded complete scalar integrity inventory required')
    checks=[]
    def capture(sql,budget):
        if type(sql)is not str or not sql or len(sql.encode())>1048576 or sql.lstrip().split(None,1)[0].upper()not in ('SELECT','WITH'):
            raise ValueError('Original bounded compiler SELECT or WITH query required')
        return capture_string_frame(spark.sql(sql,args=params),config=PathCaptureConfig(budget,65536,1048576))
    for check in integrity['checks']:
        if (type(check)is not dict or set(check)not in ({'sql','failureCode','record'},{'sql','failureCode','field','record'})
                or check.get('failureCode')not in ('WFT-BINDING','WFT-NUMERIC-DOMAIN')):
            raise ValueError('Unknown compiler scalar integrity check meaning')
        observed=capture(check['sql'],1)
        if len(observed['schema'])!=1 or observed['rows']!=[['0']]:raise ValueError('Original scalar integrity check refused')
        checks.append({'check':check,'rows':observed['rows']})
    observed=capture(artifact['sql'],maximum_rows)
    if observed['schema']!=[[name,'STRING']for name in names]:raise ValueError('Original compiler result schema differs')
    if count and len(observed['rows'])!=1:raise ValueError('One complete global COUNT row required')
    rows=[{name:decode_exact_scalar(columns[index]['representation'],value,count=count).value for index,(name,value)in enumerate(zip(names,row))}for row in observed['rows']]
    return rows,checks


def compile_truss_string_query(driver,paths,sql,*,revision,document_id):
    """Compile original SQL under publication pins without executing its SQL."""
    return _truss_string_query(driver,paths,sql,revision=revision,document_id=document_id)


def execute_truss_string_query(driver,paths,sql,*,revision,document_id,maximum_rows=1000):
    """Execute STRING singleton/list projections or one closed global COUNT inside one full publication hold.

    Original generated SQL is unchanged; owned external catalog aliases select
    original Delta paths, UUIDs and VERSION AS OF snapshots. Optional-state, numeric-field and non-global-count outputs refuse pending
    their separate semantic decoder admission.
    """
    return _truss_string_query(driver,paths,sql,revision=revision,document_id=document_id,maximum_rows=maximum_rows)


def admit_truss_global_count(admission,artifact,binding,*,revision,document_id):
    """Bind public global-count IR to actual accepted Truss record allocation."""
    from ashlar.weft_decode import admit_global_count_ir
    if type(admission)is not TrussGraphAdmission or revision not in admission.revisions:
        raise ValueError('Actual accepted Truss count source required')
    selected=admission.revisions[revision]
    if document_id not in selected['models']:raise ValueError('Original accepted count document required')
    model,raw=selected['models'][document_id];digest=hashlib.sha256(raw).hexdigest()
    pin={'documentId':document_id,'revision':digest,'sha256':digest,'umfVersion':model['umf']}
    identities=[{'documentId':owner[0],'revision':digest,'module':owner[1],'element':owner[2]}
        for item in selected['types'].values()for owner in (item['owner'],)if owner[0]==document_id]
    if binding['modelPins']!=[pin]:raise ValueError('Original accepted count model pin differs')
    identity=admit_global_count_ir(artifact['columns'][0],artifact,model_pin=pin,record_identities=identities)
    records=[record for record in binding['records']if record['logical']==identity]
    installation=json.loads(admission.batch.begin)['prior']['installationId']
    if len(records)!=1:raise ValueError('Exact original count physical record required')
    record=records[0];actual=selected['types'].get(record['typeId'])
    tables=binding['publication']['tables'];index=record.get('table')
    if type(index)is not int or not 0<=index<len(tables)or tables[index]['name'][-1]!='object_current':
        raise ValueError('Original global count object table required')
    if (actual is None or actual['owner']!=(document_id,identity['module'],identity['element'])
            or record['kind']!='object' or record['schemaRevision']!=revision or record['sourceSystem']!=installation):
        raise ValueError('Actual original count type/source/revision differs')
    return identity
