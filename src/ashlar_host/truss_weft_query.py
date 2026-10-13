"""Original Truss STRING bindings and publication-pinned public Weft compilation.

Execution remains separate: compiler receipts supply no native/source authority.
"""
import hashlib
import json
import re
from ashlar.manifest import manifest_pin_vector,validate_manifest_row
from ashlar.publication import resolve_publication
from ashlar.weft_binding import LAYOUT_SHA256
from ashlar.weft_distribution import DistributionPaths,compile_distribution
from .delta_custody import encoded
from .driver import NativeDriver
from .indexed_query import decode_compile_artifact
from .lifecycle import owned_context
from .truss_feed_sessions import TrussFeedSessions
from .truss_graph_admission import TrussGraphAdmission

BACKEND={'backendId':'ashlar.databricks','backendVersion':'0.1.0-candidate',
    'interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql-candidate'}


def truss_string_compile_request(admission,manifest,table_uuids,manifest_uuid,sql,*,revision,document_id):
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
    tables=[{'name':name.split('.'),'uuid':table_uuids[name],'version':versions[name]}for name in names]
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


def compile_truss_string_query(driver,paths,sql,*,revision,document_id):
    """Compile exact authored singleton/list SQL under the actual publication hold.

    Returns original request/response bytes after closing native pins and writer.
    No generated SQL is executed and outer resources remain caller owned.
    """
    if (type(driver)is not NativeDriver or type(paths)is not DistributionPaths
            or type(driver.source_sessions)is not TrussFeedSessions or driver.ack_sessions is not driver.source_sessions):
        raise PermissionError('Actual Truss publication/compiler owners required')
    with owned_context(driver.writer(driver.policy.active['request']['stream'],driver.context)):
        manifest=dict(driver.policy.active['manifest'])
        uuids={table:driver.transport.targets[table].uuid for table in json.loads(manifest['table_versions_json'])}
        vector=manifest_pin_vector(manifest,uuids,authority='private-local-process')
        with owned_context(driver.hold(vector,context=driver.context)):
            resolved=resolve_publication(driver,manifest['publication_id'],uuids,context=driver.context,
                supported_profiles=['ashlar-delta/0.3'],supported_revisions={key:[value]for key,value in json.loads(manifest['schema_revisions_json']).items()})
            if dict(resolved.descriptor.raw)!=manifest:raise PermissionError('Original pinned Truss manifest differs')
            request=truss_string_compile_request(driver.source_admission,manifest,uuids,
                driver.transport.targets[driver.tables['manifest']].uuid,sql,revision=revision,document_id=document_id)
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
            publication=next(item['parameters']for item in obligations if item['id']=='ashlar.candidate.publication')
            if any(publication.get(key)!=binding[key]for key in ('publication','modelPins','layoutRevision','layoutSha256')):
                raise ValueError('Original compiled Truss publication obligation differs')
            integrity=next(item['parameters']for item in obligations if item['id']=='ashlar.candidate.scalarIntegrity')
            if (integrity.get('phase')!='before-user-query' or integrity.get('samePublicationRequired')is not True
                    or integrity.get('noPartialPublication')is not True or type(integrity.get('checks'))is not list):
                raise ValueError('Original candidate scalar integrity lifecycle differs')
            driver.validate_manifest(manifest,driver.context)
    return {'request_bytes':request_bytes,'response_bytes':response,
        'request_sha256':hashlib.sha256(request_bytes).hexdigest(),'response_sha256':hashlib.sha256(response).hexdigest(),
        'publication_id':manifest['publication_id'],'qualification':'Original accepted UMF0.7 STRING record mapping for the selected physical catalog revision; public candidate Weft compilation under native publication pins. No SQL execution or scalar-integrity discharge.'}
