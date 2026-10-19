# SPDX-License-Identifier: MIT OR Apache-2.0
"""Accept an explicit compatible record revision and update one new exact scalar field."""
import base64,json,os
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from .lifecycle import finish
from .truss_record_mutation import scalar_tag


def read(path,maximum):
    with Path(path).open('rb')as stream:raw=stream.read(maximum+1)
    if len(raw)>maximum:raise ValueError('Selected input exceeds example bound')
    return raw


def capture_catalog(assembly,transaction,revision):
    """Retain exact public catalog bytes while the selected revision is current."""
    from dataclasses import replace
    from truss import Ok,CatalogViewAvailable,HistoricalCatalogSelection
    request=replace(assembly.catalog_view_request(),selection=HistoricalCatalogSelection(catalog_revision=str(revision)))
    result=assembly.catalog_view_in_transaction(transaction,request)
    if not isinstance(result,Ok)or not isinstance(result.value,CatalogViewAvailable):raise RuntimeError('Original evolution catalog capture unavailable')
    raw=result.value.view.original_bytes
    if type(raw)is not bytes or len(raw)>16777216 or str(json.loads(raw)['catalogRevision'])!=str(revision):raise RuntimeError('Exact original evolution catalog revision required')
    return raw


def require_new_record_field(revised_bytes,previous_bytes,*,module_id,record_id,field_name,scalar_type='string'):
    """Resolve the selected element to its original authored name; SDK owns acceptance."""
    if scalar_type not in ('string','integer','decimal','boolean'):raise ValueError('Supported exact scalar field family required')
    def members(raw):
        model=json.loads(raw)
        modules=[module for module in model['modules']if module['id']==module_id]
        if len(modules)!=1:raise ValueError('One original selected record module required')
        records=[element for element in modules[0]['elements']if element['id']==record_id and element.get('kind')=='record']
        if len(records)!=1:raise ValueError('One original selected record declaration required')
        return model,records[0]['members']
    previous,old_members=members(previous_bytes)
    revised,new_members=members(revised_bytes)
    if any(member['element']==field_name for member in old_members):
        raise ValueError('Selected new field was already declared in the original record')
    selected=[member for member in new_members if member['element']==field_name]
    if len(selected)!=1:raise ValueError('Revised record must declare the selected new field')
    ref=selected[0]
    fields=[element for module in revised['modules']if module['id']==ref['module']for element in module['elements']if element['id']==ref['element']]
    if (len(fields)!=1 or fields[0].get('kind')!='field' or fields[0].get('scalarType')!=scalar_type
            or fields[0].get('nullability')not in ('required','absent-allowed')or fields[0].get('cardinality')!='one'):
        raise ValueError('Selected new member must match exact scalar field family')
    authored_name=fields[0].get('name')
    if type(authored_name)is not str or not authored_name or len(authored_name.encode())>4096 or '\0'in authored_name:
        raise ValueError('Original declared Field name required')
    return authored_name


def require_matching_record_versions(revised_bytes,previous_bytes):
    """Select the original owning version; cross-version evolution is separate."""
    revised=json.loads(revised_bytes);previous=json.loads(previous_bytes)
    version=revised.get('umf')
    if version not in ('0.7.0','0.8.0')or previous.get('umf')!=version:
        raise ValueError('Matching original declared UMF0.7 or0.8 revisions required')
    return version


def selected_field_value(value):
    selected=scalar_tag({'kind':'string','text':value}if type(value)is str else value)
    if selected[0]=='null':raise ValueError('Non-null exact scalar field value required')
    return selected


def public_field_values(kind,body):
    from truss.contracts import TextValue,BooleanValue
    authored=BooleanValue(value=body)if kind=='boolean'else TextValue(kind=kind,text=body)
    return authored,authored if kind in ('integer','decimal')else body


def parse_truss_record_evolution_configuration(config,output):
    required={'connection_uri','bun','document_path','document_revision','origin_path','accept_request_key','update_request_key','object_id','document_id','module_id','record_id','field_name','field_value'}
    if type(config)is not dict or set(config)!=required:raise ValueError('Exact record evolution configuration required')
    if type(config['object_id'])is not int or not 1<=config['object_id']<=9223372036854775807:raise ValueError('Actual positive object ID required')
    if any(type(config[k])is not str or not config[k]or len(config[k])>4096 for k in ('connection_uri','document_revision','accept_request_key','update_request_key','document_id','module_id','record_id','field_name'))or config['accept_request_key']==config['update_request_key']:raise ValueError('Explicit record/field/revision and distinct request keys required')
    selected_field_value(config['field_value'])
    source=read(config['document_path'],1048576)
    if json.loads(source).get('id')!=config['document_id']:raise ValueError('Original selected document identity required')
    require_matching_record_versions(source,source)
    bun=Path(config['bun'])
    if not bun.is_absolute()or not bun.is_file():raise ValueError('Selected installed Bun required')
    origin_bytes=read(config['origin_path'],65536);origin=json.loads(origin_bytes)
    if type(origin)is not dict or any(type(v)is not str for v in origin.values()):raise ValueError('String-valued original origin map required')
    destination=Path(output)
    if not destination.is_absolute()or not destination.parent.is_dir()or destination.parent.resolve()!=destination.parent or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical report required')
    if any(destination==Path(config[key]).resolve()for key in ('document_path','bun','origin_path')):raise ValueError('Evolution report must be separate from original inputs')
    return source,origin,destination,bun,origin_bytes


def evolve_truss_record(config,output,*,required_original_fields=None):
    source,origin,destination,bun,origin_bytes=parse_truss_record_evolution_configuration(config,output)
    import psycopg
    from truss import (Ok,CatalogDocument,CatalogRuntimeConfiguration,CatalogRegistration,
        AcceptedDocument,ObjectFound,create_reference_assembly,adopt_psycopg_host)
    from truss.contracts import MapValue,MapEntry,TextValue
    from truss.groups import UpdateObject,StoredObjectReference,AuthoredValue,GroupSuccess,CommittedReceiptReplay
    kind,body=selected_field_value(config['field_value'])
    authored,observed=public_field_values(kind,body)
    document=CatalogDocument(config['document_id'],config['document_revision'],source)
    asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind='string',text=v))for k,v in origin.items()))
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss operation refused')
        return result.value
    @contextmanager
    def interval(connection):
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction());assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host));yield assembly,tx
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('Host detach refused; recovery required')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    def current(assembly,tx):
        found=value(assembly.get_object_in_transaction(tx,document_id=config['document_id'],module_id=config['module_id'],record_id=config['record_id'],object_id=config['object_id']))
        if not isinstance(found,ObjectFound):raise RuntimeError('Current authorized selected record required')
        return found.object
    with psycopg.connect(config['connection_uri'],connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            before=current(assembly,tx)
            previous_document=value(assembly.accepted_document_in_transaction(tx,document_id=config['document_id']))
            if not isinstance(previous_document,AcceptedDocument)or previous_document.document_revision==config['document_revision']:raise RuntimeError('Distinct current accepted document revision required')
            source_version=require_matching_record_versions(source,previous_document.content)
            authored_name=require_new_record_field(source,previous_document.content,module_id=config['module_id'],record_id=config['record_id'],field_name=config['field_name'],scalar_type=kind)
            if authored_name in dict(before.values) or (required_original_fields is not None and set(dict(before.values))!=set(required_original_fields)):raise RuntimeError('Original selected field must be absent and complete original values retained')
            old_report=value(assembly.report_in_transaction(tx,before.catalog_revision))
            old_catalog=capture_catalog(assembly,tx,before.catalog_revision)
            prepared=assembly.prepare_catalog((document,),bun_executable=str(bun),source_version=source_version)
            accepted=value(assembly.accept_in_transaction(tx,prepared,request_key=config['accept_request_key'],origin_bytes=origin_bytes))
            if not isinstance(accepted,CatalogRegistration)or accepted.revision!=before.catalog_revision+1:raise RuntimeError('Actual next compatible catalog registration required')
        connection.commit()
        with interval(connection)as (assembly,tx):
            published=value(assembly.accepted_document_in_transaction(tx,document_id=config['document_id']))
            if not isinstance(published,AcceptedDocument)or published.content!=source or published.document_revision!=config['document_revision']or published.catalog_revision!=accepted.revision:raise RuntimeError('Exact committed revised document differs')
            report=value(assembly.report_in_transaction(tx,accepted.revision));evolved=current(assembly,tx)
            catalog=capture_catalog(assembly,tx,accepted.revision)
            reference=value(assembly.get_type_reference_in_transaction(tx,document_id=config['document_id'],module_id=config['module_id'],record_id=config['record_id']))
            if reference is None or reference.definition_pin!=evolved.identity.definition_pin:raise RuntimeError('Actual current identity/pin required')
            if (evolved.id,evolved.type_id,evolved.version,evolved.values,evolved.retained)!=(before.id,before.type_id,before.version,before.values,before.retained)or evolved.definition_pin==before.definition_pin:raise RuntimeError('Evolution changed original identity, values or version')
            context=value(assembly.group_context_in_transaction(tx))
            original=context.input((UpdateObject(target=StoredObjectReference(identity=evolved.identity),set=(AuthoredValue(name=authored_name,value=authored),),unset=(),expected_version=str(evolved.version)),),asserted)
            request=context.request(config['update_request_key'])
            changed=value(assembly.apply_in_transaction(tx,original,request,documents=(document,),bun_executable=str(bun)))
            if not isinstance(changed,GroupSuccess)or len(changed.response.semantic.results)!=1:raise RuntimeError('New-field update refused')
        connection.commit()
        with interval(connection)as (assembly,tx):
            after=current(assembly,tx);expected=dict(before.values);expected[authored_name]=observed
            if dict(after.values)!=expected or (after.id,after.type_id,after.retained)!=(before.id,before.type_id,before.retained)or after.version!=before.version+1 or after.identity!=evolved.identity:raise RuntimeError('Complete evolved record readback differs')
            replay=value(assembly.apply_in_transaction(tx,original,request,documents=(document,),bun_executable=str(bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=changed.response.semantic:raise RuntimeError('Original committed update receipt differs')
            result={'before':asdict(before),'evolved':asdict(evolved),'after':asdict(after),
                'previous_document':{**{k:v for k,v in asdict(previous_document).items()if k!='content'},'content_original_base64':base64.b64encode(previous_document.content).decode('ascii')},
                'accepted_document':{**{k:v for k,v in asdict(published).items()if k!='content'},'content_original_base64':base64.b64encode(published.content).decode('ascii')},
                'previous_catalog_original_base64':base64.b64encode(old_catalog).decode('ascii'),
                'catalog_original_base64':base64.b64encode(catalog).decode('ascii'),
                'previous_report_original_base64':base64.b64encode(old_report.original_bytes).decode('ascii'),
                'report_original_base64':base64.b64encode(report.original_bytes).decode('ascii'),
                'registration':{'revision':accepted.revision,'registration_id':accepted.registration_id,'replayed':accepted.replayed,'status':accepted.status,'report_original_base64':base64.b64encode(accepted.report_bytes).decode('ascii')},
                'update_original':asdict(changed),'update_committed_receipt':asdict(replay),'caller_commits_confirmed':True,
                'scope':'Compatible selected original declared UMF0.7/0.8 record new-field evolution and update/replay only; actual Truss acceptance owns compatibility. No feed, ACK or Delta integration.'}
        connection.commit()
    payload=(json.dumps(result,indent=2,ensure_ascii=False)+'\n').encode()
    descriptor=os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb')as stream:
        os.fchmod(stream.fileno(),0o600)
        if stream.write(payload)!=len(payload):raise OSError('Complete original evolution report write required')
        stream.flush();os.fsync(stream.fileno())
    return result



def run_evolve_truss_record_command(configuration,output):
    """Installed selected record evolution using original source and live SDK."""
    config=json.loads(read(configuration,65536))
    return evolve_truss_record(config,output)
