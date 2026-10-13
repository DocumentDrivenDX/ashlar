"""Update one typed record using the original public Truss group."""
from pathlib import Path
from dataclasses import dataclass,asdict,field
from contextlib import contextmanager
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish
from .exact_report import report_body
from .truss_record_mutation import scalar_tag

@dataclass(frozen=True)
class RecordUpdateConfiguration:
    connection_uri:str=field(repr=False)
    bun:Path
    document_path:Path
    document_revision:str
    document_id:str
    module_id:str
    record_id:str
    origin_path:Path
    request_key:str
    changes:tuple
    unset:tuple
    object_id:int
    expected_version:int


def preflight(configuration,output):
    path=Path(configuration);destination=Path(output)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);raw=decode_custody_bytes(original.raw)
    keys={'connection_uri','bun','document_path','document_revision','document_id','module_id','record_id','origin_path','request_key','set','unset','object_id','expected_version'}
    if type(raw)is not dict or set(raw)!=keys:raise ValueError('Exact record update configuration required')
    for key in ('connection_uri','document_revision','document_id','module_id','record_id','request_key'):
        if type(raw[key])is not str or not raw[key]or len(raw[key].encode())>4096 or '\0'in raw[key]:raise ValueError('Bounded explicit record selection required')
    paths=[]
    for key in ('bun','document_path','origin_path'):
        if type(raw[key])is not str:raise ValueError('Explicit canonical input required')
        selected=Path(raw[key])
        if not selected.is_absolute()or selected.resolve()!=selected or not selected.is_file():raise ValueError('Existing canonical input required')
        paths.append(selected)
    if type(raw['set'])is not dict or len(raw['set'])>256 or any(type(k)is not str or not k or len(k.encode())>4096 or '\0'in k for k in raw['set']):raise ValueError('Bounded authored field map required')
    values=tuple((name,*scalar_tag(value))for name,value in raw['set'].items())
    unset=raw['unset']
    if type(unset)is not list or len(unset)>256 or any(type(k)is not str or not k or len(k.encode())>4096 or '\0'in k for k in unset)or len(set(unset))!=len(unset):raise ValueError('Unique bounded unset fields required')
    if not values and not unset or set(raw['set'])&set(unset):raise ValueError('Nonempty disjoint changes required')
    if any(type(raw[k])is not int or raw[k]<=0 for k in ('object_id','expected_version')):raise ValueError('Explicit positive identity/version required')
    model=snapshot(paths[1],1048576);document=decode_custody_bytes(model)
    if document.get('id')!=raw['document_id']or document.get('umf')!='0.7.0':raise ValueError('Original declared UMF0.7 document required')
    origin=InvocationFile.read(paths[2],65536);origin_map=decode_custody_bytes(origin.raw)
    if type(origin_map)is not dict or any(type(k)is not str or type(v)is not str for k,v in origin_map.items()):raise ValueError('Original string origin map required')
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical report required')
    if any(destination==p or destination in p.parents or p in destination.parents for p in (path,*paths)):raise ValueError('Separate report required')
    config=RecordUpdateConfiguration(raw['connection_uri'],paths[0],paths[1],raw['document_revision'],raw['document_id'],raw['module_id'],raw['record_id'],paths[2],raw['request_key'],values,tuple(unset),raw['object_id'],raw['expected_version'])
    return config,destination,original,model,origin,origin_map


def run_update_truss_record_command(configuration,output):
    config,destination,original,model,origin,origin_map=preflight(configuration,output)
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,ObjectFound
    from truss.contracts import TextValue,BooleanValue,NullValue,MapValue,MapEntry
    from truss.groups import UpdateObject,StoredObjectReference,AuthoredValue,GroupSuccess,CommittedReceiptReplay
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss operation refused')
        return result.value
    def scalar(kind,body):return NullValue()if kind=='null'else BooleanValue(value=body)if kind=='boolean'else TextValue(kind=kind,text=body)
    @contextmanager
    def interval(connection):
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED');connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction());assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host));yield assembly,tx
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('Public host detach refused')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    documents=(CatalogDocument(config.document_id,config.document_revision,model),)
    asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind='string',text=v))for k,v in origin_map.items()))
    with psycopg.connect(config.connection_uri,connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            before=value(assembly.get_object_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,record_id=config.record_id,object_id=config.object_id))
            if not isinstance(before,ObjectFound)or before.object.id!=config.object_id or before.object.version!=config.expected_version:raise RuntimeError('Original caller identity/version required')
            expected=dict(before.object.values)
            for name in config.unset:expected.pop(name,None)
            for name,kind,body in config.changes:expected[name]=scalar(kind,body)if kind in ('integer','decimal')else body
            operation=UpdateObject(target=StoredObjectReference(identity=before.object.identity),set=tuple(AuthoredValue(name=name,value=scalar(kind,body))for name,kind,body in config.changes),unset=config.unset,expected_version=str(config.expected_version))
            context=value(assembly.group_context_in_transaction(tx));semantic=context.input((operation,),asserted);request=context.request(config.request_key)
            applied=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(applied,GroupSuccess)or len(applied.response.semantic.results)!=1:raise RuntimeError('Complete one-object update group required')
            created=applied.response.semantic.results[0]
            if created.identity!=before.object.identity or int(created.version)not in (config.expected_version,config.expected_version+1):raise RuntimeError('Original update identity/version required')
        connection.commit()
        with interval(connection)as (assembly,tx):
            found=value(assembly.get_object_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,record_id=config.record_id,object_id=int(created.identity.id)))
            if not isinstance(found,ObjectFound)or found.object.id!=config.object_id or created.identity!=before.object.identity or int(created.version)not in (config.expected_version,config.expected_version+1) or found.object.version!=int(created.version)or found.object.identity!=created.identity:raise RuntimeError('Original committed identity/version required')
            if dict(found.object.values)!=expected:raise RuntimeError('Complete exact committed values differ')
            replay=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=applied.response.semantic:raise RuntimeError('Original committed receipt replay required')
            result={'group_original':asdict(applied),'committed_receipt_replay':asdict(replay),'before':asdict(before.object),'object':asdict(found.object),'qualification':'One original public typed record update and committed readback/replay; no publication or query qualification.'}
        connection.commit()
    original.renew(65536);origin.renew(65536)
    if snapshot(config.document_path,1048576)!=model:raise ValueError('Original model changed')
    from .truss_example import private_file
    private_file(destination,report_body(result,1048576));return result
