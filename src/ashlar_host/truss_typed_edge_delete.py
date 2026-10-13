"""Delete one authored edge using the original public Truss group."""
from pathlib import Path
from dataclasses import dataclass,asdict,field
from contextlib import contextmanager
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish
from .exact_report import report_body

@dataclass(frozen=True)
class EdgeDeleteConfiguration:
    connection_uri:str=field(repr=False)
    bun:Path
    document_path:Path
    document_revision:str
    document_id:str
    module_id:str
    relationship_id:str
    origin_path:Path
    request_key:str
    edge_id:int
    expected_version:int


def preflight(configuration,output):
    path=Path(configuration);destination=Path(output)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);raw=decode_custody_bytes(original.raw)
    keys={'connection_uri','bun','document_path','document_revision','document_id','module_id','relationship_id','origin_path','request_key','edge_id','expected_version'}
    if type(raw)is not dict or set(raw)!=keys:raise ValueError('Exact edge deletion configuration required')
    for key in ('connection_uri','document_revision','document_id','module_id','relationship_id','request_key'):
        if type(raw[key])is not str or not raw[key]or len(raw[key].encode())>4096 or '\0'in raw[key]:raise ValueError('Bounded explicit relationship selection required')
    paths=[]
    for key in ('bun','document_path','origin_path'):
        if type(raw[key])is not str:raise ValueError('Explicit canonical input required')
        selected=Path(raw[key])
        if not selected.is_absolute()or selected.resolve()!=selected or not selected.is_file():raise ValueError('Existing canonical input required')
        paths.append(selected)
    if any(type(raw[k])is not int or not 1<=raw[k]<=2**63-1 for k in ('edge_id','expected_version')):raise ValueError('Explicit positive identity/version required')
    model=snapshot(paths[1],1048576);document=decode_custody_bytes(model)
    if document.get('id')!=raw['document_id']or document.get('umf')!='0.7.0':raise ValueError('Original declared UMF0.7 document required')
    origin=InvocationFile.read(paths[2],65536);origin_map=decode_custody_bytes(origin.raw)
    if type(origin_map)is not dict or any(type(k)is not str or type(v)is not str for k,v in origin_map.items()):raise ValueError('Original string origin map required')
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical report required')
    if any(destination==p or destination in p.parents or p in destination.parents for p in (path,*paths)):raise ValueError('Separate report required')
    config=EdgeDeleteConfiguration(raw['connection_uri'],paths[0],paths[1],raw['document_revision'],raw['document_id'],raw['module_id'],raw['relationship_id'],paths[2],raw['request_key'],raw['edge_id'],raw['expected_version'])
    return config,destination,original,model,origin,origin_map


def run_delete_truss_edge_command(configuration,output):
    config,destination,original,model,origin,origin_map=preflight(configuration,output)
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,EdgeFound,EdgeNotFound
    from truss.contracts import TextValue,MapValue,MapEntry
    from truss.groups import Delete,StoredObjectReference,Deleted,GroupSuccess,CommittedReceiptReplay
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
            if not host.detach():raise RuntimeError('Public host detach refused')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    documents=(CatalogDocument(config.document_id,config.document_revision,model),)
    asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind='string',text=v))for k,v in origin_map.items()))
    with psycopg.connect(config.connection_uri,connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            reference=value(assembly.get_relationship_reference_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id))
            before=value(assembly.get_edge_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id,edge_id=config.edge_id))
            if reference is None or not isinstance(before,EdgeFound)or int(before.edge.identity.id)!=config.edge_id or before.edge.version!=config.expected_version or before.edge.identity.definition_pin!=reference.definition_pin:raise RuntimeError('Original caller identity/version required')
            operation=Delete(operation='delete_edge',target=StoredObjectReference(identity=before.edge.identity),expected_version=str(config.expected_version))
            context=value(assembly.group_context_in_transaction(tx));semantic=context.input((operation,),asserted);request=context.request(config.request_key)
            applied=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(applied,GroupSuccess)or len(applied.response.semantic.results)!=1:raise RuntimeError('Complete one-edge deletion group required')
            deleted=applied.response.semantic.results[0]
            if not isinstance(deleted,Deleted)or deleted.operation!='delete_edge'or deleted.identity!=before.edge.identity or int(deleted.deletion_version)!=config.expected_version+1:raise RuntimeError('Original deletion identity/version required')
            if len(deleted.deleted)!=1 or deleted.deleted[0].identity!=before.edge.identity or deleted.deleted[0].deletion_version!=deleted.deletion_version:raise RuntimeError('Complete original deletion list required')
        connection.commit()
        with interval(connection)as (assembly,tx):
            absent=value(assembly.get_edge_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id,edge_id=config.edge_id))
            if not isinstance(absent,EdgeNotFound):raise RuntimeError('Original deleted edge remains readable')
            replay=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=applied.response.semantic:raise RuntimeError('Original committed receipt replay required')
            result={'group_original':asdict(applied),'committed_receipt_replay':asdict(replay),'before':asdict(before.edge),'absent':asdict(absent),'qualification':'One original public authored edge deletion and committed absence readback/replay; no publication or query qualification.'}
        connection.commit()
    original.renew(65536);origin.renew(65536)
    if snapshot(config.document_path,1048576)!=model:raise ValueError('Original model changed')
    from .truss_example import private_file
    private_file(destination,report_body(result,1048576));return result
