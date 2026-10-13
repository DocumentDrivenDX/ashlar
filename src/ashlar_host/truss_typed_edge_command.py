"""Create one authored edge using original public endpoint and relationship references."""
from pathlib import Path
from dataclasses import dataclass,asdict,field
from contextlib import contextmanager
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish
from .exact_report import report_body

@dataclass(frozen=True)
class EdgeCreationConfiguration:
    connection_uri:str=field(repr=False)
    bun:Path
    document_path:Path
    document_revision:str
    document_id:str
    module_id:str
    relationship_id:str
    source:tuple
    target:tuple
    order_key:tuple
    origin_path:Path
    request_key:str
    values:tuple


from .truss_record_mutation import scalar_tag


def preflight(configuration,output):
    path=Path(configuration);destination=Path(output)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);raw=decode_custody_bytes(original.raw)
    keys={'connection_uri','bun','document_path','document_revision','document_id','module_id','relationship_id','origin_path','request_key','values','source','target','order_key'}
    if type(raw)is not dict or set(raw)!=keys:raise ValueError('Exact edge creation configuration required')
    for key in ('connection_uri','document_revision','document_id','module_id','relationship_id','request_key'):
        if type(raw[key])is not str or not raw[key]or len(raw[key].encode())>4096 or '\0'in raw[key]:raise ValueError('Bounded explicit relationship selection required')
    paths=[]
    for key in ('bun','document_path','origin_path'):
        if type(raw[key])is not str:raise ValueError('Explicit canonical input required')
        selected=Path(raw[key])
        if not selected.is_absolute()or selected.resolve()!=selected or not selected.is_file():raise ValueError('Existing canonical input required')
        paths.append(selected)
    if type(raw['values'])is not dict or len(raw['values'])>256 or any(type(k)is not str or not k or len(k.encode())>4096 or '\0'in k for k in raw['values']):raise ValueError('Bounded authored field map required')
    values=tuple((name,*scalar_tag(value))for name,value in raw['values'].items())
    endpoints=[]
    for role in ('source','target'):
        endpoint=raw[role]
        if type(endpoint)is not dict or set(endpoint)!={'document_id','module_id','record_id','entity_id'}or type(endpoint['entity_id'])is not int or not 1<=endpoint['entity_id']<=2**63-1 or any(type(endpoint[k])is not str or not endpoint[k]or len(endpoint[k].encode())>4096 or '\0'in endpoint[k]for k in ('document_id','module_id','record_id')):raise ValueError('Exact authored endpoint selection required')
        endpoints.append(tuple(endpoint[k]for k in ('document_id','module_id','record_id','entity_id')))
    order=raw['order_key']
    if type(order)is not dict:raise ValueError('Closed original order key required')
    if order=={'state':'null'}:order_key=('null',None)
    elif set(order)=={'state','text'}and order['state']=='text'and type(order['text'])is str and len(order['text'].encode())<=65536:order_key=('text',order['text'])
    else:raise ValueError('Exact public null or text order key required')
    model=snapshot(paths[1],1048576);document=decode_custody_bytes(model)
    if document.get('id')!=raw['document_id']or document.get('umf')!='0.7.0':raise ValueError('Original declared UMF0.7 document required')
    origin=InvocationFile.read(paths[2],65536);origin_map=decode_custody_bytes(origin.raw)
    if type(origin_map)is not dict or any(type(k)is not str or type(v)is not str for k,v in origin_map.items()):raise ValueError('Original string origin map required')
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical report required')
    if any(destination==p or destination in p.parents or p in destination.parents for p in (path,*paths)):raise ValueError('Separate report required')
    config=EdgeCreationConfiguration(raw['connection_uri'],paths[0],paths[1],raw['document_revision'],raw['document_id'],raw['module_id'],raw['relationship_id'],endpoints[0],endpoints[1],order_key,paths[2],raw['request_key'],values)
    return config,destination,original,model,origin,origin_map


def run_create_truss_edge_command(configuration,output):
    config,destination,original,model,origin,origin_map=preflight(configuration,output)
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,ObjectFound,EdgeFound
    from truss.contracts import TextValue,BooleanValue,NullValue,MapValue,MapEntry
    from truss.groups import CreateEdge,StoredObjectReference,NullOrderKey,TextOrderKey,AuthoredValue,GroupSuccess,CommittedReceiptReplay
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
            ref=value(assembly.get_relationship_reference_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id))
            if ref is None:raise RuntimeError('Current authorized relationship reference required')
            endpoints=[]
            for doc,module,record,entity_id in (config.source,config.target):
                found=value(assembly.get_object_in_transaction(tx,document_id=doc,module_id=module,record_id=record,object_id=entity_id))
                if not isinstance(found,ObjectFound)or found.object.id!=entity_id or (found.object.document_id,found.object.module_id,found.object.record_id)!=(doc,module,record):raise RuntimeError('Actual authorized endpoint identity required')
                endpoints.append(found.object.identity)
            operation=CreateEdge(relationship_definition_pin=ref.definition_pin,source=StoredObjectReference(identity=endpoints[0]),target=StoredObjectReference(identity=endpoints[1]),values=tuple(AuthoredValue(name=name,value=scalar(kind,body))for name,kind,body in config.values),order_key=NullOrderKey()if config.order_key[0]=='null'else TextOrderKey(text=config.order_key[1]),alias='created')
            context=value(assembly.group_context_in_transaction(tx));semantic=context.input((operation,),asserted);request=context.request(config.request_key)
            applied=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(applied,GroupSuccess)or len(applied.response.semantic.results)!=1:raise RuntimeError('Complete one-edge group required')
            created=applied.response.semantic.results[0]
        connection.commit()
        with interval(connection)as (assembly,tx):
            found=value(assembly.get_edge_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id,edge_id=int(created.identity.id)))
            if not isinstance(found,EdgeFound)or int(found.edge.identity.id)!=int(created.identity.id)or found.edge.version!=int(created.version)or found.edge.identity!=created.identity or found.edge.source!=endpoints[0]or found.edge.target!=endpoints[1]or found.edge.order_key!=config.order_key[1]:raise RuntimeError('Original committed edge identity/endpoints/version/order required')
            expected={name:(scalar(kind,body)if kind in ('integer','decimal')else body)for name,kind,body in config.values}
            if len(found.edge.retained)!=len(config.values)or dict(found.edge.retained)!=expected:raise RuntimeError('Complete exact committed retained values differ')
            replay=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=applied.response.semantic:raise RuntimeError('Original committed receipt replay required')
            result={'group_original':asdict(applied),'committed_receipt_replay':asdict(replay),'edge':asdict(found.edge),'qualification':'One original public authored edge creation and committed readback/replay; no publication or query qualification.'}
        connection.commit()
    original.renew(65536);origin.renew(65536)
    if snapshot(config.document_path,1048576)!=model:raise ValueError('Original model changed')
    from .truss_example import private_file
    private_file(destination,report_body(result,1048576));return result
