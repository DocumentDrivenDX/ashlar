# SPDX-License-Identifier: MIT OR Apache-2.0
"""Update one authored typed edge using the original public Truss group."""
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
class EdgeUpdateConfiguration:
    connection_uri:str=field(repr=False)
    bun:Path
    document_path:Path
    document_revision:str
    document_id:str
    module_id:str
    relationship_id:str
    origin_path:Path
    request_key:str
    changes:tuple
    unset:tuple
    edge_id:int
    expected_version:int
    endpoint_change:tuple
    order_key_change:tuple


def endpoint_selection(value):
    keys=('document_id','module_id','record_id','entity_id')
    if type(value)is not dict or set(value)!=set(keys)or type(value['entity_id'])is not int or not 1<=value['entity_id']<=2**63-1 or any(type(value[k])is not str or not value[k]or len(value[k].encode())>4096 or '\0'in value[k]for k in keys[:-1]):raise ValueError('Exact authored endpoint selection required')
    return tuple(value[k]for k in keys)


def preflight(configuration,output):
    path=Path(configuration);destination=Path(output)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);raw=decode_custody_bytes(original.raw)
    keys={'connection_uri','bun','document_path','document_revision','document_id','module_id','relationship_id','origin_path','request_key','set','unset','edge_id','expected_version','endpoint_change','order_key_change'}
    if type(raw)is not dict or set(raw)!=keys:raise ValueError('Exact edge update configuration required')
    for key in ('connection_uri','document_revision','document_id','module_id','relationship_id','request_key'):
        if type(raw[key])is not str or not raw[key]or len(raw[key].encode())>4096 or '\0'in raw[key]:raise ValueError('Bounded explicit relationship selection required')
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
    if set(raw['set'])&set(unset):raise ValueError('Disjoint exact edge changes required')
    endpoints=raw['endpoint_change']
    if endpoints=={'state':'unchanged'}:endpoint_change=('unchanged',)
    elif type(endpoints)is dict and set(endpoints)=={'state','source','target'}and endpoints['state']=='replace':endpoint_change=('replace',endpoint_selection(endpoints['source']),endpoint_selection(endpoints['target']))
    else:raise ValueError('Closed explicit endpoint change required')
    order=raw['order_key_change']
    if order=={'state':'unchanged'}:order_key_change=('unchanged',None)
    elif order=={'state':'null'}:order_key_change=('null',None)
    elif type(order)is dict and set(order)=={'state','text'}and order['state']=='text'and type(order['text'])is str and len(order['text'].encode())<=65536:order_key_change=('text',order['text'])
    else:raise ValueError('Closed exact edge order key change required')
    if not values and not unset and endpoint_change[0]=='unchanged'and order_key_change[0]=='unchanged':raise ValueError('Explicit nonempty edge update required')
    if any(type(raw[k])is not int or not 1<=raw[k]<=2**63-1 for k in ('edge_id','expected_version')):raise ValueError('Explicit positive identity/version required')
    model=snapshot(paths[1],1048576);document=decode_custody_bytes(model)
    if document.get('id')!=raw['document_id']or document.get('umf')not in ('0.7.0','0.8.0'):raise ValueError('Original declared UMF0.7 or0.8 document required')
    origin=InvocationFile.read(paths[2],65536);origin_map=decode_custody_bytes(origin.raw)
    if type(origin_map)is not dict or any(type(k)is not str or type(v)is not str for k,v in origin_map.items()):raise ValueError('Original string origin map required')
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink():raise ValueError('Fresh canonical report required')
    if any(destination==p or destination in p.parents or p in destination.parents for p in (path,*paths)):raise ValueError('Separate report required')
    config=EdgeUpdateConfiguration(raw['connection_uri'],paths[0],paths[1],raw['document_revision'],raw['document_id'],raw['module_id'],raw['relationship_id'],paths[2],raw['request_key'],values,tuple(unset),raw['edge_id'],raw['expected_version'],endpoint_change,order_key_change)
    return config,destination,original,model,origin,origin_map


def run_update_truss_edge_command(configuration,output):
    config,destination,original,model,origin,origin_map=preflight(configuration,output)
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,ObjectFound,EdgeFound
    from truss.contracts import TextValue,BooleanValue,NullValue,MapValue,MapEntry
    from truss.groups import UpdateEdge,EndpointChange,NullOrderKey,TextOrderKey,StoredObjectReference,AuthoredValue,Changed,Unchanged,GroupSuccess,CommittedReceiptReplay
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
            reference=value(assembly.get_relationship_reference_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id))
            before=value(assembly.get_edge_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id,edge_id=config.edge_id))
            if reference is None or not isinstance(before,EdgeFound)or int(before.edge.identity.id)!=config.edge_id or before.edge.version!=config.expected_version or before.edge.identity.definition_pin!=reference.definition_pin:raise RuntimeError('Current original edge identity/pin/version required')
            expected=dict(before.edge.retained)
            if len(expected)!=len(before.edge.retained):raise RuntimeError('Unique original retained field inventory required')
            for name in config.unset:
                expected.pop(name,None)
            for name,kind,body in config.changes:expected[name]=scalar(kind,body)if kind in ('integer','decimal')else body
            endpoints=(before.edge.source,before.edge.target);extra={}
            if config.endpoint_change[0]=='replace':
                observed=[]
                for doc,module,record,entity_id in config.endpoint_change[1:]:
                    found=value(assembly.get_object_in_transaction(tx,document_id=doc,module_id=module,record_id=record,object_id=entity_id))
                    if not isinstance(found,ObjectFound)or found.object.id!=entity_id or (found.object.document_id,found.object.module_id,found.object.record_id)!=(doc,module,record):raise RuntimeError('Actual authorized replacement endpoint required')
                    observed.append(found.object.identity)
                endpoints=tuple(observed);extra['endpoint_change']=EndpointChange(source=StoredObjectReference(identity=endpoints[0]),target=StoredObjectReference(identity=endpoints[1]))
            expected_order=before.edge.order_key
            if config.order_key_change[0]!='unchanged':
                expected_order=config.order_key_change[1];extra['order_key_change']=NullOrderKey()if config.order_key_change[0]=='null'else TextOrderKey(text=expected_order)
            operation=UpdateEdge(target=StoredObjectReference(identity=before.edge.identity),set=tuple(AuthoredValue(name=name,value=scalar(kind,body))for name,kind,body in config.changes),unset=config.unset,expected_version=str(config.expected_version),**extra)
            context=value(assembly.group_context_in_transaction(tx));semantic=context.input((operation,),asserted);request=context.request(config.request_key)
            applied=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(applied,GroupSuccess)or len(applied.response.semantic.results)!=1:raise RuntimeError('Complete one-edge update group required')
            created=applied.response.semantic.results[0]
            if not isinstance(created,(Changed,Unchanged))or created.operation!='update_edge'or created.identity!=before.edge.identity or int(created.version)!=(config.expected_version+1 if isinstance(created,Changed)else config.expected_version):raise RuntimeError('Original update identity/version required')
        connection.commit()
        with interval(connection)as (assembly,tx):
            found=value(assembly.get_edge_in_transaction(tx,document_id=config.document_id,module_id=config.module_id,relationship_id=config.relationship_id,edge_id=config.edge_id))
            if not isinstance(found,EdgeFound)or found.edge.identity!=before.edge.identity or found.edge.identity!=created.identity or found.edge.version!=int(created.version)or (found.edge.source,found.edge.target)!=endpoints or found.edge.order_key!=expected_order:raise RuntimeError('Original committed edge identity/version/endpoints/order required')
            if len(found.edge.retained)!=len(expected)or dict(found.edge.retained)!=expected:raise RuntimeError('Complete exact committed retained values differ')
            replay=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=applied.response.semantic:raise RuntimeError('Original committed receipt replay required')
            result={'group_original':asdict(applied),'committed_receipt_replay':asdict(replay),'before':asdict(before.edge),'edge':asdict(found.edge),'qualification':'One original public authored edge update and committed readback/replay; no publication or query qualification.'}
        connection.commit()
    original.renew(65536);origin.renew(65536)
    if snapshot(config.document_path,1048576)!=model:raise ValueError('Original model changed')
    from .truss_example import private_file
    private_file(destination,report_body(result,1048576));return result
