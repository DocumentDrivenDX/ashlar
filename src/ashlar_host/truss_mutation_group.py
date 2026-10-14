"""Bounded stored-record/link mutations through one original public Truss group."""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass,asdict,field
from pathlib import Path
from .truss_record_group import name,closed,authored_values,endpoint,release_report
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .lifecycle import finish,owned_context
from .exact_report import report_body

PROFILE='ashlar-truss-mutation-group/0.1'

@dataclass(frozen=True)
class MutationSelection:
    operation:str
    module_id:str
    element_id:str
    entity_id:int|None=None
    expected_version:int|None=None
    alias:str|None=None
    source:object=None
    target:object=None
    values:tuple=()
    order_key:tuple=()

@dataclass(frozen=True)
class MutationGroupConfiguration:
    connection_uri:str=field(repr=False)
    bun:Path
    document_path:Path
    document_revision:str
    document_id:str
    source_version:str
    origin_path:Path
    request_key:str
    operations:tuple[MutationSelection,...]


def preflight(configuration: str|Path,output: str|Path) -> tuple:
    """Snapshot closed original selections; no SDK construction or effect admission."""
    path=Path(configuration);destination=Path(output)
    if not path.is_absolute()or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);raw=decode_custody_bytes(original.raw)
    closed(raw,('profile','connection_uri','bun','document_path','document_revision','document_id','source_version','origin_path','request_key','operations'))
    if raw['profile']!=PROFILE or raw['source_version']not in ('0.7.0','0.8.0'):raise ValueError('Explicit original source version required')
    for key in ('connection_uri','document_revision','document_id','request_key'):name(raw[key])
    paths=[]
    for key in ('bun','document_path','origin_path'):
        p=Path(name(raw[key]))
        if not p.is_absolute()or p.resolve()!=p or not p.is_file()or any(q.is_symlink()for q in(p,*p.parents)):raise ValueError('Original canonical file required')
        paths.append(p)
    if type(raw['operations'])is not list or not 1<=len(raw['operations'])<=32:raise ValueError('Bounded ordered mutations required')
    selections=[];aliases=set();deleted=set();deleted_objects=set();endpoints=set()
    for op in raw['operations']:
        if type(op)is not dict:raise ValueError('Closed mutation required')
        kind=op.get('operation');module=name(op.get('module_id'))
        if kind=='create_edge':
            closed(op,('operation','module_id','relationship_id','alias','source','target','values','order_key'))
            alias=name(op['alias'])
            if alias in aliases:raise ValueError('Unique created edge alias required')
            aliases.add(alias);source=endpoint(op['source'],frozenset(),raw['document_id']);target=endpoint(op['target'],frozenset(),raw['document_id'])
            endpoints.update(( (p.module_id,p.record_id,p.entity_id)for p in(source,target)))
            order=op['order_key']
            if order=={'state':'null'}:order_key=('null',None)
            elif type(order)is dict and set(order)=={'state','text'}and order['state']=='text'and type(order['text'])is str and len(order['text'].encode())<=65536:order_key=('text',order['text'])
            else:raise ValueError('Exact original order key required')
            selections.append(MutationSelection(kind,module,name(op['relationship_id']),alias=alias,source=source,target=target,values=authored_values(op['values']),order_key=order_key))
        elif kind in ('delete_edge','delete_object'):
            element='relationship_id'if kind=='delete_edge'else'record_id'
            closed(op,('operation','module_id',element,'entity_id','expected_version'))
            if any(type(op[k])is not int or not 1<=op[k]<2**63 for k in('entity_id','expected_version')):raise ValueError('Exact positive identity/version required')
            element_id=name(op[element]);identity=(kind,module,element_id,op['entity_id'])
            if identity in deleted:raise ValueError('Unique deleted selection required')
            deleted.add(identity)
            if kind=='delete_object':deleted_objects.add((module,element_id,op['entity_id']))
            selections.append(MutationSelection(kind,module,element_id,op['entity_id'],op['expected_version']))
        else:raise ValueError('Selected public mutation required')
    if deleted_objects&endpoints:raise ValueError('Created edge endpoint cannot be deleted in group')
    model=snapshot(paths[1],1048576);document=decode_custody_bytes(model)
    if type(document)is not dict or(document.get('id'),document.get('umf'))!=(raw['document_id'],raw['source_version']):raise ValueError('Original source correspondence required')
    origin=InvocationFile.read(paths[2],65536);origin_map=decode_custody_bytes(origin.raw)
    if type(origin_map)is not dict or len(origin_map)>256 or any(type(v)is not str or len(v.encode())>4096 or '\0'in v for v in origin_map.values()):raise ValueError('Bounded original String origin required')
    for key in origin_map:name(key)
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink()or any(destination==p or destination in p.parents or p in destination.parents for p in(path,*paths)):raise ValueError('Fresh separate report required')
    return MutationGroupConfiguration(raw['connection_uri'],paths[0],paths[1],raw['document_revision'],raw['document_id'],raw['source_version'],paths[2],raw['request_key'],tuple(selections)),destination,original,model,origin,tuple(origin_map.items())


def run_mutate_truss_record_group_command(configuration: str|Path,output: str|Path) -> dict:
    config,destination,original,model,origin,origin_map=preflight(configuration,output)
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,ObjectFound,ObjectNotFound,EdgeFound,EdgeNotFound
    from truss.contracts import TextValue,BooleanValue,NullValue,MapValue,MapEntry
    from truss.groups import CreateEdge,Delete,StoredObjectReference,AuthoredValue,NullOrderKey,TextOrderKey,Created,Deleted,GroupSuccess,CommittedReceiptReplay
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss mutation refused')
        return result.value
    def scalar(kind,body):return NullValue()if kind=='null'else BooleanValue(value=body)if kind=='boolean'else TextValue(kind=kind,text=body)
    def expected(values):return {key:scalar(kind,body)if kind in('integer','decimal')else body for key,kind,body in values}
    def renew():
        original.renew(65536);origin.renew(65536)
        if snapshot(config.document_path,1048576)!=model:raise ValueError('Original mutation source changed')
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
    def lookup(assembly,tx,selection,entity_id=None):
        entity=selection.entity_id if entity_id is None else entity_id
        if selection.operation=='delete_object':return value(assembly.get_object_in_transaction(tx,document_id=config.document_id,module_id=selection.module_id,record_id=selection.element_id,object_id=entity))
        return value(assembly.get_edge_in_transaction(tx,document_id=config.document_id,module_id=selection.module_id,relationship_id=selection.element_id,edge_id=entity))
    documents=(CatalogDocument(config.document_id,config.document_revision,model),)
    asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind='string',text=v))for k,v in origin_map))
    renew();connection=psycopg.connect(config.connection_uri,connect_timeout=5);primary=None;report=None
    try:
        with owned_context(interval(connection))as(assembly,tx):
            operations=[];before=[];stored={};definition_pins=[]
            def reference(endpoint):
                found=value(assembly.get_object_in_transaction(tx,document_id=endpoint.document_id,module_id=endpoint.module_id,record_id=endpoint.record_id,object_id=endpoint.entity_id))
                if not isinstance(found,ObjectFound)or(found.object.document_id,found.object.module_id,found.object.record_id,found.object.id)!=(endpoint.document_id,endpoint.module_id,endpoint.record_id,endpoint.entity_id):raise RuntimeError('Actual authorized endpoint required')
                stored[endpoint]=found.object.identity;return StoredObjectReference(identity=found.object.identity)
            for selection in config.operations:
                object_kind=selection.operation=='delete_object'
                ref=value(assembly.get_type_reference_in_transaction(tx,document_id=config.document_id,module_id=selection.module_id,record_id=selection.element_id))if object_kind else value(assembly.get_relationship_reference_in_transaction(tx,document_id=config.document_id,module_id=selection.module_id,relationship_id=selection.element_id))
                if ref is None:raise RuntimeError('Current authorized definition required')
                definition_pins.append(ref.definition_pin)
                if selection.operation=='create_edge':
                    operations.append(CreateEdge(relationship_definition_pin=ref.definition_pin,source=reference(selection.source),target=reference(selection.target),values=tuple(AuthoredValue(name=k,value=scalar(kind,body))for k,kind,body in selection.values),order_key=NullOrderKey()if selection.order_key[0]=='null'else TextOrderKey(text=selection.order_key[1]),alias=selection.alias));before.append(None)
                else:
                    found=lookup(assembly,tx,selection);kind=ObjectFound if object_kind else EdgeFound
                    if not isinstance(found,kind):raise RuntimeError('Original selected entity required')
                    entity=found.object if object_kind else found.edge
                    if int(entity.identity.id)!=selection.entity_id or entity.identity.definition_pin!=ref.definition_pin or entity.version!=selection.expected_version:raise RuntimeError('Current original identity/pin/version required')
                    if object_kind and(entity.document_id,entity.module_id,entity.record_id,entity.id)!=(config.document_id,selection.module_id,selection.element_id,selection.entity_id):raise RuntimeError('Complete original record identity required')
                    operations.append(Delete(operation=selection.operation,target=StoredObjectReference(identity=entity.identity),expected_version=str(selection.expected_version)));before.append(entity)
            context=value(assembly.group_context_in_transaction(tx));semantic=context.input(tuple(operations),asserted);request=context.request(config.request_key)
            renew();applied=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(applied,GroupSuccess)or len(applied.response.semantic.results)!=len(config.operations):raise RuntimeError('Complete ordered mutation group required')
            results=applied.response.semantic.results;aliases=[]
            for selection,prior,result,pin in zip(config.operations,before,results,definition_pins):
                if result.operation!=selection.operation:raise RuntimeError('Original operation order required')
                if selection.operation=='create_edge':
                    if not isinstance(result,Created)or result.alias!=selection.alias or int(result.version)!=1 or result.identity.definition_pin!=pin:raise RuntimeError('Original created edge response required')
                    aliases.append((selection.alias,result.identity))
                elif not isinstance(result,Deleted)or result.identity!=prior.identity or int(result.deletion_version)!=selection.expected_version+1 or len(result.deleted)!=1 or result.deleted[0].identity!=prior.identity or result.deleted[0].deletion_version!=result.deletion_version:raise RuntimeError('Complete original deleted response required')
            if tuple((alias.alias,alias.identity)for alias in applied.response.semantic.aliases)!=tuple(aliases):raise RuntimeError('Complete created alias inventory required')
            renew()
        connection.commit()  # No retry of unknown commit outcomes.
        with owned_context(interval(connection))as(assembly,tx):
            observations=[]
            for selection,result in zip(config.operations,results):
                observed=lookup(assembly,tx,selection,int(result.identity.id))
                if selection.operation=='create_edge':
                    if not isinstance(observed,EdgeFound)or observed.edge.identity!=result.identity or observed.edge.version!=int(result.version)or(observed.edge.source,observed.edge.target)!=(stored[selection.source],stored[selection.target])or observed.edge.order_key!=selection.order_key[1]or len(observed.edge.retained)!=len(selection.values)or dict(observed.edge.retained)!=expected(selection.values):raise RuntimeError('Complete committed created edge required')
                elif not isinstance(observed,ObjectNotFound if selection.operation=='delete_object'else EdgeNotFound):raise RuntimeError('Original deleted entity remains readable')
                observations.append(asdict(observed))
            replay=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=applied.response.semantic:raise RuntimeError('Original committed semantic replay required')
            report={'profile':PROFILE,'source_version':config.source_version,'group_original':asdict(applied),'committed_receipt_replay':asdict(replay),'before':[asdict(v)if v is not None else None for v in before],'observed':observations,'qualification':'One original public atomic mutation group, readback and replay; no publication/feed/ACK qualification.'};renew()
        connection.commit()
    except BaseException as error:primary=error
    finish(primary,[connection.close,renew]);release_report(destination,report_body(report,1048576));return report
