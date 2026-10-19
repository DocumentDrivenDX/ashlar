# SPDX-License-Identifier: MIT OR Apache-2.0
"""Atomic public typed-record/link groups; source interpretation belongs to Truss."""
from __future__ import annotations
from contextlib import contextmanager
from dataclasses import dataclass,asdict,field
from pathlib import Path
import os,tempfile
from .evolution_cli import InvocationFile
from .evolution_producer import snapshot
from .private_graph_custody import decode_custody_bytes
from .truss_record_mutation import scalar_tag
from .lifecycle import finish,owned_context
from .exact_report import report_body

PROFILE='ashlar-truss-record-group/0.1'


def name(value: object) -> str:
    if type(value)is not str or not value or len(value.encode())>4096 or '\x00' in value:
        raise ValueError('Bounded original group name required')
    return value


def closed(value: object, keys: tuple) -> dict:
    if type(value)is not dict or set(value)!=set(keys):raise ValueError('Closed record group required')
    return value


def authored_values(value: object) -> tuple:
    if type(value)is not dict or len(value)>256:raise ValueError('Bounded authored values required')
    return tuple((name(key),*scalar_tag(item))for key,item in value.items())


@dataclass(frozen=True)
class EndpointSelection:
    state:str
    alias:str|None=None
    document_id:str|None=None
    module_id:str|None=None
    record_id:str|None=None
    entity_id:int|None=None


@dataclass(frozen=True)
class RecordSelection:
    alias:str
    module_id:str
    record_id:str
    values:tuple


@dataclass(frozen=True)
class EdgeSelection:
    alias:str
    module_id:str
    relationship_id:str
    source:EndpointSelection
    target:EndpointSelection
    values:tuple
    order_key:tuple


@dataclass(frozen=True)
class RecordGroupConfiguration:
    connection_uri:str=field(repr=False)
    bun:Path
    document_path:Path
    document_revision:str
    document_id:str
    source_version:str
    origin_path:Path
    request_key:str
    records:tuple[RecordSelection,...]
    edges:tuple[EdgeSelection,...]


def endpoint(raw: object, aliases: frozenset, document_id: str) -> EndpointSelection:
    if type(raw)is not dict:raise ValueError('Closed endpoint required')
    if raw.get('state')=='alias':
        closed(raw,('state','alias'))
        if name(raw['alias'])not in aliases:raise ValueError('Prior created record alias required')
        return EndpointSelection('alias',alias=raw['alias'])
    closed(raw,('state','document_id','module_id','record_id','entity_id'))
    if raw['state']!='stored' or name(raw['document_id'])!=document_id or type(raw['entity_id'])is not int or not 1<=raw['entity_id']<2**63:
        raise ValueError('Original document endpoint selection required')
    return EndpointSelection('stored',document_id=document_id,module_id=name(raw['module_id']),record_id=name(raw['record_id']),entity_id=raw['entity_id'])


def preflight(configuration: str | Path, output: str | Path) -> tuple:
    """Freeze exact selections and source bytes without constructing any client."""
    path=Path(configuration);destination=Path(output)
    if not path.is_absolute() or path.resolve()!=path:raise ValueError('Canonical configuration required')
    original=InvocationFile.read(path,65536);value=decode_custody_bytes(original.raw)
    closed(value,('profile','connection_uri','bun','document_path','document_revision','document_id','source_version','origin_path','request_key','records','edges'))
    if value['profile']!=PROFILE or value['source_version']not in ('0.7.0','0.8.0'):raise ValueError('Explicit selected source version required')
    for key in ('connection_uri','document_revision','document_id','request_key'):name(value[key])
    selected=[]
    for key in ('bun','document_path','origin_path'):
        p=Path(name(value[key]))
        if not p.is_absolute()or p.resolve()!=p or not p.is_file()or any(q.is_symlink()for q in(p,*p.parents)):raise ValueError('Original canonical file required')
        selected.append(p)
    if type(value['records'])is not list or not 1<=len(value['records'])<=16 or type(value['edges'])is not list or not 1<=len(value['edges'])<=32:raise ValueError('Bounded records and links required')
    records=[];edges=[];aliases=set()
    for raw in value['records']:
        closed(raw,('alias','module_id','record_id','values'));alias=name(raw['alias'])
        if alias in aliases:raise ValueError('Unique original alias required')
        aliases.add(alias);records.append(RecordSelection(alias,name(raw['module_id']),name(raw['record_id']),authored_values(raw['values'])))
    record_aliases=frozenset(aliases)
    for raw in value['edges']:
        closed(raw,('alias','module_id','relationship_id','source','target','values','order_key'));alias=name(raw['alias'])
        if alias in aliases:raise ValueError('Unique original alias required')
        aliases.add(alias);order=raw['order_key']
        if order=={'state':'null'}:order_key=('null',None)
        elif type(order)is dict and set(order)=={'state','text'}and order['state']=='text'and type(order['text'])is str and len(order['text'].encode())<=65536:order_key=('text',order['text'])
        else:raise ValueError('Exact null or text order key required')
        edges.append(EdgeSelection(alias,name(raw['module_id']),name(raw['relationship_id']),endpoint(raw['source'],record_aliases,value['document_id']),endpoint(raw['target'],record_aliases,value['document_id']),authored_values(raw['values']),order_key))
    model=snapshot(selected[1],1048576);document=decode_custody_bytes(model)
    if type(document)is not dict or document.get('id')!=value['document_id']or document.get('umf')!=value['source_version']:raise ValueError('Original declared source correspondence required')
    origin=InvocationFile.read(selected[2],65536);origin_map=decode_custody_bytes(origin.raw)
    if type(origin_map)is not dict or len(origin_map)>256 or any(type(v)is not str or len(v.encode())>4096 or '\x00'in v for v in origin_map.values()):raise ValueError('Bounded original String origin required')
    for key in origin_map:name(key)
    if not destination.is_absolute()or destination.parent.resolve()!=destination.parent or not destination.parent.is_dir()or destination.exists()or destination.is_symlink()or any(destination==p or destination in p.parents or p in destination.parents for p in(path,*selected)):raise ValueError('Fresh separate canonical report required')
    config=RecordGroupConfiguration(value['connection_uri'],selected[0],selected[1],value['document_revision'],value['document_id'],value['source_version'],selected[2],value['request_key'],tuple(records),tuple(edges))
    return config,destination,original,model,origin,tuple(origin_map.items())


def release_report(destination: Path, raw: bytes) -> None:
    """Close a bounded private stage before no-clobber final availability; no crash claim."""
    if type(raw)is not bytes or not 0<len(raw)<=1048576 or not isinstance(destination,Path)or not destination.is_absolute()or destination.parent.resolve()!=destination.parent:
        raise ValueError('Bounded private report required')
    fd,stage=tempfile.mkstemp(prefix='.truss-group-',dir=destination.parent);primary=None
    try:
        offset=0
        while offset<len(raw):
            count=os.write(fd,raw[offset:])
            if type(count)is not int or count<=0:raise OSError('Complete report write required')
            offset+=count
        os.fsync(fd)
    except BaseException as error:primary=error
    try:finish(primary,[lambda:os.close(fd)])
    except BaseException as error:
        finish(error,[lambda:os.unlink(stage)])
    primary=None
    try:os.link(stage,destination)
    except BaseException as error:primary=error
    finish(primary,[lambda:os.unlink(stage)])


def run_create_truss_record_group_command(configuration: str | Path, output: str | Path) -> dict:
    config,destination,original,model,origin,origin_map=preflight(configuration,output)
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,ObjectFound,EdgeFound
    from truss.contracts import TextValue,BooleanValue,NullValue,MapValue,MapEntry
    from truss.groups import CreateObject,CreateEdge,RootlessOwnership,AuthoredValue,AliasObjectReference,StoredObjectReference,NullOrderKey,TextOrderKey,GroupSuccess,CommittedReceiptReplay
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss group refused')
        return result.value
    def scalar(kind,body):return NullValue()if kind=='null'else BooleanValue(value=body)if kind=='boolean'else TextValue(kind=kind,text=body)
    def authored(values):return tuple(AuthoredValue(name=key,value=scalar(kind,body))for key,kind,body in values)
    def expected(values):return {key:scalar(kind,body)if kind in('integer','decimal')else body for key,kind,body in values}
    def renew():
        original.renew(65536);origin.renew(65536)
        if snapshot(config.document_path,1048576)!=model:raise ValueError('Original group source changed')
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
    asserted=MapValue(entries=tuple(MapEntry(key=key,value=TextValue(kind='string',text=body))for key,body in origin_map))
    renew();connection=psycopg.connect(config.connection_uri,connect_timeout=5);primary=None;report=None
    try:
        with owned_context(interval(connection))as(assembly,tx):
            operations=[];stored={}
            for record in config.records:
                ref=value(assembly.get_type_reference_in_transaction(tx,document_id=config.document_id,module_id=record.module_id,record_id=record.record_id))
                if ref is None:raise RuntimeError('Current authorized type required')
                operations.append(CreateObject(type_definition_pin=ref.definition_pin,values=authored(record.values),ownership=RootlessOwnership(),alias=record.alias))
            def reference(selection):
                if selection.state=='alias':return AliasObjectReference(alias=selection.alias)
                found=value(assembly.get_object_in_transaction(tx,document_id=selection.document_id,module_id=selection.module_id,record_id=selection.record_id,object_id=selection.entity_id))
                if not isinstance(found,ObjectFound)or(found.object.document_id,found.object.module_id,found.object.record_id,found.object.id)!=(selection.document_id,selection.module_id,selection.record_id,selection.entity_id):raise RuntimeError('Actual authorized stored endpoint required')
                stored[selection]=found.object.identity;return StoredObjectReference(identity=found.object.identity)
            for edge in config.edges:
                ref=value(assembly.get_relationship_reference_in_transaction(tx,document_id=config.document_id,module_id=edge.module_id,relationship_id=edge.relationship_id))
                if ref is None:raise RuntimeError('Current authorized relationship required')
                operations.append(CreateEdge(relationship_definition_pin=ref.definition_pin,source=reference(edge.source),target=reference(edge.target),values=authored(edge.values),order_key=NullOrderKey()if edge.order_key[0]=='null'else TextOrderKey(text=edge.order_key[1]),alias=edge.alias))
            context=value(assembly.group_context_in_transaction(tx));semantic=context.input(tuple(operations),asserted);request=context.request(config.request_key)
            renew();applied=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(applied,GroupSuccess):raise RuntimeError('Complete atomic group required')
            results=applied.response.semantic.results;selections=(*config.records,*config.edges)
            if len(results)!=len(selections)or any(result.operation!=('create_object'if index<len(config.records)else'create_edge')or result.outcome!='created'or result.alias!=selections[index].alias for index,result in enumerate(results)):raise RuntimeError('Complete ordered created responses required')
            identities={selection.alias:result.identity for selection,result in zip(selections,results)}
            if tuple((a.alias,a.identity)for a in applied.response.semantic.aliases)!=tuple(identities.items()):raise RuntimeError('Complete original aliases required')
            renew()
        connection.commit()  # One effect commit; never automatically retry uncertainty.
        with owned_context(interval(connection))as(assembly,tx):
            objects=[];edges=[]
            for selection,result in zip(config.records,results[:len(config.records)]):
                found=value(assembly.get_object_in_transaction(tx,document_id=config.document_id,module_id=selection.module_id,record_id=selection.record_id,object_id=int(result.identity.id)))
                if not isinstance(found,ObjectFound)or found.object.identity!=result.identity or found.object.version!=int(result.version)or(found.object.document_id,found.object.module_id,found.object.record_id,found.object.id)!=(config.document_id,selection.module_id,selection.record_id,int(result.identity.id))or len(found.object.values)!=len(selection.values)or found.object.retained!=()or dict(found.object.values)!=expected(selection.values):raise RuntimeError('Complete original object readback required')
                objects.append(asdict(found.object))
            for selection,result in zip(config.edges,results[len(config.records):]):
                found=value(assembly.get_edge_in_transaction(tx,document_id=config.document_id,module_id=selection.module_id,relationship_id=selection.relationship_id,edge_id=int(result.identity.id)))
                def identity(endpoint):return identities[endpoint.alias]if endpoint.state=='alias'else stored[endpoint]
                if not isinstance(found,EdgeFound)or found.edge.identity!=result.identity or found.edge.version!=int(result.version)or found.edge.source!=identity(selection.source)or found.edge.target!=identity(selection.target)or found.edge.order_key!=selection.order_key[1]or len(found.edge.retained)!=len(selection.values)or dict(found.edge.retained)!=expected(selection.values):raise RuntimeError('Complete original edge readback required')
                edges.append(asdict(found.edge))
            replay=value(assembly.apply_in_transaction(tx,semantic,request,documents=documents,bun_executable=str(config.bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=applied.response.semantic:raise RuntimeError('Original committed semantic replay required')
            report={'profile':PROFILE,'source_version':config.source_version,'group_original':asdict(applied),'committed_receipt_replay':asdict(replay),'objects':objects,'edges':edges,'qualification':'One ordinary public atomic record/link group and exact readback/replay; no publication/feed/ACK/query or inferred required-edge enforcement qualification.'}
            renew()
        connection.commit()  # Commit the separately observed replay transaction.
    except BaseException as error:primary=error
    finish(primary,[connection.close,renew])
    release_report(destination,report_body(report,1048576));return report
