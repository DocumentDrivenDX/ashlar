# SPDX-License-Identifier: MIT OR Apache-2.0
"""Create two cohort objects and their independent edge through public Truss groups."""
import json
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from .lifecycle import finish


def read(path,maximum):
    with Path(path).open('rb')as stream:raw=stream.read(maximum+1)
    if len(raw)>maximum:raise ValueError('Selected input exceeds example bound')
    return raw


def run_mutate_truss_graph_command(configuration,output):
    import psycopg
    from truss import (Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,
        adopt_psycopg_host,ObjectFound,EdgeFound)
    from truss.contracts import MapValue,MapEntry,TextValue
    from truss.groups import (CreateObject,CreateEdge,RootlessOwnership,AuthoredValue,
        AliasObjectReference,NullOrderKey,GroupSuccess,CommittedReceiptReplay)
    config=json.loads(read(configuration,65536))
    if type(config)is not dict or set(config)!={'connection_uri','bun','document_path','document_revision','request_key','origin_path','a','b'}:raise ValueError('Exact cohort mutation configuration required')
    for key in ('a','b'):
        if type(config[key])is not dict or set(config[key])!={'label','value'}or any(type(v)is not str for v in config[key].values()):raise ValueError('Exact string cohort values required')
    source=read(config['document_path'],1048576)
    if json.loads(source).get('id')!='cohort':raise ValueError('Original accepted cohort document required')
    documents=(CatalogDocument('cohort',config['document_revision'],source),)
    bun=Path(config['bun'])
    if not bun.is_absolute()or not bun.is_file():raise ValueError('Selected installed Bun required')
    original_origin=read(config['origin_path'],65536);origin=json.loads(original_origin)
    if type(origin)is not dict or any(type(v)is not str for v in origin.values()):raise ValueError('This example supports a string-valued origin map only')
    asserted_origin=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind='string',text=v))for k,v in origin.items()))
    destination=Path(output)
    if destination.exists():raise ValueError('Fresh report required')
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss operation refused')
        return result.value
    @contextmanager
    def assembly_interval(connection):
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction())
            assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host))
            yield assembly,tx
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('Host detach refused; recovery required')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    with psycopg.connect(config['connection_uri'],connect_timeout=5)as connection:
        with assembly_interval(connection)as (assembly,tx):
            references={name:value(assembly.get_type_reference_in_transaction(tx,document_id='cohort',module_id='m',record_id=name))for name in ('A','B')}
            relationship=value(assembly.get_relationship_reference_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B'))
            if relationship is None or any(v is None for v in references.values()):raise RuntimeError('Current authorized cohort references unavailable')
            operations=[]
            for name,alias in (('A','a'),('B','b')):
                operations.append(CreateObject(type_definition_pin=references[name].definition_pin,
                    values=tuple(AuthoredValue(name=name+'-'+field,value=TextValue(kind='string',text=config[alias][field]))for field in ('label','value')),
                    ownership=RootlessOwnership(),alias=alias))
            operations.append(CreateEdge(relationship_definition_pin=relationship.definition_pin,
                source=AliasObjectReference(alias='a'),target=AliasObjectReference(alias='b'),
                values=(),order_key=NullOrderKey(),alias='relationship'))
            context=value(assembly.group_context_in_transaction(tx))
            semantic_input=context.input(tuple(operations),asserted_origin)
            request=context.request(config['request_key'])
            applied=value(assembly.apply_in_transaction(tx,semantic_input,request,documents=documents,bun_executable=str(bun)))
            if not isinstance(applied,GroupSuccess):raise RuntimeError('Mutation group refused')
            created=applied.response.semantic.results
            if len(created)!=3:raise RuntimeError('Complete three-operation result required')
            identities=tuple(item.identity for item in created)
        connection.commit()  # No automatic retry if the commit result is unknown.
        with assembly_interval(connection)as (assembly,tx):
            objects=[]
            for name,identity in zip(('A','B'),identities[:2]):
                found=value(assembly.get_object_in_transaction(tx,document_id='cohort',module_id='m',record_id=name,object_id=int(identity.id)))
                if not isinstance(found,ObjectFound):raise RuntimeError('Committed object read unavailable')
                expected={name+'-'+field:config['a'if name=='A'else'b'][field]for field in ('label','value')}
                if dict(found.object.values)!=expected or found.object.id!=int(identity.id) or found.object.version!=int(created[len(objects)].version):raise RuntimeError('Original object values/version differ')
                objects.append(asdict(found.object))
            found=value(assembly.get_edge_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B',edge_id=int(identities[2].id)))
            if not isinstance(found,EdgeFound):raise RuntimeError('Committed edge read unavailable')
            if found.edge.identity!=identities[2]or found.edge.source!=identities[0]or found.edge.target!=identities[1]or found.edge.version!=int(created[2].version)or found.edge.order_key is not None or found.edge.retained:raise RuntimeError('Original edge identity/endpoints/version differ')
            replay=value(assembly.apply_in_transaction(tx,semantic_input,request,documents=documents,bun_executable=str(bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=applied.response.semantic or type(replay.response.receipt_token)is not str:raise RuntimeError('Original committed receipt replay unavailable')
            result={'group_original':asdict(applied),'committed_receipt_replay':asdict(replay),'objects':objects,'edge':asdict(found.edge),
                'scope':'One committed public cohort group and identity readback; no feed/ACK/import/query qualification.'}
        connection.commit()
    with destination.open('x',encoding='utf8')as stream:json.dump(result,stream,indent=2,ensure_ascii=False);stream.write('\n')
    return result

