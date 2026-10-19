# SPDX-License-Identifier: MIT OR Apache-2.0
"""Update then delete the original cohort pair using current authorized Truss reads."""
import json
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from .lifecycle import finish

def read(path,maximum):
    with Path(path).open('rb')as stream:raw=stream.read(maximum+1)
    if len(raw)>maximum:raise ValueError('Selected input exceeds example bound')
    return raw

def run_change_truss_graph_command(configuration,output):
    import psycopg
    from truss import (Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,
        adopt_psycopg_host,ObjectFound,ObjectNotFound,EdgeFound,EdgeNotFound)
    from truss.contracts import MapValue,MapEntry,TextValue
    from truss.groups import (UpdateEdge,Delete,StoredObjectReference,TextOrderKey,
        AuthoredValue,GroupSuccess,CommittedReceiptReplay)
    config=json.loads(read(configuration,65536))
    if type(config)is not dict or set(config)!={'connection_uri','bun','document_path','document_revision','origin_path','update_request_key','delete_request_key','prior_report','note','order_key'}:raise ValueError('Exact change configuration required')
    if any(type(config[k])is not str or not config[k]for k in ('update_request_key','delete_request_key','note','order_key'))or config['update_request_key']==config['delete_request_key']:raise ValueError('Distinct request keys and explicit string changes required')
    source=read(config['document_path'],1048576)
    if json.loads(source).get('id')!='cohort':raise ValueError('Original cohort document required')
    documents=(CatalogDocument('cohort',config['document_revision'],source),)
    bun=Path(config['bun'])
    if not bun.is_absolute()or not bun.is_file():raise ValueError('Selected installed Bun required')
    origin=json.loads(read(config['origin_path'],65536))
    if type(origin)is not dict or any(type(v)is not str for v in origin.values()):raise ValueError('String-valued original origin map required')
    asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind='string',text=v))for k,v in origin.items()))
    prior=json.loads(read(config['prior_report'],1048576))
    edge_id=int(prior['edge']['identity']['id'])
    object_ids=tuple(int(item['id'])for item in prior['objects'])
    if len(object_ids)!=2:raise ValueError('Select original pair report')
    destination=Path(output)
    if destination.exists():raise ValueError('Fresh result path required')
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss operation refused')
        return result.value
    @contextmanager
    def interval(connection):
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction());assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host))
            yield assembly,tx
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('Host detach refused; recovery required')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    def apply(assembly,tx,operations,key):
        context=value(assembly.group_context_in_transaction(tx))
        original_input=context.input(tuple(operations),asserted);request=context.request(key)
        applied=value(assembly.apply_in_transaction(tx,original_input,request,documents=documents,bun_executable=str(bun)))
        if not isinstance(applied,GroupSuccess):raise RuntimeError('Mutation group refused')
        return applied,original_input,request
    def replay(assembly,tx,original):
        applied,original_input,request=original
        observed=value(assembly.apply_in_transaction(tx,original_input,request,documents=documents,bun_executable=str(bun)))
        if not isinstance(observed,GroupSuccess)or not isinstance(observed.response,CommittedReceiptReplay)or observed.response.semantic!=applied.response.semantic or type(observed.response.receipt_token)is not str:raise RuntimeError('Original committed receipt unavailable')
        return asdict(observed)
    with psycopg.connect(config['connection_uri'],connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            refs={record:value(assembly.get_type_reference_in_transaction(tx,document_id='cohort',module_id='m',record_id=record))for record in ('A','B')}
            relationship=value(assembly.get_relationship_reference_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B'))
            if relationship is None or any(ref is None for ref in refs.values()):raise RuntimeError('Current cohort authority required')
            found=value(assembly.get_edge_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B',edge_id=edge_id))
            if not isinstance(found,EdgeFound):raise RuntimeError('Current edge required')
            edge=found.edge
            if (edge.source.id,edge.target.id)!=tuple(str(i)for i in object_ids)or edge.identity.definition_pin!=relationship.definition_pin:raise RuntimeError('Selected report and current edge differ')
            before=[]
            for record,identifier,identity in zip(('A','B'),object_ids,(edge.source,edge.target)):
                found=value(assembly.get_object_in_transaction(tx,document_id='cohort',module_id='m',record_id=record,object_id=identifier))
                if not isinstance(found,ObjectFound)or identity.definition_pin!=refs[record].definition_pin or found.object.type_id!=int(identity.type_id):raise RuntimeError('Current original object authority required')
                before.append(found.object)
            updated=apply(assembly,tx,(UpdateEdge(target=StoredObjectReference(identity=edge.identity),
                set=(AuthoredValue(name='note',value=TextValue(kind='string',text=config['note'])),),unset=(),
                order_key_change=TextOrderKey(text=config['order_key']),expected_version=str(edge.version)),),config['update_request_key'])
        connection.commit()
        with interval(connection)as (assembly,tx):
            observed=value(assembly.get_edge_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B',edge_id=edge_id))
            if not isinstance(observed,EdgeFound):raise RuntimeError('Updated edge missing')
            after=observed.edge
            if after.identity!=edge.identity or after.source!=edge.source or after.target!=edge.target or after.version!=edge.version+1 or dict(after.retained)!={**dict(edge.retained),'note':config['note']}or after.order_key!=config['order_key']:raise RuntimeError('Complete edge update readback differs')
            update_receipt=replay(assembly,tx,updated)
            deletes=[Delete(operation='delete_edge',target=StoredObjectReference(identity=after.identity),expected_version=str(after.version))]
            for record,identifier,identity in zip(('A','B'),object_ids,(edge.source,edge.target)):
                found=value(assembly.get_object_in_transaction(tx,document_id='cohort',module_id='m',record_id=record,object_id=identifier))
                if not isinstance(found,ObjectFound):raise RuntimeError('Current delete object missing')
                deletes.append(Delete(operation='delete_object',target=StoredObjectReference(identity=identity),expected_version=str(found.object.version)))
            deleted=apply(assembly,tx,deletes,config['delete_request_key'])
        connection.commit()
        with interval(connection)as (assembly,tx):
            absent_edge=value(assembly.get_edge_in_transaction(tx,document_id='cohort',module_id='m',relationship_id='A-B',edge_id=edge_id))
            if not isinstance(absent_edge,EdgeNotFound):raise RuntimeError('Deleted edge still present')
            absences=[]
            for record,identifier in zip(('A','B'),object_ids):
                absent=value(assembly.get_object_in_transaction(tx,document_id='cohort',module_id='m',record_id=record,object_id=identifier))
                if not isinstance(absent,ObjectNotFound):raise RuntimeError('Deleted object still present')
                absences.append(asdict(absent))
            delete_receipt=replay(assembly,tx,deleted)
            result={'objects_before':[asdict(o)for o in before],'edge_before':asdict(edge),'edge_updated':asdict(after),
                'update_original':asdict(updated[0]),'update_committed_receipt':update_receipt,
                'delete_original':asdict(deleted[0]),'delete_committed_receipt':delete_receipt,
                'edge_absent':asdict(absent_edge),'objects_absent':absences,'scope':'Original cohort public update/delete/readback/receipt only.'}
        connection.commit()
    with destination.open('x',encoding='utf8')as stream:json.dump(result,stream,indent=2,ensure_ascii=False);stream.write('\n')
    return result

