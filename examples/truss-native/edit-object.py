#!/usr/bin/env python3
"""Create and edit a Note's authored non-key text through public Truss groups."""
import argparse,json,sys
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from ashlar_host.lifecycle import finish

def read(path,maximum):
    with Path(path).open('rb')as stream:raw=stream.read(maximum+1)
    if len(raw)>maximum:raise ValueError('Selected input exceeds example bound')
    return raw

def run(configuration,output):
    import psycopg
    from truss import Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,adopt_psycopg_host,ObjectFound
    from truss.contracts import MapValue,MapEntry,TextValue
    from truss.groups import CreateObject,UpdateObject,RootlessOwnership,AuthoredValue,StoredObjectReference,GroupSuccess,CommittedReceiptReplay
    config=json.loads(read(configuration,65536))
    if type(config)is not dict or set(config)!={'connection_uri','bun','document_path','document_revision','origin_path','create_request_key','update_request_key','key','text','updated_text'}:raise ValueError('Exact Note edit configuration required')
    if any(type(config[k])is not str or not config[k]for k in ('create_request_key','update_request_key','key','text','updated_text'))or config['create_request_key']==config['update_request_key']or config['text']==config['updated_text']:raise ValueError('Distinct keys and changed string text required')
    source=read(config['document_path'],1048576)
    if json.loads(source).get('id')!='ashlar-note':raise ValueError('Original authored ashlar-note document required')
    documents=(CatalogDocument('ashlar-note',config['document_revision'],source),)
    bun=Path(config['bun'])
    if not bun.is_absolute()or not bun.is_file():raise ValueError('Selected installed Bun required')
    origin=json.loads(read(config['origin_path'],65536))
    if type(origin)is not dict or any(type(v)is not str for v in origin.values()):raise ValueError('String-valued original origin map required')
    asserted=MapValue(entries=tuple(MapEntry(key=k,value=TextValue(kind='string',text=v))for k,v in origin.items()))
    destination=Path(output)
    if destination.exists():raise ValueError('Fresh report required')
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
    def apply(assembly,tx,operation,key):
        context=value(assembly.group_context_in_transaction(tx));original=context.input((operation,),asserted);request=context.request(key)
        result=value(assembly.apply_in_transaction(tx,original,request,documents=documents,bun_executable=str(bun)))
        if not isinstance(result,GroupSuccess)or len(result.response.semantic.results)!=1:raise RuntimeError('Complete object mutation refused')
        return result,original,request
    def replay(assembly,tx,original):
        result=value(assembly.apply_in_transaction(tx,original[1],original[2],documents=documents,bun_executable=str(bun)))
        if not isinstance(result,GroupSuccess)or not isinstance(result.response,CommittedReceiptReplay)or result.response.semantic!=original[0].response.semantic or type(result.response.receipt_token)is not str:raise RuntimeError('Original committed receipt unavailable')
        return asdict(result)
    def current(assembly,tx,identity,text):
        reference=value(assembly.get_type_reference_in_transaction(tx,document_id='ashlar-note',module_id='notes',record_id='Note'))
        if reference is None or reference.definition_pin!=identity.definition_pin:raise RuntimeError('Current Note type authority required')
        found=value(assembly.get_object_in_transaction(tx,document_id='ashlar-note',module_id='notes',record_id='Note',object_id=int(identity.id)))
        if not isinstance(found,ObjectFound)or found.object.type_id!=int(identity.type_id)or dict(found.object.values)!={'note-id':config['key'],'note-text':text}:raise RuntimeError('Complete original Note readback differs')
        return found.object
    with psycopg.connect(config['connection_uri'],connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            reference=value(assembly.get_type_reference_in_transaction(tx,document_id='ashlar-note',module_id='notes',record_id='Note'))
            if reference is None:raise RuntimeError('Current Note type authority required')
            create=apply(assembly,tx,CreateObject(type_definition_pin=reference.definition_pin,
                values=(AuthoredValue(name='note-id',value=TextValue(kind='string',text=config['key'])),AuthoredValue(name='note-text',value=TextValue(kind='string',text=config['text']))),ownership=RootlessOwnership(),alias='note'),config['create_request_key'])
            created=create[0].response.semantic.results[0];identity=created.identity
        connection.commit()
        with interval(connection)as (assembly,tx):
            before=current(assembly,tx,identity,config['text'])
            if before.version!=int(created.version):raise RuntimeError('Created version differs')
            create_receipt=replay(assembly,tx,create)
            update=apply(assembly,tx,UpdateObject(target=StoredObjectReference(identity=identity),set=(AuthoredValue(name='note-text',value=TextValue(kind='string',text=config['updated_text'])),),unset=(),expected_version=str(before.version)),config['update_request_key'])
        connection.commit()
        with interval(connection)as (assembly,tx):
            after=current(assembly,tx,identity,config['updated_text'])
            if after.id!=before.id or after.type_id!=before.type_id or after.version!=before.version+1 or after.retained!=before.retained:raise RuntimeError('Original key/identity/version correspondence differs')
            update_receipt=replay(assembly,tx,update)
            result={'create_original':asdict(create[0]),'create_committed_receipt':create_receipt,'before':asdict(before),
                'update_original':asdict(update[0]),'update_committed_receipt':update_receipt,'after':asdict(after),
                'scope':'Authored Note create/non-key text update/readback/receipt only; no schema evolution or feed.'}
        connection.commit()
    with destination.open('x',encoding='utf8')as stream:json.dump(result,stream,indent=2,ensure_ascii=False);stream.write('\n')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Object edit example refused; inspect commit outcomes before retrying.',file=sys.stderr);raise SystemExit(2)from None
