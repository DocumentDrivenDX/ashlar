#!/usr/bin/env python3
"""Accept a compatible Note revision, then use its new field through public Truss."""
import argparse,base64,json,sys
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
    from truss import (Ok,CatalogDocument,CatalogRuntimeConfiguration,CatalogRegistration,
        AcceptedDocument,ObjectFound,create_reference_assembly,adopt_psycopg_host)
    from truss.contracts import MapValue,MapEntry,TextValue
    from truss.groups import UpdateObject,StoredObjectReference,AuthoredValue,GroupSuccess,CommittedReceiptReplay
    config=json.loads(read(configuration,65536))
    required={'connection_uri','bun','document_path','document_revision','origin_path','accept_request_key','update_request_key','object_id','tag'}
    if type(config)is not dict or set(config)!=required:raise ValueError('Exact Note evolution configuration required')
    if type(config['object_id'])is not int or not 1<=config['object_id']<=9223372036854775807:raise ValueError('Actual positive object ID required')
    if any(type(config[k])is not str or not config[k]for k in ('document_revision','accept_request_key','update_request_key','tag'))or config['accept_request_key']==config['update_request_key']:raise ValueError('Explicit revision, tag and distinct request keys required')
    source=read(config['document_path'],1048576)
    if json.loads(source).get('id')!='ashlar-note':raise ValueError('Original authored ashlar-note revision required')
    document=CatalogDocument('ashlar-note',config['document_revision'],source)
    bun=Path(config['bun'])
    if not bun.is_absolute()or not bun.is_file():raise ValueError('Selected installed Bun required')
    origin_bytes=read(config['origin_path'],65536);origin=json.loads(origin_bytes)
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
    def current(assembly,tx):
        found=value(assembly.get_object_in_transaction(tx,document_id='ashlar-note',module_id='notes',record_id='Note',object_id=config['object_id']))
        if not isinstance(found,ObjectFound):raise RuntimeError('Current authorized Note required')
        return found.object
    with psycopg.connect(config['connection_uri'],connect_timeout=5)as connection:
        with interval(connection)as (assembly,tx):
            before=current(assembly,tx)
            if set(dict(before.values))!={'note-id','note-text'}:raise RuntimeError('Original complete r1 Note required')
            previous_document=value(assembly.accepted_document_in_transaction(tx,document_id='ashlar-note'))
            if not isinstance(previous_document,AcceptedDocument)or previous_document.document_revision==config['document_revision']:raise RuntimeError('Distinct current accepted document revision required')
            old_report=value(assembly.report_in_transaction(tx,before.catalog_revision))
            prepared=assembly.prepare_catalog((document,),bun_executable=str(bun))
            accepted=value(assembly.accept_in_transaction(tx,prepared,request_key=config['accept_request_key'],origin_bytes=origin_bytes))
            if not isinstance(accepted,CatalogRegistration)or accepted.revision!=before.catalog_revision+1:raise RuntimeError('Actual next compatible catalog registration required')
        connection.commit()
        with interval(connection)as (assembly,tx):
            published=value(assembly.accepted_document_in_transaction(tx,document_id='ashlar-note'))
            if not isinstance(published,AcceptedDocument)or published.content!=source or published.document_revision!=config['document_revision']or published.catalog_revision!=accepted.revision:raise RuntimeError('Exact committed revised document differs')
            report=value(assembly.report_in_transaction(tx,accepted.revision));evolved=current(assembly,tx)
            reference=value(assembly.get_type_reference_in_transaction(tx,document_id='ashlar-note',module_id='notes',record_id='Note'))
            if reference is None or reference.definition_pin!=evolved.identity.definition_pin:raise RuntimeError('Actual current identity/pin required')
            if (evolved.id,evolved.type_id,evolved.version,evolved.values,evolved.retained)!=(before.id,before.type_id,before.version,before.values,before.retained)or evolved.definition_pin==before.definition_pin:raise RuntimeError('Evolution changed original identity, values or version')
            context=value(assembly.group_context_in_transaction(tx))
            original=context.input((UpdateObject(target=StoredObjectReference(identity=evolved.identity),set=(AuthoredValue(name='note-tag',value=TextValue(kind='string',text=config['tag'])),),unset=(),expected_version=str(evolved.version)),),asserted)
            request=context.request(config['update_request_key'])
            changed=value(assembly.apply_in_transaction(tx,original,request,documents=(document,),bun_executable=str(bun)))
            if not isinstance(changed,GroupSuccess)or len(changed.response.semantic.results)!=1:raise RuntimeError('New-field update refused')
        connection.commit()
        with interval(connection)as (assembly,tx):
            after=current(assembly,tx);expected=dict(before.values);expected['note-tag']=config['tag']
            if dict(after.values)!=expected or (after.id,after.type_id,after.retained)!=(before.id,before.type_id,before.retained)or after.version!=before.version+1 or after.identity!=evolved.identity:raise RuntimeError('Complete evolved Note readback differs')
            replay=value(assembly.apply_in_transaction(tx,original,request,documents=(document,),bun_executable=str(bun)))
            if not isinstance(replay,GroupSuccess)or not isinstance(replay.response,CommittedReceiptReplay)or replay.response.semantic!=changed.response.semantic:raise RuntimeError('Original committed update receipt differs')
            result={'before':asdict(before),'evolved':asdict(evolved),'after':asdict(after),
                'previous_document':{**{k:v for k,v in asdict(previous_document).items()if k!='content'},'content_original_base64':base64.b64encode(previous_document.content).decode('ascii')},
                'accepted_document':{**{k:v for k,v in asdict(published).items()if k!='content'},'content_original_base64':base64.b64encode(published.content).decode('ascii')},
                'previous_report_original_base64':base64.b64encode(old_report.original_bytes).decode('ascii'),
                'report_original_base64':base64.b64encode(report.original_bytes).decode('ascii'),
                'registration':{'revision':accepted.revision,'registration_id':accepted.registration_id,'replayed':accepted.replayed,'status':accepted.status,'report_original_base64':base64.b64encode(accepted.report_bytes).decode('ascii')},
                'update_original':asdict(changed),'update_committed_receipt':asdict(replay),'caller_commits_confirmed':True,
                'scope':'Compatible Note optional-field evolution and update/replay only; no feed, ACK or Delta integration.'}
        connection.commit()
    with destination.open('x',encoding='utf8')as stream:json.dump(result,stream,indent=2,ensure_ascii=False);stream.write('\n')

if __name__=='__main__':
    parser=argparse.ArgumentParser(description=__doc__);parser.add_argument('--configuration',required=True);parser.add_argument('--output',required=True);args=parser.parse_args()
    try:run(args.configuration,args.output)
    except Exception:
        print('Note evolution refused; inspect durable outcomes before retrying.',file=sys.stderr);raise SystemExit(2)from None
