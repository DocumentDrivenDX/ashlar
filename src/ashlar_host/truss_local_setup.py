"""Fresh local-only Truss Note setup through the installed public SDK."""
import base64,json
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from .lifecycle import finish

def read(path,maximum):
    with path.open('rb')as stream:raw=stream.read(maximum+1)
    if len(raw)>maximum:raise ValueError('Selected input exceeds example bound')
    return raw


def selections(configuration,output):
    """Validate the small local-only selection before optional imports or effects."""
    configuration=Path(configuration).resolve();output=Path(output)
    config=json.loads(read(configuration,65536))
    if type(config)is not dict or set(config)!={'data_directory','bun','document_path','document_id','document_revision','origin_path','request_key'}:raise ValueError('Exact local setup configuration required')
    paths={name:Path(config[name])for name in ('data_directory','bun','document_path','origin_path')}
    for path in (*paths.values(),output):
        if not path.is_absolute()or path.resolve()!=path:raise ValueError('Explicit canonical absolute paths required')
    if not paths['data_directory'].parent.is_dir()or not output.parent.is_dir():raise ValueError('Select existing parents for fresh data/report')
    if paths['data_directory'].exists()or output.exists():raise ValueError('Data directory and report must be fresh')
    if not paths['bun'].is_file():raise ValueError('Select installed Bun')
    for key in ('document_id','document_revision','request_key'):
        if type(config[key])is not str or not config[key]or len(config[key])>256:raise ValueError('Explicit document/revision/request selections required')
    if config['document_id']!='ashlar-note':raise ValueError('This setup supports the authored ashlar-note document')
    all_paths=[configuration,*paths.values(),output]
    for index,left in enumerate(all_paths):
        for right in all_paths[index+1:]:
            if left==right or left in right.parents or right in left.parents:raise ValueError('Selected paths must not overlap')
    source=read(paths['document_path'],1048576);origin=read(paths['origin_path'],65536)
    if json.loads(source).get('id')!=config['document_id']:raise ValueError('Original document identity differs')
    return config,paths,output,source,origin


def run_setup_truss_local_command(configuration,output):
    config,paths,output,source,origin=selections(configuration,output)
    import psycopg
    from psycopg import sql
    from truss import (LocalPostgres,Ok,CatalogDocument,CatalogRuntimeConfiguration,create_reference_assembly,
        adopt_psycopg_host,CatalogRegistration,CatalogUnchanged,ModuleAccessGrant)
    consumer='ashlar_note_consumer';reader='ashlar_note_reader';writer='ashlar_note_writer'
    def value(result):
        if not isinstance(result,Ok):raise RuntimeError('Public Truss setup operation refused')
        return result.value
    @contextmanager
    def interval(connection):
        connection.execute('BEGIN ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection);assembly=None;primary=None
        try:
            tx=value(host.adopt_transaction());assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host));yield assembly,tx
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('Host detach refused; recovery required')
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach])
    runtime=LocalPostgres(paths['data_directory']);entered=False;admin=None;ordinary=None;primary=None
    try:
        runtime.__enter__();entered=True
        parameters=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
        admin=psycopg.connect(**parameters,autocommit=True,connect_timeout=5,options='-c statement_timeout=15000')
        admin.execute(sql.SQL('CREATE ROLE {} LOGIN').format(sql.Identifier(consumer)))
        for role in (reader,writer):admin.execute(sql.SQL('CREATE ROLE {} NOLOGIN').format(sql.Identifier(role)))
        with interval(admin)as (assembly,tx):installed=value(assembly.install_in_transaction(tx))
        admin.commit()
        admin.execute(sql.SQL('GRANT {} TO {} WITH SET FALSE').format(sql.Identifier(installed.registrar_role),sql.Identifier(consumer)))
        parameters['user']=consumer
        ordinary=psycopg.connect(**parameters,autocommit=True,connect_timeout=5,options='-c statement_timeout=15000')
        with interval(ordinary)as (assembly,tx):
            prepared=assembly.prepare_catalog((CatalogDocument(config['document_id'],config['document_revision'],source),),bun_executable=str(paths['bun']))
            accepted=value(assembly.accept_in_transaction(tx,prepared,request_key=config['request_key'],origin_bytes=origin))
            if not isinstance(accepted,(CatalogRegistration,CatalogUnchanged)):raise RuntimeError('Original Note acceptance refused')
        ordinary.commit()
        with interval(admin)as (assembly,tx):
            value(assembly.configure_module_access_in_transaction(tx,(ModuleAccessGrant('ashlar-note','notes',reader,writer),)))
        admin.commit()
        for role in (reader,writer):admin.execute(sql.SQL('GRANT {} TO {} WITH SET FALSE').format(sql.Identifier(role),sql.Identifier(consumer)))
        with interval(ordinary)as (assembly,tx):
            status=value(assembly.status_in_transaction(tx));report=value(assembly.report_in_transaction(tx,accepted.revision))
            if not status.installed:raise RuntimeError('Current installed catalog required')
            result={'data_directory':str(paths['data_directory']),'installation':asdict(installed),
                'caller_commits_confirmed':True,'catalog_status':asdict(status),'registration_id':accepted.registration_id,'revision':accepted.revision,
                'roles':{'consumer':consumer,'reader':reader,'writer':writer,'registrar':installed.registrar_role},
                'report_original_base64':base64.b64encode(report.original_bytes).decode('ascii'),
                'scope':'Fresh local development Note bootstrap; stopped server. No remote/feed/ACK/schema-evolution qualification.'}
        ordinary.commit()
    except BaseException as error:primary=error
    callbacks=[]
    for connection in (ordinary,admin):
        if connection is not None:callbacks.extend([connection.rollback,connection.close])
    if entered:callbacks.append(lambda:runtime.__exit__(None,None,None))
    finish(primary,callbacks)
    with output.open('x',encoding='utf8')as stream:json.dump(result,stream,indent=2,ensure_ascii=False);stream.write('\n')
    return result

