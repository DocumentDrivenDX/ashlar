"""Register original UMF documents in an EXISTING compatible Truss installation."""
import base64,json
from dataclasses import replace
from .lifecycle import finish
from pathlib import Path


def read_bounded(path,maximum):
    with Path(path).open('rb') as stream:
        raw=stream.read(maximum+1)
    if len(raw)>maximum:raise ValueError('Input exceeds example bound')
    return raw


def run_accept_truss_schema_command(configuration,output):
    # Optional native dependencies are imported only when this example is run.
    import psycopg
    from truss import (Ok,CatalogDocument,CatalogRuntimeConfiguration,
        CatalogRegistration,CatalogUnchanged,CatalogViewAvailable,HistoricalCatalogSelection,
        adopt_psycopg_host,create_reference_assembly)
    config=json.loads(read_bounded(configuration,65536))
    if type(config)is not dict or set(config)!={'connection_uri','bun','documents','request_key','origin_path'}:raise ValueError('Exact example configuration required')
    if not isinstance(config['documents'],list)or not 1<=len(config['documents'])<=16:raise ValueError('Select 1..16 original documents')
    bun=Path(config['bun'])
    if not bun.is_absolute()or not bun.is_file():raise ValueError('Select an installed Bun executable')
    documents=[]
    for item in config['documents']:
        if type(item)is not dict or set(item)!={'path','document_id','document_revision'}:raise ValueError('Exact document selection required')
        documents.append(CatalogDocument(item['document_id'],item['document_revision'],read_bounded(item['path'],1048576)))
    origin=read_bounded(config['origin_path'],65536)
    destination=Path(output)
    if destination.exists():raise ValueError('Select a fresh report file')
    def value(outcome):
        if not isinstance(outcome,Ok):raise RuntimeError('Truss operation refused')
        return outcome.value
    def begin(connection):
        connection.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED')
        connection.execute("SET LOCAL statement_timeout = '15000ms'")
        host=adopt_psycopg_host(connection)
        try:
            transaction=value(host.adopt_transaction())
            assembly=value(create_reference_assembly(CatalogRuntimeConfiguration(),host))
        except BaseException as error:
            finish(error,[host.detach])
        return host,transaction,assembly
    def close(primary,assembly,host):
        def detach():
            if not host.detach():raise RuntimeError('Host detach refused; rollback/recovery required')
        finish(primary,[assembly.dispose,detach])
    with psycopg.connect(config['connection_uri'],autocommit=False,connect_timeout=5) as connection:
        host,transaction,assembly=begin(connection)
        primary=None
        try:
            if not value(assembly.status_in_transaction(transaction)).installed:raise RuntimeError('Compatible Truss installation required')
            prepared=assembly.prepare_catalog(tuple(documents),bun_executable=str(bun))
            registered=value(assembly.accept_in_transaction(transaction,prepared,request_key=config['request_key'],origin_bytes=origin))
            if not isinstance(registered,(CatalogRegistration,CatalogUnchanged)):raise RuntimeError('Catalog acceptance refused')
        except BaseException as error:primary=error
        finally:close(primary,assembly,host)
        connection.commit()  # The caller owns commit. Never retry an unknown commit automatically.
        host,transaction,assembly=begin(connection)
        primary=None
        try:
            report=value(assembly.report_in_transaction(transaction,registered.revision))
            request=replace(assembly.catalog_view_request(encoded_bytes='16777216'),
                selection=HistoricalCatalogSelection(catalog_revision=str(registered.revision)))
            catalog=value(assembly.catalog_view_in_transaction(transaction,request))
            if not isinstance(catalog,CatalogViewAvailable)or str(json.loads(catalog.view.original_bytes)['catalogRevision'])!=str(registered.revision):raise RuntimeError('Accepted historical catalog view unavailable')
            result={'revision':registered.revision,'registration_id':registered.registration_id,
                    'replayed':registered.replayed,
                    'report_original_base64':base64.b64encode(report.original_bytes).decode('ascii'),
                    'catalog_original_base64':base64.b64encode(catalog.view.original_bytes).decode('ascii'),
                    'scope':'Committed schema acceptance/catalog only; no mutations, feed or ACK qualification.'}
        except BaseException as error:primary=error
        finally:close(primary,assembly,host)
        connection.commit()
    with destination.open('x',encoding='utf8') as stream:json.dump(result,stream,ensure_ascii=False,indent=2);stream.write('\n')
    return result

