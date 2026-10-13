"""Installed local Truss graph restart command using original owned custody."""
import json
import os
import sqlite3
from contextlib import closing
from pathlib import Path
from urllib.parse import quote
from .delta_custody import DeltaTarget, LocalDeltaTransport, encoded
from .driver import NativeDriver, PrivatePolicy
from .evolution_producer import snapshot
from .lifecycle import finish, owned_context
from .private_graph_custody import decode_custody_bytes, require_custody_digest
from .truss_catalog_reader import parse_archived_acceptance_files, read_historical_catalog_views
from .truss_page_archive import decode_truss_page


def parse_resume_truss_configuration(configuration, output):
    """Effect-free bounded local selection; no SDK or Spark loading."""
    config = decode_custody_bytes(snapshot(Path(configuration),65536))
    base = {'connection_uri','publication_directory','jars'}
    selections = {'feed','source_page_path','source_page_sha256','request_digest'}
    required = base | ({'publication_intent'} if type(config) is dict and 'publication_intent' in config else selections)
    if type(config) is not dict or not required <= set(config) <= required | {'archived_acceptance_files','destination_id'}:
        raise ValueError('Exact original Truss resume configuration required')
    intent_path = None
    if 'publication_intent' in config:
        selection = config['publication_intent']
        if type(selection) is not dict or set(selection) != {'path','sha256'}:
            raise ValueError('Exact original publication intent selection required')
        require_custody_digest(selection['sha256'])
        if type(selection['path']) is not str:
            raise ValueError('Canonical publication intent path required')
        intent_path = Path(selection['path'])
        if not intent_path.is_absolute() or intent_path.resolve() != intent_path:
            raise ValueError('Canonical publication intent path required')
        raw_intent = snapshot(intent_path,65536)
        import hashlib
        if hashlib.sha256(raw_intent).hexdigest() != selection['sha256']:
            raise ValueError('Original publication intent digest differs')
        intent = decode_custody_bytes(raw_intent)
        if (type(intent) is not dict or set(intent) != selections | {'publication_id'}
                or type(intent['publication_id']) is not str or not intent['publication_id']
                or len(intent['publication_id']) > 4096):
            raise ValueError('Original bounded publication intent required')
        config = {key:value for key,value in config.items() if key != 'publication_intent'} | {key:intent[key] for key in selections}
    from .truss_feed_sessions import receipt_destination_id,require_truss_receipt_sdk
    destination_id=receipt_destination_id(config.get('destination_id'))
    if 'destination_id'in config and destination_id is None:raise ValueError('Explicit destination identity required')
    if destination_id is not None:require_truss_receipt_sdk()
    for key in ('connection_uri','feed'):
        if type(config[key]) is not str or not config[key] or len(config[key]) > 4096 or '\0' in config[key]:
            raise ValueError('Bounded explicit ordinary Truss source required')
    for key in ('source_page_sha256','request_digest'): require_custody_digest(config[key])
    def path(value, *, directory=False):
        if type(value) is not str: raise ValueError('Explicit canonical path required')
        selected = Path(value)
        if (not selected.is_absolute() or selected.resolve() != selected
                or any(char in value for char in ('`',"'",'\\','\0','\n','\r'))
                or not (selected.is_dir() if directory else selected.is_file())):
            raise ValueError('Existing canonical local input required')
        return selected
    publication = path(config['publication_directory'],directory=True)
    page = path(config['source_page_path'])
    if type(config['jars']) is not list or len(config['jars']) != 2:
        raise ValueError('Two existing Delta JARs required')
    jars = tuple(path(value) for value in config['jars'])
    if len(set(jars)) != 2: raise ValueError('Distinct existing Delta JARs required')
    archives = parse_archived_acceptance_files(config.get('archived_acceptance_files',[]))
    destination = Path(output)
    if (not destination.is_absolute() or not destination.parent.is_dir() or destination.parent.resolve() != destination.parent
            or destination.exists() or destination.is_symlink()
            or any(char in str(destination) for char in ('`',"'",'\\','\0','\n','\r'))):
        raise ValueError('Fresh canonical private resume output required')
    protected = (publication,page,Path(configuration).resolve(),*jars,*(item[0] for item in archives))
    if intent_path is not None: protected += (intent_path,)
    if any(destination == selected or destination in selected.parents or selected in destination.parents for selected in protected):
        raise ValueError('Resume output must be separate from original custody')
    warehouse = destination.parent/(destination.name+'-warehouse')
    if warehouse.exists() or warehouse.is_symlink(): raise ValueError('Fresh separate Spark warehouse required')
    page_raw = snapshot(page,2097152)
    import hashlib
    if hashlib.sha256(page_raw).hexdigest() != config['source_page_sha256']:
        raise ValueError('Original source page archive digest differs')
    return config,publication,page_raw,jars,archives,destination,warehouse


def run_resume_truss_graph_command(configuration, output):
    def resume(driver,config):
        from .truss_resume import resume_truss_publication
        descriptor = resume_truss_publication(driver,config['request_digest'])
        return {'request_digest':config['request_digest'],'manifest':dict(descriptor.raw),
            'qualification':('Original private local Truss graph publication resumed with exact source-retained destination ACK receipt; consumer verifies destination and caller controls source commit; no history release or Unity Catalog qualification.'if 'destination_id'in config else'Original private local Truss graph publication resumed; caller-asserted delivery ACK, no protected outbox receipt or Unity Catalog qualification.')}
    return run_selected_truss_command(configuration,output,resume)


def run_selected_truss_command(configuration, output, operation, *, report_writer=None):
    """Open original local Truss custody and close it before writing a report."""
    config,publication,page_raw,jars,archives,destination,warehouse = parse_resume_truss_configuration(configuration,output)
    journal = publication/'operations.sqlite'
    if not journal.is_file() or journal.is_symlink() or journal.resolve()!=journal:
        raise ValueError('Original local Truss publication journal unavailable; retain source checkpoint and restore original custody')
    # Normal read-only SQLite observes the original WAL; immutable mode is unsafe.
    with owned_context(closing(sqlite3.connect('file:'+quote(str(journal))+'?mode=ro',uri=True))) as registry_reader:
        rows = registry_reader.execute('SELECT length(CAST(original AS BLOB)),substr(CAST(original AS BLOB),1,65537) FROM local_installation WHERE id=1').fetchall()
    if len(rows) != 1 or type(rows[0][0]) is not int or not 0 < rows[0][0] <= 65536:
        raise ValueError('Complete bounded original local installation registry required')
    registry = decode_custody_bytes(rows[0][1])
    if type(registry) is not dict or not {'installation','targets'} <= set(registry) <= {'installation','targets','operation_capacity'}:
        raise ValueError('Original local installation registry required')
    targets = []
    if type(registry['targets']) is not list or len(registry['targets']) != 6:
        raise ValueError('Complete original six-role installation required')
    for entry in registry['targets']:
        if type(entry) is not dict or set(entry) != {'table','path','uuid'}:
            raise ValueError('Original complete native target required')
        selected = Path(entry['path'])
        role = entry['table'].split('.')[-1]
        if selected != publication/role or selected.resolve() != selected or not selected.is_dir():
            raise ValueError('Original canonical publication target required')
        targets.append(DeltaTarget(entry['table'],selected,entry['uuid']))
    tables = {target.table.split('.')[-1]:target.table for target in targets}
    if set(tables) != {'object_current','edge_current','tombstone','whole_source_history','attempts','manifest'}:
        raise ValueError('Complete original graph/phase/manifest roles required')
    capacity = registry.get('operation_capacity')
    LocalDeltaTransport.require_existing_journal(journal,registry['installation'],tuple(targets),capacity=capacity)
    from .truss_runtime import require_truss_psycopg_runtime
    require_truss_psycopg_runtime()
    page = decode_truss_page(page_raw,config['source_page_sha256'],config['feed'])
    import psycopg
    from .commerce_export_cli import open_commerce_export_spark
    from .supply_chain_native import PrivateFiniteLease
    from .truss_note_publication import bind_ordinary_connection
    from .truss_feed_sessions import TrussFeedSessions
    from .truss_graph_admission import TrussGraphAdmission
    from .schema_rows import fixture_columns
    from .resources import RESOURCE_ROOT
    context = object()
    def connection(): return psycopg.connect(config['connection_uri'],autocommit=False,connect_timeout=5)
    sessions = TrussFeedSessions(page,config['feed'],bind_ordinary_connection(connection,context),context,**({'destination_receipts':True,'destination_id':config['destination_id']}if 'destination_id'in config else {}))
    admission = TrussGraphAdmission(sessions.batch(),read_historical_catalog_views(connection,sessions.batch(),archives))
    lease = PrivateFiniteLease(publication,create=False)
    try:
        with owned_context(open_commerce_export_spark(jars,warehouse)) as spark:
            policy = PrivatePolicy(context,tuple(targets));policy.initializing = False
            transport = LocalDeltaTransport(spark,journal,registry['installation'],tuple(targets),policy,capacity=capacity)
            primary = None;descriptor = None
            try:
                driver = NativeDriver.from_existing_truss(transport,policy,context,tables,admission.changes,
                    fixture_columns(RESOURCE_ROOT),source_admission=admission,source_sessions=sessions,ack_sessions=sessions)
                result = operation(driver,config)
            except BaseException as error: primary = error
            finish(primary,[transport.close,lease.renew])
    except BaseException as error:
        finish(error,[lease.close])
    else: lease.close()
    if report_writer is not None:
        return report_writer(result,destination)
    raw = (encoded(result)+'\n').encode()
    descriptor = os.open(destination,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(descriptor,'wb') as stream:
        if stream.write(raw) != len(raw): raise OSError('Complete private resume report write required')
    return result
