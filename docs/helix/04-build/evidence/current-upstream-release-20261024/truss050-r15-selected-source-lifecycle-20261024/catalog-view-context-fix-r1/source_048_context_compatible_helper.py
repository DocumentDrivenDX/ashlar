#!/usr/bin/env python3
"""Context-compatible helper for the retained .48 pre-migration source/feed."""
from __future__ import annotations
import argparse, hashlib, json, os, sys
from pathlib import Path

REPO_SRC = Path('/private/tmp/ashlar-truss049-r4-source-f5e89926/src')
if str(REPO_SRC) not in sys.path:
    sys.path.insert(0, str(REPO_SRC))
SOURCE = Path('/private/tmp/ashlar-truss045-real-source-20261021/model/source.umf.json')
SOURCE_SHA = '498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b'
REPORT_SHA = 'c797191c90c516b0997801a63605d297e5e429b50e922cd7da2d39640d570c9a'
VIEW_SHA = 'cc36fe489326f73c2eaf153ba2e025feeb76eaa88ffa0d6b675a1f4772264dc7'
FEED = 'ashlar-current-045-lifecycle'
CONSUMER = 'ashlar-current-048-lifecycle-20261022'
REGISTRATION = '9f03eb5d-ce72-437f-8b32-0112c130e724'
READER = 'ashlar048_r5_source_reader'
REGISTRAR = 'ashlar048_accept_consumer_r1'
EXPECTED_SOURCE = {
    'document_id': 'ashlar-current-045-lifecycle-20261021',
    'document_revision': 'ashlar-current-045-lifecycle-r1',
    'registration_id': 'ccfcd423-5893-4df9-8f88-6504e086bd4d',
}

def custody_access_mode(user):
    if user == REGISTRAR:
        return 'read_write'
    if user == READER:
        return 'read_only'
    raise ValueError('Selected .48 custody role required')

def digest(raw): return hashlib.sha256(raw).hexdigest()

def write_new(path: Path, raw: bytes):
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())

def connection(user):
    import psycopg
    uri=os.environ.get('ASHLAR_LOCAL_PG_CONNINFO')
    if not uri or 'password=' in uri.lower():
        raise RuntimeError('Secret-free local PostgreSQL conninfo required')
    params=psycopg.conninfo.conninfo_to_dict(uri)
    params['user']=user
    return psycopg.connect(**params,connect_timeout=5,options='-c statement_timeout=2000')

def finish(primary, cleanup):
    from ashlar_host.lifecycle import finish as lifecycle_finish
    lifecycle_finish(primary,cleanup)

def verify_source():
    from truss import Ok,CatalogRuntimeConfiguration,CatalogViewAvailable,adopt_psycopg_host,create_reference_assembly
    observed={}
    for user in (REGISTRAR,READER):
        # Truss 0.0.48 catalog_view_basis returns NULL in READ ONLY transactions
        # and takes a FOR SHARE lock. Only the admitted registrar path uses the
        # READ WRITE transaction; the accepted-document reader stays read-only.
        mode=custody_access_mode(user)
        with connection(user) as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED '+('READ WRITE' if mode=='read_write' else 'READ ONLY'))
            conn.execute("SET LOCAL statement_timeout='2000ms'")
            host=adopt_psycopg_host(conn);assembly=None;primary=None
            try:
                tx=host.adopt_transaction(access_mode=mode)
                built=create_reference_assembly(CatalogRuntimeConfiguration(),host)
                if not isinstance(tx,Ok) or not isinstance(built,Ok):raise RuntimeError('Public .48 retained-source read refused')
                assembly=built.value
                if user==REGISTRAR:
                    status=assembly.status_in_transaction(tx.value)
                    report=assembly.report_in_transaction(tx.value,1)
                    view=assembly.catalog_view_in_transaction(tx.value,assembly.catalog_view_request(definition_count='65536',encoded_bytes='16777216'))
                    if not all(isinstance(x,Ok) for x in (status,report,view)) or not status.value.installed or status.value.revision!=1:
                        raise RuntimeError('Exact retained .48 catalog head required')
                    raw_view=view.value.view.original_bytes if isinstance(view.value,CatalogViewAvailable) else b''
                    view_doc=json.loads(raw_view)
                    if (digest(report.value.original_bytes)!=REPORT_SHA or digest(raw_view)!=VIEW_SHA
                            or view_doc.get('scope')!={'kind':'complete'} or str(view_doc.get('catalogRevision'))!='1'):
                        raise RuntimeError('Retained .48 report/complete view differs from accepted receipt')
                    observed.update(catalog_revision=1,report_sha256=REPORT_SHA,view_sha256=VIEW_SHA)
                else:
                    result=assembly.accepted_document_in_transaction(tx.value,document_id=EXPECTED_SOURCE['document_id'],maximum_bytes=1048576)
                    if not isinstance(result,Ok):raise RuntimeError('Public .48 accepted-document read refused')
                    value=result.value
                    if (value.content!=SOURCE.read_bytes() or value.sha256!=SOURCE_SHA
                            or value.document_id!=EXPECTED_SOURCE['document_id']
                            or value.document_revision!=EXPECTED_SOURCE['document_revision']
                            or value.registration_id!=EXPECTED_SOURCE['registration_id']):
                        raise RuntimeError('Retained accepted source bytes/identity differ')
                    observed.update(document_id=value.document_id,document_revision=value.document_revision,
                        source_registration_id=value.registration_id,source_sha256=value.sha256)
            except BaseException as exc:primary=exc
            def detach():
                if not host.detach():raise RuntimeError('Public .48 read host detach refused')
            finish(primary,([assembly.dispose] if assembly is not None else [])+[detach])
            conn.rollback()
    return observed

def read_page(output):
    from truss import Ok,CatalogRuntimeConfiguration,adopt_psycopg_host,create_reference_assembly
    from truss.feed import FeedPage
    from ashlar_host.truss_page_archive import encode_truss_page
    with connection(REGISTRAR) as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')
        conn.execute("SET LOCAL statement_timeout='2000ms'")
        host=adopt_psycopg_host(conn);assembly=None;primary=None;page=None
        try:
            tx=host.adopt_transaction(access_mode='read_only')
            built=create_reference_assembly(CatalogRuntimeConfiguration(),host)
            if not isinstance(tx,Ok) or not isinstance(built,Ok):raise RuntimeError('Public .48 read-only transaction refused')
            assembly=built.value
            result=assembly.read_feed_in_transaction(tx.value,consumer_id=CONSUMER,maximum_facts=4096,maximum_bytes=8388608)
            if not isinstance(result,Ok) or not isinstance(result.value,FeedPage) or result.value.outcome!='transaction':
                raise RuntimeError('Actual complete pending feed transaction required')
            page=result.value
        except BaseException as exc:primary=exc
        def detach():
            if not host.detach():raise RuntimeError('Public .48 read host detach refused')
        finish(primary,([assembly.dispose] if assembly is not None else [])+[detach])
        conn.rollback()
    archive=encode_truss_page(page,FEED)
    if len(archive)>2097152:raise RuntimeError('Pending genesis archive exceeds 2 MiB')
    write_new(Path(output),archive)
    prior=json.loads(page.prior.content); nxt=json.loads(page.next.content)
    if (page.consumer_id!=CONSUMER or page.registration_id!=REGISTRATION
            or prior.get('registrationId')!=REGISTRATION or prior.get('consumerId')!=CONSUMER
            or prior.get('nextXid')!='0'):
        raise RuntimeError('Exact original registered genesis checkpoint required')
    return {'consumer_id':page.consumer_id,'registration_id':page.registration_id,
        'writer_xid':page.writer_xid,'page_sha256':page.sha256,'archive_sha256':digest(archive),
        'archive_path':str(output),'prior_checkpoint_sha256':digest(page.prior.content),
        'next_checkpoint_sha256':digest(page.next.content),'facts':len(page.facts)}

def main():
    parser=argparse.ArgumentParser();mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--preflight',action='store_true');mode.add_argument('--custody',action='store_true');mode.add_argument('--read-page',type=Path)
    args=parser.parse_args()
    if args.preflight:
        import truss,psycopg
        from truss.migrations import RuntimeMigrations
        if importlib_metadata_version('truss-toolkit')!='0.0.48':raise RuntimeError('Pinned .48 compatibility runtime required')
        print(json.dumps({'profile':'ashlar-truss048-readonly-compat-preflight/0.1','truss':'0.0.48','pg_started':False,'spark_started':False},sort_keys=True));return
    if args.custody:result=verify_source()
    else:result=read_page(args.read_page)
    print(json.dumps(result,sort_keys=True,separators=(',',':')))

def importlib_metadata_version(name):
    import importlib.metadata
    return importlib.metadata.version(name)

if __name__=='__main__':main()
