#!/usr/bin/env python3
"""One bounded local Truss .51 to SDK 0.10.1 Delta publication; source ACK deferred."""
from __future__ import annotations

import argparse, base64, hashlib, importlib.util, inspect, json, os, shutil, stat, subprocess, time
from pathlib import Path
import sys

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[5]
sys.path.insert(0,str(REPO/'src'))
SCOPE_PATH=HERE/'r20-publication-scope.json'
R15=HERE.parent/'r15-truss051-view-transaction'
R16B=HERE.parent/'r16b-truss051-seed-resume'
R19=HERE.parent/'r19-truss051-delta-publication'
R16B_OUTPUT=Path('/private/tmp/ashlar-r16b-truss051-seed-resume-20261101')
EXPECTED_PATHS={
 'output_root':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101',
 'supervisor_root':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101-supervisor',
 'cold_backup_pgdata':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101/r16b-stopped-source-backup-pgdata',
 'migration_clone_pgdata':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101/r16b-publication-clone-pgdata',
 'recovery_registry':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101/unused-recovery-registry.sqlite3',
 'receipt_root':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101/receipts',
 'delta_root':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101/delta',
 'warehouse':'/private/tmp/ashlar-r20-truss051-registrar-catalog-20261101/warehouse'}
MAX_BYTES=512*1024*1024

class StopRun(RuntimeError): pass

def validate_attempt_paths(paths, *, supervisor_started, expected_paths=None):
    """Require the supervisor's exact private log directory, but no worker outputs yet."""
    expected_paths = EXPECTED_PATHS if expected_paths is None else expected_paths
    if paths != expected_paths:
        raise StopRun('R20 exact output and supervisor paths required')
    for key, value in paths.items():
        path = Path(value)
        if key == 'supervisor_root':
            continue
        if path.exists() or path.is_symlink():
            raise StopRun(f'R20 output path must be absent before work: {key}')
    supervisor = Path(paths['supervisor_root'])
    if not supervisor_started:
        if supervisor.exists() or supervisor.is_symlink():
            raise StopRun('R20 supervisor root must be absent before supervisor launch')
        return
    if supervisor.is_symlink() or not supervisor.is_dir():
        raise StopRun('R20 supervisor root must be an owned private directory')
    root_info = supervisor.lstat()
    if root_info.st_uid != os.getuid() or stat.S_IMODE(root_info.st_mode) != 0o700:
        raise StopRun('R20 supervisor root ownership or mode mismatch')
    expected_logs = {'child.stdout.log', 'child.stderr.log'}
    if {item.name for item in supervisor.iterdir()} != expected_logs:
        raise StopRun('R20 supervisor log set mismatch')
    for name in expected_logs:
        log = supervisor / name
        if log.is_symlink() or not log.is_file():
            raise StopRun(f'R20 supervisor log must be a regular owned file: {name}')
        info = log.lstat()
        if info.st_uid != os.getuid() or stat.S_IMODE(info.st_mode) & 0o077:
            raise StopRun(f'R20 supervisor log ownership or privacy mismatch: {name}')

def sha(raw): return hashlib.sha256(raw).hexdigest()
def unique_pairs(pairs):
    result={}
    for key,value in pairs:
        if key in result: raise StopRun(f'duplicate JSON key {key!r}')
        result[key]=value
    return result
def read_json(path): return json.loads(path.read_text(),object_pairs_hook=unique_pairs)
def write_new(path,value):
    raw=(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
    path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:
        if f.write(raw)!=len(raw): raise OSError('short receipt write')
        f.flush();os.fsync(f.fileno())
    return sha(raw)

def load_r15():
    spec=importlib.util.spec_from_file_location('r15_r18_helpers',R19/'r19_truss051_local_worker.py')
    if spec is None or spec.loader is None: raise StopRun('R15 reviewed runtime/custody helper unavailable')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module

def verify_r16b(scope):
    binding=scope['r16b_packet']
    for key,name in (('scope_sha256','r16b-resume-scope.json'),('worker_sha256','r16b_seed_resume_worker.py'),('supervisor_sha256','r16b_supervisor.py')):
        if sha((R16B/name).read_bytes())!=binding[key]: raise StopRun(f'R16b packet changed: {name}')
    receipts=R16B_OUTPUT/'receipts';source=scope['r16b_source']
    files={'completed.json':'completion_receipt_sha256','first-feed-page.json':'first_page_receipt_sha256',
           'accepted-document-readback.json':'accepted_document_receipt_sha256',
           'consumer-genesis-and-empty-seed-readonly-proof.json':'consumer_state_receipt_sha256',
           'group-commit-confirmed.json':'group_commit_receipt_sha256'}
    values={name:read_json(receipts/name) for name in files}
    for name,key in files.items():
        if sha((receipts/name).read_bytes())!=source[key]: raise StopRun(f'R16b receipt changed: {name}')
    done,page,doc,state,group=(values[name] for name in files)
    if (done.get('status')!='completed_local_development_only' or done.get('acknowledgement') is not False
            or done.get('publication') is not False or done.get('cloud') is not False
            or page.get('acknowledgement') is not False or page.get('sha256')!=source['first_page']['page_sha256']
            or page.get('bytes')!=source['first_page']['page_bytes'] or page.get('writer_xid')!=source['first_page']['writer_xid']
            or page.get('registration_id')!=source['registration_id'] or page.get('outcome')!='transaction'
            or doc.get('document_sha256')!=source['accepted_document_sha256']
            or doc.get('document_id')!='ashlar-local-dev-typed-graph-r6-20261031'
            or group.get('result_count')!=7 or group.get('commit_confirmed_after_transaction_context') is not True
            or state.get('checkpoint_sha256')!=source['checkpoint_sha256'] or state.get('ack_rows')!=0
            or state.get('checkpoint',{}).get('nextXid')!='0'):
        raise StopRun('R16b accepted document, committed seed, genesis checkpoint, or no-ACK evidence differs')
    expected=source['first_page']
    if expected['prior_sha256']!=state['checkpoint_sha256']:
        raise StopRun('R20 expected prior digest must be the exact R16b checkpoint_sha256')
    return expected

def validate(scope,scope_raw,auth,worker_raw,supervisor_raw,preflight):
    expected_auth={'schema':'ashlar.standing-local-dispatch-binding/1',
      'authorization_basis':'standing_user_local_release_authorization',
      'user_authorization_statement':'User authorized fresh Truss source creation, a local seed, and a tested release; bounded local R20 Truss-to-Delta publication with source ACK deferred is within that standing authorization and requires no per-attempt user approval.',
      'scope_sha256':sha(scope_raw),'worker_sha256':sha(worker_raw),'supervisor_sha256':sha(supervisor_raw),
      'attempts':1,'cloud':False}
    if any(auth.get(k)!=v for k,v in expected_auth.items()) or set(auth)!=set(expected_auth)|{'sol_review_status','dispatch_enabled'}:
        raise StopRun('R20 standing authorization hash binding mismatch')
    if preflight:
        if auth['sol_review_status'] not in ('pending_sol_review','sol_reviewed') or type(auth['dispatch_enabled']) is not bool:
            raise StopRun('R20 preflight authorization state malformed')
    elif auth.get('sol_review_status')!='sol_reviewed' or auth.get('dispatch_enabled') is not True:
        raise StopRun('R20 native dispatch remains disabled pending Sol review')
    if (scope.get('profile')!='ashlar-r20-truss051-registrar-catalog-delta-publication-deferred-ack/0.1'
        or scope.get('authorization')!='standing_user_local_release_authorization'
        or scope.get('status')!='standing_local_native_authorization'
        or scope.get('paths')!=EXPECTED_PATHS or scope.get('cloud') is not False
        or scope.get('acknowledgement') is not False or scope.get('production_authority') is not False):
        raise StopRun('R20 exact local source/publication scope required')
    if scope['limits']!={'attempts':1,'automatic_retries':0,'wall_seconds':900,'process_group_rss_bytes':2147483648,
       'retained_plus_attempt_bytes':2147483648,'attempt_root_bytes':536870912,'migration_preservation_bytes':268435456,
       'combined_log_bytes':16777216,'storage_sample_seconds':15,'parallelism':'sequential local only','cloud':False,'cleanup':False}:
        raise StopRun('R20 bounded local attempt limits changed')
    dependency_files={
      'r15_truss051_supervisor.py':R15/'r15_truss051_supervisor.py',
      'r15_supervisor_custody.py':R15/'r15_supervisor_custody.py',
      'r15-runtime-proof.json':R19/'r15-runtime-proof.json',
      'r19_truss051_local_worker.py':R19/'r19_truss051_local_worker.py'}
    if set(scope.get('r19_helper_dependencies',{}))!=set(dependency_files):
        raise StopRun('R20 helper dependency inventory mismatch')
    for name,path in dependency_files.items():
        if sha(path.read_bytes())!=scope['r19_helper_dependencies'][name]:
            raise StopRun(f'R20 immutable helper dependency changed: {name}')
    source_custody=scope.get('r20_source_custody',{})
    custody_path=R19/'source-copy-custody.json'
    if (source_custody.get('path')!='/private/tmp/ashlar-r19-truss051-delta-publication-20261101/r16b-stopped-source-backup-pgdata'
        or not custody_path.is_file() or sha(custody_path.read_bytes())!=source_custody.get('source_copy_receipt_sha256')):
        raise StopRun('R20 selected R19 cold-backup source custody receipt mismatch')
    custody_receipt=read_json(custody_path)
    if (custody_receipt.get('cold_backup')!=source_custody['path']
        or custody_receipt.get('bytes')!=source_custody.get('bytes')
        or custody_receipt.get('files')!=source_custody.get('files')
        or custody_receipt.get('tree_sha256')!=source_custody.get('tree_sha256')
        or custody_receipt.get('migration_replayed') is not False):
        raise StopRun('R20 R19 cold-backup custody record differs from selected immutable source')
    validator=scope.get('r19_validator',{})
    if validator.get('path')!='r19_truss051_local_worker.py' or sha((R19/validator['path']).read_bytes())!=validator.get('sha256'):
        raise StopRun('R20 versioned release-cohort validator binding mismatch')
    try:
        fix_tree=subprocess.run(['git','-C',str(REPO),'rev-parse',validator['reviewed_fix_commit']+'^{tree}'],capture_output=True,text=True,check=True,timeout=10).stdout.strip()
        fix_ancestor=subprocess.run(['git','-C',str(REPO),'merge-base','--is-ancestor',validator['reviewed_fix_commit'],'HEAD'],capture_output=True,timeout=10)
    except (OSError,KeyError,subprocess.SubprocessError) as error: raise StopRun('R20 reviewed resolver-fix provenance unavailable') from error
    if fix_tree!=validator.get('reviewed_fix_tree') or fix_ancestor.returncode!=0:
        raise StopRun('R20 reviewed resolver-fix tree/ancestry mismatch')
    validate_attempt_paths(scope['paths'], supervisor_started=not preflight)
    implementation=scope['source_implementation']
    try:
        ancestry=subprocess.run(['git','-C',str(REPO),'merge-base','--is-ancestor',implementation['commit'],'HEAD'],capture_output=True,timeout=10)
        tree=subprocess.run(['git','-C',str(REPO),'rev-parse',implementation['commit']+'^{tree}'],capture_output=True,text=True,check=True,timeout=10).stdout.strip()
    except (OSError,subprocess.SubprocessError) as error: raise StopRun('R20 reviewed deferred-ACK implementation ancestry unavailable') from error
    if ancestry.returncode!=0 or tree!=implementation['tree']: raise StopRun('R20 reviewed deferred-ACK implementation commit/tree changed')
    for relative,digest in implementation['files'].items():
        path=(REPO/relative).resolve()
        try: path.relative_to(REPO.resolve())
        except ValueError: raise StopRun('R20 implementation source path escapes the repository') from None
        if sha(path.read_bytes())!=digest: raise StopRun(f'R20 deferred-ACK implementation file changed: {relative}')
    publication=scope['publication']
    if (publication.get('maximum_pages')!=1 or publication.get('maximum_page_bytes')!=1048576
       or publication.get('defer_source_acknowledgement') is not True
       or publication.get('acknowledgement')!='deferred_not_attempted'
       or publication.get('carrier_profile')!='ashlar-delta/0.4-xxhash64'
       or publication.get('umf_sdk_version')!='0.10.1'
       or publication.get('catalog_evidence_pre_spark') is not True
       or publication.get('catalog_authority_role')!='ashlar_local_dev_registrar_r6_20261031'):
        raise StopRun('R20 one-page SDK0101 deferred-ACK contract changed')
    r15=load_r15();r15.validate_runtime_paths(scope['runtime']);r15.validate_release_cohort(scope['release_cohort'],scope['runtime']);r15.validate_local_runtime_extras(Path(scope['runtime']['local_extra_overlay']))
    expected_page=verify_r16b(scope)
    import inspect, psycopg
    from truss import (LocalPostgres,CatalogRuntimeConfiguration,adopt_psycopg_host,create_reference_assembly,
                       CatalogReferenceAssembly,RegistrarStatus,AcceptedDocument,FeedPage)
    from ashlar_host.truss_note_publication import (publish_graph_pages,read_page,
        validate_expected_first_page_shape,validate_deferred_ack_pair)
    from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
    from ashlar_host.truss_current_catalog import CurrentCatalogEvidence
    from ashlar.truss_native_feed import native_feed_batch
    from ashlar_host.truss_feed_sessions import require_truss_receipt_sdk
    from ashlar_host.configured_local_runtime_profile import attest,verify_loaded_delta_class
    from ashlar_host.local_current_carrier_profile import create_local_current_xxhash64_sdk0101_tables
    from ashlar_host.local_current_delta4_umf_sdk0101 import LocalUmfDelta4Sdk010Bridge
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    for function,required in ((publish_graph_pages,{'maximum_pages','destination_id','maximum_page_bytes','carrier_profile','umf_sdk_version','defer_source_acknowledgement','expected_first_page','catalog_connection_factory'}),
       (read_graph_catalog_evidence,{'catalog_connection_factory'}),
       (native_feed_batch,{'content','sha256','prior','next','feed'}),
       (read_page,{'maximum_bytes'}),(validate_expected_first_page_shape,set()),(validate_deferred_ack_pair,{'expected_publication_id'})):
        if not required.issubset(inspect.signature(function).parameters): raise StopRun(f'R20 public publisher argument shape changed: {function.__name__}')
    if 'data_directory' not in inspect.signature(LocalPostgres).parameters: raise StopRun('Truss LocalPostgres clone constructor changed')
    if set(inspect.signature(CatalogReferenceAssembly.accepted_document_in_transaction).parameters)!={'self','transaction','document_id','maximum_bytes'}:
        raise StopRun('Truss accepted-document public API argument shape changed')
    if set(inspect.signature(CatalogReferenceAssembly.registrar_status_in_transaction).parameters)!={'self','transaction'}:
        raise StopRun('Truss registrar-status public API argument shape changed')
    if set(inspect.signature(CatalogReferenceAssembly.read_feed_in_transaction).parameters)!={'self','transaction','consumer_id','maximum_facts','maximum_bytes'}:
        raise StopRun('Truss feed-page public API argument shape changed')
    if not callable(getattr(CatalogRuntimeConfiguration,'__init__',None)) or not callable(require_truss_receipt_sdk): raise StopRun('Truss accepted-document/feed receipt public API unavailable')
    receipt_api=require_truss_receipt_sdk()
    if len(receipt_api)!=3: raise StopRun('Truss public destination receipt API shape changed')
    if not callable(create_local_current_xxhash64_sdk0101_tables) or not callable(LocalUmfDelta4Sdk010Bridge):
        raise StopRun('SDK 0.10.1 generated carrier bridge import unavailable')
    if SparkSession.getActiveSession() is not None or getattr(SparkSession,'_instantiatedSession',None) is not None or getattr(SparkContext,'_active_spark_context',None) is not None:
        raise StopRun('Effect-free preflight found an already active Spark context')
    validate_expected_first_page_shape(expected_page)
    source_path=Path(scope['runtime']['delta_jar_directory']);java_home=Path(scope['runtime']['java_home'])
    jar_inventory=scope['runtime']['jar_inventory']
    profile,identity,jars=attest(type('Runtime',(),{'jars':source_path,'java_home':java_home,'jar_inventory':jar_inventory})())
    if profile!='ashlar-configured-local-runtime/0.2' or len(jars)!=2: raise StopRun('Configured local Spark/Delta runtime attestation changed')
    # All local Truss/PG/Spark/Delta/SDK extras and call shapes are resolved before
    # hashing or copying the retained PGDATA tree.
    source=Path(scope['r16b_source']['path'])
    custody=r15.check_process_custody(source)
    tree=r15.pg18_preflight(source)
    if tree!=(scope['r16b_source']['bytes'],scope['r16b_source']['tree_sha256'],scope['r16b_source']['files']):
        raise StopRun('R16b stopped PG18 source tree differs from exact retained receipt')
    required_free=2*tree[0]+scope['limits']['attempt_root_bytes']
    free_bytes=shutil.disk_usage(Path(scope['paths']['output_root']).parent).free
    if 2*tree[0]>scope['limits']['migration_preservation_bytes'] or free_bytes<required_free:
        raise StopRun(f'insufficient bounded storage for R20 cold backup/clone/Delta output: {free_bytes} < {required_free}')
    return {'source_tree':tree,'process_custody':custody,'expected_first_page':expected_page,'spark_runtime':identity,
      'public_api_imports':'passed', 'separate_catalog_factory_api':'passed','output_root_created':False,'postgres_started':False,'database_contacted':False,
      'spark_started':False,'delta_written':False,'acknowledgement':False,'dispatch_enabled':False,
      'free_bytes':free_bytes,'required_free_bytes':required_free}

def run_native(scope,expected_page,r15,validated_source_tree):
    import psycopg
    from truss import (LocalPostgres,CatalogRuntimeConfiguration,adopt_psycopg_host,create_reference_assembly,
                       RegistrarStatus,AcceptedDocument)
    from ashlar_host.truss_note_publication import publish_graph_pages,read_page
    from ashlar_host.configured_local_runtime_profile import attest,verify_loaded_delta_class
    from ashlar_host.truss_note_publication import validate_expected_first_page
    from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
    from ashlar_host.truss_current_catalog import CurrentCatalogEvidence
    from ashlar.truss_native_feed import native_feed_batch
    from types import SimpleNamespace
    from pyspark import SparkContext
    from pyspark.sql import SparkSession
    paths={k:Path(v) for k,v in scope['paths'].items()};root=paths['output_root'];receipts=paths['receipt_root']
    source=Path(scope['r16b_source']['path']);backup=paths['cold_backup_pgdata'];clone=paths['migration_clone_pgdata']
    root.mkdir(mode=0o700);os.chmod(root,0o700);receipts.mkdir(mode=0o700);os.chmod(receipts,0o700)
    original=validated_source_tree;source_stat=source.stat();source_sig=r15.root_directory_signature(source)
    if original!=(scope['r16b_source']['bytes'],scope['r16b_source']['tree_sha256'],scope['r16b_source']['files']): raise StopRun('R16b source changed immediately before cold copy')
    shutil.copytree(source,backup,symlinks=False,copy_function=shutil.copy2);os.chmod(backup,0o700)
    if r15.pg18_preflight(backup)!=original: raise StopRun('R20 cold backup differs from stopped R16b PGDATA')
    shutil.copytree(backup,clone,symlinks=False,copy_function=shutil.copy2);os.chmod(clone,0o700)
    if r15.pg18_preflight(clone)!=original or r15.root_directory_signature(source)!=source_sig or source.stat().st_ino!=source_stat.st_ino:
        raise StopRun('R20 working clone mismatch or retained R16b source changed')
    write_new(receipts/'source-copy-custody.json',{'source':str(source),'bytes':original[0],'files':original[2],
       'tree_sha256':original[1],'cold_backup':str(backup),'working_clone':str(clone),'source_unchanged':True,'migration_replayed':False})
    database_effects_may_remain=False;publication_may_be_committed=False;spark=None
    try:
      with LocalPostgres(clone) as pg:
        if pg.info.server_version!='18.6' or Path(pg.info.data_directory)!=clone: raise StopRun('R20 exact PG18.6 working clone did not start')
        database_effects_may_remain=True
        params=psycopg.conninfo.conninfo_to_dict(pg.info.connection_uri)
        reader=scope['r16b_source']['reader_login_role'];consumer=scope['r16b_source']['consumer_id'];doc_expected=scope['source']['catalog_accepted']
        reader_params=dict(params);reader_params['user']=reader
        r16_scope=read_json(R16B/'r16b-resume-scope.json')
        registrar=scope['publication']['catalog_authority_role']
        if registrar!=r16_scope['consumer_and_model']['registrar_login_role']:
            raise StopRun('R20 catalog authority role differs from retained R16b registrar identity')
        catalog_params=dict(params);catalog_params['user']=registrar
        def connect_reader(): return psycopg.connect(**reader_params,connect_timeout=5)
        def connect_catalog(): return psycopg.connect(**catalog_params,connect_timeout=5)
        # Prove this existing registrar's exact public-call rights before any
        # accepted-source/catalog calls or Spark startup. This SELECT-only
        # preflight runs in PostgreSQL READ ONLY mode.
        admin_params=dict(params);admin_params['user']='postgres'
        with psycopg.connect(**admin_params,connect_timeout=5) as acl_conn:
          acl_conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY')
          acl_conn.execute("SET LOCAL statement_timeout='5000ms'")
          acl=acl_conn.execute("""
            WITH RECURSIVE principal_members(role_oid) AS (
              SELECT oid FROM pg_catalog.pg_roles WHERE rolname=%s
              UNION
              SELECT m.roleid FROM pg_catalog.pg_auth_members m
                JOIN principal_members p ON p.role_oid=m.member
            )
            SELECT r.rolname,r.rolcanlogin,r.rolinherit,r.rolsuper,r.rolcreatedb,
                   r.rolcreaterole,r.rolreplication,r.rolbypassrls,
                   EXISTS(SELECT 1 FROM truss.catalog_runtime_installation i
                          JOIN principal_members p ON p.role_oid=i.registrar_oid WHERE i.singleton),
                   EXISTS(SELECT 1 FROM truss.module_access m
                          JOIN pg_catalog.pg_roles wr ON wr.rolname=m.writer_role
                          JOIN principal_members p ON p.role_oid=wr.oid),
                   pg_catalog.has_function_privilege(r.oid,
                     'truss.person_accepted_document(text,integer,text,text)'::pg_catalog.regprocedure,'EXECUTE'),
                   pg_catalog.has_function_privilege(r.oid,
                     'truss.accepted_document(text,integer,text,text)'::pg_catalog.regprocedure,'EXECUTE'),
                   pg_catalog.has_function_privilege(r.oid,
                     'truss.person_catalog_view_basis(integer,integer,integer)'::pg_catalog.regprocedure,'EXECUTE'),
                   pg_catalog.has_function_privilege(r.oid,
                     'truss.catalog_view_basis(integer,integer,integer)'::pg_catalog.regprocedure,'EXECUTE')
              FROM pg_catalog.pg_roles r WHERE r.rolname=%s
          """,(registrar,registrar)).fetchone()
          if (acl is None or acl[0]!=registrar or acl[1] is not True or acl[2] is not True
              or any(value is not False for value in acl[3:8]) or any(value is not True for value in acl[8:])):
            raise StopRun('R20 registrar public-call EXECUTE, registrar membership, module-writer membership, or safe LOGIN preflight refused')
          acl_conn.rollback()
        write_new(receipts/'registrar-catalog-acl-preflight.json',{
          'principal':registrar,'role_attributes':{'login':acl[1],'inherit':acl[2],'superuser':acl[3],
            'createdb':acl[4],'createrole':acl[5],'replication':acl[6],'bypassrls':acl[7]},
          'registrar_membership':acl[8],'module_writer_membership':acl[9],
          'person_accepted_document_execute':acl[10],'accepted_document_execute':acl[11],
          'person_catalog_view_basis_execute':acl[12],'catalog_view_basis_execute':acl[13],
          'transaction_read_only':True,'database_mutation':False})
        # Public accepted-document reread and exact still-unacknowledged first-page proof precede Spark/Delta.
        with connect_reader() as conn:
          conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY');conn.execute("SET LOCAL statement_timeout='5000ms'")
          host=adopt_psycopg_host(conn,statement_timeout_ms=5000,operation_timeout_ms=30000);assembly=None
          try:
            tx=r15.require_ok(host.adopt_transaction(access_mode='read_only'),'R20 accepted-document transaction')
            assembly=r15.require_ok(create_reference_assembly(CatalogRuntimeConfiguration(),host),'R20 public Truss assembly')
            status=r15.require_value_type(r15.require_ok(assembly.registrar_status_in_transaction(tx),'R20 public registrar status'),RegistrarStatus,'registrar status')
            document=r15.require_value_type(r15.require_ok(assembly.accepted_document_in_transaction(tx,document_id=doc_expected['document_id'],maximum_bytes=1048576),'R20 public accepted document'),AcceptedDocument,'accepted document')
            if (status.can_register is not False or document.document_id!=doc_expected['document_id'] or document.document_revision!=doc_expected['document_revision']
                or document.registration_id!=doc_expected['registration_id'] or document.catalog_revision!=doc_expected['catalog_revision']
                or sha(document.content)!=doc_expected['document_sha256']): raise StopRun('R20 public accepted-document reread differs from R16b')
            model=document.content
          finally:
            if assembly is not None: assembly.dispose()
            if not host.detach(): raise StopRun('R20 accepted-document host detach refused')
          conn.rollback()
        write_new(receipts/'accepted-document-public-reread.json',{'document_id':doc_expected['document_id'],'document_revision':doc_expected['document_revision'],
          'registration_id':doc_expected['registration_id'],'catalog_revision':doc_expected['catalog_revision'],'document_sha256':sha(model),
          'public_api':'Truss .51 accepted_document_in_transaction','transaction_read_only':True})
        page=read_page(connect_reader,consumer,maximum_bytes=scope['publication']['maximum_page_bytes']);validate_expected_first_page(page,expected_page)
        from ashlar.truss_native_feed import native_feed_batch
        page_batch=native_feed_batch(page.content,sha256=page.sha256,prior=page.prior.content,next=page.next.content,feed=scope['publication']['feed'])
        catalog_evidence=read_graph_catalog_evidence(connect_reader,page_batch,(),catalog_connection_factory=connect_catalog)
        if type(catalog_evidence) is not CurrentCatalogEvidence:
            raise StopRun('R20 separate registrar did not return complete public current catalog evidence')
        if (len(catalog_evidence.reports)!=1 or len(catalog_evidence.views)!=1
            or not catalog_evidence.sources
            or {document_id for (_,document_id) in catalog_evidence.sources} != {doc_expected['document_id']}
            or any(sha(document.content)!=doc_expected['document_sha256']
                   for document in catalog_evidence.sources.values())):
            raise StopRun('R20 registrar report, complete view, or accepted-source bytes differ from the accepted document')
        catalog_receipt={'profile':catalog_evidence.profile,'truss_version':catalog_evidence.truss_version,
          'reader_consumer_id':page.consumer_id,'reader_registration_id':page.registration_id,'reader_page_sha256':page.sha256,
          'catalog_authority_role':registrar,'transaction_access_mode':'read_write_effect_free_rollback',
          'page_batch_sha256':sha(page_batch.begin),'page_batch_feed':page_batch.feed,
          'reports':{str(revision):sha(raw) for revision,raw in catalog_evidence.reports.items()},
          'views':{str(revision):sha(raw) for revision,raw in catalog_evidence.views.items()},
          'accepted_sources':{str(revision)+':'+document_id:sha(document.content) for (revision,document_id),document in catalog_evidence.sources.items()},
          'runtime_wheel_sha256':catalog_evidence.runtime_wheel_sha256,'runtime_manifest_sha256':catalog_evidence.runtime_manifest_sha256,
          'acknowledgement':False,'public_api':'read_graph_catalog_evidence with Truss .51 accepted_document_in_transaction and catalog_view_in_transaction'}
        write_new(receipts/'registrar-catalog-public-reread.json',catalog_receipt)
        write_new(receipts/'original-unacknowledged-page-reread.json',{'page_sha256':page.sha256,'prior_sha256':sha(page.prior.content),
          'next_sha256':sha(page.next.content),'bytes':len(page.content),'registration_id':page.registration_id,'writer_xid':page.writer_xid,
          'acknowledgement':False,'matches_r16b_receipt':True})
        if any(os.environ.get(k) for k in ('SPARK_REMOTE','MASTER','SPARK_CONNECT_MODE_ENABLED','PYSPARK_SUBMIT_ARGS','PYSPARK_GATEWAY_PORT','PYSPARK_GATEWAY_SECRET')):
            raise StopRun('Remote or externally configured Spark mode is prohibited')
        java_home=Path(scope['runtime']['java_home']);jar_dir=Path(scope['runtime']['delta_jar_directory'])
        runtime=type('RuntimeIdentity',(),{'java_home':java_home,'jars':jar_dir,'jar_inventory':scope['runtime']['jar_inventory']})()
        _,runtime_identity,jars=attest(runtime)
        os.environ['JAVA_HOME']=str(java_home);os.environ['PYSPARK_PYTHON']=scope['runtime']['python_path'];os.environ['PYSPARK_DRIVER_PYTHON']=scope['runtime']['python_path']
        if SparkSession.getActiveSession() is not None or getattr(SparkSession,'_instantiatedSession',None) is not None or getattr(SparkContext,'_active_spark_context',None) is not None:
            raise StopRun('R20 requires a fresh local Spark process')
        warehouse=paths['warehouse'];warehouse.mkdir(mode=0o700)
        spark=(SparkSession.builder.master('local[1]').appName('Ashlar R20 local Truss Delta publication')
          .config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
          .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
          .config('spark.sql.legacy.parameterSubstitution.constantsOnly','true').config('spark.sql.ansi.enabled','true')
          .config('spark.sql.decimalOperations.allowPrecisionLoss','false').config('spark.jars',','.join(str(p) for p in jars))
          .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
          .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
          .config('spark.sql.warehouse.dir',str(warehouse)).getOrCreate())
        if spark.conf.get('spark.driver.memory')!='512m': raise StopRun('R20 local Spark heap setting differs')
        verify_loaded_delta_class(spark,jars)
        write_new(receipts/'spark-delta-runtime.json',{'runtime':runtime_identity,'local_master':spark.sparkContext.master,
          'spark_driver_memory':spark.conf.get('spark.driver.memory'),'delta_class_verified':True})
        spec=scope['publication']
        publication_may_be_committed=True
        result=publish_graph_pages(spark,paths['delta_root'],connection_factory=connect_reader,consumer_id=consumer,feed=spec['feed'],
          maximum_pages=1,wait_seconds=0,archived_acceptance_files=(),export_graph=False,destination_id=spec['destination_id'],
          maximum_page_bytes=spec['maximum_page_bytes'],carrier_profile=spec['carrier_profile'],umf_sdk_version=spec['umf_sdk_version'],
          defer_source_acknowledgement=True,expected_first_page=expected_page,catalog_connection_factory=connect_catalog)
        if result.get('source_acknowledgement')!='deferred_not_attempted' or len(result.get('publications',[]))!=1:
            raise StopRun('R20 must publish exactly one page and make no source ACK')
        deferred=result['publications'][0]['deferred_ack_intent'];sidecar=Path(deferred['path']);sidecar_raw=sidecar.read_bytes();intent=read_json(sidecar)
        if sha(sidecar_raw)!=deferred['sha256'] or intent.get('acknowledgement')!='deferred_not_attempted' or intent.get('destination_id')!=spec['destination_id']:
            raise StopRun('R20 committed deferred ACK sidecar identity mismatch')
        req_raw=base64.b64decode(intent['request_bytes_base64'],validate=True);man_raw=base64.b64decode(intent['manifest_bytes_base64'],validate=True)
        if sha(req_raw)!=intent['request_sha256'] or sha(man_raw)!=intent['manifest_sha256']: raise StopRun('R20 retained request/manifest raw digest mismatch')
        request=json.loads(req_raw,object_pairs_hook=unique_pairs);manifest=json.loads(man_raw,object_pairs_hook=unique_pairs)
        from ashlar.attempt_store import verify_request_digest
        if verify_request_digest(request)!=intent['request_digest'] or manifest!=intent['manifest'] or manifest['publication_id']!=intent['publication_id']:
            raise StopRun('R20 exact committed request/manifest pair mismatch')
        vector=intent['publication_vector']['targets'];native=[]
        for table,(uuid,version) in sorted(vector.items()):
            role=table.rsplit('.',1)[-1];path=paths['delta_root']/role
            detail=spark.sql('DESCRIBE DETAIL delta.`'+str(path)+'`').limit(2).collect()
            history=spark.sql('DESCRIBE HISTORY delta.`'+str(path)+'`').select('version').limit(2).collect()
            if len(detail)!=1 or len(history)<1 or detail[0]['id']!=uuid or int(history[0]['version'])!=version:
                raise StopRun(f'R20 native Delta table/version differs from exact publication pin: {table}')
            rows=spark.read.format('delta').load(str(path)).count()
            native.append({'table':table,'uuid':uuid,'version':version,'rows':rows,'path':str(path)})
        write_new(receipts/'native-delta-publication-readback.json',{'publication_id':intent['publication_id'],'request_digest':intent['request_digest'],
          'tables':native,'deferred_ack_intent_path':str(sidecar),'deferred_ack_intent_sha256':sha(sidecar_raw),'source_acknowledgement':'deferred_not_attempted'})
        spark.stop();spark=None
        # The source's public page and checkpoint must remain exactly unacknowledged.
        reread=read_page(connect_reader,consumer,maximum_bytes=spec['maximum_page_bytes']);validate_expected_first_page(reread,expected_page)
        admin_params=dict(params);admin_params['user']='postgres'
        with psycopg.connect(**admin_params,autocommit=True,connect_timeout=5) as conn:
          with conn.transaction():
            conn.execute("SET LOCAL statement_timeout='5000ms'");conn.execute('SET TRANSACTION READ ONLY')
            row=conn.execute('SELECT checkpoint_bytes FROM truss.runtime_feed_consumer WHERE consumer_id=%s',(consumer,)).fetchone()
            acks=conn.execute('SELECT count(*) FROM truss.runtime_feed_ack_receipt').fetchone()[0]
            if row is None or sha(row[0])!=scope['r16b_source']['checkpoint_sha256'] or acks!=0:
                raise StopRun('R20 source checkpoint advanced or ACK receipt appeared')
        write_new(receipts/'source-ack-deferred-proof.json',{'page_sha256':reread.sha256,'prior_sha256':sha(reread.prior.content),
          'checkpoint_sha256':sha(row[0]),'ack_rows':acks,'acknowledgement':False,'public_page_unchanged':True})
        write_new(receipts/'completed.json',{'status':'local_development_delta_publication_ack_deferred_review_pending',
          'publication_id':intent['publication_id'],'request_digest':intent['request_digest'],'source_acknowledgement':'deferred_not_attempted',
          'acknowledged':False,'publication_committed':True,'page_count':1,'cloud':False,'production_authority':False,
          'r16b_source_untouched':True,'migration_replayed':False})
    except BaseException as error:
        try:write_new(receipts/'failure.json',{'status':'failed_or_interrupted','failure_type':type(error).__name__,'failure':str(error)[:1200],
          'database_effects_may_remain':database_effects_may_remain,'publication_may_be_committed':publication_may_be_committed,
          'publication_commit_confirmed':False,'acknowledgement':False,'cloud':False,'production_authority':False,'r16b_source_untouched':True})
        except BaseException:pass
        raise
    finally:
        if spark is not None:
            try:spark.stop()
            except BaseException:pass

def main():
    parser=argparse.ArgumentParser();parser.add_argument('--preflight',action='store_true');args=parser.parse_args()
    scope_raw=SCOPE_PATH.read_bytes();scope=read_json(SCOPE_PATH);auth=read_json(HERE/scope['authorization_receipt'])
    worker_raw=Path(__file__).read_bytes();supervisor_raw=(HERE/'r20_supervisor.py').read_bytes()
    result=validate(scope,scope_raw,auth,worker_raw,supervisor_raw,args.preflight)
    if args.preflight:
        print(json.dumps({'status':'passed_effect_free_worker_preflight',**result},sort_keys=True));return
    run_native(scope,result['expected_first_page'],load_r15(),result['source_tree'])

if __name__=='__main__': main()
