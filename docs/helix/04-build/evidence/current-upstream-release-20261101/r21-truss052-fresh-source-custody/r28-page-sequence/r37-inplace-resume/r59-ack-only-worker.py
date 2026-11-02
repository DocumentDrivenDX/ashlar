#!/usr/bin/env python3
"""R59 bounded no-replay reconciliation and protected ACK worker.

This worker has no group submission or Delta publication path. It accepts only
R58's durable pending receipt after exact local journal/head checks, reads the
source receipt and consumer checkpoint in read-only SQL, and delegates the
single ACK to Ashlar's existing Truss .52 custody/recovery owner.
"""
from __future__ import annotations
import argparse, base64, hashlib, importlib.metadata, importlib.util, json, os, sqlite3, stat, sys, tempfile
from pathlib import Path
from urllib.parse import quote

HERE=Path(__file__).resolve().parent
REPO=HERE.parents[7]
if str(REPO/'src') not in sys.path: sys.path.insert(0,str(REPO/'src'))
PROFILE='ashlar-r59-ack-only-preflight/0.1'
SCOPE_DEFAULT=HERE/'r59-host-scope.json'

class Refusal(RuntimeError): pass
def sha(raw): return hashlib.sha256(raw).hexdigest()
def file_sha(path): return sha(Path(path).read_bytes())
def read_json(path):
    def pairs(items):
        out={}
        for k,v in items:
            if k in out: raise Refusal('duplicate JSON key')
            out[k]=v
        return out
    value=json.loads(Path(path).read_bytes(),object_pairs_hook=pairs)
    if type(value) is not dict: raise Refusal('JSON object required')
    return value
def tree(root,maximum=128*1024**2):
    root=Path(root)
    if root.is_symlink() or root.resolve(strict=True)!=root or not root.is_dir(): raise Refusal('canonical PGDATA required')
    h=hashlib.sha256(); files=total=0
    entries=sorted(root.rglob('*'),key=lambda p:p.relative_to(root).as_posix().encode())
    if len(entries)>200000: raise Refusal('PGDATA entry limit exceeded')
    for p in entries:
        st=p.lstat(); rel=p.relative_to(root).as_posix().encode()
        if stat.S_ISLNK(st.st_mode) or st.st_uid!=os.getuid(): raise Refusal('foreign or linked PGDATA entry')
        if p.is_dir(): kind=b'D'
        elif p.is_file(): kind=b'F'; files+=1; total+=st.st_size
        else: raise Refusal('special PGDATA entry')
        if total>maximum: raise Refusal('PGDATA byte limit exceeded')
        h.update(kind+len(rel).to_bytes(4,'big')+rel+(st.st_mode&0o777).to_bytes(4,'big')+st.st_size.to_bytes(8,'big'))
        if kind==b'F':
            with p.open('rb') as stream:
                while chunk:=stream.read(1024*1024): h.update(chunk)
    return {'sha256':h.hexdigest(),'files':files,'bytes':total}
def sidecar(path):
    path=Path(path)
    try:
        st=path.lstat()
        if not stat.S_ISREG(st.st_mode) or stat.S_ISLNK(st.st_mode): raise Refusal('invalid SQLite sidecar')
        return {'present':True,'bytes':st.st_size,'sha256':sha(path.read_bytes()) if path.suffix=='.wal' else None}
    except FileNotFoundError: return {'present':False,'bytes':0,'sha256':None}
def load_module(name,path):
    spec=importlib.util.spec_from_file_location(name,path)
    if spec is None or spec.loader is None: raise Refusal('scope-bound helper import failed')
    module=importlib.util.module_from_spec(spec); spec.loader.exec_module(module); return module

def preflight(scope_path,*,require_absent=True,supervisor_root_created=None):
    scope_path=Path(scope_path).resolve(strict=True); raw=scope_path.read_bytes(); s=read_json(scope_path)
    dispatchable=s.get('status')=='sol_reviewed_and_dispatchable'
    if (s.get('profile')!='ashlar-r59-ack-only/0.1' or s.get('status') not in ('pending_sol_review','sol_reviewed_and_dispatchable') or
        type(s.get('native_dispatch')) is not bool or s.get('native_dispatch') is not dispatchable):
        raise Refusal('R59 preflight/dispatch scope and native_dispatch bit disagree')
    if s.get('cloud') is not False or s.get('production_authority') is not False: raise Refusal('local-only scope required')
    if s.get('limits')!={'wall_seconds':900,'process_rss_bytes':2147483648,'attempt_root_bytes':536870912,'attempts':1,'automatic_retries':0,'cloud':False,'network_listen':False}:
        raise Refusal('bounded single-attempt limits differ')
    if Path(s['worker']['path']).resolve(strict=True)!=Path(__file__).resolve(strict=True) or file_sha(s['worker']['path'])!=s['worker']['sha256']:
        raise Refusal('exact R59 worker path/hash binding differs')
    paths=s['paths']; attempt=Path(paths['output_root']); supervisor=Path(paths['supervisor_root']); pg=Path(paths['data_directory']); delta=Path(paths['publication_directory'])
    if not attempt.is_absolute() or not supervisor.is_absolute() or attempt.exists(): raise Refusal('fresh R59 attempt root required')
    if require_absent and supervisor.exists(): raise Refusal('fresh R59 supervisor root required')
    if not require_absent:
        if supervisor_root_created!=supervisor or not supervisor.is_dir() or supervisor.is_symlink() or {p.name for p in supervisor.iterdir()}!={'child.stdout.log','child.stderr.log'}: raise Refusal('exact supervisor-created R59 logs required')
    if pg.resolve(strict=True)!=pg or delta.resolve(strict=True)!=delta or (pg/'postmaster.pid').exists(): raise Refusal('existing canonical stopped PGDATA and Delta required')
    pg_tree=tree(pg)
    if pg_tree!=s['current_source_state_binding']['tree']: raise Refusal('current stopped PGDATA tree differs from R59 binding')
    pending_path=Path(s['r58']['pending_receipt_path']); pending_raw=pending_path.read_bytes(); pending=read_json(pending_path)
    if sha(pending_raw)!=s['r58']['pending_receipt_sha256'] or pending.get('status')!='publication_returned_native_readback_pending' or pending.get('acknowledgement')!='not_attempted': raise Refusal('R58 durable pending ACK receipt differs')
    digest=s['r58']['request_digest']; pubid=s['r58']['publication_id']; page=pending.get('source_page',{})
    if (pending.get('request_digest')!=digest or pending.get('publication_id')!=pubid or pubid!='truss-graph-'+digest or
        page.get('page_sha256')!=s['r58']['page_sha256'] or page.get('prior_sha256')!=s['predecessor']['checkpoint_sha256'] or
        page.get('writer_xid')!=s['r58']['writer_xid'] or pending.get('feed')!=s['source']['feed_selection']):
        raise Refusal('R58 pending receipt does not bind exact R55 page and R49 checkpoint')
    for key in ('result_path','held_ack_path','native_readback_path','page_selection_path','deferred_ack_path','page_archive_path','publication_intent_path'):
        p=Path(s['r58'][key]); expected=s['r58'][key.replace('_path','_sha256')]
        if p.resolve(strict=True)!=p or file_sha(p)!=expected: raise Refusal('retained R58/R55 artifact changed: '+key)
    held=read_json(s['r58']['held_ack_path']); rb=read_json(s['r58']['native_readback_path']); selection=read_json(s['r58']['page_selection_path'])
    if (held.get('source_ack_attempted') is not False or held.get('status')!='native_readback_complete_ack_not_attempted' or
        rb.get('ack_attempted') is not False or rb.get('publication_replayed') is not False or rb.get('publication_id')!=pubid or
        rb.get('independent_native_readback') is not True or selection.get('source_ack_attempted') is not False or
        selection.get('page_sha256')!=s['r58']['page_sha256'] or selection.get('prior_cursor_sha256')!=s['predecessor']['checkpoint_sha256'] or
        not {'page_custody','r49_property_crosscheck','r36_frozen_request_correspondence'}<=set(selection.get('committed_page_correspondence',{}))):
        raise Refusal('R58 deferred ACK and independent native readback custody differs')
    r55=s['retained_r55']
    for path_key,digest_key in (('group_receipt_path','group_receipt_sha256'),('source_page_path','source_page_sha256'),('publication_intent_path','publication_intent_sha256')):
        p=Path(r55[path_key])
        if p.resolve(strict=True)!=p or file_sha(p)!=r55[digest_key]: raise Refusal('R55 committed source custody changed: '+path_key)
    group=read_json(r55['group_receipt_path'])
    if (group.get('commit_confirmed') is not True or group.get('group_variant')!='GroupSuccess' or group.get('response_variant')!='AppliedPending' or group.get('result_count')!=2 or group.get('request_key')!=s['continuation']['request_key']): raise Refusal('R55 original GroupSuccess/AppliedPending two-result receipt differs')
    page_match=selection['committed_page_correspondence']['r36_frozen_request_correspondence']
    if (page_match.get('group_request_key')!=s['continuation']['request_key'] or page_match.get('writer_xid')!=s['r58']['writer_xid'] or
        page_match.get('object_creates')!=2 or page_match.get('edge_creates')!=0 or page_match.get('typed_entity_changes')!=2):
        raise Refusal('R58 public page no longer binds the exact two-operation R55 request')
    journal=Path(s['delta']['journal_path'])
    if journal!=delta/'operations.sqlite' or file_sha(journal)!=s['delta']['journal_sha256']: raise Refusal('current SQLite journal changed')
    before={suffix:sidecar(str(journal)+suffix) for suffix in ('-wal','-shm')}
    with sqlite3.connect('file:'+quote(str(journal))+'?mode=ro&immutable=1',uri=True,timeout=5) as db:
        integrity=db.execute('PRAGMA integrity_check').fetchone()[0]
        counts={table:db.execute(f'SELECT count(*) FROM {table}').fetchone()[0] for table in ('local_operation','local_plan','local_publication_artifact','local_source_plan')}
        r55_plan=db.execute('SELECT count(*) FROM local_source_plan WHERE request_digest=?',(digest,)).fetchone()[0]
        r55_art=db.execute('SELECT request,artifact FROM local_publication_artifact WHERE request_digest=?',(digest,)).fetchall()
    after={suffix:sidecar(str(journal)+suffix) for suffix in ('-wal','-shm')}
    if (integrity!='ok' or counts!=s['delta']['journal_counts'] or r55_plan!=1 or len(r55_art)!=1 or before!=after): raise Refusal('R55 immutable journal evidence/sidecars differ')
    req=json.loads(r55_art[0][0]); artifact=json.loads(r55_art[0][1])
    if req.get('request_digest')!=digest or req.get('batch_id')!=s['r58']['writer_xid'] or artifact.get('manifest',{}).get('publication_id')!=pubid: raise Refusal('R55 applied journal artifact does not bind R58 page')
    heads=[]
    for binding in s['delta']['heads']:
        p=Path(binding['path'])
        if not p.is_file() or file_sha(p)!=binding['sha256']: raise Refusal('Delta head changed: '+binding['table'])
        heads.append(binding['table'])
    role_heads={item['table'].split('.')[-1]:item for item in rb['roles'].values()}
    if set(role_heads)!=set(('edge_current','object_current','tombstone','whole_source_history')): raise Refusal('R58 complete four-role native readback required')
    for role,item in role_heads.items():
        binding=next(v for v in s['delta']['heads'] if v['table']==role)
        if (item['head_json_sha256']!=binding['sha256'] or item['head_version']!=binding['version'] or
            list(rb['vector'][item['table']])!=[item['uuid'],item['version']]): raise Refusal('R58 native role/vector differs from current Delta head')
    r49=s['retained_r49']
    for field,digest_field in (('scope_path','scope_sha256'),('worker_path','worker_sha256'),('reconciliation_path','reconciliation_sha256')):
        p=Path(r49[field])
        if p.resolve(strict=True)!=p or file_sha(p)!=r49[digest_field]: raise Refusal('R49 ACK predecessor custody changed')
    r49_recon=read_json(r49['reconciliation_path'])
    if r49_recon.get('ack',{}).get('status')!='matched_local_development_only' or r49_recon.get('ack',{}).get('checkpoint_sha256')!=s['predecessor']['checkpoint_sha256']: raise Refusal('exact R49 predecessor ACK required')
    owner_binding=s['r49_ack_owner_binding'];base_binding=s['native_helper_bindings']['r37_local_worker']
    if (owner_binding['worker_path']!=r49['worker_path'] or owner_binding['worker_sha256']!=r49['worker_sha256'] or
        file_sha(owner_binding['worker_path'])!=owner_binding['worker_sha256'] or file_sha(base_binding['path'])!=base_binding['sha256']):
        raise Refusal('exact R49 ACK owner and R37 helper import bindings required')
    r49intent=s['predecessor']['publication_intent']
    if file_sha(r49intent['path'])!=r49intent['sha256']: raise Refusal('applied R49 resume intent changed')
    runtime=s['runtime_candidate']; python=Path(runtime['local_runtime_base_python']); wheel=Path(runtime['wheel_path'])
    if Path(sys.executable).resolve(strict=True)!=python.resolve(strict=True) or file_sha(wheel)!=runtime['wheel_sha256']: raise Refusal('selected local Python/wheel binding differs')
    sys.path[:0]=[str(Path(runtime['new_overlay_or_venv']).resolve(strict=True)/'site-packages'),str(Path(runtime['local_extra_overlay']).resolve(strict=True))]
    if importlib.metadata.version('umf-truss')!='0.0.52': raise Refusal('Truss .52 runtime required')
    truss_spec=importlib.util.find_spec('truss')
    if truss_spec is None or truss_spec.origin is None: raise Refusal('selected Truss package origin required')
    sdk_root=Path(truss_spec.origin).resolve(strict=True).parent
    sql_contracts={name:sdk_root/'_catalog_runtime'/name for name in ('feed.sql','operations.sql','groups.sql','group-recovery.sql')}
    if any(not p.is_file() or p.is_symlink() for p in sql_contracts.values()): raise Refusal('selected Truss .52 SQL contract sources unavailable')
    feed_sql=sql_contracts['feed.sql'].read_text();ops_sql=sql_contracts['operations.sql'].read_text();groups_sql=sql_contracts['groups.sql'].read_text();recover_sql=sql_contracts['group-recovery.sql'].read_text()
    if ('CREATE TABLE truss.runtime_feed_consumer' not in feed_sql or 'registration_id uuid NOT NULL UNIQUE' not in feed_sql or
        'checkpoint_bytes bytea NOT NULL' not in feed_sql or 'CREATE TABLE truss.runtime_object_attempt' not in ops_sql or
        any(field not in ops_sql for field in ('request_bytes bytea NOT NULL','documents_bytes bytea NOT NULL','input_bytes bytea NOT NULL','results jsonb NOT NULL','receipt_id bigint')) or
        "'truss-object-group-receipt/0.1'" not in ops_sql or 'truss.runtime_authorize_retained_request' not in recover_sql or
        'sha256(rk)' not in recover_sql): raise Refusal('R59 read-only SQL projection does not match the selected public .52 schema/result contract')
    sql_contract_hashes={name:file_sha(path) for name,path in sql_contracts.items()}
    # Prove the public recovery owner is importable and still exposes its no-replay ACK entrypoint.
    import ashlar_host.truss_local_publication_recovery as recovery
    import inspect
    api=inspect.signature(recovery.recover_committed_truss_publication_and_ack).parameters
    if not callable(recovery.recover_committed_truss_publication_and_ack) or not {'driver','request_digest','pending_receipt_bytes','anchor','spark','custody_session_factory','completion_path','ack_attempt_path','ack_reconciliation_path','publication_intent_path','publication_intent_sha256'}<=set(api): raise Refusal('protected ACK owner API mismatch')
    from ashlar_host.truss_resume_cli import parse_resume_truss_configuration
    rt=s['spark_runtime']
    parser_config={'connection_uri':'postgresql://preflight.invalid/no-connect','catalog_connection_uri':'postgresql://preflight.invalid/no-connect',
      'publication_directory':str(delta),'jars':[item['path'] for item in rt['jars']],
      'publication_intent':{'path':s['predecessor']['publication_intent']['path'],'sha256':s['predecessor']['publication_intent']['sha256']},
      'destination_id':s['publication']['destination_id'],'carrier_profile':s['publication']['carrier_profile'],
      'runtime_profile':{'profile':rt['runtime_profile'],'java_home':rt['java_home'],'jar_inventory':{item['name']:item['sha256'] for item in rt['jars']}},
      'archived_acceptance_files':[]}
    with tempfile.TemporaryDirectory(prefix='ashlar-r59-parser-',dir='/private/tmp') as scratch_name:
        scratch=Path(scratch_name); config=scratch/'resume-config.json'; output=scratch/'resume-output.json'
        config.write_text(json.dumps(parser_config,sort_keys=True,separators=(',',':'))+'\n')
        parsed=parse_resume_truss_configuration(config,output)[0]
        if parsed['request_digest']!=s['predecessor']['request_digest']: raise Refusal('native-shaped resume parser must select applied R49 predecessor')
    # Exercise the established R49 ACK-owner request/page shape path without
    # making public API calls, connecting to PostgreSQL, or opening Spark.
    owner=load_module('ashlar_r49_ack_owner_preflight',Path(s['r49_ack_owner_binding']['worker_path']))
    base_binding=load_module('ashlar_r37_base_preflight',Path(s['native_helper_bindings']['r37_local_worker']['path']))
    base_helpers=base_binding.load_base();owner_scope=dict(s)
    owner_scope['recovery']={'publication_intent_path':s['r58']['publication_intent_path'],'publication_intent_sha256':s['r58']['publication_intent_sha256'],
      'request_digest':digest,'publication_id':pubid,'source_page_path':s['r58']['page_archive_path'],'source_page_archive_sha256':s['r58']['page_archive_sha256']}
    owner_scope['retained_r36']={'ack_checkpoint_sha256':s['predecessor']['checkpoint_sha256']}
    owner_scope['publication']=dict(s['publication']);owner_scope['publication']['predecessor']=s['predecessor']['publication_id']
    resume_dir=HERE.parent/Path(s['resume']['receipt_directory'])
    evidence={name:read_json(resume_dir/name) for name in ('acceptance.json','installation.json','consumer-registration.json')}
    operation=owner._public_recovery_operation(owner_scope,pending_raw,attempt/'receipts',lambda *_a,**_k:None,base_helpers,evidence)
    try: operation(None,{},validate_only=True)
    except owner.RecoveryInputShapeValidated as validated:
        recovery_shape={'status':'passed_no_api_recovery_shape_sentinel','request_digest':validated.inputs['pending']['request_digest'],
          'prior_checkpoint_sha256':validated.inputs['page']['prior_sha256'],'publication_intent_sha256':s['r58']['publication_intent_sha256'],
          'postgres_opened':False,'spark_started':False,'source_ack_attempted':False}
    else: raise Refusal('R49 ACK-owner effect-free validation sentinel not reached')
    return {'profile':PROFILE,'status':'passed_effect_free_ack_preflight','scope_sha256':sha(raw),'worker_sha256':file_sha(__file__),
      'pgdata_tree':pg_tree,'postmaster_stopped':True,'journal_integrity':integrity,'journal_counts':counts,'sqlite_sidecars_before':before,'sqlite_sidecars_after':after,
      'r55_source_plan_count':r55_plan,'r55_applied_artifact_count':len(r55_art),'delta_head_tables':heads,'native_readback_role_count':len(role_heads),
      'publication_id':pubid,'request_digest':digest,'r49_checkpoint_sha256':s['predecessor']['checkpoint_sha256'],
      'real_resume_parser':{'status':'passed_effect_free_parser','selected_request_digest':parsed['request_digest'],'intent_selector_keys':['path','sha256'],'selected_intent_sha256':s['predecessor']['publication_intent']['sha256'],'native_effects':False},
      'selected_truss_sql_contract_hashes':sql_contract_hashes,
      'r49_ack_owner_shape':recovery_shape,
      'source_sql_confirmation':'mandatory_in_supervised_native_worker_before_ack','postgres_opened':False,'spark_started':False,'delta_written':False,'source_ack_attempted':False,'group_resubmitted':False,'native_dispatch':False,'cloud':False}

def write_private(path,value):
    path=Path(path); raw=(json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)+'\n').encode()
    if path.exists() or path.is_symlink() or path.parent.stat().st_mode&0o077: raise Refusal('fresh private receipt path required')
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        n=0
        while n<len(raw): n+=os.write(fd,raw[n:])
        os.fsync(fd)
    finally: os.close(fd)
    dfd=os.open(path.parent,os.O_RDONLY|getattr(os,'O_DIRECTORY',0))
    try: os.fsync(dfd)
    finally: os.close(dfd)
    return sha(raw)

def native(scope_path,supervisor_root):
    scope_path=Path(scope_path).resolve(strict=True); scope=read_json(scope_path); raw=scope_path.read_bytes()
    if (scope.get('status')!='sol_reviewed_and_dispatchable' or scope.get('native_dispatch') is not True or
        os.environ.get('ASHLAR_R36_SUPERVISED')!='1' or os.environ.get('ASHLAR_R36_SCOPE_SHA256')!=sha(raw) or
        Path(os.environ.get('ASHLAR_R36_ATTEMPT_ROOT',''))!=Path(scope['paths']['output_root']) or
        Path(os.environ.get('ASHLAR_R36_SUPERVISOR_ROOT',''))!=Path(scope['paths']['supervisor_root']) or
        os.environ.get('ASHLAR_R36_CLOUD')!='0'):
        raise Refusal('exact reviewed R59 supervisor handshake required')
    preflight(scope_path,require_absent=False,supervisor_root_created=Path(supervisor_root))
    runtime_selection=scope['runtime_candidate']; overlay=Path(runtime_selection['new_overlay_or_venv']).resolve(strict=True)/'site-packages'; extras=Path(runtime_selection['local_extra_overlay']).resolve(strict=True)
    sys.path[:0]=[str(overlay),str(extras)]
    if importlib.metadata.version('umf-truss')!='0.0.52': raise Refusal('Truss .52 required')
    dist=importlib.metadata.distribution('umf-truss'); direct=json.loads(dist.read_text('direct_url.json') or '{}')
    if direct.get('archive_info',{}).get('hashes',{}).get('sha256')!=runtime_selection['wheel_sha256']: raise Refusal('selected Truss wheel origin differs')
    import psycopg
    from truss import LocalPostgres, CatalogRuntimeStatus, CatalogRuntimeConfiguration, create_reference_assembly
    from truss.catalog_results import CatalogRegistration
    from truss.feed import FeedConsumer,FeedPage
    from truss.reader_catalog import AcceptedDocument
    from ashlar_host.truss_note_publication import read_page
    from ashlar_host.truss_resume_cli import parse_resume_truss_configuration,run_selected_truss_command
    from ashlar_host.configured_local_runtime_profile import attest
    from types import SimpleNamespace
    rt=scope['spark_runtime']; jars=[Path(i['path']).resolve(strict=True) for i in rt['jars']]; java_home=Path(rt['java_home']).resolve(strict=True)
    for item,jar in zip(rt['jars'],jars):
        if file_sha(jar)!=item['sha256']: raise Refusal('selected Delta JAR changed')
    os.environ['JAVA_HOME']=str(java_home);os.environ['SPARK_LOCAL_IP']='127.0.0.1';os.environ['PYSPARK_PYTHON']=str(Path(runtime_selection['local_runtime_base_python']).resolve());os.environ['PYSPARK_DRIVER_PYTHON']=os.environ['PYSPARK_PYTHON']
    profile,identity,verified=attest(SimpleNamespace(java_home=java_home,jars=jars[0].parent,jar_inventory={i['name']:i['sha256'] for i in rt['jars']}))
    if profile!=rt['runtime_profile'] or list(verified)!=jars or identity.get('jdk')!='21': raise Refusal('pinned local Spark runtime attestation differs')
    attempt=Path(scope['paths']['output_root']);attempt.mkdir(mode=0o700);os.chmod(attempt,0o700);receipts=attempt/'receipts';receipts.mkdir(mode=0o700)
    pg=Path(scope['paths']['data_directory']); effects={'postgres_opened':False,'spark_started':False,'delta_written':False,'source_ack_attempted':False,'group_resubmitted':False,'publication_replayed':False,'cloud':False}
    postgres=LocalPostgres(pg);primary=None
    try:
        postgres.__enter__();effects['postgres_opened']=True;info=postgres.info
        if info.server_version!='18.6' or Path(info.data_directory)!=pg: raise Refusal('exact retained PostgreSQL identity required')
        params=psycopg.conninfo.conninfo_to_dict(info.connection_uri);source=scope['source']
        def connect(role,autocommit=False): return psycopg.connect(**dict(params,user=role),autocommit=autocommit,connect_timeout=5,options='-c statement_timeout=10000')
        # Explicit read-only SQL reconciliation is executed inside the supervised run,
        # before the protected owner can create its durable ACK-attempt marker.
        pending_path=Path(scope['r58']['pending_receipt_path']);pending_raw=pending_path.read_bytes();pending=read_json(pending_path)
        request_key=scope['continuation']['request_key']; feed=source['feed_selection']; consumer_id=source['consumer_id']
        def source_sql_reconcile():
            with connect('postgres',False) as c:
                c.execute('SET TRANSACTION READ ONLY');c.execute("SET LOCAL statement_timeout='10000ms'")
                identity=c.execute("SELECT current_user,session_user,current_setting('transaction_read_only')").fetchone()
                row=c.execute("""SELECT storage_row_id,payload_state,original_writer_xid::text,
                    encode(namespace_sha256,'hex'),encode(request_sha256,'hex'),
                    convert_from(namespace_identity_utf8,'UTF8'),convert_from(request_id_utf8,'UTF8'),
                    original_writer_context_bytes,octet_length(original_receipt_bytes),
                    encode(pg_catalog.sha256(original_receipt_bytes),'hex'),
                    CASE WHEN octet_length(original_receipt_bytes)<=16777216 THEN original_receipt_bytes END
                    FROM truss.request_receipt WHERE request_id_utf8=convert_to(%s,'UTF8') ORDER BY storage_row_id LIMIT 2""",(request_key,)).fetchall()
                consumer=c.execute("SELECT consumer_id,registration_id::text,checkpoint_bytes FROM truss.runtime_feed_consumer WHERE consumer_id=%s",(consumer_id,)).fetchall()
                ro=c.execute("SELECT current_setting('transaction_read_only')").fetchone()[0]
                c.rollback()
            if len(row)!=1 or len(consumer)!=1 or ro!='on' or identity!=('postgres','postgres','on'): raise Refusal('exact one administrator-owned read-only request receipt and consumer row required')
            x=row[0]
            try: writer=json.loads(bytes(x[7]))
            except Exception as e: raise Refusal('source writer context is not valid JSON') from e
            try: receipt=json.loads(bytes(x[10]))
            except Exception as e: raise Refusal('source original receipt is not valid JSON') from e
            if (x[1]!='complete' or x[2]!=scope['r58']['writer_xid'] or x[4]!=sha(request_key.encode()) or x[8]>16777216 or x[10] is None or
                x[6]!=request_key or type(receipt) is not dict or type(writer) is not dict or
                consumer[0][1]!=source['consumer_registration_id']): raise Refusal('source request receipt/consumer identity differs from committed R55 page')
            if writer.get('profile')!='truss-object-group-writer/0.1' or writer.get('xid')!=x[2] or type(writer.get('attemptId')) is not str or type(writer.get('actorOid')) is not str:
                raise Refusal('source receipt lacks exact original Truss .52 group writer context')
            expected_namespace='truss-person:'+scope['installation']['installation_id']+':'+writer['actorOid']
            if x[5]!=expected_namespace or x[3]!=sha(expected_namespace.encode('utf-8')): raise Refusal('source receipt namespace differs from the prepared R55 group scope')
            with connect('postgres',False) as c:
                c.execute('SET TRANSACTION READ ONLY');c.execute("SET LOCAL statement_timeout='10000ms'")
                attempt_rows=c.execute("""SELECT id::text,writer_xid::text,complete,receipt_id,request_bytes,documents_bytes,input_bytes,results,context
                    FROM truss.runtime_object_attempt WHERE id=%s::uuid""",(writer['attemptId'],)).fetchall()
                attempt_ro=c.execute("SELECT current_setting('transaction_read_only')").fetchone()[0];c.rollback()
            if len(attempt_rows)!=1 or attempt_ro!='on': raise Refusal('exact original group attempt row required')
            ar=attempt_rows[0]
            try:
                original_request=json.loads(bytes(ar[4])); attempt_context=ar[8] if type(ar[8]) is dict else json.loads(ar[8])
            except Exception as e: raise Refusal('original Truss group attempt bytes/context are not valid JSON') from e
            if (ar[0]!=writer['attemptId'] or ar[1]!=x[2] or ar[2] is not True or ar[3]!=x[0] or
                attempt_context.get('authorizedScopeIdentity')!=expected_namespace or
                original_request.get('identity',{}).get('requestId')!=request_key or
                original_request.get('identity',{}).get('authorizedScopeIdentity')!=expected_namespace or
                type(ar[7]) is not list or len(ar[7])!=2): raise Refusal('source group attempt does not bind the committed two-operation request')
            model_path=Path(scope['model']['derived_path']);model_path=model_path if model_path.is_absolute() else HERE.parent/model_path
            expected_documents=[{'documentId':scope['model']['document_id'],'sourceText':model_path.read_bytes().decode('utf-8')}]
            try: actual_documents=json.loads(bytes(ar[5]))
            except Exception as e: raise Refusal('original group document bytes are not valid JSON') from e
            if actual_documents!=expected_documents: raise Refusal('original group document bytes differ from the exact accepted source document')
            try: group_input=json.loads(bytes(ar[6]))
            except Exception as e: raise Refusal('original Truss group input bytes are not valid JSON') from e
            operations=group_input.get('operations') if type(group_input) is dict else None
            if (group_input.get('interfaceVersion')!='truss-group-input/0.1.0' or type(operations) is not list or len(operations)!=2 or
                any(type(op) is not dict or op.get('operation')!='create_object' for op in operations)):
                raise Refusal('original Truss group input is not the exact two-create R55 group')
            if (receipt.get('profile')!='truss-object-group-receipt/0.1' or receipt.get('attemptId')!=writer['attemptId'] or
                base64.b64decode(receipt.get('inputBase64',''),validate=True)!=bytes(ar[6]) or
                type(receipt.get('semantic')) is not dict or receipt['semantic'].get('interfaceVersion')!='truss-group-result/0.1.0' or
                type(receipt['semantic'].get('results')) is not list or len(receipt['semantic']['results'])!=2 or
                receipt['semantic'].get('results')!=ar[7]): raise Refusal('source original receipt lacks exact Truss GroupSuccess semantic result custody')
            claimed=original_request.get('identity',{}).get('claimedSha256')
            if (len(bytes(ar[4]))==0 or len(bytes(ar[5]))==0 or len(bytes(ar[6]))==0 or
                original_request.get('state')!='present' or (claimed is not None and claimed!=sha(bytes(ar[6])))):
                raise Refusal('source request digest/input/document binding differs from exact Truss attempt')
            return {'profile':'ashlar-r59-read-only-source-sql/0.1','status':'matched_source_request_receipt_and_consumer','transaction_read_only':True,
                'request_key':x[6],'request_sha256':x[4],'namespace_identity':x[5],'payload_state':x[1],'writer_xid':x[2],
                'writer_context_sha256':sha(bytes(x[7])),'original_receipt_bytes':x[8],'original_receipt_sha256':x[9],
                'attempt_id':ar[0],'group_input_sha256':sha(bytes(ar[6])),'group_document_bytes_sha256':sha(bytes(ar[5]),),
                'group_request_bytes_sha256':sha(bytes(ar[4])),'group_result_count':len(ar[7]),'semantic_interface_version':receipt['semantic']['interfaceVersion'],
                'consumer_id':consumer[0][0],'consumer_registration_id':consumer[0][1],
                'consumer_checkpoint_sha256':sha(bytes(consumer[0][2])),'expected_pre_ack_checkpoint_sha256':scope['predecessor']['checkpoint_sha256']}
        sql_evidence=source_sql_reconcile()
        if sql_evidence['consumer_checkpoint_sha256']!=scope['predecessor']['checkpoint_sha256']: raise Refusal('live consumer is not at exact R49 ACK checkpoint before R59 ACK')
        write_private(receipts/'pre-ack-source-sql-reconciliation.json',sql_evidence)
        # The public R49 recovery worker owns page selection, native readback,
        # custody open/re-read, the single ACK, and same-session reconciliation.
        r49=scope['retained_r49'];r49mod=load_module('ashlar_r49_ack_owner',Path(r49['worker_path']))
        base_helper=load_module('ashlar_r37_base',Path(scope['native_helper_bindings']['r37_local_worker']['path']))
        base=base_helper.load_base()
        resume_dir=HERE.parent/Path(scope['resume']['receipt_directory'])
        evidence={name:read_json(resume_dir/name) for name in ('acceptance.json','installation.json','consumer-registration.json')}
        owner_scope=dict(scope)
        owner_scope['recovery']={'publication_intent_path':scope['r58']['publication_intent_path'],
            'publication_intent_sha256':scope['r58']['publication_intent_sha256'],'request_digest':scope['r58']['request_digest'],
            'publication_id':scope['r58']['publication_id'],'source_page_path':scope['r58']['page_archive_path'],
            'source_page_archive_sha256':scope['r58']['page_archive_sha256']}
        owner_scope['retained_r36']={'ack_checkpoint_sha256':scope['predecessor']['checkpoint_sha256']}
        owner_scope['publication']=dict(scope['publication']);owner_scope['publication']['predecessor']=scope['predecessor']['publication_id']
        def operation(driver,config):
            op=r49mod._public_recovery_operation(owner_scope,pending_raw,receipts,connect,base,evidence)
            try: return op(driver,config)
            except BaseException:
                effects['source_ack_attempted']=(receipts/'protected-ack-attempt.json').exists()
                raise
            finally: effects['source_ack_attempted']=(receipts/'protected-ack-attempt.json').exists()
        predecessor_intent=scope['predecessor']['publication_intent']
        config={'connection_uri':psycopg.conninfo.make_conninfo(**dict(params,user=source['ordinary_reader_role'])),
            'catalog_connection_uri':psycopg.conninfo.make_conninfo(**dict(params,user=source['registrar_login_role'])),
            'publication_directory':scope['paths']['publication_directory'],'jars':[str(j) for j in jars],
            'publication_intent':{'path':predecessor_intent['path'],'sha256':predecessor_intent['sha256']},
            'destination_id':scope['publication']['destination_id'],'carrier_profile':scope['publication']['carrier_profile'],
            'runtime_profile':{'profile':rt['runtime_profile'],'java_home':str(java_home),'jar_inventory':{i['name']:i['sha256'] for i in rt['jars']}},'archived_acceptance_files':[]}
        config_path=attempt/'resume-config-r59.json';write_private(config_path,config)
        parse_resume_truss_configuration(config_path,attempt/'resume-output-r59.json')
        effects['spark_started']=True
        result=run_selected_truss_command(config_path,attempt/'resume-output-r59.json',operation)
        if result.get('status')!='matched_local_development_only' or result.get('publication_id')!=scope['r58']['publication_id'] or result.get('publication_replayed') is not False:
            raise Refusal('protected ACK recovery did not reconcile exact R58 publication')
        write_private(receipts/'protected-ack-result.json',result)
        # Independent post-ACK readback in fresh read-only SQL and public FeedPage.
        source_page=read_json(Path(scope['r58']['page_archive_path']))
        next_raw=base64.b64decode(source_page['next'],validate=True)
        with connect('postgres',False) as c:
            c.execute('SET TRANSACTION READ ONLY');c.execute("SET LOCAL statement_timeout='10000ms'")
            after=c.execute("SELECT registration_id::text,checkpoint_bytes FROM truss.runtime_feed_consumer WHERE consumer_id=%s",(consumer_id,)).fetchone();ro=c.execute("SELECT current_setting('transaction_read_only')").fetchone()[0];c.rollback()
        if after is None or after[0]!=source['consumer_registration_id'] or bytes(after[1])!=next_raw or ro!='on': raise Refusal('post-ACK SQL checkpoint readback differs from exact R55 next cursor')
        reader=base.ordinary_reader_connection_factory(psycopg,params,source['ordinary_reader_role'])
        fresh=read_page(reader,consumer_id,maximum_bytes=scope['publication']['maximum_page_bytes'])
        if (type(fresh) is not FeedPage or fresh.outcome!='empty_interval' or fresh.consumer_id!=consumer_id or
            fresh.registration_id!=source['consumer_registration_id'] or fresh.prior.content!=next_raw or fresh.writer_xid is not None):
            raise Refusal('fresh post-ACK public feed page does not confirm empty interval after the R55 acknowledged cursor')
        post={'profile':'ashlar-r59-post-ack-checkpoint/0.1','status':'matched_local_development_only','publication_id':scope['r58']['publication_id'],
            'source_page_sha256':scope['r58']['page_sha256'],'checkpoint_sha256':sha(next_raw),'fresh_empty_page_next_cursor_sha256':sha(fresh.next.content),'public_feed_outcome':fresh.outcome,
            'sql_transaction_read_only':True,'source_ack_replayed':False,'group_resubmitted':False,'delta_republished':False}
        write_private(receipts/'post-ack-checkpoint-readback.json',post)
        write_private(receipts/'recovery-complete.json',{'profile':'ashlar-r59-ack-only/0.1','status':'matched_local_development_only',
            'publication_id':scope['r58']['publication_id'],'source_ack_attempted_once':True,'source_ack_replayed':False,'group_resubmitted':False,'delta_republished':False,
            'pre_ack_sql_sha256':file_sha(receipts/'pre-ack-source-sql-reconciliation.json'),'post_ack_checkpoint_sha256':file_sha(receipts/'post-ack-checkpoint-readback.json')})
    except BaseException as error:
        primary=error
        try: write_private(receipts/'failure.json',{'profile':'ashlar-r59-ack-only/0.1','status':'failed_or_interrupted','failure_type':type(error).__name__,'failure':str(error)[:1000],**effects,
            'ack_may_have_been_attempted':(receipts/'protected-ack-attempt.json').exists()})
        except BaseException: pass
        raise
    finally:
        postgres.__exit__(type(primary) if primary else None,primary,primary.__traceback__ if primary else None)

def main():
    p=argparse.ArgumentParser();p.add_argument('--scope',type=Path,default=SCOPE_DEFAULT);p.add_argument('--preflight-only',action='store_true');p.add_argument('--attempt-root',type=Path);p.add_argument('--supervisor-root',type=Path)
    a=p.parse_args()
    try:
        if a.preflight_only: print(json.dumps(preflight(a.scope),sort_keys=True,separators=(',',':')));return 0
        if a.attempt_root!=Path(read_json(a.scope)['paths']['output_root']) or a.supervisor_root!=Path(read_json(a.scope)['paths']['supervisor_root']): raise Refusal('exact R59 roots required')
        native(a.scope,a.supervisor_root);return 0
    except Exception as e:
        print(json.dumps({'profile':PROFILE,'status':'refused','reason':f'{type(e).__name__}: {e}'[:1200],'native_dispatch':False},sort_keys=True,separators=(',',':')),file=sys.stderr);return 2
if __name__=='__main__': raise SystemExit(main())
