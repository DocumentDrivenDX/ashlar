#!/usr/bin/env python3
"""Single-use public Truss .49 migration and .4 two-page lifecycle campaign.

This candidate performs no native work on import or preflight. Execute is gated
by a fresh exact approval and Page-owned UMF bridge readiness pin. It never
repeats the retained .48 roles or feed registration and never retries source,
publication, or migration effects.
"""
from __future__ import annotations
import argparse, base64, csv, hashlib, importlib.metadata as metadata, io, json, os, shutil, subprocess, sys, time, urllib.parse
from pathlib import Path

REPO=Path('/private/tmp/ashlar-truss049-r3-source-0506fb7')
if str(REPO/'src') not in sys.path:sys.path.insert(0,str(REPO/'src'))
ROOT=Path('/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r3-0506fb7-v2')
ATTEMPT=ROOT/'attempt-r3'
PREP=ROOT/'preparation.json'
RUNTIME_RECEIPT=ROOT/'runtime/runtime-preflight.json'
BRIDGE=ROOT/'bridge-readiness.json'
APPROVAL=ROOT/'execution-approval.json'
SUPERVISOR=ROOT/'run_supervisor.py'
VENV=ROOT/'runtime/venv'
DATA=Path('/private/tmp/ashlar-truss048-source-acceptance-attempt-01-r4/postgres/data')
BACKUP=ATTEMPT/'cold-backup/pgdata'
REGISTRY=ATTEMPT/'truss-migration-recovery.sqlite3'
SOURCE_ROOT=Path('/private/tmp/ashlar-truss045-real-source-20261021')
SOURCE=SOURCE_ROOT/'model/source.umf.json'
ORIGIN=SOURCE_ROOT/'model/acceptance-origin.json'
REQUEST=ROOT/'seed-request.json'
FEED='ashlar-current-045-lifecycle'
CONSUMER='ashlar-current-048-lifecycle-20261022'
REGISTRAR='ashlar048_accept_consumer_r1'
SOURCE_READER='ashlar048_r5_source_reader'
WRITER_LOGIN='ashlar048_lifecycle_seed_writer_r1'
WRITER_ROLE='ashlar048_r5_module_writer'
READER_ROLE='ashlar048_r5_module_reader'
SOURCE_SHA='498a82f313fd3ceaeb6195e133b403aa829acfcda7cec676a33a5790a57d456b'
ORIGIN_SHA='a16b9405b213649ebfbda85ce0bf47877646eccb93b09176190be32bcb674fed'
REQUEST_SHA='56aed4d7a4789f45cdaa7af55a834ff38ca2b4bcb75f44658b3031ba5f8aa771'
REPORT_SHA='c797191c90c516b0997801a63605d297e5e429b50e922cd7da2d39640d570c9a'
VIEW_SHA='cc36fe489326f73c2eaf153ba2e025feeb76eaa88ffa0d6b675a1f4772264dc7'
SOURCE_REGISTRATION='ccfcd423-5893-4df9-8f88-6504e086bd4d'
CONSUMER_REGISTRATION='9f03eb5d-ce72-437f-8b32-0112c130e724'
GENESIS=ATTEMPT/'genesis-page.json'
DESTINATION='ashlar-current-049-lifecycle-destination-20261022'
CARRIER='ashlar-delta/0.4-xxhash64'
JAVA_HOME=Path('/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home')
JARS=(Path('/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-spark_4.2_2.13-4.4.1.jar'),
      Path('/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-storage-4.4.1.jar'))
RUNTIME_SHA='7ee860832a3b7947e9f744b5af2383b4a68229a7bf5d5a6bf5a7350c0dad1ed8'
HELPER=ROOT/'source_048_readonly_helper.py'
HELPER_SHA='ea596c81fbded3879dc1b4ed2ccd2ccab03189f2525e74bbc7401b5adea38d81'
OLD48_VENV=Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/venv')
OLD48_WHEEL_SHA='1aedf614ff3d3ff7a61bd4d91887ede650a7135d1239e8564c1c297c0ddc37c3'
OLD48_RUNTIME_RECEIPT=Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/runtime-preflight.json')
OLD48_RUNTIME_SHA='bbc590badfa88ea7c950a40f867ddd62f409df078aa461575a3b7db6c7288b7b'
EXPECTED_WHEELS={
 'truss-toolkit':('0.0.49','7ea14b2787f02cbeec9ad99d835adbcc30c684fd5be8e0765a1d8198b50e7182'),
 'umf-core':('0.10.0','49633924b45a3c8499d4fcc1a24c30d5a8fcb34d974ab019bbda987cb47da0f9'),
 'umf_access':('0.1.0.dev6','8f6e86ed94983d0156c2d6e9e1bf83fae186fe1d4a288a3846722e64aa365cf0'),
 'weft-sql':('0.4.52','598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46')}
MIGRATION_MANIFEST='96addcdb95523f478d8f50b0b47198e47a860680c7400dd359711930e837e535'
NATIVE_MANIFEST='288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4'
SOURCE_MANIFEST='c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0'
MAX_BYTES=1073741824
STAGES=('backup-started.json','backup-committed.json','migration-observation.json','migration-prepared.json',
        'migration-apply-started.json','migration-apply-result.json','migration-reconcile-started.json',
        'migration-reconcile-result.json','migration-verified.json','source-after-migration.json',
        'genesis-publication-started.json','genesis-publication-committed.json','seed-started.json',
        'seed-committed.json','page-admitted.json','continuation-started.json','continuation-committed.json',
        'campaign-completed.json')


def sha_path(path:Path)->str:
    h=hashlib.sha256()
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()

def digest(raw:bytes)->str:return hashlib.sha256(raw).hexdigest()
def read_json(path:Path,limit=8*1024*1024):
    if path.is_symlink() or not path.is_file() or path.stat().st_size>limit:raise RuntimeError('Pinned bounded regular input required: '+str(path))
    return json.loads(path.read_bytes())
def write_new(path:Path,value):
    raw=(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
    path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())
def write_bytes_new(path:Path,raw:bytes):
    if len(raw)>16*1024*1024:raise RuntimeError('Private receipt exceeds 16 MiB bound')
    path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as f:f.write(raw);f.flush();os.fsync(f.fileno())

def tree_manifest(root:Path):
    total=0;rows=[];hasher=hashlib.sha256()
    for base,dirs,files in os.walk(root,followlinks=False):
        dirs.sort();files.sort();basep=Path(base)
        for name in dirs+files:
            p=basep/name
            if p.is_symlink():raise RuntimeError('Symlink in retained/attempt data tree')
        for name in files:
            p=basep/name
            try:st=p.stat()
            except FileNotFoundError:continue
            total+=st.st_size
            row={'path':p.relative_to(root).as_posix(),'bytes':st.st_size,'sha256':sha_path(p)}
            rows.append(row);hasher.update((row['path']+'\0'+str(row['bytes'])+'\0'+row['sha256']+'\n').encode())
            if total>MAX_BYTES:raise RuntimeError('1 GiB source/attempt tree bound exceeded')
    return {'bytes':total,'file_count':len(rows),'tree_sha256':hasher.hexdigest(),'files':rows}

def verify_runtime():
    if sha_path(RUNTIME_RECEIPT)!=RUNTIME_SHA:raise RuntimeError('Fresh .49 runtime receipt changed')
    receipt=read_json(RUNTIME_RECEIPT)
    if receipt.get('profile')!='ashlar-truss-0.0.49-lifecycle-runtime-preparation/0.2':raise RuntimeError('Exact .49 runtime profile required')
    record_payload_counts={}
    for name,(version,digest_expected) in EXPECTED_WHEELS.items():
        dist=metadata.distribution(name)
        if dist.version!=version or not Path(dist.locate_file('')).resolve().is_relative_to(VENV.resolve()):raise RuntimeError('Selected package version/prefix differs: '+name)
        direct=dist.read_text('direct_url.json')
        if not direct:raise RuntimeError('Direct wheel origin missing: '+name)
        record=json.loads(direct);wheel=Path(__import__('urllib.parse',fromlist=['urlparse']).unquote(__import__('urllib.parse',fromlist=['urlparse']).urlparse(record['url']).path))
        if record.get('archive_info',{}).get('hashes',{}).get('sha256')!=digest_expected or sha_path(wheel)!=digest_expected:raise RuntimeError('Direct wheel hash differs: '+name)
        record_text=dist.read_text('RECORD')
        if not record_text:raise RuntimeError('Installed selected distribution RECORD missing: '+name)
        payloads=0
        for relative,hash_field,size_field in csv.reader(io.StringIO(record_text)):
            if not hash_field:continue
            algorithm,separator,encoded_hash=hash_field.partition('=')
            if not separator or algorithm!='sha256':raise RuntimeError('Unsupported selected distribution RECORD hash: '+name)
            target=Path(dist.locate_file(relative))
            resolved=target.resolve()
            if target.is_symlink() or not resolved.is_relative_to(VENV.resolve()) or not resolved.is_file():
                raise RuntimeError('Installed selected distribution payload escapes the fresh runtime: '+name)
            expected=base64.urlsafe_b64decode(encoded_hash+'===').hex()
            if sha_path(resolved)!=expected or (size_field and resolved.stat().st_size!=int(size_field)):
                raise RuntimeError('Installed selected distribution payload hash differs: '+name)
            payloads+=1
        if payloads==0:raise RuntimeError('Selected distribution has no hashed RECORD payloads: '+name)
        record_payload_counts[name]=payloads
    from truss import LocalPostgres
    from truss.migrations import RuntimeMigrations
    from truss.recovery_registry import HostRecoveryRegistry
    selected_receipts={'build':'1396c4fc2b50ac34eb413fd40e59d54b3bea3e678c563d8edc0b743a15ffcf05',
        'packaging':'3bf22976c0921461ad56f5aa9cffda9b3f73521558d29c6df36a2979246a3447',
        'functional':'16b987ebb2e0ad5e419676e5a63cd827ca3eb94716fd75d3afff31374c10e441'}
    for kind,expected in selected_receipts.items():
        if sha_path(ROOT/'runtime'/('weft-'+kind+'-receipt.json'))!=expected:raise RuntimeError('Selected Weft '+kind+' receipt changed')
    native=Path(metadata.distribution('weft-sql').locate_file('weft/weft.abi3.so'))
    if sha_path(native)!='f4be63caaacedeaa3d8af1b1f0587c561a2ff1ae0a279f4f8f7dcf836f6bb24f':raise RuntimeError('Selected Weft native extension differs')
    return {'runtime_receipt_sha256':RUNTIME_SHA,'versions':{n:metadata.version(n) for n in EXPECTED_WHEELS},
        'installed_record_payloads_verified':record_payload_counts,
        'migration_api':str(__import__('inspect').signature(RuntimeMigrations)),
        'registry_api':str(__import__('inspect').signature(HostRecoveryRegistry)),
        'local_postgres_api':str(__import__('inspect').signature(LocalPostgres))}

def probe_umf_realizations():
    """Run the owner-pinned six-role V8 proposal path without PG or Spark."""
    from ashlar_host.local_current_delta4_umf import LocalUmfDelta4Bridge,ROLE_SOURCE_SHA256
    from ashlar_host.managed_current_carriers import load_current_managed_carriers
    from ashlar_host.schema_rows import LOCAL_CURRENT_ROLE_TO_UMF
    carriers=load_current_managed_carriers(CARRIER);sources=dict(carriers.inputs);rows=[]
    if set(LOCAL_CURRENT_ROLE_TO_UMF.values())!=set(ROLE_SOURCE_SHA256):
        raise RuntimeError('Exact complete six-role UMF realization map required')
    with LocalUmfDelta4Bridge.open() as bridge:
        for local_role,carrier_role in LOCAL_CURRENT_ROLE_TO_UMF.items():
            source=sources.get(carrier_role+'.umf.json')
            if type(source)is not bytes or digest(source)!=ROLE_SOURCE_SHA256[carrier_role]:
                raise RuntimeError('Exact `.4` owner source bytes required: '+carrier_role)
            location=str(ATTEMPT/'delta-destination'/local_role)
            admitted=bridge.realize(source,location,carrier_role)
            if admitted.get('fields')!=carriers.ordered_fields(carrier_role):
                raise RuntimeError('Owner-generated local `.4` field schema differs: '+carrier_role)
            proposal=admitted['proposal']
            rows.append({'local_role':local_role,'carrier_role':carrier_role,
                'source_bytes_sha256':admitted['source_bytes_sha256'],
                'proposal_sha256':admitted['proposal_sha256'],
                'sql_sha256':digest(proposal['sql'].encode('utf-8'))})
    return {'profile':'ashlar-local-delta4-six-role-umf-probe/0.1','native_started':False,'roles':rows}

def bridge_state():
    if not BRIDGE.exists():return {'status':'unresolved','reason':'Page-owned UMF local path bridge and immutable installation pins have not been frozen'}
    pin=read_json(BRIDGE,65536)
    required={'profile','status','ashlar_commit','runtime_input_objects','umf_commit','bridge_sha256','resource_sha256','build_inputs_sha256','role_semantics_sha256','carrier_profile'}
    if set(pin)!=required or pin.get('profile')!='ashlar-truss049-local-delta-bridge-readiness/0.2':raise RuntimeError('Exact Page-owned bridge readiness pin required')
    if pin.get('status')!='ready' or pin.get('carrier_profile')!=CARRIER:raise RuntimeError('Local `.4` UMF bridge readiness is not approved')
    for name in ('bridge_sha256','resource_sha256','build_inputs_sha256','role_semantics_sha256'):
        value=pin[name]
        if type(value)is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):raise RuntimeError('Malformed bridge digest: '+name)
    # Pin the reviewed runtime inputs by Git object identity. Documentation-only
    # commits and evidence files may change HEAD without changing executable code.
    reviewed='0506fb7cea6e2f795c5bfbfe70aa8c30d9333642'
    expected_objects={'src':'53eaf827025f02cf62274ccf9aae2856564f7d55',
        'sql':'3cfe250fadd42ef4b25597f0eec7d10a426ce8ff',
        'pyproject.toml':'170164da487d567bfa058288d32604f401125ab0'}
    if pin['ashlar_commit']!=reviewed or pin['runtime_input_objects']!=expected_objects:
        raise RuntimeError('Reviewed Ashlar source revision/runtime object pins differ')
    for path,expected in expected_objects.items():
        actual=subprocess.check_output(['git','-C',str(REPO),'rev-parse',reviewed+':'+path],text=True).strip()
        if actual!=expected:raise RuntimeError('Reviewed source object differs: '+path)
    for cached in (False,True):
        command=['git','-C',str(REPO),'diff']+(['--cached'] if cached else [])+['--quiet',reviewed,'--','src','sql','pyproject.toml']
        result=subprocess.run(command,stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,stderr=subprocess.PIPE)
        if result.returncode!=0:raise RuntimeError('Ashlar runtime inputs differ from reviewed objects (index/worktree)')
    untracked=subprocess.check_output(['git','-C',str(REPO),'ls-files','--others','--exclude-standard','--','src','sql'],text=True).strip()
    if untracked:raise RuntimeError('Untracked runtime input under src/sql is not permitted')
    observed_head=subprocess.check_output(['git','-C',str(REPO),'rev-parse','HEAD'],text=True).strip()
    from ashlar_host.local_current_delta4_umf import (UMF_CANDIDATE_COMMIT,BRIDGE_SHA256,
        REALIZATION_MANIFEST_SHA256,BUILD_INPUTS_SHA256,ROLE_SEMANTICS_SHA256)
    from ashlar_host.local_current_carrier_profile import XXHASH64_PROFILE
    if (pin['umf_commit']!=UMF_CANDIDATE_COMMIT or pin['bridge_sha256']!=BRIDGE_SHA256
            or pin['resource_sha256']!=REALIZATION_MANIFEST_SHA256 or pin['build_inputs_sha256']!=BUILD_INPUTS_SHA256
            or pin['role_semantics_sha256']!=ROLE_SEMANTICS_SHA256
            or pin['carrier_profile']!=XXHASH64_PROFILE):raise RuntimeError('Pinned owner-generated UMF local Delta bridge/resource differs')
    return {**pin,'observed_head':observed_head}

def preflight():
    if Path(sys.executable).resolve()!=(VENV/'bin/python').resolve() or sys.version_info[:3]!=(3,12,14):raise RuntimeError('Exact fresh Python 3.12.14 interpreter required')
    if sha_path(HELPER)!=HELPER_SHA:raise RuntimeError('Preserved .48 read-only helper changed')
    if sha_path(OLD48_RUNTIME_RECEIPT)!=OLD48_RUNTIME_SHA:raise RuntimeError('Exact preserved .48 read-only runtime receipt required')
    if metadata.version('truss-toolkit')!='0.0.49':raise RuntimeError('Current .49 campaign runtime required')
    prep=read_json(PREP,65536);rt=read_json(RUNTIME_RECEIPT)
    if prep.get('profile')!='ashlar-truss-0.0.49-lifecycle-campaign-preparation/0.1' or prep['runtime_receipt']['sha256']!=RUNTIME_SHA:raise RuntimeError('Frozen campaign config/runtime receipt differs')
    if sha_path(SOURCE)!=SOURCE_SHA or sha_path(ORIGIN)!=ORIGIN_SHA or sha_path(REQUEST)!=REQUEST_SHA:raise RuntimeError('Original source/origin/seed request bytes differ')
    for key in ('existing_feed_registration_receipt','prior_public_source_reobservation_receipt'):
        rec=prep['immutable_inputs'][key]
        if sha_path(Path(rec['path']))!=rec['sha256']:raise RuntimeError('Retained source/registration custody pin differs: '+key)
    if not DATA.is_dir() or (DATA/'PG_VERSION').read_text().strip()!='18' or (DATA/'postmaster.pid').exists():raise RuntimeError('Retained PostgreSQL 18 data must be present and stopped')
    if ATTEMPT.exists() and any(ATTEMPT.iterdir()):raise RuntimeError('Fresh single-use attempt required; no retry/reuse')
    if not (ROOT/'runtime/venv/bin/python').is_file():raise RuntimeError('Fresh .49 venv missing')
    direct=verify_runtime()
    old48_preflight=run_048_helper('host=127.0.0.1 port=1 dbname=postgres user=postgres','--preflight')
    if old48_preflight.get('truss')!='0.0.48' or old48_preflight.get('pg_started') is not False or old48_preflight.get('spark_started') is not False:
        raise RuntimeError('Preserved `.48` pre-migration helper import preflight required')
    bridge=bridge_state();umf_probe=probe_umf_realizations()
    import psycopg,py_mini_racer
    if metadata.version('psycopg')!='3.3.6' or metadata.version('mini-racer')!='0.14.1':raise RuntimeError('Pinned local support dependencies required')
    from ashlar_host.truss_current_catalog import CURRENT_TRUSS_049_VERSION
    if CURRENT_TRUSS_049_VERSION!='0.0.49':raise RuntimeError('Ashlar .49 provider version gate missing')
    # Import public pipeline entrypoints without opening PG or Spark.
    from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
    from ashlar_host.truss_graph_admission import TrussGraphAdmission
    from ashlar_host.truss_record_group import run_create_truss_record_group_command
    from ashlar_host.truss_note_publication import publish_graph_pages
    from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
    from ashlar_host.truss_page_archive import encode_truss_page,decode_truss_page
    if not all(callable(x) for x in (read_graph_catalog_evidence,TrussGraphAdmission,run_create_truss_record_group_command,publish_graph_pages,run_continue_truss_graph_command,encode_truss_page,decode_truss_page)):
        raise RuntimeError('Exact public source/admission/publication API required')
    return {'profile':'ashlar-truss049-lifecycle-preflight/0.3','outcome':'prepared_bridge_unresolved_no_native_started' if bridge['status']=='unresolved' else 'ready_pending_hash_bound_operator_approval',
      'pg_started':False,'spark_started':False,'migration_apply_invoked':False,'bridge':bridge,
      'umf_realization_probe':umf_probe,'runtime':direct,
      'readonly_048_runtime_preflight':old48_preflight,
      'source_sha256':SOURCE_SHA,'origin_sha256':ORIGIN_SHA,'seed_request_sha256':REQUEST_SHA,
      'retained_pg_stopped':True,'existing_consumer_registration_id':CONSUMER_REGISTRATION,
      'readonly_048_helper_sha256':HELPER_SHA,'readonly_048_runtime_receipt_sha256':OLD48_RUNTIME_SHA}

def approval_payload(preflight_result):
    if preflight_result['bridge'].get('status')!='ready':raise RuntimeError('Unresolved bridge blocks execute before any native start')
    gate_digest=os.environ.get('ASHLAR_R3_ADMISSION_GATES_SHA256','')
    if len(gate_digest)!=64 or any(c not in '0123456789abcdef' for c in gate_digest):
        raise RuntimeError('Supervisor admission-gate digest is missing or malformed')
    return {'profile':'ashlar-truss049-lifecycle-execution-approval/0.1','scope':'one cold-backup + one latest-only .49 migration + reuse existing source/consumer + genesis protected ACK + one approved seed + one actual continuation protected ACK',
      'worker_sha256':sha_path(Path(__file__)),'supervisor_sha256':sha_path(SUPERVISOR),'preparation_sha256':sha_path(PREP),
      'runtime_receipt_sha256':RUNTIME_SHA,'seed_request_sha256':REQUEST_SHA,'bridge_pin_sha256':sha_path(BRIDGE),
      'admission_gates_sha256':gate_digest,'migration_manifest_sha256':MIGRATION_MANIFEST,'native_manifest_sha256':NATIVE_MANIFEST}

def persist_migration_result(path,result):
    from dataclasses import asdict
    value=asdict(result)
    # Keep exact public typed outcome; errors are bounded and remain private.
    detail=value.get('detail')
    if isinstance(detail,str) and len(detail)>2048:value['detail']=detail[:2048]
    write_new(path,value)

def backup_before_start():
    if (DATA/'postmaster.pid').exists():raise RuntimeError('Cold backup requires retained PostgreSQL stopped')
    if BACKUP.exists() or BACKUP.is_symlink():raise RuntimeError('Fresh cold backup path required; no retry')
    write_new(ATTEMPT/'backup-started.json',{'source_pgdata':str(DATA),'backup_path':str(BACKUP),'source_postmaster_absent':True})
    before=tree_manifest(DATA)
    if before['bytes']>MAX_BYTES:raise RuntimeError('Retained PGDATA exceeds 1 GiB campaign bound')
    BACKUP.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    shutil.copytree(DATA,BACKUP,symlinks=False,copy_function=shutil.copy2)
    copied=tree_manifest(BACKUP);after=tree_manifest(DATA)
    if before['tree_sha256']!=copied['tree_sha256'] or before['tree_sha256']!=after['tree_sha256'] or before['bytes']!=copied['bytes']:
        raise RuntimeError('Cold PGDATA copy is not byte-identical to stopped source')
    write_new(ATTEMPT/'backup-committed.json',{'source':before,'backup':copied,'source_after_copy':after,'cold_copy_verified':True})

def local_connect_factory(conninfo):
    import psycopg
    params=psycopg.conninfo.conninfo_to_dict(conninfo)
    if 'password' in params:raise RuntimeError('Local profile must not persist a password')
    def connect():return psycopg.connect(**params,autocommit=True,connect_timeout=5)
    return connect

def run_048_helper(conninfo,*args):
    helper=HELPER
    cmd=[str(OLD48_VENV/'bin/python'),'-I','-B',str(helper),*args]
    env={'PATH':str(OLD48_VENV/'bin')+':/usr/bin:/bin',
      'HOME':'/Users/erik','PYTHONDONTWRITEBYTECODE':'1','ASHLAR_LOCAL_PG_CONNINFO':conninfo}
    result=subprocess.run(cmd,cwd=ROOT,env=env,text=True,capture_output=True,timeout=90)
    if result.returncode:raise RuntimeError('Preserved .48 read-only helper failed: '+result.stderr[-2000:])
    lines=result.stdout.splitlines()
    if len(lines)!=1:raise RuntimeError('Exact bounded .48 helper output required')
    return json.loads(lines[0])

def verify_source49(conninfo):
    from truss import Ok,CatalogRuntimeConfiguration,CatalogViewAvailable,adopt_psycopg_host,create_reference_assembly
    observed={}
    for user,mode in ((REGISTRAR,'read_write'),(SOURCE_READER,'read_only')):
        import psycopg
        params=psycopg.conninfo.conninfo_to_dict(conninfo);params['user']=user
        with psycopg.connect(**params,connect_timeout=5,options='-c statement_timeout=2000') as conn:
            conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED '+('READ WRITE' if mode=='read_write' else 'READ ONLY'))
            conn.execute("SET LOCAL statement_timeout='2000ms'")
            host=adopt_psycopg_host(conn);assembly=None;primary=None
            try:
                tx=host.adopt_transaction(access_mode=mode);built=create_reference_assembly(CatalogRuntimeConfiguration(),host)
                if not isinstance(tx,Ok) or not isinstance(built,Ok):raise RuntimeError('Public .49 source read refused')
                assembly=built.value
                if user==REGISTRAR:
                    status=assembly.status_in_transaction(tx.value);report=assembly.report_in_transaction(tx.value,1)
                    view=assembly.catalog_view_in_transaction(tx.value,assembly.catalog_view_request(definition_count='65536',encoded_bytes='16777216'))
                    if not all(isinstance(x,Ok)for x in(status,report,view)) or not status.value.installed or status.value.revision!=1:raise RuntimeError('Retained catalog revision 1 required')
                    raw=view.value.view.original_bytes if isinstance(view.value,CatalogViewAvailable) else b''
                    if digest(report.value.original_bytes)!=REPORT_SHA or digest(raw)!=VIEW_SHA or json.loads(raw).get('scope')!={'kind':'complete'}:raise RuntimeError('Retained public report/complete view differs')
                    observed.update(catalog_revision=1,report_sha256=REPORT_SHA,view_sha256=VIEW_SHA)
                else:
                    result=assembly.accepted_document_in_transaction(tx.value,document_id='ashlar-current-045-lifecycle-20261021',maximum_bytes=1048576)
                    if not isinstance(result,Ok):raise RuntimeError('Public .49 accepted source read refused')
                    value=result.value
                    if (value.content!=SOURCE.read_bytes() or value.sha256!=SOURCE_SHA or value.document_revision!='ashlar-current-045-lifecycle-r1' or value.registration_id!=SOURCE_REGISTRATION):raise RuntimeError('Retained accepted source identity/bytes changed')
                    observed.update(document_id=value.document_id,document_revision=value.document_revision,source_sha256=value.sha256,source_registration_id=value.registration_id)
            except BaseException as error:primary=error
            def detach():
                if not host.detach():raise RuntimeError('.49 source host detach refused')
            from ashlar_host.lifecycle import finish
            finish(primary,([assembly.dispose]if assembly is not None else [])+[detach]);conn.rollback()
    return observed

def read_page49(conninfo,output):
    from truss import Ok,CatalogRuntimeConfiguration,adopt_psycopg_host,create_reference_assembly
    from truss.feed import FeedPage
    from ashlar_host.truss_page_archive import encode_truss_page
    import psycopg
    params=psycopg.conninfo.conninfo_to_dict(conninfo);params['user']=REGISTRAR
    with psycopg.connect(**params,connect_timeout=5,options='-c statement_timeout=2000') as conn:
        conn.execute('SET TRANSACTION ISOLATION LEVEL READ COMMITTED READ ONLY');conn.execute("SET LOCAL statement_timeout='2000ms'")
        host=adopt_psycopg_host(conn);assembly=None;primary=None;page=None
        try:
            tx=host.adopt_transaction(access_mode='read_only');built=create_reference_assembly(CatalogRuntimeConfiguration(),host)
            if not isinstance(tx,Ok)or not isinstance(built,Ok):raise RuntimeError('Public .49 feed read refused')
            assembly=built.value;result=assembly.read_feed_in_transaction(tx.value,consumer_id=CONSUMER,maximum_facts=4096,maximum_bytes=8388608)
            if not isinstance(result,Ok)or not isinstance(result.value,FeedPage)or result.value.outcome!='transaction':raise RuntimeError('Exact actual transaction page required')
            page=result.value
        except BaseException as error:primary=error
        def detach():
            if not host.detach():raise RuntimeError('.49 feed reader detach refused')
        from ashlar_host.lifecycle import finish
        finish(primary,([assembly.dispose]if assembly is not None else [])+[detach]);conn.rollback()
    archive=encode_truss_page(page,FEED)
    if len(archive)>2097152:raise RuntimeError('Feed page exceeds 2 MiB')
    write_bytes_new(output,archive)
    return {'path':str(output),'archive_sha256':digest(archive),'page_sha256':page.sha256,'writer_xid':page.writer_xid,
      'consumer_id':page.consumer_id,'registration_id':page.registration_id,'prior_checkpoint_sha256':digest(page.prior.content),
      'next_checkpoint_sha256':digest(page.next.content),'facts':len(page.facts),'page':page}

def run_migration(conninfo):
    from truss.migrations import RuntimeMigrations
    from truss.recovery_registry import HostRecoveryRegistry
    connect=local_connect_factory(conninfo)
    if REGISTRY.exists() or REGISTRY.is_symlink():raise RuntimeError('Fresh restart-durable migration registry required')
    registry=HostRecoveryRegistry(REGISTRY)
    migration=RuntimeMigrations(connect,registry,maximum_preservation_bytes=268435456,operation_timeout_ms=30000,statement_timeout_ms=10000)
    status=migration.status();backup_meta=migration.backup_metadata()
    if status.get('state')!='upgrade_required' or status.get('manifestSha256')!=SOURCE_MANIFEST or status.get('target')!=NATIVE_MANIFEST:
        raise RuntimeError('Exact retained .48 source and public .49 latest target required; no prepare/apply')
    if backup_meta.get('sourceManifestSha256')!=SOURCE_MANIFEST or backup_meta.get('context')!=status.get('context'):
        raise RuntimeError('Migration backup metadata does not bind exact retained source/context')
    write_new(ATTEMPT/'migration-observation.json',{'profile':'ashlar-truss049-migration-observation/0.1','status':status,'backup_metadata':backup_meta,
        'registry_path':str(REGISTRY),'migration_manifest_sha256':MIGRATION_MANIFEST,'native_manifest_sha256':NATIVE_MANIFEST})
    attempt_id=read_json(PREP)['migration_recovery']['attempt_id']
    prepared=migration.prepare(attempt_id=attempt_id)
    if prepared.outcome!='prepared' or not prepared.reference:raise RuntimeError('Public .49 migration prepare did not return a frozen reference')
    retained=registry.inspect(prepared.reference,status['context'])
    write_new(ATTEMPT/'migration-prepared.json',{'outcome':prepared.outcome,'reference':prepared.reference,'attempt_id':prepared.attempt_id,
      'source':prepared.source,'target':prepared.target,'original_obligation':retained['obligation'],'observations':retained['observations']})
    write_new(ATTEMPT/'migration-apply-started.json',{'reference':prepared.reference,'attempt_id':prepared.attempt_id,'single_apply':True})
    applied=migration.apply(prepared.reference)
    persist_migration_result(ATTEMPT/'migration-apply-result.json',applied)
    final=applied
    if applied.outcome in ('commit_unknown','recovery_required','committed_unverified'):
        write_new(ATTEMPT/'migration-reconcile-started.json',{'reference':prepared.reference,'attempt_id':prepared.attempt_id,
            'reason':'public apply returned an uncertain/recovery outcome; same-reference reconciliation only'})
        final=migration.reconcile(prepared.reference)
        persist_migration_result(ATTEMPT/'migration-reconcile-result.json',final)
    if final.outcome not in ('migrated','already_applied') or not final.current_verification or final.current_verification.get('state')!='current':
        raise RuntimeError('Migration did not reach an exact verified latest state; preserve reference and stop')
    receipt=migration.receipt(prepared.reference)
    write_new(ATTEMPT/'migration-verified.json',{'outcome':final.outcome,'reference':prepared.reference,
      'attempt_id':prepared.attempt_id,'current_verification':final.current_verification,'receipt':receipt})
    return {'reference':prepared.reference,'attempt_id':prepared.attempt_id,'outcome':final.outcome,
      'current_verification':final.current_verification}

def assert_bridge_and_approval():
    result=preflight()
    expected=approval_payload(result)
    approval=read_json(APPROVAL,65536)
    if approval!=expected:raise RuntimeError('Fresh root/Sol execution approval must bind exact worker/runtime/config/bridge hashes')
    return result

def make_connection_uri(runtime):
    import psycopg
    params=psycopg.conninfo.conninfo_to_dict(runtime.info.connection_uri)
    if 'password' in params:raise RuntimeError('No local password is allowed')
    return psycopg.conninfo.make_conninfo(**params)

def _verify_manifest(manifest,bridge):
    if manifest.get('profile_version')!=CARRIER:raise RuntimeError('Exact local `.4` physical publication profile required')
    report=json.loads(manifest['validation_report_json'])
    if report.get('physical_carrier_profile')!=CARRIER or report.get('lookup_hash_profile')!='spark-xxhash64-typed-key/0.1':raise RuntimeError('Signed 64-bit local `.4` hash profile required')
    if report.get('umf_revision')!='2ede4c879bea5db13794a6fa30e67c821306c17f':raise RuntimeError('Exact accepted UMF schema revision required')
    actual=report.get('installation_profile_sha256')
    # The publication backend verifies the exact UMF installation profile and
    # native schema/protocol/layout before its protected receipt ACK. The signed
    # manifest carries the resulting immutable profile digest.
    if type(actual)is not str or len(actual)!=64:raise RuntimeError('Concrete generated installation profile digest required')
    versions=json.loads(manifest['table_versions_json'])
    required={f'local.trussgraph.{role}' for role in ('object_current','edge_current','tombstone','whole_source_history')}
    if set(versions)!=required:raise RuntimeError('Complete four-role destination publication vector required')
    return report,versions

def run_genesis_publication(conninfo,genesis_info,bridge):
    from pyspark import SparkConf,SparkContext
    from pyspark.sql import SparkSession
    from ashlar_host.configured_local_runtime_profile import PARAMETER_SUBSTITUTION_SETTING,attest as attest_runtime,verify_loaded_delta_class
    from ashlar_host.truss_note_publication import publish_graph_pages
    from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
    from ashlar_host.truss_graph_admission import TrussGraphAdmission
    from ashlar.truss_native_feed import native_feed_batch
    from ashlar_host.truss_page_archive import decode_truss_page
    import psycopg
    output=ATTEMPT/'delta-destination';warehouse=ATTEMPT/'spark-warehouse'
    if output.exists() or output.is_symlink() or warehouse.exists() or warehouse.is_symlink():raise RuntimeError('Fresh `.4` destination and warehouse required')
    current_archive=Path(genesis_info['path']).read_bytes()
    page=genesis_info['page'];archived=decode_truss_page(current_archive,genesis_info['archive_sha256'],FEED)
    pre_info=read_json(ATTEMPT/'pre-migration-source-and-checkpoint.json')
    observed=pre_info['pending_genesis_page']
    if (archived.content!=page.content or page.consumer_id!=CONSUMER or page.registration_id!=CONSUMER_REGISTRATION
            or len(page.facts)!=observed.get('facts')
            or digest(page.content)!=observed['page_sha256'] or digest(page.prior.content)!=observed['prior_checkpoint_sha256']
            or digest(page.next.content)!=observed['next_checkpoint_sha256'] or page.writer_xid!=observed['writer_xid']):
        raise RuntimeError('Actual genesis page identity/checkpoints differ from retained pre-migration page')
    batch=native_feed_batch(page.content,sha256=page.sha256,prior=page.prior.content,next=page.next.content,feed=FEED)
    evidence=read_graph_catalog_evidence(role_connection_factory(conninfo,REGISTRAR),batch)
    admission=TrussGraphAdmission(batch,evidence)
    admission_profile=admission.metadata()
    if (admission.changes or evidence.truss_version!='0.0.49'
            or admission_profile.get('profile')!='ashlar-truss-current-umf010-graph-semantic-admission/0.3'):
        raise RuntimeError('Actual .49 catalog genesis must admit no graph mutations')
    conf=SparkConf(loadDefaults=False).setMaster('local[1]').setAppName('Ashlar .49 local .4 campaign')
    values={'spark.driver.memory':'512m','spark.ui.enabled':'false','spark.sql.shuffle.partitions':'1',
      'spark.databricks.delta.snapshotPartitions':'1','spark.sql.session.timeZone':'UTC',PARAMETER_SUBSTITUTION_SETTING:'true',
      'spark.sql.ansi.enabled':'true','spark.sql.decimalOperations.allowPrecisionLoss':'false','spark.jars':','.join(map(str,JARS)),
      'spark.sql.extensions':'io.delta.sql.DeltaSparkSessionExtension','spark.sql.catalog.spark_catalog':'org.apache.spark.sql.delta.catalog.DeltaCatalog',
      'spark.sql.warehouse.dir':str(warehouse)}
    for k,v in values.items():conf.set(k,v)
    context=None;spark=None;primary=None;answer=None
    try:
        context=SparkContext(conf=conf);spark=SparkSession(context);spark.sparkContext.setLogLevel('ERROR')
        runtime_config=type('RuntimeConfig',(),{'java_home':JAVA_HOME,'jar_inventory':{
          'delta-spark_4.2_2.13-4.4.1.jar':'2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be',
          'delta-storage-4.4.1.jar':'f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f'},'jars':JARS[0].parent})()
        _profile,_runtime_report,selected=attest_runtime(runtime_config)
        if (spark.version!='4.2.0' or spark.sparkContext.master!='local[1]'
                or spark.sparkContext._jvm.java.lang.System.getProperty('java.specification.version')!='21'
                or spark.conf.get(PARAMETER_SUBSTITUTION_SETTING)!='true'
                or spark.conf.get('spark.jars')!=','.join(map(str,JARS))):raise RuntimeError('Exact local Spark4.2/Delta4.4/JDK21 runtime required')
        verify_loaded_delta_class(spark,selected)
        from ashlar_host.truss_note_publication import publish_graph_pages
        result=publish_graph_pages(spark,output,connection_factory=role_connection_factory(conninfo,REGISTRAR),consumer_id=CONSUMER,
          feed=FEED,maximum_pages=1,wait_seconds=0,maximum_page_bytes=2097152,destination_id=DESTINATION,carrier_profile=CARRIER)
        pubs=result.get('publications')
        if type(pubs)is not list or len(pubs)!=1:raise RuntimeError('Exactly one actual genesis publication required')
        item=pubs[0];manifest=item.get('manifest')
        if (type(manifest)is not dict or item.get('page_sha256')!=page.sha256
                or 'source-retained receipt ACK' not in result.get('scope','')):
            raise RuntimeError('Protected receipt-ACKed actual genesis publication required')
        report,versions=_verify_manifest(manifest,bridge)
        if digest(Path(item['source_page']['path']).read_bytes())!=item['source_page']['sha256']:
            raise RuntimeError('Genesis source page archive digest mismatch')
        answer={'profile':'ashlar-truss049-genesis-publication/0.1','publication_id':manifest['publication_id'],
          'page_sha256':page.sha256,'archive_sha256':item['source_page']['sha256'],'source_page_path':item['source_page']['path'],
          'publication_intent':item.get('publication_intent'),'profile_version':manifest['profile_version'],
          'validation_report_json':manifest['validation_report_json'],'table_versions_json':manifest['table_versions_json'],
          'installation_profile_sha256':report['installation_profile_sha256'],'destination_id':DESTINATION,
          'carrier_profile':CARRIER,'receipt_acknowledged':True,'admission_profile':admission_profile['profile'],
          'change_count':0,'vector':versions,'bridge_pin_sha256':sha_path(BRIDGE)}
    except BaseException as e:primary=e
    for stop in ([spark.stop]if spark else [])+([context.stop]if context else []):
        try:stop()
        except BaseException as e:
            if primary is None:primary=e
    if primary:raise primary
    if answer is None:raise RuntimeError('Genesis protected publication result required')
    write_new(ATTEMPT/'genesis-publication-result.json',answer)
    return answer

def validate_committed_seed_report(path,request):
    report=read_json(path,1048576)
    original=report.get('group_original',{}).get('response',{})
    replay=report.get('committed_receipt_replay',{}).get('response',{})
    semantic=original.get('semantic',{});results=semantic.get('results')
    expected=['create_object']*len(request.get('records',[]))+['create_edge']*len(request.get('edges',[]))
    if (type(results)is not list or len(results)!=7 or len(expected)!=7
            or len(report.get('objects',[]))!=4 or len(report.get('edges',[]))!=3
            or semantic!=replay.get('semantic')):
        raise RuntimeError('Committed exact seven-operation readback and receipt replay required')
    event_count=0
    for result,operation in zip(results,expected,strict=True):
        if result.get('outcome')!='created' or result.get('operation')!=operation:
            raise RuntimeError('Committed seed result order/outcome differs from frozen request')
        events=result.get('events')
        if type(events)is not list or not events:raise RuntimeError('Every committed seed result must retain source events')
        event_count+=len(events)
    if event_count!=7:raise RuntimeError('Exact seven source events required for the approved seed group')
    return {'objects':4,'edges':3,'operations':7,'events':event_count,
        'report_sha256':sha_path(path),'replay_semantics_verified':True}

def admit_actual_seed_page(conninfo,genesis,seed_report_path):
    import psycopg
    from ashlar_host.truss_catalog_reader import read_graph_catalog_evidence
    from ashlar_host.truss_graph_admission import TrussGraphAdmission
    from ashlar.truss_native_feed import native_feed_batch
    from ashlar_host.truss_page_archive import decode_truss_page,encode_truss_page
    page_info=read_page49(conninfo,ATTEMPT/'actual-seed-page.json');page=page_info['page']
    genesis_page=decode_truss_page(GENESIS.read_bytes(),digest(GENESIS.read_bytes()),FEED)
    if (page.consumer_id!=CONSUMER or page.registration_id!=CONSUMER_REGISTRATION
            or page.prior.content!=genesis_page.next.content):raise RuntimeError('Seed page must immediately continue receipt-ACKed genesis checkpoint')
    batch=native_feed_batch(page.content,sha256=page.sha256,prior=page.prior.content,next=page.next.content,feed=FEED)
    evidence=read_graph_catalog_evidence(role_connection_factory(conninfo,REGISTRAR),batch);admission=TrussGraphAdmission(batch,evidence)
    if evidence.truss_version!='0.0.49' or admission.metadata().get('profile')!='ashlar-truss-current-umf010-graph-semantic-admission/0.3':raise RuntimeError('Exact .49 graph admission required')
    group=json.loads(seed_report_path.read_bytes());results=group['group_original']['response']['semantic']['results']
    validate_committed_seed_report(seed_report_path,json.loads(REQUEST.read_bytes()))
    cursor=json.loads(page.prior.content);source_identity=cursor.get('installationId')
    if type(source_identity)is not str or not source_identity:raise RuntimeError('Actual source installation identity required')
    expected=[]
    for item in results:
        identity=item['identity']
        if item.get('outcome')!='created' or item.get('operation') not in ('create_object','create_edge'):raise RuntimeError('Seven-operation fresh seed group required')
        for event in item.get('events',[]):
            if event.get('xid')!=page.writer_xid or event.get('source_epoch')!=batch.epoch:raise RuntimeError('Seed event differs from actual feed page')
            expected.append((source_identity,event['xid']+':'+event['seq'],'object'if item['operation']=='create_object'else'edge',str(identity['type_id']),str(identity['id'])))
    observed=[]
    for change in admission.changes:
        key=change.state.key;observed.append((key.source,change.delivery_id,key.kind,str(key.type_id),str(key.id)))
        if change.operation!='create':raise RuntimeError('Fresh seed page contains non-create operation')
    if len(results)!=7 or len(expected)!=7 or sorted(observed)!=sorted(expected):raise RuntimeError('Admitted actual seed page differs from committed seven-operation group')
    raw=encode_truss_page(page,FEED);archive=ATTEMPT/'actual-seed-page.json'
    if sha_path(archive)!=digest(raw):raise RuntimeError('Captured actual page archive changed')
    return {'page':page,'page_sha256':page.sha256,'archive_sha256':sha_path(archive),'page_path':str(archive),
      'writer_xid':page.writer_xid,'prior_checkpoint_sha256':digest(page.prior.content),'next_checkpoint_sha256':digest(page.next.content),
      'admission_profile':admission.metadata()['profile'],'admitted_change_count':len(admission.changes),
      'seed_report_sha256':sha_path(seed_report_path)}

def continue_actual_seed_page(conninfo,genesis,admitted,bridge):
    import psycopg
    from ashlar_host.truss_continue_cli import run_continue_truss_graph_command
    intent=genesis.get('publication_intent')
    if type(intent)is not dict or set(intent)!={'path','sha256'}:raise RuntimeError('Exact genesis continuation intent required')
    intent_path=Path(intent['path'])
    if intent_path.is_symlink() or sha_path(intent_path)!=intent['sha256']:raise RuntimeError('Retained exact genesis publication intent changed')
    params=psycopg.conninfo.conninfo_to_dict(conninfo);params['user']=REGISTRAR
    registrar_uri=psycopg.conninfo.make_conninfo(**params)
    config={'connection_uri':registrar_uri,'publication_directory':str(ATTEMPT/'delta-destination'),'jars':[str(x)for x in JARS],
      'feed':FEED,'publication_intent':intent,'destination_id':DESTINATION,'carrier_profile':CARRIER,
      'runtime_profile':{'profile':'ashlar-configured-local-runtime/0.2','java_home':str(JAVA_HOME),
       'jar_inventory':{'delta-spark_4.2_2.13-4.4.1.jar':'2b1d2cd411bf0bbced64b96fe621f8ffce7bb6e394c8064ab783fc6edce449be',
         'delta-storage-4.4.1.jar':'f585818d646cb3625b305270ec24f3e6f0b6ea63ed9bac1ae8f340f8faaeae8f'}}}
    config_path=ATTEMPT/'continuation-runtime-config.json';write_new(config_path,config)
    report=ATTEMPT/'continuation-report.json'
    result=run_continue_truss_graph_command(config_path,report,maximum_pages=1,wait_seconds=0,maximum_page_bytes=2097152)
    pubs=result.get('publications')
    if result.get('predecessor')!=genesis['publication_id'] or type(pubs)is not list or len(pubs)!=1:raise RuntimeError('Exactly one actual continuation from genesis publication required')
    item=pubs[0];manifest=item.get('manifest');source=item.get('source_page')
    if type(manifest)is not dict or type(source)is not dict or set(source)!={'path','sha256'}:raise RuntimeError('Bounded actual continuation result required')
    report_profile,vector=_verify_manifest(manifest,bridge)
    raw=Path(source['path']).read_bytes()
    if digest(raw)!=source['sha256']:raise RuntimeError('Continued source page archive hash mismatch')
    from ashlar_host.truss_page_archive import decode_truss_page
    continued=decode_truss_page(raw,source['sha256'],FEED);page=admitted['page']
    if (continued.content!=page.content or continued.prior.content!=page.prior.content or continued.next.content!=page.next.content
            or continued.writer_xid!=page.writer_xid or 'source-retained destination receipt ACK' not in result.get('scope','')):
        raise RuntimeError('Continuation is not the exact admitted source page with protected ACK')
    if report_profile['installation_profile_sha256']!=genesis['installation_profile_sha256']:raise RuntimeError('Continuation changed the original six-role `.4` installation')
    result_receipt={'profile':'ashlar-truss049-seed-continuation/0.1','publication_id':manifest['publication_id'],
      'predecessor':genesis['publication_id'],'page_sha256':continued.sha256,'archive_sha256':source['sha256'],
      'profile_version':manifest['profile_version'],'validation_report_json':manifest['validation_report_json'],
      'table_versions_json':manifest['table_versions_json'],'vector':vector,'destination_id':DESTINATION,
      'carrier_profile':CARRIER,'receipt_acknowledged':True,'admitted_change_count':admitted['admitted_change_count'],
      'installation_profile_sha256':report_profile['installation_profile_sha256'],'report_sha256':sha_path(report)}
    write_new(ATTEMPT/'continuation-publication-result.json',result_receipt)
    return result_receipt

def execute():
    pre=assert_bridge_and_approval()  # exact bridge + reviewed hashes before first effect
    if ATTEMPT.exists() and any(ATTEMPT.iterdir()):raise RuntimeError('Fresh single-use attempt only; existing stage marker forbids replay')
    ATTEMPT.mkdir(mode=0o700,exist_ok=True)
    backup_before_start()  # stopped retained PGDATA copied/hashed before LocalPostgres starts
    from truss import LocalPostgres
    runtime=LocalPostgres(DATA)
    write_new(ATTEMPT/'postgres-start-intent.json',{'profile':'Truss LocalPostgres 18.6.1 reused retained PGDATA','data_directory':str(DATA),'prior_cold_backup_verified':True,'no_upgrade_or_reset':True})
    with runtime:
        write_new(ATTEMPT/'postgres-started.json',{'profile':'Truss LocalPostgres 18.6.1 reused retained PGDATA','data_directory':str(DATA),'no_upgrade_or_reset':True})
        conninfo=make_connection_uri(runtime)
        write_new(ATTEMPT/'pre-migration-source-read-started.json',{'runtime':'preserved public .48 read-only helper','helper_sha256':HELPER_SHA})
        pre_source=run_048_helper(conninfo,'--custody')
        pre_page=run_048_helper(conninfo,'--read-page',str(GENESIS))
        write_new(ATTEMPT/'pre-migration-source-and-checkpoint.json',{'profile':'ashlar-truss048-readonly-before-049-migration/0.1',
          'source_custody':pre_source,'pending_genesis_page':pre_page,'feed':FEED,'consumer_id':CONSUMER,
          'consumer_registration_id':CONSUMER_REGISTRATION,'no_source_mutation_or_ack':True})
        migration=run_migration(conninfo)
        source=verify_source49(conninfo)
        write_new(ATTEMPT/'source-after-migration.json',{'profile':'ashlar-truss049-source-and-view-readback/0.1',
          'source_custody':source,'migration_reference':migration['reference'],'consumer_id':CONSUMER,
          'registration_id':CONSUMER_REGISTRATION,'no_reacceptance_or_reregistration':True})
        genesis_page=read_page49(conninfo,ATTEMPT/'postmigration-genesis-page.json')
        if (genesis_page['page_sha256']!=pre_page['page_sha256']
                or genesis_page['prior_checkpoint_sha256']!=pre_page['prior_checkpoint_sha256']
                or genesis_page['next_checkpoint_sha256']!=pre_page['next_checkpoint_sha256']
                or genesis_page['writer_xid']!=pre_page['writer_xid']
                or genesis_page['registration_id']!=CONSUMER_REGISTRATION
                or genesis_page['consumer_id']!=CONSUMER):raise RuntimeError('Pending genesis page changed across the .49 migration')
        write_new(ATTEMPT/'genesis-publication-started.json',{'consumer_id':CONSUMER,'registration_id':CONSUMER_REGISTRATION,
          'destination_id':DESTINATION,'carrier_profile':CARRIER,'bridge_pin_sha256':sha_path(BRIDGE)})
        genesis=run_genesis_publication(conninfo,genesis_page,pre['bridge'])
        if not genesis.get('receipt_acknowledged') or genesis.get('destination_id')!=DESTINATION or genesis.get('page_sha256')!=genesis_page['page_sha256']:
            raise RuntimeError('Verified protected genesis ACK is prerequisite for first seed submission')
        write_new(ATTEMPT/'genesis-publication-committed.json',{'publication_id':genesis['publication_id'],
          'page_sha256':genesis['page_sha256'],'archive_sha256':genesis['archive_sha256'],'profile_version':genesis['profile_version'],
          'table_versions_json':genesis['table_versions_json'],'installation_profile_sha256':genesis['installation_profile_sha256'],
          'destination_id':DESTINATION,'carrier_profile':CARRIER,'receipt_acknowledged':True})
        seed=json.loads(REQUEST.read_bytes())
        if seed.get('request_key')!='ashlar-current-048-lifecycle-seed-r1' or len(seed.get('records',[]))+len(seed.get('edges',[]))!=7:
            raise RuntimeError('Exact original approved seven-operation seed request required')
        import psycopg
        params=psycopg.conninfo.conninfo_to_dict(conninfo);params['user']=WRITER_LOGIN
        seed_config={'profile':'ashlar-truss-record-group/0.1','connection_uri':psycopg.conninfo.make_conninfo(**params),
          'bun':'/opt/homebrew/Cellar/bun/1.4.3/bin/bun','document_path':str(SOURCE),'origin_path':str(ORIGIN),
          'document_revision':seed['document_revision'],'document_id':seed['document_id'],'source_version':seed['source_version'],
          'request_key':seed['request_key'],'records':seed['records'],'edges':seed['edges']}
        cfg=ATTEMPT/'seed-runtime-config.json';write_new(cfg,seed_config)
        write_new(ATTEMPT/'seed-started.json',{'request_key':seed['request_key'],'request_sha256':REQUEST_SHA,
          'require_fresh':True,'genesis_publication_id':genesis['publication_id'],'genesis_acknowledged':True})
        from ashlar_host.truss_record_group import run_create_truss_record_group_command
        seed_report=run_create_truss_record_group_command(cfg,ATTEMPT/'seed-group-report.json',require_fresh=True)
        if not (ATTEMPT/'seed-group-report.json').is_file():raise RuntimeError('Public committed group report missing')
        seed_receipt=validate_committed_seed_report(ATTEMPT/'seed-group-report.json',seed)
        write_new(ATTEMPT/'seed-committed.json',{'request_key':seed['request_key'],'request_sha256':REQUEST_SHA,
          'report_sha256':seed_receipt['report_sha256'],'request_config_sha256':sha_path(cfg),
          'object_count':seed_receipt['objects'],'edge_count':seed_receipt['edges'],'operation_count':seed_receipt['operations'],
          'event_count':seed_receipt['events'],'require_fresh':True,'committed_receipt_replay_verified':True})
        admitted=admit_actual_seed_page(conninfo,genesis,ATTEMPT/'seed-group-report.json')
        admitted_safe={k:v for k,v in admitted.items() if k!='page'}
        write_new(ATTEMPT/'page-admitted.json',admitted_safe)
        write_new(ATTEMPT/'continuation-started.json',{'predecessor':genesis['publication_id'],'carrier_profile':CARRIER,
          'destination_id':DESTINATION,'page_sha256':admitted['page_sha256'],'writer_xid':admitted['writer_xid'],
          'installation_profile_sha256':genesis['installation_profile_sha256']})
        continued=continue_actual_seed_page(conninfo,genesis,admitted,pre['bridge'])
        if not continued.get('receipt_acknowledged') or continued.get('destination_id')!=DESTINATION:
            raise RuntimeError('Protected actual seed-page continuation ACK required')
        write_new(ATTEMPT/'continuation-committed.json',continued)
        write_new(ATTEMPT/'campaign-completed.json',{'profile':'ashlar-truss049-local-lifecycle-completed/0.1',
          'migration_reference':migration['reference'],'genesis_publication_id':genesis['publication_id'],
          'seed_publication_id':continued['publication_id'],'destination_id':DESTINATION,
          'carrier_profile':CARRIER,'genesis_acknowledged':True,'seed_acknowledged':True,
          'request_sha256':REQUEST_SHA,'bridge_pin_sha256':sha_path(BRIDGE)})
    if (DATA/'postmaster.pid').exists():raise RuntimeError('Owned PostgreSQL did not stop cleanly')
    result={'profile':'ashlar-truss049-lifecycle-result/0.1','outcome':'one_public_migration_and_two_actual_pages_published_acknowledged',
      'attempt_directory':str(ATTEMPT),'migration_manifest_sha256':MIGRATION_MANIFEST,
      'destination_id':DESTINATION,'carrier_profile':CARRIER,'pg_stopped':True,'automatic_retries':0}
    write_new(ATTEMPT/'campaign-result.json',result)
    return result

def main():
    parser=argparse.ArgumentParser();m=parser.add_mutually_exclusive_group(required=True)
    m.add_argument('--preflight',action='store_true');m.add_argument('--execute',action='store_true');args=parser.parse_args()
    result=preflight() if args.preflight else execute()
    print(json.dumps(result,sort_keys=True,separators=(',',':')))

if __name__=='__main__':main()
