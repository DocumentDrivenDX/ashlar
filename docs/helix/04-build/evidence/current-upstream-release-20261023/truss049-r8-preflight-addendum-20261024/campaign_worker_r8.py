#!/usr/bin/env python3
"""Effect-free Truss .49 runtime/source preflight for the R8 addendum."""
from __future__ import annotations
import argparse, hashlib, importlib.metadata as metadata, json, os, shutil, subprocess, sys, time, urllib.parse
from pathlib import Path

REPO=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/source-inputs-7c4006e')
ROOT=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r8')
INPUT_ROOT=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7')
SOURCE_ARCHIVE=INPUT_ROOT/'source-inputs-7c4006e.tar'
SELECTED_SOURCE_MANIFEST=INPUT_ROOT/'source-inputs-manifest.json'
SOURCE_ARCHIVE_SHA256='49f1eea46cbfcf8ddc33ce841669fd9de4e41ad427863e4598f30bf92b6dfaba'
SELECTED_SOURCE_MANIFEST_SHA256='00d1902bda5821e478c76d8b1a264b11e59e88d3d978d7b5d34ea7a6691cb9d5'
SOURCE_COMMIT='7c4006e880b037e2de318d04b45a8aeae9dfea12'
SOURCE_TREE='65377e9c97bab62b50a1eef7d0314cb390f8f2e4'
SOURCE_OBJECTS={'src':'6678e0d1cff4f8c43b0a28af618253559d4de1ce',
    'sql':'3cfe250fadd42ef4b25597f0eec7d10a426ce8ff',
    'pyproject.toml':'ba55a552124b69cfc927cefc5d309af9c76e6ded'}
if str(REPO/'src') not in sys.path:sys.path.insert(0,str(REPO/'src'))
ATTEMPT=ROOT/'attempt-r8-revision3'
PREP=INPUT_ROOT/'preparation.json'
RUNTIME_RECEIPT=INPUT_ROOT/'runtime-receipt.json'
BRIDGE=INPUT_ROOT/'bridge-readiness.json'
SUPERVISOR=ROOT/'run_supervisor_r8.py'
VENV=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/runtime/venv')
DATA=Path('/private/tmp/ashlar-truss048-source-acceptance-attempt-01-r4/postgres/data')
BACKUP=ATTEMPT/'cold-backup/pgdata'
REGISTRY=ATTEMPT/'truss-migration-recovery.sqlite3'
SOURCE_ROOT=Path('/private/tmp/ashlar-truss045-real-source-20261021')
SOURCE=SOURCE_ROOT/'model/source.umf.json'
ORIGIN=SOURCE_ROOT/'model/acceptance-origin.json'
REQUEST=INPUT_ROOT/'seed-request.json'
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
DESTINATION='ashlar-current-049-lifecycle-destination-r7-20261023'
CARRIER='ashlar-delta/0.4-xxhash64'
JAVA_HOME=Path('/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home')
JARS=(Path('/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-spark_4.2_2.13-4.4.1.jar'),
      Path('/private/tmp/ashlar-release-20261021-m3rde5p2/jars/delta-storage-4.4.1.jar'))
RUNTIME_SHA='517ac6892280e911060688afc75528907e62aa5d6d290abf6d9b166f5da5617a'
R7_PREPARATION_SHA='f47ed93bba13fdb741ff63be83431fe7eb26463e6608ac7fcec84ab49a8bdad7'
BRIDGE_SHA='5f746047122cb9d222a4f4501ef1b7ce2970ecc5d4de3ca9c03aa40b5d1315e8'
HELPER=INPUT_ROOT/'source_048_readonly_helper.py'
HELPER_SHA='994bff49f6797477a728429d08561c3162a0eeaa5a6fb5f560766b4bde2b30e1'
OLD48_VENV=Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/venv')
OLD48_WHEEL_SHA='1aedf614ff3d3ff7a61bd4d91887ede650a7135d1239e8564c1c297c0ddc37c3'
OLD48_RUNTIME_RECEIPT=Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/runtime-preflight.json')
OLD48_RUNTIME_SHA='bbc590badfa88ea7c950a40f867ddd62f409df078aa461575a3b7db6c7288b7b'
MIGRATION_MANIFEST='96addcdb95523f478d8f50b0b47198e47a860680c7400dd359711930e837e535'
NATIVE_MANIFEST='288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4'
TRUSS_MIGRATION_SOURCE_MANIFEST_SHA256='c4339ac7f09ab5a004c272ab00962107a970938004c1765ec4163e4d5ca9b6f0'
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
    if sha_path(RUNTIME_RECEIPT)!=RUNTIME_SHA:raise RuntimeError('Exact selected current-cohort runtime receipt changed')
    receipt=read_json(RUNTIME_RECEIPT)
    if receipt.get('schema')!='ashlar-current-cohort-runtime-receipt/v1':raise RuntimeError('Current isolated runtime receipt required')
    if (sys.version_info[:3]!=(3,12,14)
            or Path(sys.executable).resolve()!=(VENV/'bin/python').resolve()
            or Path(sys.prefix).resolve()!=VENV.resolve()
            or Path(sys.prefix).resolve()==Path(sys.base_prefix).resolve()
            or [Path(p).resolve() for p in sys.path if 'site-packages' in p]!=[VENV.resolve()/'lib/python3.12/site-packages']
            or receipt.get('runtime',{}).get('python')!='3.12.14'
            or receipt.get('runtime',{}).get('platform')!='macOS arm64'
            or receipt.get('runtime',{}).get('include_system_site_packages') is not False
            or receipt.get('runtime',{}).get('site_packages_count_on_sys_path')!=1
            ):raise RuntimeError('Exact isolated CPython 3.12.14 runtime required')
    if receipt.get('install',{}).get('pip_check')!='No broken requirements found.':raise RuntimeError('Pinned runtime pip-check observation differs')
    pip_result=subprocess.run([str(VENV/'bin/python'),'-I','-m','pip','check'],cwd=ROOT,
        env={'PATH':str(VENV/'bin')+':/usr/bin:/bin','PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1'},
        stdin=subprocess.DEVNULL,text=True,capture_output=True,timeout=30,check=False)
    if pip_result.returncode!=0 or pip_result.stdout.strip()!=receipt['install']['pip_check'] or pip_result.stderr.strip():
        raise RuntimeError('Live pip check differs from pinned successful dependency observation')
    selected=receipt.get('selected_wheels')
    if type(selected)is not list or len(selected)!=27 or receipt['install'].get('wheel_count')!=27:
        raise RuntimeError('Exact 27-wheel runtime closure required')
    if len({row.get('distribution') for row in selected})!=27:raise RuntimeError('Duplicate distribution in pinned runtime receipt')
    if receipt.get('install',{}).get('installed_record_verification')!='All hashed RECORD payload hashes and sizes verified against installed files.' or receipt.get('install',{}).get('verified_record_payload_count')!=4001:
        raise RuntimeError('Prior installed RECORD verification receipt is incomplete')
    for item in selected:
        name=item['distribution'];expected_version=item['version'];expected_wheel_hash=item['sha256']
        dist=metadata.distribution(name)
        if dist.version!=expected_version or not Path(dist.locate_file('')).resolve().is_relative_to(VENV.resolve()):
            raise RuntimeError('Selected distribution version/prefix differs: '+name)
        direct=dist.read_text('direct_url.json')
        if not direct:raise RuntimeError('Selected wheel origin missing: '+name)
        origin=json.loads(direct);wheel_url=urllib.parse.urlparse(origin['url'])
        wheel=Path(urllib.parse.unquote(wheel_url.path))
        if (origin.get('archive_info',{}).get('hashes',{}).get('sha256')!=expected_wheel_hash
                or wheel.name!=item['filename'] or not wheel.is_file() or wheel.is_symlink()
                or wheel.stat().st_size!=item['size_bytes'] or sha_path(wheel)!=expected_wheel_hash):
            raise RuntimeError('Selected local wheel origin/hash differs: '+name)
    required={'truss-toolkit':'0.0.49','umf-core':'0.10.1','umf_access':'0.1.0.dev7',
        'weft-sql':'0.4.53','pyspark':'4.2.0','delta-spark':'4.4.1','pgserver-pg18':'18.6.1','psycopg':'3.3.6'}
    observed={name:metadata.version(name) for name in required}
    if observed!=required:raise RuntimeError('Current selected runtime tuple differs')
    compatibility=INPUT_ROOT/'weft053-compatibility-receipt.md'
    if sha_path(compatibility)!='880558526e9703c1585e8e04ede80c0bde7220b9ade5cd270fc43f8c04fc65b8':
        raise RuntimeError('Selected Weft 0.4.53 compiler receipt changed')
    return {'runtime_receipt_sha256':RUNTIME_SHA,'versions':observed,
        'wheel_count':len(selected),'prior_installed_record_verification':{'receipt_sha256':RUNTIME_SHA,'verified_record_payload_count':4001,'statement':'All hashed RECORD payload hashes and sizes verified against installed files.','rechecked_during_this_preflight':False},
        'live_pip_check':{'exit_code':pip_result.returncode,'stdout':pip_result.stdout,'stderr':pip_result.stderr},
        'scope':'current interpreter isolation, live pip check, installed distribution/version/prefix and local wheel-origin identity; installed RECORD payload verification is inherited from the exact pinned prior runtime receipt and was not repeated; no lifecycle or native qualification'}

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

def git_blob_sha1(path:Path)->str:
    size=path.stat().st_size;h=hashlib.sha1();h.update(f'blob {size}\0'.encode())
    with path.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''):h.update(chunk)
    return h.hexdigest()

def verify_source_snapshot():
    if sha_path(SOURCE_ARCHIVE)!=SOURCE_ARCHIVE_SHA256:raise RuntimeError('Selected 7c4006e source archive changed')
    if sha_path(SELECTED_SOURCE_MANIFEST)!=SELECTED_SOURCE_MANIFEST_SHA256:raise RuntimeError('Selected source input manifest changed')
    manifest=read_json(SELECTED_SOURCE_MANIFEST,1024*1024)
    if (manifest.get('profile')!='ashlar-selected-source-inputs/1'
            or manifest.get('commit')!=SOURCE_COMMIT or manifest.get('tree')!=SOURCE_TREE
            or manifest.get('objects')!=SOURCE_OBJECTS
            or manifest.get('archive_sha256')!=SOURCE_ARCHIVE_SHA256):
        raise RuntimeError('Selected source archive manifest binding differs')
    rows=manifest.get('files')
    if type(rows)is not list or len(rows)!=587:raise RuntimeError('Exact selected source file inventory required')
    expected={}
    for row in rows:
        if type(row)is not dict or set(row)!={'path','bytes','sha256','git_blob'}:raise RuntimeError('Closed source manifest row required')
        rel=row['path']
        if type(rel)is not str or rel.startswith('/') or '..' in Path(rel).parts or rel in expected:raise RuntimeError('Unsafe or duplicate source path')
        if not (rel.startswith('src/') or rel.startswith('sql/') or rel=='pyproject.toml'):raise RuntimeError('Unexpected selected source path')
        expected[rel]=row
    actual=set()
    for base,dirs,files in os.walk(REPO,followlinks=False):
        for name in dirs+files:
            p=Path(base)/name
            if p.is_symlink():raise RuntimeError('Symlink in selected source snapshot')
        for name in files:actual.add((Path(base)/name).relative_to(REPO).as_posix())
    if actual!=set(expected):raise RuntimeError('Selected source file inventory drifted')
    for rel,row in expected.items():
        path=REPO/rel
        if path.stat().st_size!=row['bytes'] or sha_path(path)!=row['sha256'] or git_blob_sha1(path)!=row['git_blob']:
            raise RuntimeError('Selected source Git blob or content digest differs: '+rel)
    return {'commit':SOURCE_COMMIT,'tree':SOURCE_TREE,'runtime_input_objects':SOURCE_OBJECTS,
        'archive_sha256':SOURCE_ARCHIVE_SHA256,'manifest_sha256':SELECTED_SOURCE_MANIFEST_SHA256,
        'file_count':len(rows),'content_bytes':sum(row['bytes'] for row in rows)}

def bridge_state():
    if not BRIDGE.exists():return {'status':'unresolved','reason':'Page-owned UMF local path bridge and immutable installation pins have not been frozen'}
    pin=read_json(BRIDGE,65536)
    required={'profile','status','ashlar_commit','runtime_input_objects','umf_commit','bridge_sha256','resource_sha256','build_inputs_sha256','role_semantics_sha256','carrier_profile'}
    if set(pin)!=required or pin.get('profile')!='ashlar-truss049-local-delta-bridge-readiness/0.2':raise RuntimeError('Exact Page-owned bridge readiness pin required')
    if pin.get('status')!='ready' or pin.get('carrier_profile')!=CARRIER:raise RuntimeError('Local `.4` UMF bridge readiness is not approved')
    for name in ('bridge_sha256','resource_sha256','build_inputs_sha256','role_semantics_sha256'):
        value=pin[name]
        if type(value)is not str or len(value)!=64 or any(c not in '0123456789abcdef' for c in value):raise RuntimeError('Malformed bridge digest: '+name)
    source=verify_source_snapshot()
    if pin['ashlar_commit']!=SOURCE_COMMIT or pin['runtime_input_objects']!=SOURCE_OBJECTS:
        raise RuntimeError('Reviewed Ashlar source commit/object pins differ')
    from ashlar_host.local_current_delta4_umf import (UMF_CANDIDATE_COMMIT,BRIDGE_SHA256,
        REALIZATION_MANIFEST_SHA256,BUILD_INPUTS_SHA256,ROLE_SEMANTICS_SHA256)
    from ashlar_host.local_current_carrier_profile import XXHASH64_PROFILE
    if (pin['umf_commit']!=UMF_CANDIDATE_COMMIT or pin['bridge_sha256']!=BRIDGE_SHA256
            or pin['resource_sha256']!=REALIZATION_MANIFEST_SHA256 or pin['build_inputs_sha256']!=BUILD_INPUTS_SHA256
            or pin['role_semantics_sha256']!=ROLE_SEMANTICS_SHA256
            or pin['carrier_profile']!=XXHASH64_PROFILE):raise RuntimeError('Pinned owner-generated UMF local Delta bridge/resource differs')
    return {**pin,'source_snapshot':source}

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

def preflight():
    if Path(sys.executable).resolve()!=(VENV/'bin/python').resolve() or sys.version_info[:3]!=(3,12,14):raise RuntimeError('Exact fresh Python 3.12.14 interpreter required')
    if sha_path(HELPER)!=HELPER_SHA:raise RuntimeError('Preserved .48 read-only helper changed')
    if sha_path(OLD48_RUNTIME_RECEIPT)!=OLD48_RUNTIME_SHA:raise RuntimeError('Exact preserved .48 read-only runtime receipt required')
    if metadata.version('truss-toolkit')!='0.0.49':raise RuntimeError('Current .49 campaign runtime required')
    if sha_path(PREP)!=R7_PREPARATION_SHA:raise RuntimeError('Exact R7 preparation snapshot changed')
    if sha_path(BRIDGE)!=BRIDGE_SHA:raise RuntimeError('Exact R7 bridge pin changed')
    prep=read_json(PREP,65536);rt=read_json(RUNTIME_RECEIPT)
    if prep.get('profile')!='ashlar-truss-0.0.49-current-cohort-preparation/0.1' or prep['runtime_receipt']['sha256']!=RUNTIME_SHA:raise RuntimeError('Frozen campaign config/runtime receipt differs')
    if sha_path(SOURCE)!=SOURCE_SHA or sha_path(ORIGIN)!=ORIGIN_SHA or sha_path(REQUEST)!=REQUEST_SHA:raise RuntimeError('Original source/origin/seed request bytes differ')
    for key in ('existing_feed_registration_receipt','prior_public_source_reobservation_receipt'):
        rec=prep['immutable_inputs'][key]
        if sha_path(Path(rec['path']))!=rec['sha256']:raise RuntimeError('Retained source/registration custody pin differs: '+key)
    if not DATA.is_dir() or (DATA/'PG_VERSION').read_text().strip()!='18' or (DATA/'postmaster.pid').exists():raise RuntimeError('Retained PostgreSQL 18 data must be present and stopped')
    if ATTEMPT.exists() and any(ATTEMPT.iterdir()):raise RuntimeError('Fresh single-use attempt required; no retry/reuse')
    if not (VENV/'bin/python').is_file():raise RuntimeError('Fresh .49 venv missing')
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
    return {'profile':'ashlar-truss049-r8-worker-preflight/0.1','outcome':'prepared_bridge_unresolved_no_native_started' if bridge['status']=='unresolved' else 'ready_preflight_only_no_execution_authority',
      'pg_started':False,'spark_started':False,'migration_apply_invoked':False,'bridge':bridge,
      'umf_realization_probe':umf_probe,'runtime':direct,
      'readonly_048_runtime_preflight':old48_preflight,
      'source_sha256':SOURCE_SHA,'origin_sha256':ORIGIN_SHA,'seed_request_sha256':REQUEST_SHA,
      'retained_pg_stopped':True,'existing_consumer_registration_id':CONSUMER_REGISTRATION,
      'readonly_048_helper_sha256':HELPER_SHA,'readonly_048_runtime_receipt_sha256':OLD48_RUNTIME_SHA}

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--preflight',action='store_true',required=True)
    parser.parse_args()
    result=preflight()
    print(json.dumps(result,sort_keys=True))

if __name__=='__main__':main()
