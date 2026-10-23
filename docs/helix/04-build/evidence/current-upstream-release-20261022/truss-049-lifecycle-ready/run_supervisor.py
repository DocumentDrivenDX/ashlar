#!/usr/bin/env python3
"""One-shot 600-second supervisor for the .49 migration/local lifecycle."""
from __future__ import annotations
import argparse,hashlib,json,os,signal,subprocess,sys,time
from pathlib import Path

ROOT=Path('/private/tmp/ashlar-truss049-lifecycle-campaign-20261022')
REPO=Path('/Users/erik/Projects/ashlar')
VENV=ROOT/'runtime/venv'
WORKER=ROOT/'campaign_worker.py'
HELPER=ROOT/'source_048_readonly_helper.py'
PREP=ROOT/'preparation.json'
RUNTIME=ROOT/'runtime/runtime-preflight.json'
BRIDGE=ROOT/'bridge-readiness.json'
APPROVAL=ROOT/'execution-approval.json'
ATTEMPT=ROOT/'attempt-r2'
DATA=Path('/private/tmp/ashlar-truss048-source-acceptance-attempt-01-r4/postgres/data')
BACKUP=ATTEMPT/'cold-backup/pgdata'
OLD48_VENV=Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/venv')
PG_CTL=Path('/private/tmp/ashlar-release-20261021-m3rde5p2/venv/lib/python3.12/site-packages/pgserver/pginstall/bin/pg_ctl')
PG_CTL_SHA256='59b641aabae44b43acfc34fd0bdeb9af9480338d39b25f3088d7c486fdd8b999'
LOGDIR=ROOT/'logs'
MAX_SECONDS=600
MAX_BYTES=1073741824
MAX_LOG_BYTES=16*1024*1024


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()

def write_new(path,value):
    raw=(json.dumps(value,sort_keys=True,indent=2)+'\n').encode();path.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb')as f:f.write(raw);f.flush();os.fsync(f.fileno())

def tree_bytes(path):
    total=0
    if not path.exists():return 0
    if path.is_symlink():raise RuntimeError('Symlink at bounded path')
    for base,dirs,files in os.walk(path,followlinks=False):
        for name in dirs+files:
            item=Path(base)/name
            if item.is_symlink():raise RuntimeError('Symlink in bounded campaign data')
            try:st=item.stat()
            except FileNotFoundError:continue
            if item.is_file():total+=st.st_size
            if total>MAX_BYTES:raise RuntimeError('Combined retained/new byte cap exceeded')
    return total

def combined_bytes(logs=()):
    total=tree_bytes(DATA)+tree_bytes(ATTEMPT)
    for p in logs:
        try:total+=p.stat().st_size
        except FileNotFoundError:pass
    return total

def clean_environment():
    java='/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home'
    return {'PATH':str(VENV/'bin')+':'+java+'/bin:/usr/bin:/bin:/opt/homebrew/bin',
      'HOME':'/Users/erik','JAVA_HOME':java,
      'PYSPARK_PYTHON':str(VENV/'bin/python'),'PYSPARK_DRIVER_PYTHON':str(VENV/'bin/python'),
      'PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1'}

def preflight():
    result=subprocess.run([str(VENV/'bin/python'),'-I','-B',str(WORKER),'--preflight'],cwd=ROOT,
      env=clean_environment(),stdin=subprocess.DEVNULL,text=True,capture_output=True,timeout=90)
    if result.returncode:raise RuntimeError('Effect-free worker preflight failed: '+result.stderr[-3000:])
    lines=result.stdout.splitlines()
    if len(lines)!=1:raise RuntimeError('Single bounded worker preflight result required')
    value=json.loads(lines[0])
    if value.get('pg_started') is not False or value.get('spark_started') is not False or value.get('migration_apply_invoked') is not False:
        raise RuntimeError('Effect-free no-native preflight outcome required')
    return value

def expected_approval(pre):
    if pre.get('bridge',{}).get('status')!='ready':raise RuntimeError('Final owner-generated UMF bridge pin unresolved')
    return {'profile':'ashlar-truss049-lifecycle-execution-approval/0.1',
      'scope':'one cold-backup + one latest-only .49 migration + reuse existing source/consumer + genesis protected ACK + one approved seed + one actual continuation protected ACK',
      'worker_sha256':digest(WORKER),'supervisor_sha256':digest(Path(__file__)),'preparation_sha256':digest(PREP),
      'runtime_receipt_sha256':digest(RUNTIME),'seed_request_sha256':digest(ROOT/'seed-request.json'),
      'bridge_pin_sha256':digest(BRIDGE),'migration_manifest_sha256':'96addcdb95523f478d8f50b0b47198e47a860680c7400dd359711930e837e535',
      'native_manifest_sha256':'288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4'}

def owns_postgres():
    marker=DATA/'postmaster.pid'
    if not marker.exists():return False
    if marker.is_symlink():raise RuntimeError('Unexpected PostgreSQL PID symlink')
    lines=marker.read_text().splitlines()
    if len(lines)<2 or Path(lines[1]).resolve()!=DATA.resolve():raise RuntimeError('PID file does not bind the retained owned PGDATA')
    return True

def stop_postgres_if_owned(started):
    if not owns_postgres():return 'postmaster_pid_absent'
    if not started:return 'postmaster_present_without_owned-start-marker'
    if PG_CTL.is_symlink() or not PG_CTL.is_file():return 'pinned_pg_ctl_missing'
    stop=subprocess.run([str(PG_CTL),'-D',str(DATA),'-m','fast','-w','-t','20','stop'],
      cwd=ROOT,env=clean_environment(),stdin=subprocess.DEVNULL,text=True,capture_output=True,timeout=25)
    if stop.returncode==0 and not owns_postgres():return 'owned_postmaster_stopped_fast'
    return 'owned_postmaster_stop_failed:'+((stop.stderr or stop.stdout or '')[-500:])

def process_group_members(group_id):
    result=subprocess.run(['/bin/ps','-axo','pid=,pgid='],stdin=subprocess.DEVNULL,
      text=True,capture_output=True,timeout=5)
    if result.returncode:raise RuntimeError('Bounded process-group inventory failed')
    members=[]
    for line in result.stdout.splitlines():
        fields=line.split()
        if len(fields)==2 and fields[1].isdigit() and int(fields[1])==group_id and fields[0].isdigit():
            members.append(int(fields[0]))
    return sorted(set(members))

def reap_worker_group(group_id):
    """Boundedly terminate any worker descendants, including after leader exit."""
    members=process_group_members(group_id)
    if not members:return []
    try:os.killpg(group_id,signal.SIGTERM)
    except ProcessLookupError:return []
    deadline=time.monotonic()+2
    while time.monotonic()<deadline:
        members=process_group_members(group_id)
        if not members:return []
        time.sleep(.1)
    try:os.killpg(group_id,signal.SIGKILL)
    except ProcessLookupError:return []
    deadline=time.monotonic()+2
    while time.monotonic()<deadline:
        members=process_group_members(group_id)
        if not members:return []
        time.sleep(.1)
    return process_group_members(group_id)

def validate_roots():
    info=ROOT.lstat()
    if ROOT.is_symlink() or not ROOT.is_dir() or info.st_mode&0o077:raise RuntimeError('Mode-0700 private campaign root required')
    if not ATTEMPT.is_dir() or ATTEMPT.is_symlink() or ATTEMPT.stat().st_mode&0o077:raise RuntimeError('Fresh private r2 attempt directory required')
    if DATA.is_symlink() or not DATA.is_dir() or (DATA/'PG_VERSION').read_text().strip()!='18':raise RuntimeError('Exact retained PG18 data required')
    if PG_CTL.is_symlink() or not PG_CTL.is_file() or digest(PG_CTL)!=PG_CTL_SHA256:raise RuntimeError('Exact retained PostgreSQL 18.6 cleanup binary required')
    version=subprocess.run([str(PG_CTL),'--version'],stdin=subprocess.DEVNULL,text=True,capture_output=True,timeout=5)
    if version.returncode or version.stdout.strip()!='pg_ctl (PostgreSQL) 18.6':raise RuntimeError('Exact PostgreSQL 18.6 cleanup binary required')

def main():
    parser=argparse.ArgumentParser();mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--preflight',action='store_true');mode.add_argument('--execute',action='store_true');args=parser.parse_args()
    validate_roots()
    if Path(sys.executable).resolve()!=(VENV/'bin/python').resolve():raise RuntimeError('Exact fresh .49 campaign interpreter required')
    LOGDIR.mkdir(mode=0o700,exist_ok=True)
    if LOGDIR.stat().st_mode&0o077:raise RuntimeError('Private mode-0700 log directory required')
    if args.preflight:
        result=preflight()
        print(json.dumps({'profile':'ashlar-truss049-offline-supervisor/0.2','outcome':result['outcome'],
          'worker_sha256':digest(WORKER),'supervisor_sha256':digest(Path(__file__)),'preflight':result},sort_keys=True))
        return 0
    result=preflight()
    expected=expected_approval(result)
    approval=json.loads(APPROVAL.read_bytes()) if APPROVAL.is_file() and not APPROVAL.is_symlink() else None
    if approval!=expected:raise RuntimeError('Fresh exact root/Sol hash-bound execution approval required')
    if owns_postgres():raise RuntimeError('Retained PostgreSQL already running; no attach or retry')
    if ATTEMPT.exists() and any(ATTEMPT.iterdir()):raise RuntimeError('Single-use attempt contains output; no retry/replay')
    stdout=LOGDIR/'worker-attempt-r2.stdout.log';stderr=LOGDIR/'worker-attempt-r2.stderr.log';receipt=LOGDIR/'supervisor-attempt-r2.json';started_marker=ROOT/'execution-started-r2.json'
    for p in (stdout,stderr,receipt,started_marker):
        if p.exists() or p.is_symlink():raise RuntimeError('Fresh single-use supervisor outputs required')
    write_new(started_marker,{'profile':'ashlar-truss049-execution-started/0.1','approval_sha256':digest(APPROVAL),
      'worker_sha256':digest(WORKER),'supervisor_sha256':digest(Path(__file__)),'preflight_outcome':result['outcome'],
      'retained_pg_stopped_before_start':True,'cold_backup_precedes_pg_start':True})
    fdo=os.open(stdout,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600);fde=os.open(stderr,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    log_out=os.fdopen(fdo,'w',encoding='utf8');log_err=os.fdopen(fde,'w',encoding='utf8')
    start=time.monotonic();cause=None
    child=subprocess.Popen([str(VENV/'bin/python'),'-I','-B',str(WORKER),'--execute'],cwd=ROOT,env=clean_environment(),
      stdin=subprocess.DEVNULL,stdout=log_out,stderr=log_err,start_new_session=True,close_fds=True)
    while child.poll() is None:
        try:used=combined_bytes((stdout,stderr))
        except BaseException as e:cause='bounded_tree_integrity_failure:'+type(e).__name__;break
        if used>MAX_BYTES:cause='combined_retained_and_attempt_bytes_exceeded';break
        if stdout.stat().st_size>MAX_LOG_BYTES or stderr.stat().st_size>MAX_LOG_BYTES:cause='worker_log_limit_exceeded';break
        if time.monotonic()-start>MAX_SECONDS:cause='outer_deadline_exceeded';break
        time.sleep(.25)
    if cause and child.poll() is None:
        try:os.killpg(child.pid,signal.SIGTERM)
        except ProcessLookupError:pass
        try:child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            try:os.killpg(child.pid,signal.SIGKILL)
            except ProcessLookupError:pass
            child.wait(timeout=10)
    rc=child.wait()
    try:remaining_group=reap_worker_group(child.pid)
    except BaseException as e:remaining_group=['inventory_failed:'+type(e).__name__]
    if remaining_group:cause=cause or 'worker_process_group_descendants_not_reaped'
    log_out.flush();os.fsync(log_out.fileno());log_out.close();log_err.flush();os.fsync(log_err.fileno());log_err.close()
    cleanup=stop_postgres_if_owned((ATTEMPT/'postgres-start-intent.json').is_file())
    elapsed=round(time.monotonic()-start,3)
    if cleanup not in ('postmaster_pid_absent','owned_postmaster_stopped_fast'):cause=cause or 'postgres_cleanup_incomplete'
    total=combined_bytes((stdout,stderr))
    summary=None
    if rc==0 and cause is None:
        lines=stdout.read_bytes().splitlines()
        if len(lines)!=1 or len(lines[0])>65536:cause='worker_summary_shape_invalid'
        else:summary=json.loads(lines[0])
    result={'profile':'ashlar-truss049-supervisor-result/0.1','mode':'single_native_campaign',
      'exit_code':rc,'elapsed_seconds':elapsed,'outer_deadline_seconds':MAX_SECONDS,'automatic_retries':0,
      'combined_retained_and_attempt_bytes':total,'combined_byte_limit':MAX_BYTES,'cleanup_status':cleanup,
      'stop_cause':cause,'worker_summary':summary,'worker_process_group_remaining':remaining_group,
      'postmaster_pid_absent':not owns_postgres(),
      'scope':'one retained-host cold backup and public .49 migration followed by existing consumer genesis/ACK, original approved seed, and one `.4` continuation/ACK'}
    write_new(receipt,result)
    print(json.dumps({k:result[k] for k in ('profile','exit_code','elapsed_seconds','combined_retained_and_attempt_bytes','cleanup_status','stop_cause','scope')},sort_keys=True))
    return 0 if rc==0 and cause is None and summary is not None else 1

if __name__=='__main__':raise SystemExit(main())
