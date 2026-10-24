#!/usr/bin/env python3
"""One-shot 600-second supervisor for the .50 migration/local lifecycle."""
from __future__ import annotations
import argparse,ctypes,hashlib,importlib.util,json,os,signal,subprocess,sys,time
from pathlib import Path

ROOT=Path('/private/tmp/ashlar-truss050-current-source-2eeecf40-r15')
REPO=Path('/private/tmp/ashlar-truss050-current-source-2eeecf40-r15/source-inputs-2eeecf40')
VENV=Path('/private/tmp/ashlar-truss050-current-cohort-9c745278-r13/runtime/venv')
WORKER=ROOT/'campaign_worker_r15.py'
HELPER=ROOT/'source_048_readonly_helper.py'
PREP=ROOT/'preparation.json'
RUNTIME=ROOT/'runtime-receipt.json'
BRIDGE=ROOT/'bridge-readiness.json'
APPROVAL=ROOT/'execution-approval.json'
ATTEMPT=ROOT/'attempt-r15-91d4ef97-5998-4ddb-83c3-869233ac4c8f'
DATA=Path('/private/tmp/ashlar-truss048-source-acceptance-attempt-01-r4/postgres/data')
BACKUP=ATTEMPT/'cold-backup/pgdata'
OLD48_VENV=Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/venv')
PG_CTL=VENV/'lib/python3.12/site-packages/pgserver/pginstall/bin/pg_ctl'
PG_CTL_SHA256='59b641aabae44b43acfc34fd0bdeb9af9480338d39b25f3088d7c486fdd8b999'
LOGDIR=ROOT/'logs'
MAX_CHILD_SECONDS=600
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

def check_log_limits(logs):
    for path in logs:
        try:size=path.stat().st_size
        except FileNotFoundError:continue
        if path.is_symlink() or size>MAX_LOG_BYTES:
            raise RuntimeError('Worker log limit exceeded: '+path.name)

def child_deadline_exceeded(runtime_seconds):
    return runtime_seconds>MAX_CHILD_SECONDS

def clean_environment(admission_digest=None):
    java='/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home'
    env={'PATH':str(VENV/'bin')+':'+java+'/bin:/usr/bin:/bin:/opt/homebrew/bin',
      'HOME':'/Users/erik','JAVA_HOME':java,
      'PYSPARK_PYTHON':str(VENV/'bin/python'),'PYSPARK_DRIVER_PYTHON':str(VENV/'bin/python'),
      'PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1'}
    if admission_digest is not None:env['ASHLAR_R15_ADMISSION_GATES_SHA256']=admission_digest
    return env

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

def admission_gates_digest(gates):
    stable={'process_inventory':{'status':gates.get('process_inventory',{}).get('status'),
        'supervisor_pid_present':gates.get('process_inventory',{}).get('supervisor_pid_present')},
      'sysv_shared_memory':{key:gates.get('sysv_shared_memory',{}).get(key)
        for key in ('status','segment_created','attached','detached','ipc_rmid_confirmed')}}
    return hashlib.sha256(json.dumps(stable,sort_keys=True,separators=(',',':')).encode()).hexdigest()

def expected_approval(pre):
    if pre.get('bridge',{}).get('status')!='ready':raise RuntimeError('Final owner-generated UMF bridge pin unresolved')
    gates=pre.get('admission_gates',{})
    if gates.get('process_inventory',{}).get('status')!='ready' or gates.get('process_inventory',{}).get('supervisor_pid_present') is not True:raise RuntimeError('Process inventory admission gate is pending')
    if gates.get('sysv_shared_memory',{}).get('status')!='ready' or gates.get('sysv_shared_memory',{}).get('ipc_rmid_confirmed') is not True:raise RuntimeError('SysV shared-memory admission gate is pending')
    route=pre.get('migration_route',{})
    if route.get('status')!='verified_declared_metadata_only' or route.get('native_action') is not False:raise RuntimeError('Exact effect-free .50 migration route pin required')
    return {'profile':'ashlar-truss050-lifecycle-execution-approval/0.3',
      'scope':'one cold-backup + one latest-only .50 runtime migration following the exact .48→35fd intermediate→current native manifest route + reuse existing A source/consumer + genesis verify/protected ACK + one approved seed + one actual continuation verify/protected ACK',
      'worker_sha256':digest(WORKER),'supervisor_sha256':digest(Path(__file__)),'preparation_sha256':digest(PREP),
      'runtime_receipt_sha256':digest(RUNTIME),'seed_request_sha256':digest(ROOT/'seed-request.json'),
      'bridge_pin_sha256':digest(BRIDGE),'admission_gates_sha256':admission_gates_digest(gates),
      'migration_manifest_sha256':'96addcdb95523f478d8f50b0b47198e47a860680c7400dd359711930e837e535',
      'migration_index_sha256':route['migration_index_sha256'],
      'migration_source_manifest_sha256':route['source_manifest_sha256'],
      'migration_intermediate_manifest_sha256':route['intermediate_manifest_sha256'],
      'native_manifest_sha256':'288801f7177338328441b5d3ce9328af31740ebf513f04ea45e30ae316a9f3a4'}

def approval_contract_parity(pre):
    supervisor_payload=expected_approval(pre)
    gate_digest=supervisor_payload['admission_gates_sha256']
    spec=importlib.util.spec_from_file_location('r15_campaign_worker_contract',WORKER)
    if spec is None or spec.loader is None:raise RuntimeError('Could not load worker approval contract')
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    prior=os.environ.get('ASHLAR_R15_ADMISSION_GATES_SHA256')
    try:
        os.environ['ASHLAR_R15_ADMISSION_GATES_SHA256']=gate_digest
        worker_payload=module.approval_payload(pre)
    finally:
        if prior is None:os.environ.pop('ASHLAR_R15_ADMISSION_GATES_SHA256',None)
        else:os.environ['ASHLAR_R15_ADMISSION_GATES_SHA256']=prior
    if worker_payload!=supervisor_payload:raise RuntimeError('Supervisor/worker approval payload parity failed')
    canonical=json.dumps(supervisor_payload,sort_keys=True,separators=(',',':')).encode()
    return {'status':'ready','exact_dict_match':True,'approval_payload_sha256':hashlib.sha256(canonical).hexdigest(),
      'admission_gates_sha256':gate_digest}

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
    # Use macOS libproc directly so preflight does not spawn a process or
    # depend on shell/process-list permissions. proc_pidinfo(PROC_PIDTBSDINFO)
    # exposes pbi_pgid at byte offset 100 in the stable 136-byte BSD record.
    libproc=ctypes.CDLL('/usr/lib/libproc.dylib',use_errno=True)
    list_pids=libproc.proc_listallpids
    list_pids.argtypes=(ctypes.c_void_p,ctypes.c_int);list_pids.restype=ctypes.c_int
    pid_info=libproc.proc_pidinfo
    pid_info.argtypes=(ctypes.c_int,ctypes.c_int,ctypes.c_uint64,ctypes.c_void_p,ctypes.c_int)
    pid_info.restype=ctypes.c_int
    capacity=65536
    pids=(ctypes.c_int*capacity)()
    count=list_pids(pids,ctypes.sizeof(pids))
    if count<0 or count>=capacity:raise RuntimeError('Bounded libproc PID inventory failed')
    members=[];record=ctypes.create_string_buffer(256)
    for pid in pids[:count]:
        if pid<=0:continue
        size=pid_info(pid,3,0,record,256)  # PROC_PIDTBSDINFO
        if size<104:continue  # process exited or is not inspectable
        observed_pid=int.from_bytes(record.raw[12:16],sys.byteorder)
        observed_pgid=int.from_bytes(record.raw[100:104],sys.byteorder)
        if observed_pid==pid and observed_pgid==group_id:members.append(pid)
    return sorted(set(members))

def process_inventory_gate():
    pid=os.getpid();pgid=os.getpgid(pid)
    members=process_group_members(pgid)
    if pid not in members:raise RuntimeError('Process inventory omitted the supervisor PID')
    return {'status':'ready','supervisor_pid':pid,'process_group_id':pgid,
      'observed_member_count':len(members),'supervisor_pid_present':True}

def sysv_shmat_gate():
    """Attach/detach a temporary one-byte SysV segment and always request IPC_RMID."""
    libc=ctypes.CDLL(None,use_errno=True)
    libc.shmget.argtypes=(ctypes.c_int,ctypes.c_size_t,ctypes.c_int);libc.shmget.restype=ctypes.c_int
    libc.shmat.argtypes=(ctypes.c_int,ctypes.c_void_p,ctypes.c_int);libc.shmat.restype=ctypes.c_void_p
    libc.shmdt.argtypes=(ctypes.c_void_p,);libc.shmdt.restype=ctypes.c_int
    libc.shmctl.argtypes=(ctypes.c_int,ctypes.c_int,ctypes.c_void_p);libc.shmctl.restype=ctypes.c_int
    shmid=libc.shmget(0,1,0o1000|0o2000|0o600)  # IPC_PRIVATE, IPC_CREAT|IPC_EXCL|0600
    if shmid<0:raise OSError(ctypes.get_errno(),'shmget failed')
    address=None;attached=False;detached=False;removed=False;error=None
    try:
        address=libc.shmat(shmid,None,0)
        if address==ctypes.c_void_p(-1).value:raise OSError(ctypes.get_errno(),'shmat failed')
        attached=True
        if libc.shmdt(address)!=0:raise OSError(ctypes.get_errno(),'shmdt failed')
        detached=True;attached=False
    except BaseException as exc:error=exc
    finally:
        if attached and address is not None:libc.shmdt(address)
        removed=libc.shmctl(shmid,0,None)==0  # IPC_RMID, also marks a still-attached segment for deletion
    if not removed:raise RuntimeError('SysV probe IPC_RMID cleanup failed') from error
    if error is not None:raise error
    return {'status':'ready','segment_created':True,'attached':True,'detached':detached,'ipc_rmid_confirmed':removed}

def admission_gates():
    return {'process_inventory':process_inventory_gate(),'sysv_shared_memory':sysv_shmat_gate()}

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
    if not ATTEMPT.is_dir() or ATTEMPT.is_symlink() or ATTEMPT.stat().st_mode&0o077:raise RuntimeError('Fresh private R15 attempt directory required')
    if DATA.is_symlink() or not DATA.is_dir() or (DATA/'PG_VERSION').read_text().strip()!='18':raise RuntimeError('Exact retained PG18 data required')
    if PG_CTL.is_symlink() or not PG_CTL.is_file() or digest(PG_CTL)!=PG_CTL_SHA256:raise RuntimeError('Exact retained PostgreSQL 18.6 cleanup binary required')
    version=subprocess.run([str(PG_CTL),'--version'],stdin=subprocess.DEVNULL,text=True,capture_output=True,timeout=5)
    if version.returncode or version.stdout.strip()!='pg_ctl (PostgreSQL) 18.6':raise RuntimeError('Exact PostgreSQL 18.6 cleanup binary required')

def main():
    parser=argparse.ArgumentParser();mode=parser.add_mutually_exclusive_group(required=True)
    mode.add_argument('--preflight',action='store_true');mode.add_argument('--execute',action='store_true');args=parser.parse_args()
    validate_roots()
    if Path(sys.executable).resolve()!=(VENV/'bin/python').resolve():raise RuntimeError('Exact isolated .50 campaign interpreter required')
    LOGDIR.mkdir(mode=0o700,exist_ok=True)
    if LOGDIR.stat().st_mode&0o077:raise RuntimeError('Private mode-0700 log directory required')
    if args.preflight:
        result=preflight();gate_error=None
        try:
            result['admission_gates']=admission_gates()
            result['approval_contract_parity']=approval_contract_parity(result)
        except BaseException as e:
            gate_error=type(e).__name__+': '+str(e)
            result['admission_gates']=result.get('admission_gates',{'status':'pending','error':gate_error})
            result['approval_contract_parity']={'status':'pending','error':gate_error}
        outcome=result['outcome'] if gate_error is None else 'pending_execution_admission'
        print(json.dumps({'profile':'ashlar-truss050-offline-supervisor/0.5','outcome':outcome,
          'worker_sha256':digest(WORKER),'supervisor_sha256':digest(Path(__file__)),'preflight':result},sort_keys=True))
        return 0
    result=preflight()
    gates=admission_gates()  # one bounded admission probe per execution attempt
    result['admission_gates']=gates
    parity=approval_contract_parity(result)
    result['approval_contract_parity']=parity
    expected=expected_approval(result)
    approval=json.loads(APPROVAL.read_bytes()) if APPROVAL.is_file() and not APPROVAL.is_symlink() else None
    if approval!=expected:raise RuntimeError('Fresh exact root/Sol hash-bound execution approval required')
    if owns_postgres():raise RuntimeError('Retained PostgreSQL already running; no attach or retry')
    if ATTEMPT.exists() and any(ATTEMPT.iterdir()):raise RuntimeError('Single-use attempt contains output; no retry/replay')
    # Reuse the exact gates bound by approval parity; do not create a second
    # temporary SysV segment for the same one-shot attempt.
    gate_digest=parity['admission_gates_sha256']
    if gate_digest!=expected['admission_gates_sha256']:raise RuntimeError('Live admission gates differ from the approved digest')
    stdout=LOGDIR/'worker-attempt-r15-91d4ef97-5998-4ddb-83c3-869233ac4c8f.stdout.log';stderr=LOGDIR/'worker-attempt-r15-91d4ef97-5998-4ddb-83c3-869233ac4c8f.stderr.log';receipt=LOGDIR/'supervisor-attempt-r15-91d4ef97-5998-4ddb-83c3-869233ac4c8f.json';started_marker=ROOT/'execution-started-r15-91d4ef97-5998-4ddb-83c3-869233ac4c8f.json'
    for p in (stdout,stderr,receipt,started_marker):
        if p.exists() or p.is_symlink():raise RuntimeError('Fresh single-use supervisor outputs required')
    write_new(started_marker,{'profile':'ashlar-truss050-execution-started/0.1','approval_sha256':digest(APPROVAL),
      'worker_sha256':digest(WORKER),'supervisor_sha256':digest(Path(__file__)),'preflight_outcome':result['outcome'],
      'retained_pg_stopped_before_start':True,'cold_backup_precedes_pg_start':True,
      'admission_gates_sha256':gate_digest})
    fdo=os.open(stdout,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600);fde=os.open(stderr,os.O_CREAT|os.O_EXCL|os.O_WRONLY|os.O_NOFOLLOW,0o600)
    log_out=os.fdopen(fdo,'w',encoding='utf8');log_err=os.fdopen(fde,'w',encoding='utf8')
    start=time.monotonic();cause=None
    child=subprocess.Popen([str(VENV/'bin/python'),'-I','-B',str(WORKER),'--execute'],cwd=ROOT,env=clean_environment(gate_digest),
      stdin=subprocess.DEVNULL,stdout=log_out,stderr=log_err,start_new_session=True,close_fds=True)
    while child.poll() is None:
        try:
            used=combined_bytes((stdout,stderr));check_log_limits((stdout,stderr))
        except BaseException as e:cause='bounded_tree_integrity_failure:'+type(e).__name__;break
        if used>MAX_BYTES:cause='combined_retained_and_attempt_bytes_exceeded';break
        if child_deadline_exceeded(time.monotonic()-start):cause='child_execution_deadline_exceeded';break
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
    child_runtime=round(time.monotonic()-start,3)
    if child_deadline_exceeded(child_runtime):cause=cause or 'child_execution_deadline_exceeded'
    try:remaining_group=reap_worker_group(child.pid)
    except BaseException as e:remaining_group=['inventory_failed:'+type(e).__name__]
    if remaining_group:cause=cause or 'worker_process_group_descendants_not_reaped'
    log_out.flush();os.fsync(log_out.fileno());log_out.close();log_err.flush();os.fsync(log_err.fileno());log_err.close()
    cleanup=stop_postgres_if_owned((ATTEMPT/'postgres-start-intent.json').is_file())
    elapsed=round(time.monotonic()-start,3)
    if cleanup not in ('postmaster_pid_absent','owned_postmaster_stopped_fast'):cause=cause or 'postgres_cleanup_incomplete'
    try:
        total=combined_bytes((stdout,stderr));check_log_limits((stdout,stderr))
    except BaseException as e:
        total=MAX_BYTES+1;cause=cause or 'post_exit_resource_limit_exceeded:'+type(e).__name__
    if total>MAX_BYTES:cause=cause or 'post_exit_combined_byte_limit_exceeded'
    summary=None
    if rc==0 and cause is None:
        lines=stdout.read_bytes().splitlines()
        if len(lines)!=1 or len(lines[0])>65536:cause='worker_summary_shape_invalid'
        else:summary=json.loads(lines[0])
    result={'profile':'ashlar-truss050-supervisor-result/0.3','mode':'single_native_campaign',
      'exit_code':rc,'child_runtime_seconds':child_runtime,'elapsed_seconds_including_cleanup':elapsed,
      'child_execution_deadline_seconds':MAX_CHILD_SECONDS,'automatic_retries':0,
      'combined_retained_and_attempt_bytes':total,'combined_byte_limit':MAX_BYTES,'cleanup_status':cleanup,
      'stop_cause':cause,'worker_summary':summary,'worker_process_group_remaining':remaining_group,
      'postmaster_pid_absent':not owns_postgres(),'admission_gates':gates,
      'scope':'one retained-host cold backup and public .50 migration following the pinned two-edge .48 route, then existing A consumer genesis verify/ACK, approved seed and one actual `.4` continuation verify/ACK'}
    write_new(receipt,result)
    print(json.dumps({k:result[k] for k in ('profile','exit_code','child_runtime_seconds','elapsed_seconds_including_cleanup','child_execution_deadline_seconds','combined_retained_and_attempt_bytes','cleanup_status','stop_cause','scope')},sort_keys=True))
    return 0 if rc==0 and cause is None and summary is not None else 1

if __name__=='__main__':raise SystemExit(main())
