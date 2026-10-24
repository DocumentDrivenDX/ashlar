#!/usr/bin/env python3
"""Effect-free, preflight-only supervisor for the Truss .49 R8 addendum."""
from __future__ import annotations
import argparse,ctypes,hashlib,json,os,subprocess,sys,time
from datetime import datetime,timezone
from pathlib import Path

ROOT=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r8')
INPUT_ROOT=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7')
REPO=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/source-inputs-7c4006e')
VENV=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/runtime/venv')
WORKER=ROOT/'campaign_worker_r8.py'
HELPER=INPUT_ROOT/'source_048_readonly_helper.py'
PREP=INPUT_ROOT/'preparation.json'
RUNTIME=INPUT_ROOT/'runtime-receipt.json'
R8_PREPARATION=ROOT/'r8-preparation.json'
RESULT=ROOT/'r8-revision3-preflight-result.json'
BRIDGE=INPUT_ROOT/'bridge-readiness.json'
ATTEMPT=ROOT/'attempt-r8-revision3'
DATA=Path('/private/tmp/ashlar-truss048-source-acceptance-attempt-01-r4/postgres/data')
BACKUP=ATTEMPT/'cold-backup/pgdata'
OLD48_VENV=Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/venv')
PG_CTL=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/runtime/venv/lib/python3.12/site-packages/pgserver/pginstall/bin/pg_ctl')
PG_CTL_SHA256='59b641aabae44b43acfc34fd0bdeb9af9480338d39b25f3088d7c486fdd8b999'
LOGDIR=ROOT/'logs'
MAX_CHILD_SECONDS=600
MAX_BYTES=1073741824
MAX_LOG_BYTES=16*1024*1024
MAX_RESULT_BYTES=1024*1024
MAX_WORKER_OUTPUT_BYTES=2*1024*1024


def digest(path):
    h=hashlib.sha256()
    with Path(path).open('rb') as f:
        for block in iter(lambda:f.read(1024*1024),b''):h.update(block)
    return h.hexdigest()
def clean_environment(admission_digest=None):
    java='/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home'
    env={'PATH':str(VENV/'bin')+':'+java+'/bin:/usr/bin:/bin:/opt/homebrew/bin',
      'HOME':'/Users/erik','JAVA_HOME':java,
      'PYSPARK_PYTHON':str(VENV/'bin/python'),'PYSPARK_DRIVER_PYTHON':str(VENV/'bin/python'),
      'PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1'}
    if admission_digest is not None:env['ASHLAR_R8_ADMISSION_GATES_SHA256']=admission_digest
    return env
def preflight():
    result=subprocess.run([str(VENV/'bin/python'),'-I','-B',str(WORKER),'--preflight'],cwd=ROOT,
      env=clean_environment(),stdin=subprocess.DEVNULL,text=True,capture_output=True,timeout=90)
    if len(result.stdout.encode('utf-8'))>MAX_WORKER_OUTPUT_BYTES or len(result.stderr.encode('utf-8'))>MAX_WORKER_OUTPUT_BYTES:
        raise RuntimeError('Worker preflight output exceeds 2 MiB bound')
    if result.returncode:raise RuntimeError('Effect-free worker preflight failed: '+result.stderr[-3000:])
    lines=result.stdout.splitlines()
    if len(lines)!=1:raise RuntimeError('Single bounded worker preflight result required')
    value=json.loads(lines[0])
    if value.get('pg_started') is not False or value.get('spark_started') is not False or value.get('migration_apply_invoked') is not False:
        raise RuntimeError('Effect-free no-native preflight outcome required')
    return value
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
def validate_packet_bindings():
    packet=json.loads(R8_PREPARATION.read_bytes())
    if packet.get('profile')!='ashlar-truss-0.0.49-r8-preflight-addendum/0.3':
        raise RuntimeError('Exact R8 preflight addendum required')
    for item in packet.get('immutable_inputs',{}).values():
        path=Path(item['path'])
        if path.is_symlink() or not path.is_file() or digest(path)!=item['sha256']:
            raise RuntimeError('Immutable R7 input binding changed: '+str(path))
    if packet.get('worker',{}).get('sha256')!=digest(WORKER):
        raise RuntimeError('R8 worker hash differs from reviewed addendum')
    if packet.get('result_receipt_path')!=str(RESULT):
        raise RuntimeError('R8 durable result receipt path differs from reviewed addendum')
    if packet.get('attempt_directory')!=str(ATTEMPT):
        raise RuntimeError('R8 attempt directory differs from reviewed addendum')

def validate_roots():
    info=ROOT.lstat()
    if ROOT.is_symlink() or not ROOT.is_dir() or info.st_mode&0o077:raise RuntimeError('Mode-0700 private campaign root required')
    if not ATTEMPT.is_dir() or ATTEMPT.is_symlink() or ATTEMPT.stat().st_mode&0o077:raise RuntimeError('Fresh private R8 attempt directory required')
    if DATA.is_symlink() or not DATA.is_dir() or (DATA/'PG_VERSION').read_text().strip()!='18':raise RuntimeError('Exact retained PG18 data required')
    if PG_CTL.is_symlink() or not PG_CTL.is_file() or digest(PG_CTL)!=PG_CTL_SHA256:raise RuntimeError('Exact retained PostgreSQL 18.6 cleanup binary required')

def summarized_worker(value):
    if value is None:return {'status':'unavailable','worker_result_sha256':None}
    try:
        if type(value)is not dict:raise TypeError('worker result is not an object')
        raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        runtime=value.get('runtime',{});bridge=value.get('bridge',{});source=bridge.get('source_snapshot',{})
        probe=value.get('umf_realization_probe',{});probe_raw=json.dumps(probe,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
        prior=runtime.get('prior_installed_record_verification',{})
        return {'status':'available','worker_result_sha256':hashlib.sha256(raw).hexdigest(),'profile':value.get('profile'),
          'outcome':value.get('outcome'),'native_flags':{key:value.get(key) for key in ('pg_started','spark_started','migration_apply_invoked')},
          'bridge':{'status':bridge.get('status'),'bridge_sha256':bridge.get('bridge_sha256'),
            'resource_sha256':bridge.get('resource_sha256'),'source_snapshot':{key:source.get(key) for key in ('commit','tree','runtime_input_objects','archive_sha256','manifest_sha256','file_count','content_bytes')}},
          'umf_probe':{'profile':probe.get('profile'),'native_started':probe.get('native_started'),
            'role_count':len(probe.get('roles',[])),'roles_sha256':hashlib.sha256(probe_raw).hexdigest()},
          'runtime':{'runtime_receipt_sha256':runtime.get('runtime_receipt_sha256'),'versions':runtime.get('versions'),
            'wheel_count':runtime.get('wheel_count'),'prior_installed_record_verification':prior,
            'live_pip_check':runtime.get('live_pip_check')},
          'readonly_048_runtime_preflight':{key:value.get('readonly_048_runtime_preflight',{}).get(key)
            for key in ('truss','pg_started','spark_started')},
          'source_sha256':value.get('source_sha256'),'origin_sha256':value.get('origin_sha256'),
          'seed_request_sha256':value.get('seed_request_sha256'),'retained_pg_stopped':value.get('retained_pg_stopped'),
          'existing_consumer_registration_id':value.get('existing_consumer_registration_id')}
    except BaseException as error:
        try:raw=json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False,default=str).encode()
        except BaseException:raw=repr(type(value)).encode()
        return {'status':'malformed','worker_result_sha256':hashlib.sha256(raw).hexdigest(),
          'summary_error_type':type(error).__name__,'summary_error':str(error)[:512]}

def write_result(value):
    raw=(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
    if len(raw)>MAX_RESULT_BYTES:raise RuntimeError('R8 result receipt exceeds 1 MiB bound')
    fd=os.open(RESULT,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    directory_fd=os.open(ROOT,os.O_RDONLY)
    try:os.fsync(directory_fd)
    finally:os.close(directory_fd)

def write_failure_fallback(phase,error):
    """Best-effort compact receipt if the normal result cannot be serialized or fsynced."""
    fallback=ROOT/'r8-revision3-result-write-failure.json'
    value={'profile':'ashlar-truss049-r8-preflight-result/0.1','attempt':'r8-revision3',
      'outcome':'result_write_failed','phase':phase,'error_type':type(error).__name__,
      'error':sanitized_message(error),'worker_sha256':digest(WORKER) if WORKER.is_file() else None,
      'supervisor_sha256':digest(Path(__file__)),'preparation_sha256':digest(R8_PREPARATION) if R8_PREPARATION.is_file() else None,
      'native_actions':{'postgres_started':False,'spark_started':False,'migration_invoked':False,
        'source_feed_publication_or_ack_mutated':False}}
    raw=(json.dumps(value,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
    fd=os.open(fallback,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    with os.fdopen(fd,'wb') as stream:
        stream.write(raw);stream.flush();os.fsync(stream.fileno())
    directory_fd=os.open(ROOT,os.O_RDONLY)
    try:os.fsync(directory_fd)
    finally:os.close(directory_fd)
    return value

def sanitized_message(value):
    return str(value).replace(str(ROOT),'<R8_ROOT>').replace(str(INPUT_ROOT),'<R7_ROOT>')[:2048]

def main():
    phase='argument_admission';worker_result=None;gates=None
    started=datetime.now(timezone.utc).isoformat()
    try:
        if sys.argv[1:]!=['--preflight']:raise RuntimeError('R8 accepts only --preflight')
        validate_roots()
        validate_packet_bindings()
        if Path(sys.executable).resolve()!=(VENV/'bin/python').resolve():
            raise RuntimeError('Exact selected R7 runtime interpreter required')
        if RESULT.exists() or RESULT.is_symlink():raise RuntimeError('Fresh R8 result receipt required')
        LOGDIR.mkdir(mode=0o700,exist_ok=True)
        if LOGDIR.stat().st_mode&0o077:raise RuntimeError('Private mode-0700 R8 log directory required')
        phase='worker_preflight'
        worker_result=preflight()
        phase='admission_gates'
        gates=admission_gates()
        result={'profile':'ashlar-truss049-r8-preflight-result/0.1','attempt':'r8-revision3',
          'outcome':'passed','exit_code':0,'started_at_utc':started,'finished_at_utc':datetime.now(timezone.utc).isoformat(),
          'worker_sha256':digest(WORKER),'supervisor_sha256':digest(Path(__file__)),
          'preparation_sha256':digest(R8_PREPARATION),'runtime_receipt_sha256':digest(RUNTIME),
          'worker_preflight_summary':summarized_worker(worker_result),'admission_gates':gates,
          'native_actions':{'postgres_started':False,'spark_started':False,'migration_invoked':False,
            'source_feed_publication_or_ack_mutated':False}}
        write_result(result)
        print(json.dumps(result,sort_keys=True));return 0
    except BaseException as error:
        result={'profile':'ashlar-truss049-r8-preflight-result/0.1','attempt':'r8-revision3',
          'outcome':'failed','exit_code':1,'phase':phase,'started_at_utc':started,
          'finished_at_utc':datetime.now(timezone.utc).isoformat(),
          'worker_sha256':digest(WORKER) if WORKER.is_file() else None,
          'supervisor_sha256':digest(Path(__file__)),
          'preparation_sha256':digest(R8_PREPARATION) if R8_PREPARATION.is_file() else None,
          'runtime_receipt_sha256':digest(RUNTIME) if RUNTIME.is_file() else None,
          'error_type':type(error).__name__,'error':sanitized_message(error),
          'worker_preflight_summary':summarized_worker(worker_result),'admission_gates':gates,
          'native_actions':{'postgres_started':False,'spark_started':False,'migration_invoked':False,
            'source_feed_publication_or_ack_mutated':False}}
        try:write_result(result)
        except BaseException as write_error:
            fallback=write_failure_fallback(phase,write_error)
            print(json.dumps(fallback,sort_keys=True));return 2
        print(json.dumps(result,sort_keys=True));return 1

if __name__=='__main__':raise SystemExit(main())
