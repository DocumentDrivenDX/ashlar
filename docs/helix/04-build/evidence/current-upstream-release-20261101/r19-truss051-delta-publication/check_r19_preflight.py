#!/usr/bin/env python3
"""Run R19's actual worker and supervisor effect-free preflight gates."""
import hashlib,importlib.util,json,os,subprocess,tempfile
from pathlib import Path
HERE=Path(__file__).resolve().parent
scope_path=HERE/'r19-publication-scope.json';raw=scope_path.read_bytes();scope=json.loads(raw)
env=os.environ.copy();env['PYTHONPATH']=scope['runtime']['pythonpath']
python=scope['runtime']['python_path']
def run(command): return subprocess.run(command,env=env,text=True,capture_output=True,timeout=180,check=True)
worker=run([python,str(HERE/'r19_delta_publication_worker.py'),'--preflight']);wp=json.loads(worker.stdout)
assert wp['status']=='passed_effect_free_worker_preflight' and not wp['postgres_started'] and not wp['database_contacted'] and not wp['spark_started'] and not wp['delta_written'] and not wp['acknowledgement']
supervisor=run([python,str(HERE/'r19_supervisor.py'),'--preflight']);sp=json.loads(supervisor.stdout)
assert sp['status']=='passed_effect_free_supervisor_preflight' and not sp['worker_started'] and not sp['postgres_started'] and not sp['database_contacted']
assert sp['scope_sha256']==hashlib.sha256(raw).hexdigest()
for name,path in scope['paths'].items(): assert not Path(path).exists() and not Path(path).is_symlink(),(name,path)
spec=importlib.util.spec_from_file_location('r19_handshake_worker',HERE/'r19_delta_publication_worker.py')
module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
with tempfile.TemporaryDirectory(prefix='ashlar-r19-handshake-') as temporary:
    root=Path(temporary);paths={key:str(root/Path(value).name) for key,value in scope['paths'].items()}
    module.validate_attempt_paths(paths,supervisor_started=False,expected_paths=paths)
    supervisor_root=Path(paths['supervisor_root']);supervisor_root.mkdir(mode=0o700)
    previous=os.umask(0o077)
    try:
        for name in ('child.stdout.log','child.stderr.log'):
            with (supervisor_root/name).open('xb') as stream: stream.write(b'')
    finally: os.umask(previous)
    module.validate_attempt_paths(paths,supervisor_started=True,expected_paths=paths)
    (supervisor_root/'unexpected.log').write_bytes(b'')
    try: module.validate_attempt_paths(paths,supervisor_started=True,expected_paths=paths)
    except module.StopRun: pass
    else: raise AssertionError('worker accepted an unexpected supervisor file')
print(json.dumps({'status':'passed_actual_r19_worker_supervisor_and_created_log_handshake_preflight','worker':wp,'supervisor':sp,'worker_stderr':worker.stderr,'supervisor_stderr':supervisor.stderr},sort_keys=True))
