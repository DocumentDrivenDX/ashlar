import json, os, pathlib, signal, subprocess, time
root = pathlib.Path('/private/tmp/ashlar-umf056-delta4-one-table-20261023-attempt-04')
limit, deadline = 256*1024*1024, 180
env = os.environ.copy()
env['JAVA_HOME'] = '/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home'
env['PATH'] = env['JAVA_HOME'] + '/bin:' + env.get('PATH','')
env['PYSPARK_PYTHON'] = '/private/tmp/ashlar-truss049-lifecycle-campaign-20261023-r4-f5e89926/runtime/venv/bin/python'
env['PYSPARK_DRIVER_PYTHON'] = env['PYSPARK_PYTHON']
env['SPARK_LOCAL_DIRS'] = str(root / 'spark-local')
env['TMPDIR'] = str(root / 'tmp')
(root / 'tmp').mkdir()
def size():
    total = 0
    for p in root.rglob('*'):
        try:
            if p.is_file() and not p.is_symlink(): total += p.stat().st_size
        except FileNotFoundError:
            pass
    return total
started = time.monotonic(); max_seen = 0; outcome = 'exit'; code = None
with (root/'run.log').open('wb') as log:
    p = subprocess.Popen([env['PYSPARK_PYTHON'], str(root/'run.py')], cwd=str(root), env=env,
                         stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    while True:
        code = p.poll(); current = size(); max_seen = max(max_seen, current)
        if current > limit:
            outcome = 'temp_cap_exceeded'; os.killpg(p.pid, signal.SIGTERM); break
        if code is not None: break
        if time.monotonic()-started >= deadline:
            outcome = 'timeout'; os.killpg(p.pid, signal.SIGTERM); break
        time.sleep(0.25)
    if p.poll() is None:
        try: p.wait(timeout=5)
        except subprocess.TimeoutExpired:
            os.killpg(p.pid, signal.SIGKILL); p.wait()
    if code is None: code = p.returncode
result = {'outcome':outcome, 'exitCode':code, 'elapsedSeconds':round(time.monotonic()-started,3),
          'deadlineSeconds':deadline, 'maxObservedOwnedTempBytes':max_seen, 'tempBytesCap':limit,
          'mavenPackagesRequested':False, 'root':str(root)}
(root/'supervisor-result.json').write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
print(json.dumps(result,sort_keys=True))
if code != 0 or outcome != 'exit': raise SystemExit(code if code else 1)
