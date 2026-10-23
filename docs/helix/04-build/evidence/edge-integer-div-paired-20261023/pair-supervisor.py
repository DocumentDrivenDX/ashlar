import os, signal, subprocess, time
from pathlib import Path
root=Path('/private/tmp/ashlar-edge-paired-20261023')
py='/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python'
repo='/Users/erik/Projects/ashlar'
wheel='/private/tmp/ashlar-release-20261021-m3rde5p2/wheelhouse-bc2accd8/ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl'
commit='bc2accd883a49d34c282dc002af98b4b05ea9440'
env=os.environ.copy(); env.update({'JAVA_HOME':'/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home','PATH':'/opt/homebrew/opt/openjdk@21/bin:'+os.environ.get('PATH',''),'PYSPARK_PYTHON':py,'PYSPARK_SUBMIT_ARGS':'--driver-memory 24g pyspark-shell'})
rows=[]; deadline=time.monotonic()+300
for label in ('baseline','candidate'):
    output=root/f'out-{label}'
    if output.exists(): raise SystemExit(f'output already exists: {output}')
    log=(root/f'{label}.log').open('xb')
    command=[py,str(root/f'{label}.py'),'--repo',repo,'--wheel',wheel,'--expected-commit',commit,'--output',str(output)]
    process=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
    remaining=deadline-time.monotonic()
    try: code=process.wait(timeout=max(0,remaining))
    except subprocess.TimeoutExpired:
        os.killpg(process.pid,signal.SIGTERM)
        try: code=process.wait(timeout=10)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid,signal.SIGKILL); code=process.wait()
        rows.append({'label':label,'returncode':code,'timed_out':True})
        log.close(); break
    log.close(); rows.append({'label':label,'returncode':code,'timed_out':False})
    print(label,'returncode',code,'elapsed_s',round(300-(deadline-time.monotonic()),3),flush=True)
    if code: break
print('pair_results',rows)
