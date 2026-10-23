"""Run one edge write under hard wall, output and diagnostics caps."""
import json, os, signal, subprocess, sys, time
from pathlib import Path

ROOT=Path('/private/tmp/ashlar-edge-write-attribution-20261023-attempt-04')
OUT=ROOT/'out'; LOG=ROOT/'run.log'
PY='/private/tmp/ashlar-release-20261021-m3rde5p2/venv/bin/python'
REPO='/Users/erik/Projects/ashlar'
WHEEL='/private/tmp/ashlar-release-20261021-m3rde5p2/wheelhouse-bc2accd8/ashlar_graph_toolkit-0.1.0.dev1-py3-none-any.whl'
COMMIT='bc2accd883a49d34c282dc002af98b4b05ea9440'
EVIDENCE=Path(__file__).resolve().parent

def sizes(path):
    total=0
    if path.exists():
        for f in path.rglob('*'):
            if f.is_file():
                try: total+=f.stat().st_size
                except FileNotFoundError: pass
    return total

def main():
    if ROOT.exists(): raise SystemExit('private run root exists; refusing reuse')
    ROOT.mkdir(mode=0o700,parents=True)
    env=os.environ.copy();env.update({'JAVA_HOME':'/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home','PATH':'/opt/homebrew/opt/openjdk@21/bin:'+os.environ.get('PATH',''),'PYSPARK_PYTHON':PY,'PYSPARK_SUBMIT_ARGS':'--driver-memory 24g pyspark-shell'})
    command=[PY,'-B',str(EVIDENCE/'run_jfr.py'),'--repo',REPO,'--wheel',WHEEL,'--commit',COMMIT,'--out',str(OUT)]
    started=time.monotonic(); timed_out=False; limit=None
    with LOG.open('xb') as log:
        proc=subprocess.Popen(command,env=env,stdout=log,stderr=subprocess.STDOUT,start_new_session=True)
        while proc.poll() is None:
            elapsed=time.monotonic()-started
            out_bytes=sizes(OUT); diag_bytes=LOG.stat().st_size + max(0,sizes(OUT)-sizes(OUT/'warehouse'))
            # Start termination with ten seconds left for the eight-second
            # grace period and result-file write; this is a monitored guard,
            # not an OS filesystem/time quota.
            if elapsed>=290: limit='wall-300s'
            elif out_bytes>4_000_000_000: limit='output-4GB'
            elif diag_bytes>128_000_000: limit='diagnostics-128MB'
            if limit:
                try:os.killpg(proc.pid,signal.SIGTERM)
                except ProcessLookupError:pass
                remaining=max(0,299-(time.monotonic()-started))
                try:proc.wait(timeout=min(8,remaining))
                except subprocess.TimeoutExpired:
                    try:os.killpg(proc.pid,signal.SIGKILL)
                    except ProcessLookupError:pass
                    proc.wait()
                timed_out=limit=='wall-300s'
                break
            time.sleep(.5)
        code=proc.wait()
    output_bytes=sizes(OUT);diagnostic_bytes=LOG.stat().st_size+max(0,output_bytes-sizes(OUT/'warehouse'))
    if limit is None and output_bytes>4_000_000_000:limit='output-4GB-observed-after-exit'
    if limit is None and diagnostic_bytes>128_000_000:limit='diagnostics-128MB-observed-after-exit'
    result={'returncode':code,'elapsed_seconds':round(time.monotonic()-started,3),'timed_out':timed_out,'limit_triggered':limit,'output_tree_bytes':output_bytes,'diagnostic_bytes':diagnostic_bytes,'output_cap_bytes':4_000_000_000,'diagnostic_cap_bytes':128_000_000,'wall_cap_seconds':300,'wall_termination_trigger_seconds':290,'terminate_grace_seconds':8,'monitor_interval_seconds':.5,'bounds_are_polled_thresholds_not_filesystem_quotas':True,'command':command}
    (ROOT/'supervisor-result.json').write_text(json.dumps(result,sort_keys=True,separators=(',',':'))+'\n')
    print(json.dumps(result,sort_keys=True,separators=(',',':')))
    return 0 if code==0 and limit is None else 1

if __name__=='__main__':raise SystemExit(main())
