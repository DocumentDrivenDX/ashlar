#!/usr/bin/env python3
"""Single-mode supervisor for the effect-free R13 preflight; no execution mode exists."""
import argparse, hashlib, json, os, pathlib, subprocess, sys
HERE=pathlib.Path(__file__).resolve().parent
VENV=pathlib.Path('/private/tmp/ashlar-truss050-current-cohort-9c745278-r13/runtime/venv')
WORKER=HERE/'campaign_worker_r13.py'
def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--preflight',action='store_true',required=True)
    args=parser.parse_args()
    env={'PATH':str(VENV/'bin')+':/usr/bin:/bin','TMPDIR':'/private/tmp','PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1','PIP_NO_CACHE_DIR':'1'}
    child=subprocess.run([str(VENV/'bin/python'),'-I','-B',str(WORKER),'--preflight'],capture_output=True,text=True,timeout=90,env=env)
    if child.stderr.strip(): raise SystemExit('FAIL: worker emitted stderr: '+child.stderr[-2000:])
    if child.returncode: raise SystemExit('FAIL: worker return code '+str(child.returncode)+': '+child.stdout[-2000:])
    result=json.loads(child.stdout)
    if result.get('status')!='blocked_source_admission_profile_missing': raise SystemExit('FAIL: unexpected worker outcome')
    result['supervisor']={'profile':'ashlar-truss050-r13-effect-free-supervisor/0.1','worker_sha256':hashlib.sha256(WORKER.read_bytes()).hexdigest(),'child_deadline_seconds':90,'child_returncode':child.returncode,'automatic_retries':0}
    print(json.dumps(result,sort_keys=True,separators=(',',':')))
if __name__=='__main__':main()
