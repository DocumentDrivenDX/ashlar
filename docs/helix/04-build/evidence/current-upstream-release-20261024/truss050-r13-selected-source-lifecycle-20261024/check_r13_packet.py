#!/usr/bin/env python3
"""Focused static integrity check for the prepared, blocked R13 evidence packet."""
from __future__ import annotations
import hashlib, json, pathlib, subprocess
HERE=pathlib.Path(__file__).resolve().parent
REPO=HERE.parents[5]
def read(name): return json.loads((HERE/name).read_bytes())
def sha(path): return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
def need(ok,msg):
    if not ok: raise SystemExit('FAIL: '+msg)
prep=read('preparation.json'); manifest=read('source-inputs-manifest.json')
runtime=read('runtime-receipt.json'); bridge=read('bridge-readiness.json'); result=read('r13-preflight-result.json')
source=prep['selected_source']; commit=source['commit']
need(manifest['commit']==commit and manifest['tree']==source['tree'] and manifest['objects']==source['objects'],'selected source manifest mismatch')
need(len(manifest['files'])==source['file_count']==588,'selected source inventory mismatch')
need(subprocess.check_output(['git','rev-parse',commit+'^{tree}'],cwd=REPO,text=True).strip()==source['tree'],'selected Git tree mismatch')
need(subprocess.run(['git','merge-base','--is-ancestor',commit,'HEAD'],cwd=REPO,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0,'selected source is not a packet ancestor')
for name,oid in source['objects'].items():
    need(subprocess.check_output(['git','rev-parse',commit+':'+name],cwd=REPO,text=True).strip()==oid,'source object mismatch: '+name)
need(sha(source['archive_path'])==source['archive_sha256'] and sha(source['archive_path'])==manifest['archive_sha256'],'source archive SHA mismatch')
need(sha(HERE/'runtime-receipt.json')==prep['runtime']['receipt_sha256'],'runtime receipt SHA mismatch')
need(runtime['install']['truss_wheel_sha256']==prep['selected_release']['wheel_sha256'] and runtime['install']['selected_wheel_count']==27,'runtime .50 cohort mismatch')
need(runtime['install']['record_payloads_verified']==4445 and runtime['install']['pip_check']=='No broken requirements found.','runtime RECORD/pip verification missing')
need(runtime['budgets']['allocated_bytes']<=runtime['budgets']['cap_bytes']==1073741824 and runtime['budgets']['logical_bytes']<=1073741824,'runtime exceeds byte cap')
need(sha(HERE/'bridge-readiness.json')==prep['bridge_sha256'],'bridge receipt SHA mismatch')
need(bridge['status']=='ready' and bridge['ashlar_commit']==commit and bridge['runtime_input_objects']==source['objects'],'bridge source binding mismatch')
need(bridge['bridge_sha256']=='34deb5c79b5dcb1ace8093e66a4b1213a98cbcc1fb50bfd24fdc4a21721c76b5','bridge asset pin mismatch')
need(sha(HERE/'campaign_worker_r13.py')==result['supervisor']['worker_sha256'],'worker digest mismatch')
need(result['status']=='blocked_source_admission_profile_missing' and result['source_admission_gate']['outcome']=='blocked','preflight blocker result mismatch')
need(result['source_admission_gate']['installed_version']=='0.0.50' and result['source_admission_gate']['source_literal']=='CURRENT_TRUSS_049_VERSION = 0.0.49','source profile blocker evidence mismatch')
need(len(result['bridge']['roles'])==6 and result['legacy_048_import_preflight']['truss']=='0.0.48','focused proposal/.48 gate result incomplete')
need(all(value is False for value in result['native_actions'].values()),'native/source effect claimed')
need(result['execution_authorization']['authorized'] is False and result['execution_authorization']['approval_or_start_marker_created'] is False,'authorization state is not closed')
attempt=pathlib.Path(prep['retained_pgdata']['cold_backup_path']).parents[1]
need(attempt.is_dir() and not any(attempt.iterdir()),'attempt directory is not empty')
need(prep['cold_backup_registry']=={'cold_backup_created':False,'migration_registry_created':False},'backup/registry unexpectedly present')
need((pathlib.Path(prep['retained_pgdata']['path'])/'postmaster.pid').exists() is False,'retained PGDATA not stopped at check')
need(not (HERE/'execution-approval.json').exists() and not (HERE/'started.json').exists(),'execution approval or start marker found')
index=read('artifacts.sha256.json')
actual={path.name:sha(path) for path in sorted(HERE.iterdir()) if path.is_file() and path.name!='artifacts.sha256.json'}
need(index==actual,'artifact digest index is stale or incomplete')
print(json.dumps({'profile':'ashlar-truss050-r13-static-packet-check/0.1','status':'passed_blocker_packet_integrity','source_commit':commit,'source_tree':source['tree'],'source_files':len(manifest['files']),'runtime_allocated_bytes':runtime['budgets']['allocated_bytes'],'runtime_logical_bytes':runtime['budgets']['logical_bytes'],'bridge_roles_proposed':len(result['bridge']['roles']),'preflight_status':result['status'],'native_actions':False,'execution_authorization':False},sort_keys=True))
