#!/usr/bin/env python3
"""Focused, effect-free integrity check for the R14 evidence packet."""
from __future__ import annotations
import hashlib
import json
from pathlib import Path

ROOT=Path(__file__).resolve().parent
def sha(path:Path)->str:return hashlib.sha256(path.read_bytes()).hexdigest()
def load(name:str):return json.loads((ROOT/name).read_text())

prep=load('preparation.json')
result=load('r14-preflight-result.json')
final=load('r14-supervisor-final-preflight.json')
manifest=load('source-inputs-manifest.json')
receipt=load('runtime-receipt.json')
bridge=load('bridge-readiness.json')
assert prep['selected_source']['commit']=='c33bef013d737b8a56f43faafe5f5c1966065d1f'
assert sha(ROOT/'preparation.json')==result['source_preparation_sha256']
assert prep['repository_state_at_packet_preparation']['observed_main_head']=='b926ae84dc67c1835ab91a8b043b1606d55fc6fb'
assert prep['selected_source']['objects']=={'src':'7c6b4f14f4d55e539371aa9e61fa5c4752bc2c93','sql':'3cfe250fadd42ef4b25597f0eec7d10a426ce8ff','pyproject.toml':'7af01eeb58eaf0dbd7ae7d0246618e97a96ca724'}
assert manifest['commit']==prep['selected_source']['commit']
assert manifest['tree']==prep['selected_source']['tree']
assert manifest['objects']==prep['selected_source']['objects']
assert sha(ROOT/'source-inputs-manifest.json')==prep['selected_source']['manifest_sha256']
assert sha(ROOT/'runtime-receipt.json')==prep['runtime']['receipt_sha256']
assert receipt['budgets']['allocated_bytes']<=1073741824
truss_wheel=next(w for w in receipt['install']['selected_wheels'] if w['distribution']=='truss-toolkit')
assert truss_wheel['version']=='0.0.50'
assert truss_wheel['sha256']=='7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00'
assert bridge['status']=='ready' and bridge['ashlar_commit']==prep['selected_source']['commit']
assert sha(ROOT/'bridge-readiness.json')==prep['bridge_sha256']
assert result['worker_preflight']['outcome']=='ready_pending_hash_bound_operator_approval'
assert result['supervisor_preflight']['outcome']=='pending_execution_admission'
assert result['supervisor_preflight']['approval_contract_parity'].startswith('pending')
assert result['supervisor_preflight']['sysv_shared_memory_gate']['cleanup_confirmed'] is True
supervisor_source=(ROOT/'run_supervisor_r14.py').read_text()
assert supervisor_source.count("gates=admission_gates()") == 1
assert 'gate_digest=parity[\'admission_gates_sha256\']' in supervisor_source
worker_sha=sha(ROOT/'campaign_worker_r14.py')
supervisor_sha=sha(ROOT/'run_supervisor_r14.py')
assert result['supervisor_preflight']['worker_sha256']==worker_sha
assert result['supervisor_preflight']['supervisor_sha256']=='144dde1ee20f0d648b4385a874af2591f03a4c013213a8ffcafa01992ca6aa66'
assert final['profile']=='ashlar-truss050-offline-supervisor/0.5'
assert final['outcome']=='ready_pending_hash_bound_operator_approval'
assert final['worker_sha256']==worker_sha=='e29ec48e32e51871fcad3628258227bec7fb240ed8423ea4a03253e4d1138754'
assert final['supervisor_sha256']==supervisor_sha=='179820043ef6224827cb16a2c59b7b835d8f9c8a6445ff9b3f1dc7c1bcaba336'
assert sha(ROOT/'r14-supervisor-final-preflight.json')=='92f5ccca136ee8fdc2eee7ea0ad0fd7236fc27786e04391f9e793a05b5246bc5'
final_pre=final['preflight']
assert final_pre['outcome']=='ready_pending_hash_bound_operator_approval'
assert final_pre['bridge']['source_snapshot']['commit']==prep['selected_source']['commit']
assert final_pre['approval_contract_parity']['status']=='ready'
assert final_pre['approval_contract_parity']['exact_dict_match'] is True
assert final_pre['approval_contract_parity']['approval_payload_sha256']=='3143b7ce41821aaf932e579bb4d9725c1ffbb1324404cdbbde5ed31fdf3ffc0b'
assert final_pre['approval_contract_parity']['admission_gates_sha256']=='665001fee6f02a8253542c8f3b2dc6ecd4fc9029cca6bfed2a2a2f1f4dc1fae9'
assert final_pre['admission_gates']['process_inventory']['status']=='ready'
assert final_pre['admission_gates']['process_inventory']['supervisor_pid_present'] is True
assert final_pre['admission_gates']['sysv_shared_memory']['status']=='ready'
assert final_pre['admission_gates']['sysv_shared_memory']['ipc_rmid_confirmed'] is True
assert final_pre['pg_started'] is False and final_pre['spark_started'] is False
assert final_pre['migration_apply_invoked'] is False
assert final_pre['migration_route']['status']=='verified_declared_metadata_only'
assert result['effect_boundary']=={'postgres_started':False,'spark_started':False,'truss_migration_applied':False,'source_or_consumer_mutated':False,'feed_publication_or_ack_mutated':False,'seed_submitted':False,'execution_approval_created':False,'execution_started_marker_created':False}
for name in ('execution-approval.json','execution-started-r14-ec5760a4-7462-4047-9e6c-e228791c1348.json'):
    assert not (ROOT/name).exists(),f'unexpected authorization/start artifact: {name}'
private_root=Path('/private/tmp/ashlar-truss050-current-source-c33bef01-r14')
attempt=private_root/prep['attempt']
assert attempt.is_dir() and not any(attempt.iterdir()),'single-use attempt directory is not empty'
assert not (private_root/'execution-approval.json').exists()
assert not (private_root/'execution-started-r14-ec5760a4-7462-4047-9e6c-e228791c1348.json').exists()
print('R14 packet integrity: passed; historical supervisor capture pending, final effect-free supervisor gates/parity ready; owner execution approval still required.')
