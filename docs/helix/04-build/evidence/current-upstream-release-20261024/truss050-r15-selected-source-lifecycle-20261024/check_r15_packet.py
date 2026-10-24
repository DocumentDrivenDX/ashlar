#!/usr/bin/env python3
"""Focused effect-free integrity check for the R15 packet."""
import hashlib, json
from pathlib import Path
ROOT=Path(__file__).resolve().parent
PRIVATE=Path('/private/tmp/ashlar-truss050-current-source-2eeecf40-r15')
def sha(p): return hashlib.sha256(p.read_bytes()).hexdigest()
def load(n): return json.loads((ROOT/n).read_text())
prep=load('preparation.json'); manifest=load('source-inputs-manifest.json')
result=load('r15-preflight-result.json'); final=load('r15-supervisor-final-preflight.json')
assert prep['selected_source']['commit']=='2eeecf409033c6c7c92ded72ec7e43c569dffaf9'
assert prep['selected_source']['tree']==manifest['tree']=='a97e9e1e275294e840690bbe792e795afbeffff7'
assert prep['selected_source']['objects']==manifest['objects']
assert sha(ROOT/'source-inputs-manifest.json')==prep['selected_source']['manifest_sha256']
assert sha(ROOT/'preparation.json')==result['preparation_sha256']
assert sha(ROOT/'campaign_worker_r15.py')==result['worker_sha256']==final['worker_sha256']
assert sha(ROOT/'run_supervisor_r15.py')==result['supervisor_sha256']==final['supervisor_sha256']
assert sha(ROOT/'r15-sandbox-supervisor-preflight.json')==result['failed_sandbox_preflight_sha256']
assert sha(ROOT/'r15-sysv-cleanup-receipt.json')==result['sysv_cleanup_receipt_sha256']
assert sha(ROOT/'r15-supervisor-final-preflight.json')==result['final_supervisor_preflight_sha256']
assert final['outcome']==result['outcome']=='ready_pending_hash_bound_operator_approval'
p=final['preflight']; assert p['approval_contract_parity']['exact_dict_match'] is True
assert p['approval_contract_parity']['approval_payload_sha256']==result['approval_payload_sha256']
assert p['admission_gates']['sysv_shared_memory']['ipc_rmid_confirmed'] is True
assert p['pg_started'] is False and p['spark_started'] is False and p['migration_apply_invoked'] is False
assert result['effect_boundary']=={'postgres_started':False,'spark_started':False,'migration_applied':False,'source_or_consumer_mutated':False,'publication_or_ack_mutated':False,'seed_submitted':False,'execution_approval_created':False,'execution_started_marker_created':False}
assert not (ROOT/'execution-approval.json').exists()
assert not list(ROOT.glob('execution-started*'))
attempt=PRIVATE/prep['attempt']; assert attempt.is_dir() and not any(attempt.iterdir())
assert not (PRIVATE/'execution-approval.json').exists()
assert not list(PRIVATE.glob('execution-started*'))
print('R15 packet integrity passed; final effect-free admission gates and payload parity are ready; execution remains unauthorized.')
