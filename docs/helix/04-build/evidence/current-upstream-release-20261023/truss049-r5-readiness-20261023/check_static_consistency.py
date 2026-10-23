#!/usr/bin/env python3
"""Read-only checks for R5 packet cross-file pins; never runs campaign code."""
import hashlib, json, os, pathlib, stat, uuid
ROOT = pathlib.Path(__file__).resolve().parent

def sha(path): return hashlib.sha256(path.read_bytes()).hexdigest()
def read(name): return json.loads((ROOT / name).read_text())

def require(ok, message):
    if not ok: raise SystemExit('FAIL: ' + message)

prep = read('preparation.json')
ready = read('readiness-r5.json')
proposal = read('approval-proposal-r5.json')
mode = stat.S_IMODE(ROOT.stat().st_mode)
require(mode == 0o700, f'private root mode {mode:o}, expected 700')
worker = ROOT / 'campaign_worker.py'
supervisor = ROOT / 'run_supervisor.py'
prep_path = ROOT / 'preparation.json'
ready_path = ROOT / 'readiness-r5.json'
worker_sha, supervisor_sha = sha(worker), sha(supervisor)
prep_sha, ready_sha = sha(prep_path), sha(ready_path)
require(prep['candidate_artifacts']['worker']['sha256'] == worker_sha, 'preparation worker digest')
require(prep['candidate_artifacts']['supervisor']['sha256'] == supervisor_sha, 'preparation supervisor digest')
source = ready['prepared_execution_source']
require(source['worker_sha256'] == worker_sha and source['supervisor_sha256'] == supervisor_sha, 'readiness code digests')
require(source['preparation_sha256'] == prep_sha, 'readiness preparation digest')
bind = proposal['static_bindings']
require(bind['worker_sha256'] == worker_sha and bind['supervisor_sha256'] == supervisor_sha, 'proposal code digests')
require(bind['preparation_sha256'] == prep_sha, 'proposal preparation digest')
require(bind['readiness_manifest_sha256'] == ready_sha, 'proposal readiness digest')
bridge = ROOT / 'bridge-readiness.json'
rebuild = ROOT / 'bridge-rebuild-r5.json'
require(sha(bridge) == ready['bridge']['pin_sha256'] == prep['bridge_readiness']['sha256'], 'bridge pin digest')
require(sha(rebuild) == ready['bridge']['rebuild_receipt_sha256'], 'bridge rebuild receipt digest')
require(ready['bridge']['iife_sha256'] == prep['bridge_readiness']['bridge_sha256'], 'bridge IIFE digest binding')
require(source['preparation'] == str(prep_path) and source['worker'] == str(worker) and source['supervisor'] == str(supervisor), 'readiness artifact paths')
limits = proposal['scope']['resource_limits']
footprint = proposal['scope']['retained_footprint']
require(limits.get('campaign_data_cap_bytes') == 1073741824, 'campaign-data cap value')
require('PGDATA' in limits.get('campaign_data_cap_includes', '') and 'worker logs' in limits.get('campaign_data_cap_includes', ''), 'campaign-data cap scope label')
expected_kib = limits['campaign_data_cap_bytes'] // 1024 + footprint['reused_runtime_du_kib_prior_review_observation'] + footprint['source_snapshot_du_kib_prior_review_observation']
require(footprint['conservative_total_worst_case_kib_using_larger_prior_observations'] == expected_kib, 'retained footprint total')
attempt = prep['migration_recovery']['attempt_id']
require(str(uuid.UUID(attempt)) == attempt, 'attempt ID is not canonical UUID')
require(prep['future_execution_paths']['migration_attempt_id'] == attempt, 'attempt ID differs across preparation paths')
require(ready['migration_attempt_id'] == attempt and proposal['migration_attempt_id'] == attempt, 'attempt ID differs across packet')
for key in ('attempt_directory_empty', 'logs_empty', 'preflight_run', 'start_marker_created', 'authorized', 'execution_approval_created'):
    expected = False if key in ('preflight_run','start_marker_created','authorized','execution_approval_created') else True
    require(ready['authorization'][key] is expected, f'readiness authorization.{key}')
for directory in ('attempt-r5', 'logs'):
    path = ROOT / directory
    require(path.is_dir() and not any(path.iterdir()), f'{directory} is not empty')
for key, item in prep['candidate_artifacts'].items():
    path = pathlib.Path(item['path'])
    if key == 'offline_preflight':
        require(item['status'] == 'not_run_shared_slot_may_be_occupied', 'offline preflight must remain unrun')
        continue
    require(path.exists(), f'missing referenced artifact {key}: {path}')
    if item.get('sha256'):
        require(sha(path) == item['sha256'], f'referenced artifact digest mismatch: {key}')
require(proposal['authorized'] is False and proposal['execution_approval_created'] is False, 'proposal must remain unapproved')
require(proposal['preflight']['status'] == 'not_run', 'proposal preflight status')
print('PASS: R5 packet cross-file pins, referenced artifacts, attempt identity, private root, and inert state')
print(f'attempt_id={attempt}; preparation_sha256={prep_sha}; readiness_sha256={ready_sha}')
print(f'worker_sha256={worker_sha}; supervisor_sha256={supervisor_sha}')
