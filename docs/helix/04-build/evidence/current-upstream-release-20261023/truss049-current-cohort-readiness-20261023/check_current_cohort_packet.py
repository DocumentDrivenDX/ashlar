from __future__ import annotations

import ast
import hashlib
import json
import re
import subprocess
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parents[5]
BASE = HERE.parent
PRIVATE = Path('/private/tmp/ashlar-truss049-current-cohort-6986e832-weft053-r6')
VENV = Path('/private/tmp/ashlar-truss049-current-cohort-20261023/venv')


def reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f'duplicate JSON key: {key}')
        result[key] = value
    return result


def load(path: Path):
    return json.loads(path.read_text(), object_pairs_hook=reject_duplicate_keys)


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def git(cwd: Path, *args: str) -> str:
    return subprocess.check_output(['git', '-C', str(cwd), *args], text=True).strip()


def require(condition: bool, message: str) -> None:
    if not condition:
        raise SystemExit(message)


prep = load(HERE / 'preparation.json')
bridge = load(HERE / 'bridge-readiness.json')
refresh_path = BASE / 'truss049-r5-current-cohort-refresh-20261023' / 'refresh-proposal.json'
runtime_path = BASE / 'truss049-current-cohort-runtime-20261023' / 'runtime-receipt.json'
r5_path = BASE / 'truss049-r5-readiness-20261023' / 'preparation.json'
refresh = load(refresh_path)
runtime = load(runtime_path)
r5 = load(r5_path)

require(prep['profile'] == 'ashlar-truss-0.0.49-current-cohort-preparation/0.1', 'preparation profile')
require(prep['attempt'] == 'r6', 'attempt label')
require(prep['state'] == 'prepared_static_pending_fresh_preflight', 'preparation state')
require(sha(refresh_path) == prep['refresh_proposal']['sha256'], 'current refresh proposal hash')
require(sha(runtime_path) == prep['runtime_receipt']['sha256'], 'current runtime receipt hash')
require(sha(r5_path) == prep['r5_historical_packet']['preparation_sha256'], 'historical R5 preparation hash')

source = refresh['refresh']['selected_source']
require(source['commit'] == prep['source_checkout']['commit'] == bridge['ashlar_commit'], 'selected source commit binding')
require(source['tree'] == prep['source_checkout']['tree'], 'selected source tree binding')
source_repo = PRIVATE / 'source-snapshot'
require(git(source_repo, 'rev-parse', 'HEAD') == source['commit'], 'private source checkout HEAD')
require(git(source_repo, 'rev-parse', 'HEAD^{tree}') == source['tree'], 'private source checkout tree')
require(not git(source_repo, 'status', '--porcelain', '--', 'src', 'sql', 'pyproject.toml'), 'private executable source snapshot is dirty')
for path, expected in source['runtime_input_objects'].items():
    require(git(source_repo, 'rev-parse', f'{source["commit"]}:{path}') == expected, f'selected source object {path}')
require(prep['source_checkout']['runtime_input_objects'] == source['runtime_input_objects'], 'preparation source object map')
require(bridge['runtime_input_objects'] == source['runtime_input_objects'], 'bridge source object map')

require(bridge['profile'] == 'ashlar-truss049-local-delta-bridge-readiness/0.2', 'bridge profile')
require(bridge['status'] == 'ready', 'source-bound bridge pin status')
source_module = (source_repo / 'src/ashlar_host/local_current_delta4_umf.py').read_text()
carrier_module = (source_repo / 'src/ashlar_host/local_current_carrier_profile.py').read_text()
for key, constant in (('umf_commit', 'UMF_CANDIDATE_COMMIT'), ('bridge_sha256', 'BRIDGE_SHA256'),
                      ('resource_sha256', 'REALIZATION_MANIFEST_SHA256'), ('build_inputs_sha256', 'BUILD_INPUTS_SHA256'),
                      ('role_semantics_sha256', 'ROLE_SEMANTICS_SHA256')):
    match = re.search(rf"^{constant} = '([^']+)'", source_module, re.M)
    require(match is not None and bridge[key] == match.group(1), f'bridge/source pin {key}')
match = re.search(r"^XXHASH64_PROFILE = '([^']+)'", carrier_module, re.M)
require(match is not None and bridge['carrier_profile'] == match.group(1), 'bridge carrier profile')

private_files = {
    'campaign_worker.py': 'worker',
    'run_supervisor.py': 'supervisor',
    'preparation.json': None,
    'bridge-readiness.json': None,
    'runtime-receipt.json': None,
    'seed-request.json': None,
    'source_048_readonly_helper.py': None,
    'weft053-compatibility-receipt.md': None,
}
for name, prep_key in private_files.items():
    path = PRIVATE / name
    require(path.is_file() and not path.is_symlink(), f'private packet file missing: {name}')
    if prep_key is not None:
        require(sha(path) == prep[prep_key]['sha256'], f'private {name} digest')
    if name in ('campaign_worker.py', 'run_supervisor.py'):
        ast.parse(path.read_text(), filename=str(path))
        require(sha(path) == sha(HERE / name), f'tracked/private script differs: {name}')
require(sha(PRIVATE / 'preparation.json') == sha(HERE / 'preparation.json'), 'private/tracked preparation differs')
require(sha(PRIVATE / 'bridge-readiness.json') == sha(HERE / 'bridge-readiness.json'), 'private/tracked bridge pin differs')
require(sha(PRIVATE / 'runtime-receipt.json') == sha(runtime_path), 'private runtime receipt copy')
require(sha(PRIVATE / 'seed-request.json') == r5['immutable_inputs']['seed_request']['sha256'], 'preserved seed request')
require(sha(PRIVATE / 'source_048_readonly_helper.py') == r5['readonly_helper']['sha256'], 'preserved .48 helper')
require(sha(PRIVATE / 'weft053-compatibility-receipt.md') == prep['selected_weft_candidate']['compatibility_receipt_sha256'], 'Weft 0.4.53 evidence copy')
require((PRIVATE / 'attempt-r6').is_dir() and not any((PRIVATE / 'attempt-r6').iterdir()), 'new attempt directory must be empty')
require((PRIVATE / 'attempt-r6').stat().st_mode & 0o077 == 0, 'attempt directory permissions')
for forbidden in ('execution-approval.json', 'execution-started-r6.json', 'offline-preflight-r6.json'):
    require(not (PRIVATE / forbidden).exists(), f'forbidden execution artifact exists: {forbidden}')
require(prep['execution_authorization']['authorized'] is False, 'execution authorization')
require(prep['execution_authorization']['approval_file_created'] is False, 'approval marker')
require(prep['future_preflight']['status'] == 'not_run', 'future preflight state')
require(prep['future_preflight']['command_argv'][-1] == '--preflight', 'future preflight command')
require(prep['future_preflight']['command_argv'][0] == str(VENV / 'bin/python'), 'future preflight interpreter')
require(prep['future_preflight']['command_argv'][3] == str(PRIVATE / 'run_supervisor.py'), 'future preflight supervisor')
require(prep['candidate_artifacts']['offline_preflight']['status'] == 'not_run', 'preflight artifact status')
require(prep['execution']['pg_started'] is False and prep['execution']['spark_started'] is False, 'native start flags')

expected_runtime = {'truss-toolkit':'0.0.49', 'umf-core':'0.10.0', 'umf_access':'0.1.0.dev7',
                    'weft-sql':'0.4.53', 'pyspark':'4.2.0', 'delta-spark':'4.4.1',
                    'pgserver-pg18':'18.6.1', 'psycopg':'3.3.6'}
require(runtime['schema'] == 'ashlar-current-cohort-runtime-receipt/v1', 'runtime receipt schema')
for name, version in expected_runtime.items():
    require(runtime['install']['versions'].get(name) == version, f'runtime pin {name}')
require(runtime['runtime']['venv_isolated'] is True and runtime['install']['pip_check'].startswith('passed:'), 'runtime install qualification')
require(runtime['install']['wheel_count'] == 27 and runtime['install']['verified_record_payload_count'] == 3987, 'runtime integrity counts')
require(runtime['limits'] and 'only' in runtime['scope'], 'runtime scope limitation')

old = r5['immutable_inputs']
expected_lineage = {
    'source_document_id': old['source_document_id'],
    'source_revision': old['source_revision'],
    'source_sha256': old['source_sha256'],
    'source_origin_sha256': old['source_origin_sha256'],
    'source_registration_id': old['source_registration_id'],
    'feed_registration_receipt_sha256': old['existing_feed_registration_receipt']['sha256'],
    'consumer_id': old['existing_consumer_id'],
    'consumer_registration_id': old['consumer_registration_id'],
    'genesis_checkpoint_sha256': old['genesis_checkpoint_sha256'],
    'prior_public_source_reobservation_receipt_sha256': old['prior_public_source_reobservation_receipt']['sha256'],
    'seed_request_sha256': old['seed_request']['sha256'],
}
lineage_paths = {
    'source_document_id': 'source_document_id', 'source_revision': 'source_revision',
    'source_sha256': 'source_sha256', 'source_origin_sha256': 'source_origin_sha256',
    'source_registration_id': 'source_registration_id', 'consumer_id': 'existing_consumer_id',
    'consumer_registration_id': 'consumer_registration_id', 'genesis_checkpoint_sha256': 'genesis_checkpoint_sha256',
}
for proposal_key, old_value in expected_lineage.items():
    if proposal_key in lineage_paths:
        actual = prep['immutable_inputs'][lineage_paths[proposal_key]]
    elif proposal_key == 'feed_registration_receipt_sha256':
        actual = prep['immutable_inputs']['existing_feed_registration_receipt']['sha256']
    elif proposal_key == 'prior_public_source_reobservation_receipt_sha256':
        actual = prep['immutable_inputs']['prior_public_source_reobservation_receipt']['sha256']
    else:
        actual = prep['immutable_inputs']['seed_request']['sha256']
    require(actual == old_value, f'preserved lineage {proposal_key}')
pg_ref = prep['immutable_inputs']['retained_pgdata_reference']
require(pg_ref['source'] == '../truss049-r5-readiness-20261023/preparation.json', 'PGDATA reference source')
require(pg_ref['json_pointer'] == '/immutable_inputs/retained_pgdata', 'PGDATA reference pointer')
require(pg_ref['preparation_sha256'] == sha(r5_path), 'PGDATA reference digest')
require('retained_pgdata' not in prep['immutable_inputs'], 'raw PGDATA path must not be copied')

worker = (HERE / 'campaign_worker.py').read_text()
supervisor = (HERE / 'run_supervisor.py').read_text()
require("'0.4.52'" not in worker and '598509e981e3238961d33d430c711b481e430adc945e692a1784ab758f7b6d46' not in worker, 'stale current Weft pin')
require('ASHLAR_R6_ADMISSION_GATES_SHA256' in worker and 'ASHLAR_R6_ADMISSION_GATES_SHA256' in supervisor, 'R6 gate namespace')
require("--preflight" in supervisor and "--execute" in supervisor, 'explicit preflight/execute mode gate')
require("'0.4.53'" in worker and prep['selected_weft_candidate']['wheel_sha256'] in json.dumps(prep), 'Weft 0.4.53 pin')
require(prep['future_execution_paths']['migration_attempt_id'] != r5['migration_recovery']['attempt_id'], 'fresh migration attempt ID')
require(prep['future_execution_paths']['fresh_local_delta_destination_id'].endswith('r6-20261023'), 'fresh destination identity')

# Verify every relative Markdown link in the packet and all listed packet checksums.
for line in (HERE / 'README.md').read_text().splitlines():
    if '](' not in line:
        continue
    target = line.split('](', 1)[1].split(')', 1)[0]
    if target.startswith(('http:', 'https:', '#')):
        continue
    require((HERE / target).resolve().exists(), f'broken packet link: {target}')
for line in (HERE / 'artifacts.sha256').read_text().splitlines():
    digest, name = line.split('  ', 1)
    require(sha(HERE / name) == digest, f'packet manifest mismatch: {name}')

print('R6 current-cohort packet: static source/runtime/bridge/lineage checks passed; no supervisor preflight or effects claimed')
