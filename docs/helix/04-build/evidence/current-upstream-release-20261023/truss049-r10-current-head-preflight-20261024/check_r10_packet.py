from __future__ import annotations
import ast, builtins, hashlib, json, subprocess, symtable, tarfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
PRIVATE = Path('/private/tmp/ashlar-truss049-current-cohort-e1001bd-r10')
REPO = Path('/Users/erik/Projects/ashlar')
EXPECTED_HEAD = 'e1001bd9e95c0fa555536b2c94c892b2b5b94cdb'
EXPECTED_RUNTIME_SHA = '517ac6892280e911060688afc75528907e62aa5d6d290abf6d9b166f5da5617a'
EXPECTED_RELEASE_ID = 421297021

def sha(path): return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def require(condition, message):
    if not condition: raise SystemExit(message)
def blob_sha(path):
    raw = Path(path).read_bytes()
    return hashlib.sha1(f'blob {len(raw)}\0'.encode() + raw).hexdigest()
def module_names(tree):
    names = set()
    for node in tree.body:
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)): names.add(node.name)
        elif isinstance(node, (ast.Import, ast.ImportFrom)):
            for item in node.names: names.add(item.asname or item.name.split('.')[0])
        elif isinstance(node, (ast.Assign, ast.AnnAssign, ast.AugAssign)):
            targets = node.targets if isinstance(node, ast.Assign) else [node.target]
            for target in targets:
                for child in ast.walk(target):
                    if isinstance(child, ast.Name): names.add(child.id)
    return names
def unresolved_function_globals(source, filename):
    tree = ast.parse(source, filename=str(filename)); root = symtable.symtable(source, str(filename), 'exec')
    globals_ = module_names(tree); missing = []
    def walk(table):
        if table.get_type() == 'function':
            unresolved = sorted({s.get_name() for s in table.get_symbols()
                if s.is_global() and s.get_name() not in globals_ and s.get_name() != '__file__'
                and not hasattr(builtins, s.get_name())})
            if unresolved: missing.append((table.get_name(), unresolved))
        for child in table.get_children(): walk(child)
    walk(root); return missing

def main():
    require(PRIVATE.is_dir() and not PRIVATE.is_symlink() and PRIVATE.stat().st_mode & 0o077 == 0, 'private root mode')
    prep = json.loads((HERE/'r10-preparation.json').read_bytes())
    manifest = json.loads((HERE/'source-inputs-manifest.json').read_bytes())
    bridge = json.loads((HERE/'bridge-readiness.json').read_bytes())
    worker = HERE/'campaign_worker_r10.py'; supervisor = HERE/'run_supervisor_r10.py'
    private_worker = PRIVATE/'campaign_worker_r10.py'; private_supervisor = PRIVATE/'run_supervisor_r10.py'
    require(prep['profile'] == 'ashlar-truss-0.0.49-r10-preflight-addendum/0.1', 'R10 profile')
    require(prep['status'] == 'packet_prepared_preflight_not_run_pending_sol_review', 'R10 status')
    require(prep['release_selection']['github_release_id'] == EXPECTED_RELEASE_ID and prep['release_selection']['version'] == '0.0.49', 'release selection')
    require(prep['release_selection']['authoritative_api_url'] == 'https://api.github.com/repos/DocumentDrivenDX/truss/releases/421297021', 'immutable release URL')
    require(manifest['commit'] == EXPECTED_HEAD and manifest['profile'] == 'ashlar-selected-source-inputs/1', 'current source manifest identity')
    require(prep['source_binding']['ashlar_commit'] == EXPECTED_HEAD and prep['source_binding']['tree'] == manifest['tree'], 'preparation source binding')
    current_head = subprocess.check_output(['git', '-C', str(REPO), 'rev-parse', 'HEAD'], text=True).strip()
    ancestor = subprocess.run(['git', '-C', str(REPO), 'merge-base', '--is-ancestor', EXPECTED_HEAD, 'HEAD'], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    require(ancestor.returncode == 0, 'selected source commit is not an ancestor of current HEAD')
    expected_objects = {name: subprocess.check_output(['git','-C',str(REPO),'rev-parse',f'HEAD:{name}'],text=True).strip() for name in ('src','sql','pyproject.toml')}
    require(manifest['objects'] == expected_objects, 'current HEAD selected source objects differ from packet')
    dirty = subprocess.check_output(['git','-C',str(REPO),'status','--porcelain','--untracked-files=all','--','pyproject.toml','sql','src'],text=True)
    require(dirty == '', 'tracked or untracked source-input drift in pyproject.toml/sql/src')
    archive = PRIVATE/'source-inputs-e1001bd.tar'
    require(sha(archive) == manifest['archive_sha256'] and archive.stat().st_size == manifest['archive_bytes'], 'source archive digest/size')
    extracted = PRIVATE/'source-inputs-e1001bd'
    expected = {row['path']:row for row in manifest['files']}
    actual = {p.relative_to(extracted).as_posix() for p in extracted.rglob('*') if p.is_file()}
    require(actual == set(expected), 'source archive extracted file inventory')
    for rel, row in expected.items():
        p = extracted/rel
        require(not p.is_symlink() and p.stat().st_size == row['bytes'] and sha(p) == row['sha256'] and blob_sha(p) == row['git_blob'], f'source file pin: {rel}')
    require(len(expected) == 588 and sum(x['bytes'] for x in expected.values()) == prep['source_binding']['source_content_bytes'], 'bounded complete source inventory')
    require(bridge['ashlar_commit'] == EXPECTED_HEAD and bridge['runtime_input_objects'] == manifest['objects'], 'bridge source rebinding')
    require(bridge['bridge_sha256'] == prep['source_binding']['bridge_payload_sha256'] and bridge['role_semantics_sha256'] == prep['source_binding']['bridge_role_semantics_sha256'], 'bridge payload/semantic pins')
    require(sha(HERE/'bridge-readiness.json') == prep['source_binding']['bridge_readiness_sha256'], 'bridge pin file hash')
    require(sha(HERE/'source-inputs-manifest.json') == prep['immutable_inputs']['current_source_manifest']['sha256'], 'manifest file hash')
    for name, private_path in (('campaign_worker_r10.py', private_worker), ('run_supervisor_r10.py', private_supervisor)):
        public = HERE/name
        require(public.is_file() and private_path.is_file() and sha(public) == sha(private_path), f'{name} evidence/private match')
        key = 'worker' if name.startswith('campaign') else 'supervisor'
        require(sha(public) == prep[key]['sha256'], f'{name} preparation hash')
    for key, row in prep['immutable_inputs'].items():
        p = Path(row['path'])
        require(p.is_file() and not p.is_symlink() and sha(p) == row['sha256'], f'immutable input {key}')
    attempt = Path(prep['attempt_directory'])
    require(attempt == PRIVATE/f"attempt-{prep['attempt']}" and attempt.is_dir() and not attempt.is_symlink() and attempt.stat().st_mode & 0o077 == 0 and not any(attempt.iterdir()), 'unique empty attempt root')
    result = Path(prep['result_receipt_path'])
    require(not result.exists() and not result.is_symlink(), 'preflight result does not exist')
    require(prep['preflight']['status'] == 'not_run' and prep['preflight']['result_receipt_created'] is False, 'preflight not run')
    require(prep['execution_authorization']['authorized'] is False and prep['execution_authorization']['approval_file_created'] is False and prep['execution_authorization']['started_marker_created'] is False, 'no execution authorization')
    require(prep['prior_packet_policy']['r8_and_r9_approval_applies_to_r10'] is False, 'old approval does not transfer')
    require(prep['runtime_provenance']['receipt_sha256'] == EXPECTED_RUNTIME_SHA and sha(Path(prep['runtime_provenance']['receipt_path'])) == EXPECTED_RUNTIME_SHA, 'reused runtime receipt hash')
    versions = prep['runtime_provenance']['runtime_tuple']
    require(versions == {'python':'3.12.14','platform':'macOS arm64','truss-toolkit':'0.0.49','umf-core':'0.10.1','umf_access':'0.1.0.dev7','weft-sql':'0.4.53','pyspark':'4.2.0','delta-spark':'4.4.1'}, 'runtime version tuple')
    wtext = worker.read_text(); stext = supervisor.read_text()
    require("if sys.argv[1:]!=['--preflight']" in stext and '--execute' not in wtext+stext and 'def execute(' not in wtext, 'preflight-only controller boundary')
    require('approval_payload' not in wtext and 'expected_approval' not in stext and 'source_holder_interval_proposal' not in wtext+stext, 'no approval handler or proposal-module import')
    require(wtext.count('PIP_NO_CACHE_DIR') == 1 and "'PIP_NO_CACHE_DIR':'1'" in wtext and 'PIP_NO_CACHE_DIR' not in stext, 'targeted pip-check cache environment only')
    old_worker=Path(prep['immutable_inputs']['r9_worker']['path']).read_text()
    require('PIP_NO_CACHE_DIR' not in old_worker and prep['pip_check_fix']['r9_result_sha256']==prep['immutable_inputs']['r9_failed_preflight_result']['sha256'], 'R9 diagnosis and exact delta binding')
    prior=json.loads(Path(prep['immutable_inputs']['r9_failed_preflight_result']['path']).read_bytes())
    require(prior.get('outcome')=='failed' and prior.get('phase')=='worker_preflight' and prior.get('admission_gates') is None and prior.get('native_actions')=={'migration_invoked':False,'postgres_started':False,'source_feed_publication_or_ack_mutated':False,'spark_started':False}, 'preserved R9 failed receipt scope')
    require(prior.get('attempt') != prep.get('attempt'), 'fresh R10 attempt distinct from failed R9 attempt')
    oldroot=Path('/private/tmp/ashlar-truss049-current-cohort-e1001bd-r9')
    require(archive.stat().st_ino == (oldroot/'source-inputs-e1001bd.tar').stat().st_ino, 'source archive reused by hard link')
    require(all((extracted/rel).stat().st_ino == (oldroot/'source-inputs-e1001bd'/rel).stat().st_ino for rel in expected), 'all selected source inputs reused by hard link')
    require("'pg_started':False" in wtext and "'spark_started':False" in wtext and "'migration_apply_invoked':False" in wtext and "'source_feed_publication_or_ack_mutated':False" in stext, 'false native action receipts')
    require(not unresolved_function_globals(wtext, worker), f'worker unresolved globals: {unresolved_function_globals(wtext,worker)}')
    require(not unresolved_function_globals(stext, supervisor), f'supervisor unresolved globals: {unresolved_function_globals(stext,supervisor)}')
    require((PRIVATE/'logs').is_dir() and (PRIVATE/'logs').stat().st_mode & 0o077 == 0, 'private logs root')
    total = sum(p.stat().st_size for p in PRIVATE.rglob('*') if p.is_file() and not p.is_symlink())
    require(total < 1024**3, 'private root under 1 GiB')
    print(f'R10 packet static bindings valid; selected_source={EXPECTED_HEAD}; current_HEAD={current_head}; source_files={len(expected)}; root_bytes={total}; pip_cache_delta=PIP_NO_CACHE_DIR=1; preflight=not_run; native_actions=false')
if __name__ == '__main__': main()
