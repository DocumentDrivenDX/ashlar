from __future__ import annotations
import ast, builtins, hashlib, json, symtable
from pathlib import Path
HERE=Path(__file__).resolve().parent
EXPECTED_PREP_SHA='16307b4147b41d33b0aea0c48d660b06f4f1289cae879c1117754ddab3911189'
EXPECTED_INPUT_KEYS={'r7_preparation','r7_runtime_receipt','r7_source_archive','r7_source_manifest','r7_bridge','r7_seed_request','r7_readonly_helper','r7_weft_compatibility_receipt','r7_runtime_wheel_lock','r7_worker_history','r7_supervisor_history'}
PRIVATE=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r8')
def sha(path):return hashlib.sha256(Path(path).read_bytes()).hexdigest()
def require(ok,msg):
    if not ok:raise SystemExit(msg)
def module_names(tree):
    names=set()
    for node in tree.body:
        if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):names.add(node.name)
        elif isinstance(node,(ast.Import,ast.ImportFrom)):
            for item in node.names:names.add(item.asname or item.name.split('.')[0])
        elif isinstance(node,(ast.Assign,ast.AnnAssign,ast.AugAssign)):
            targets=node.targets if isinstance(node,ast.Assign) else [node.target]
            for target in targets:
                for child in ast.walk(target):
                    if isinstance(child,ast.Name):names.add(child.id)
    return names
def unresolved_function_globals(source,path):
    tree=ast.parse(source,filename=str(path));root=symtable.symtable(source,str(path),'exec');globals_=module_names(tree);missing=[]
    def walk(table):
        if table.get_type()=='function':
            unresolved=sorted({symbol.get_name() for symbol in table.get_symbols()
                if symbol.is_global() and symbol.get_name() not in globals_ and symbol.get_name()!='__file__' and not hasattr(builtins,symbol.get_name())})
            if unresolved:missing.append((table.get_name(),unresolved))
        for child in table.get_children():walk(child)
    walk(root)
    return missing
def main():
    prep=json.loads((HERE/'r8-preparation.json').read_bytes())
    require(prep.get('profile')=='ashlar-truss-0.0.49-r8-preflight-addendum/0.3','R8 preparation profile')
    if EXPECTED_PREP_SHA:require(sha(HERE/'r8-preparation.json')==EXPECTED_PREP_SHA,'reviewed R8 preparation digest')
    require(set(prep.get('immutable_inputs',{}))==EXPECTED_INPUT_KEYS,'closed immutable input inventory')
    require(PRIVATE.is_dir() and not PRIVATE.is_symlink() and PRIVATE.stat().st_mode&0o077==0,'private R8 root mode')
    attempt=PRIVATE/'attempt-r8-revision3'
    for name in ('attempt-r8','attempt-r8-corrected','attempt-r8-revision2'):
        prior=PRIVATE/name
        require(prior.is_dir() and not prior.is_symlink() and not any(prior.iterdir()),f'preserved empty {name}')
    require(attempt.is_dir() and not attempt.is_symlink() and attempt.stat().st_mode&0o077==0 and not any(attempt.iterdir()),'fresh empty R8 revision3 attempt')
    tracked={'campaign_worker_r8.py':HERE/'campaign_worker_r8.py','run_supervisor_r8.py':HERE/'run_supervisor_r8.py','r8-preparation.json':HERE/'r8-preparation.json'}
    for name,path in tracked.items():
        require(path.is_file() and not path.is_symlink(),f'tracked {name}')
        private_path=PRIVATE/name
        require(private_path.is_file() and not private_path.is_symlink() and sha(path)==sha(private_path),f'private copy {name}')
    require(prep['worker']['sha256']==sha(tracked['campaign_worker_r8.py']),'worker binding')
    require(prep['supervisor']['sha256']==sha(tracked['run_supervisor_r8.py']),'supervisor binding')
    require(prep['attempt_directory']==str(attempt),'attempt path binding')
    result_path=PRIVATE/'r8-revision3-preflight-result.json';fallback=PRIVATE/'r8-revision3-result-write-failure.json'
    require(prep['result_receipt_path']==str(result_path) and result_path.is_file() and not fallback.exists(),'revision3 durable result paths')
    result_evidence=HERE/'r8-revision3-preflight-result-20261024.json';attempt_evidence=json.loads((HERE/'r8-revision3-preflight-attempt-20261024.json').read_bytes())
    require(sha(result_path)==sha(result_evidence) and attempt_evidence.get('durable_result_sha256')==sha(result_path),'revision3 durable result evidence matches private receipt')
    result=json.loads(result_path.read_bytes())
    require(result.get('outcome')=='passed' and result.get('worker_sha256')==prep['worker']['sha256'] and result.get('supervisor_sha256')==prep['supervisor']['sha256'] and result.get('preparation_sha256')==sha(HERE/'r8-preparation.json'),'revision3 result controller bindings')
    prior_result=PRIVATE/'r8-revision2-preflight-result.json';prior_evidence=HERE/'r8-revision2-preflight-attempt-20261024.json'
    require(prior_result.is_file() and sha(prior_result)==json.loads(prior_evidence.read_bytes())['durable_result_receipt_sha256'],'revision2 result preserved byte-for-byte')
    for row in prep['immutable_inputs'].values():
        path=Path(row['path']);require(path.is_file() and not path.is_symlink() and sha(path)==row['sha256'],f'R7 immutable input {path.name}')
    manifest=json.loads((HERE/'artifacts.sha256.json').read_bytes())
    require(manifest.get('profile')=='ashlar-truss049-r8-preflight-addendum-manifest/0.1','artifact manifest profile')
    expected_files={name:sha(path) for name,path in tracked.items()}
    for name in ('README.md','check_r8_packet.py','r8-preflight-attempt-20261024.json','r8-corrected-preflight-attempt-20261024.json','r8-revision2-preflight-attempt-20261024.json','r8-revision3-preflight-attempt-20261024.json','r8-revision3-preflight-result-20261024.json'):
        expected_files[name]=sha(HERE/name)
    require(manifest.get('files')==expected_files,'artifact manifest hashes')
    worker=tracked['campaign_worker_r8.py'].read_text();supervisor=tracked['run_supervisor_r8.py'].read_text()
    require(not unresolved_function_globals(worker,tracked['campaign_worker_r8.py']),'worker contains unresolved function globals: '+repr(unresolved_function_globals(worker,tracked['campaign_worker_r8.py'])))
    require(not unresolved_function_globals(supervisor,tracked['run_supervisor_r8.py']),'supervisor contains unresolved function globals: '+repr(unresolved_function_globals(supervisor,tracked['run_supervisor_r8.py'])))
    require('def run_048_helper(conninfo,*args):' in worker and "helper=HELPER" in worker and "str(OLD48_VENV/'bin/python')" in worker,'pinned read-only .48 helper wrapper restored')
    require('def execute(' not in worker and '--execute' not in worker and 'approval_payload' not in worker,'worker is preflight only')
    require('--execute' not in supervisor and 'expected_approval' not in supervisor and 'approval_contract_parity' not in supervisor,'supervisor is preflight only')
    require('attempt-r7' not in worker+supervisor and 'attempt-r8-revision3' in worker+supervisor,'R7 attempt is not referenced; revision3 attempt is distinct')
    require("RUNTIME_SHA='517ac6892280e911060688afc75528907e62aa5d6d290abf6d9b166f5da5617a'" in worker,'R7 runtime receipt digest embedded exactly')
    require("'rechecked_during_this_preflight':False" in worker,'prior RECORD receipt reused without payload rehash')
    require("MAX_RESULT_BYTES=1024*1024" in supervisor and 'O_EXCL' in supervisor and 'os.fsync' in supervisor and 'r8-revision3-preflight-result.json' in supervisor,'bounded durable single-use receipt')
    require('r8-revision3-result-write-failure.json' in supervisor and 'write_failure_fallback' in supervisor,'compact fallback receipt')
    require("if value is None:return {'status':'unavailable','worker_result_sha256':None}" in supervisor and "'status':'malformed'" in supervisor,'worker summary tolerates absent or malformed worker results')
    require("version=subprocess.run([str(PG_CTL),'--version']" not in supervisor,'no extra pg_ctl subprocess')
    require('sys.prefix' in worker and 'sys.base_prefix' in worker and 'pip_result.returncode!=0' in worker,'live runtime guards')
    total=sum(p.stat().st_size for p in PRIVATE.rglob('*') if p.is_file() and not p.is_symlink())
    require(total<1024**3,'R8 retained root size')
    print(f'R8 revision3 bindings/function-global audit valid; durable preflight result preserved; root_bytes={total}; native actions false')
if __name__=='__main__':main()
