from __future__ import annotations
import ast, hashlib, json, subprocess
from pathlib import Path
HERE=Path(__file__).resolve().parent
ROOT=Path('/private/tmp/ashlar-truss049-current-cohort-e1001bd-r12')
REPO=Path('/Users/erik/Projects/ashlar')
R11=Path('/private/tmp/ashlar-truss049-current-cohort-e1001bd-r11/r11-a98e3ac2-25c1-47e0-9744-0c3b21df4b3a-preflight-result.json')
COMMIT='e1001bd9e95c0fa555536b2c94c892b2b5b94cdb'
OBJECTS={'src':'0b1a7f94bede1dc2993f31d74cfff41ec8fbd236','sql':'3cfe250fadd42ef4b25597f0eec7d10a426ce8ff','pyproject.toml':'ba55a552124b69cfc927cefc5d309af9c76e6ded'}
RUNTIME_SHA='517ac6892280e911060688afc75528907e62aa5d6d290abf6d9b166f5da5617a'
R11_SHA='0dc7db291b35cdf76bba95cdf1a8b65d06a90fcc76a50c833ea879e1b0fd4d36'
def sha(p):return hashlib.sha256(Path(p).read_bytes()).hexdigest()
def req(x,m):
 if not x: raise SystemExit(m)
def blob(p):
 b=Path(p).read_bytes();return hashlib.sha1(f'blob {len(b)}\0'.encode()+b).hexdigest()
def main():
 req(ROOT.is_dir() and not ROOT.is_symlink() and ROOT.stat().st_mode&0o077==0,'private root mode')
 p=json.loads((HERE/'preparation.json').read_bytes());manifest=json.loads((HERE/'source-inputs-manifest.json').read_bytes());bridge=json.loads((HERE/'bridge-readiness.json').read_bytes())
 req(p['profile']=='ashlar-truss-0.0.49-r12-lifecycle-preparation/0.1','R12 profile')
 req(p['attempt'].startswith('r12-') and p['execution_authorization']=={'authorized':False,'approval_file_created':False,'started_marker_created':False,'required':p['execution_authorization']['required']},'no transferred execution authorization')
 req(p['execution']['native_started'] is False and p['execution']['pg_started'] is False and p['execution']['spark_started'] is False and p['execution']['migration_apply_invoked'] is False and p['execution']['acknowledgment_started'] is False,'no R12 lifecycle actions')
 req(not (ROOT/'execution-approval.json').exists() and not (ROOT/('execution-started-'+p['attempt']+'.json')).exists(),'approval and start marker absent')
 attempt=Path(p['future_execution_paths']['attempt_directory'])
 req(attempt==ROOT/f"attempt-{p['attempt']}" and attempt.is_dir() and attempt.stat().st_mode&0o077==0 and not any(attempt.iterdir()),'fresh empty single-use attempt')
 req(not Path(p['future_preflight']['status']=='not_run' and p['candidate_artifacts']['offline_preflight']['path']).exists(),'R12 preflight result not present')
 req(manifest['commit']==COMMIT and p['candidate_artifacts']['source_snapshot']['commit']==COMMIT and manifest['tree']==p['candidate_artifacts']['source_snapshot']['tree'],'current selected-source binding')
 req(manifest['objects']==OBJECTS and bridge['runtime_input_objects']==OBJECTS and bridge['ashlar_commit']==COMMIT,'source object tuple and bridge binding')
 req(sha(ROOT/'source-inputs-e1001bd.tar')==manifest['archive_sha256']==p['candidate_artifacts']['source_snapshot']['archive_sha256'],'source archive sha')
 rows={r['path']:r for r in manifest['files']};src=ROOT/'source-inputs-e1001bd'
 actual={f.relative_to(src).as_posix() for f in src.rglob('*') if f.is_file()}
 req(len(rows)==588 and actual==set(rows),'588 source file inventory')
 for name,row in rows.items():req(not (src/name).is_symlink() and (src/name).stat().st_size==row['bytes'] and sha(src/name)==row['sha256'] and blob(src/name)==row['git_blob'],'source row '+name)
 req(sha(ROOT/'runtime-receipt.json')==RUNTIME_SHA and p['runtime_receipt']['sha256']==RUNTIME_SHA,'current runtime receipt')
 pgctl=Path('/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/runtime/venv/lib/python3.12/site-packages/pgserver/pginstall/bin/pg_ctl')
 pg=p['candidate_artifacts']['postgres_cleanup_binary']
 req(pg['path']==str(pgctl) and pgctl.is_file() and pgctl.stat().st_size==pg['bytes'] and sha(pgctl)==pg['sha256'],'actual reused R7 pg_ctl path and hash')
 req(sha(R11)==R11_SHA and sha(ROOT/'r11-preflight-result.json')==R11_SHA,'R11 result bound by exact hash')
 r11=json.loads(R11.read_bytes()); req(r11['outcome']=='passed' and r11['attempt']=='r11-a98e3ac2-25c1-47e0-9744-0c3b21df4b3a','R11 pass identity')
 req(r11['native_actions']=={'migration_invoked':False,'postgres_started':False,'source_feed_publication_or_ack_mutated':False,'spark_started':False},'R11 no native effects')
 w=HERE/'campaign_worker_r12.py';s=HERE/'run_supervisor_r12.py';wt=w.read_text();st=s.read_text()
 req(sha(w)==p['worker']['sha256']==sha(ROOT/'campaign_worker_r12.py') and sha(s)==p['supervisor']['sha256']==sha(ROOT/'run_supervisor_r12.py'),'controller hashes')
 req('PIP_NO_CACHE_DIR' in wt and "'PIP_NO_CACHE_DIR':'1'" in wt,'targeted pip cache fix')
 req("APPROVAL=ROOT/'execution-approval.json'" in st and 'if approval!=expected' in st and "mode.add_argument('--execute'" in st and 'Fresh exact root/Sol hash-bound execution approval required' in st,'execute requires exact approval file')
 req("mode.add_argument('--preflight'" in st and 'def execute()' in wt,'reviewed one-shot lifecycle path')
 req('backup_before_start()' in wt and 'pre-migration-source-and-checkpoint.json' in wt and 'run_migration(conninfo)' in wt,'cold backup and pre-migration custody checks retained')
 req('VENV=Path(\'/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/runtime/venv\')' in wt and 'VENV=Path(\'/private/tmp/ashlar-truss049-current-cohort-1f8c3fc2-r7/runtime/venv\')' in st,'selected runtime path reuse')
 ast.parse(wt);ast.parse(st)
 req(wt.index("pre_source=run_048_helper(conninfo,'--custody')") < wt.index('migration=run_migration(conninfo)'),'source custody reobserve precedes migration')
 req(wt.index("pre_page=run_048_helper(conninfo,'--read-page',str(GENESIS))") < wt.index('migration=run_migration(conninfo)'),'checkpoint reobserve precedes migration')
 req("write_new(ATTEMPT/'postgres-started.json'" in wt and "write_new(ATTEMPT/'migration-apply-started.json'" in wt and 'backup_before_start()' in wt,'ordered lifecycle stop markers retained')
 req('R7/R8/R9/R10/R11 execution approval' in json.dumps(p) and p['approval_payload_proposal_status']=='proposal_only_not_owner_authorization','prior approvals explicitly excluded')
 # Proposed approval payload binds the exact R12 code/config hashes and R11 gate digest; no approval file is materialized.
 proposal=json.loads((HERE/'approval-payload-proposal.json').read_bytes());payload=proposal['payload']
 req(proposal['status']=='proposal_only_not_owner_authorization' and proposal['attempt']==p['attempt'],'approval proposal identity/status')
 for key,actual in [('worker_sha256',sha(w)),('supervisor_sha256',sha(s)),('preparation_sha256',sha(HERE/'preparation.json')),('runtime_receipt_sha256',RUNTIME_SHA),('seed_request_sha256',sha(ROOT/'seed-request.json')),('bridge_pin_sha256',sha(ROOT/'bridge-readiness.json'))]:req(payload[key]==actual,'approval payload '+key)
 g=json.loads(R11.read_bytes())['admission_gates'];stable={'process_inventory':{'status':g['process_inventory']['status'],'supervisor_pid_present':g['process_inventory']['supervisor_pid_present']},'sysv_shared_memory':{k:g['sysv_shared_memory'].get(k) for k in ('status','segment_created','attached','detached','ipc_rmid_confirmed')}}
 gate=hashlib.sha256(json.dumps(stable,sort_keys=True,separators=(',',':')).encode()).hexdigest()
 req(payload['admission_gates_sha256']==gate,'approval proposal gate digest')
 req(proposal['payload_sha256']==hashlib.sha256(json.dumps(payload,sort_keys=True,separators=(',',':')).encode()).hexdigest(),'approval proposal payload hash')
 wt_ast=ast.parse(wt);st_ast=ast.parse(st)
 def assigned(tree,name):
  values=[n.value for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets)]
  req(len(values)==1,'exact controller assignment '+name);return values[0]
 def string_constants(node):return [n.value for n in ast.walk(node) if isinstance(n,ast.Constant) and isinstance(n.value,str)]
 req('attempt-'+p['attempt'] in string_constants(assigned(wt_ast,'ATTEMPT')),'worker attempt identity')
 req('attempt-'+p['attempt'] in string_constants(assigned(st_ast,'ATTEMPT')),'supervisor attempt identity')
 req('execution-approval.json' in string_constants(assigned(st_ast,'APPROVAL')),'exact approval path')
 execute_body=wt[wt.index('def execute():'):wt.index('def main():')]
 req(execute_body.index('backup_before_start()')<execute_body.index('runtime=LocalPostgres(DATA)'),'cold backup before owned PostgreSQL start')
 req(execute_body.index('pre=assert_bridge_and_approval()')<execute_body.index('backup_before_start()'),'exact approval before cold backup/effects')
 source_ancestor=subprocess.run(['git','-C',str(REPO),'merge-base','--is-ancestor',COMMIT,'HEAD'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 req(source_ancestor.returncode==0,'selected source commit ancestry')
 current={n:subprocess.check_output(['git','-C',str(REPO),'rev-parse',f'HEAD:{n}'],text=True).strip() for n in OBJECTS}
 req(current==OBJECTS,'current source objects')
 dirty=subprocess.check_output(['git','-C',str(REPO),'status','--porcelain','--untracked-files=all','--','src','sql','pyproject.toml'],text=True)
 req(dirty=='','source-input worktree clean')
 total=sum(f.stat().st_size for f in ROOT.rglob('*') if f.is_file() and not f.is_symlink())
 req(total<1024**3,'private packet root within 1 GiB')
 print(f'R12 packet static bindings valid; selected_source={COMMIT}; source_files={len(rows)}; private_root_bytes={total}; R11_preflight=passed; execution_approval=absent; native_actions=false')
if __name__=='__main__':main()
