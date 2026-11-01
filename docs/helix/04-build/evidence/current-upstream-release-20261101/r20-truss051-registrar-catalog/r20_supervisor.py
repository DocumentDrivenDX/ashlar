#!/usr/bin/env python3
"""R20 adapter to the reviewed bounded PostgreSQL/RSS/storage supervisor."""
from __future__ import annotations
import hashlib,importlib.util,json,os,sys,types
from pathlib import Path

HERE=Path(__file__).resolve().parent
R15=HERE.parent/'r15-truss051-view-transaction'
R19=HERE.parent/'r19-truss051-delta-publication'
sys.path.insert(0,str(R15))
R15_SUPERVISOR=R15/'r15_truss051_supervisor.py'
R15_SOURCE_GATE='/private/tmp/ashlar-r13-truss051-current-cohort-20261101/resume-clone-pgdata'
R20_SOURCE_GATE='scope.get("r16b_source", {}).get("path")'
raw_supervisor=R15_SUPERVISOR.read_bytes()
old_gate=json.dumps(R15_SOURCE_GATE).encode()
if raw_supervisor.count(old_gate)!=1: raise RuntimeError('R15 supervisor source gate drifted')
derived_supervisor=raw_supervisor.replace(old_gate,R20_SOURCE_GATE.encode(),1)
base=types.ModuleType('r20_r15_bounded_supervisor')
base.__file__=str(R15_SUPERVISOR)
exec(compile(derived_supervisor,str(R15_SUPERVISOR),'exec'),base.__dict__)
spec=importlib.util.spec_from_file_location('r19_release_worker_contract',R19/'r19_truss051_local_worker.py')
if spec is None or spec.loader is None: raise RuntimeError('reviewed release-cohort validator unavailable')
r15_worker=importlib.util.module_from_spec(spec);spec.loader.exec_module(r15_worker)
SCOPE_PATH=HERE/'r20-publication-scope.json';WORKER=HERE/'r20_delta_publication_worker.py'
base.SCOPE_PATH=SCOPE_PATH;base.WORKER=WORKER;base.EXPECTED_PATHS=json.loads(SCOPE_PATH.read_text())['paths']

def standing(scope,scope_raw,*,receipt=None,worker_raw=None,supervisor_raw=None,require_dispatch=True):
    receipt=receipt if receipt is not None else json.loads((HERE/scope['authorization_receipt']).read_text())
    worker_raw=worker_raw if worker_raw is not None else WORKER.read_bytes()
    supervisor_raw=supervisor_raw if supervisor_raw is not None else Path(__file__).read_bytes()
    expected={'schema':'ashlar.standing-local-dispatch-binding/1','authorization_basis':'standing_user_local_release_authorization',
      'user_authorization_statement':'User authorized fresh Truss source creation, a local seed, and a tested release; bounded local R20 Truss-to-Delta publication with source ACK deferred is within that standing authorization and requires no per-attempt user approval.',
      'scope_sha256':hashlib.sha256(scope_raw).hexdigest(),'worker_sha256':hashlib.sha256(worker_raw).hexdigest(),
      'supervisor_sha256':hashlib.sha256(supervisor_raw).hexdigest(),'attempts':1,'cloud':False}
    if any(receipt.get(k)!=v for k,v in expected.items()) or set(receipt)!=set(expected)|{'sol_review_status','dispatch_enabled'}:
        raise base.StopSupervisor('R20 standing authorization hash binding mismatch')
    if require_dispatch:
        if receipt['sol_review_status']!='sol_reviewed' or receipt['dispatch_enabled'] is not True:
            raise base.StopSupervisor('R20 dispatch remains disabled pending Sol review')
    elif receipt['sol_review_status'] not in ('pending_sol_review','sol_reviewed') or type(receipt['dispatch_enabled']) is not bool:
        raise base.StopSupervisor('R20 effect-free authorization status malformed')

def release_scope(scope):
    if (scope.get('profile')!='ashlar-r20-truss051-registrar-catalog-delta-publication-deferred-ack/0.1'
        or scope.get('status')!='standing_local_native_authorization'
        or scope.get('authorization')!='standing_user_local_release_authorization'
        or scope.get('paths')!=base.EXPECTED_PATHS or scope.get('cloud') is not False
        or scope.get('acknowledgement') is not False or scope.get('production_authority') is not False):
        raise base.StopSupervisor('R20 exact local authorization and path scope required')
    dependency_files={
      'r15_truss051_supervisor.py':R15/'r15_truss051_supervisor.py',
      'r15_supervisor_custody.py':R15/'r15_supervisor_custody.py',
      'r15-runtime-proof.json':R19/'r15-runtime-proof.json',
      'r19_truss051_local_worker.py':R19/'r19_truss051_local_worker.py'}
    if set(scope.get('r19_helper_dependencies',{}))!=set(dependency_files):
        raise base.StopSupervisor('R20 helper dependency inventory mismatch')
    for name,path in dependency_files.items():
        if hashlib.sha256(path.read_bytes()).hexdigest()!=scope['r19_helper_dependencies'][name]:
            raise base.StopSupervisor(f'R20 immutable helper dependency changed: {name}')
    validator=scope.get('r19_validator',{})
    if validator.get('path')!='r19_truss051_local_worker.py' or hashlib.sha256((R19/validator['path']).read_bytes()).hexdigest()!=validator.get('sha256'):
        raise base.StopSupervisor('R20 versioned release-cohort validator binding mismatch')
    tree=__import__('subprocess').run(['git','-C',str(HERE.parents[5]),'rev-parse',validator['reviewed_fix_commit']+'^{tree}'],capture_output=True,text=True,check=True,timeout=10).stdout.strip()
    ancestry=__import__('subprocess').run(['git','-C',str(HERE.parents[5]),'merge-base','--is-ancestor',validator['reviewed_fix_commit'],'HEAD'],capture_output=True,timeout=10)
    if tree!=validator.get('reviewed_fix_tree') or ancestry.returncode!=0:
        raise base.StopSupervisor('R20 resolver-fix provenance mismatch')
    r15_worker.validate_release_cohort(scope['release_cohort'],scope['runtime'])
    expected=scope['r16b_source']
    adapter=scope.get('r20_supervisor_source_adapter',{})
    base_raw=R15_SUPERVISOR.read_bytes()
    old_raw=json.dumps(R15_SOURCE_GATE).encode();new_raw=R20_SOURCE_GATE.encode()
    if (adapter.get('base_helper')!='../r15-truss051-view-transaction/r15_truss051_supervisor.py'
        or adapter.get('base_sha256')!=hashlib.sha256(base_raw).hexdigest()
        or adapter.get('source_gate_literal_before')!=R15_SOURCE_GATE
        or adapter.get('source_gate_expression_after')!=R20_SOURCE_GATE
        or adapter.get('transformation')!='one exact byte substitution of the R15 hard-coded source path gate to the R20 scope r16b_source.path; all other supervisor source bytes unchanged'
        or base_raw.count(old_raw)!=1
        or adapter.get('derived_module_sha256')!=hashlib.sha256(base_raw.replace(old_raw,new_raw,1)).hexdigest()):
        raise base.StopSupervisor('R20 derived supervisor source-gate binding mismatch')
    if (scope['source'].get('existing_pgdata')!=expected.get('path')
        or scope['source'].get('pgdata_bytes_at_packet_review')!=expected.get('bytes')
        or scope['source'].get('pgdata_files_at_packet_review')!=expected.get('files')
        or scope['source'].get('pgdata_tree_sha256_at_packet_review')!=expected.get('tree_sha256')):
        raise base.StopSupervisor('R20 supervisor custody fields must bind the selected R16b backup')
    if (scope['source']['catalog_accepted']['document_id']!='ashlar-local-dev-typed-graph-r6-20261031'
        or scope['source']['catalog_accepted']['document_revision']!='1'
        or scope['source']['catalog_accepted']['document_sha256']!='2c9b295a8232cae4770255650bf634941ab223e582b79ae2bb78f6acf0b8a880'
        or expected['consumer_id']!='ashlar-local-dev-typed-graph-consumer-r6-20261031'
        or expected['path']!='/private/tmp/ashlar-r19-truss051-delta-publication-20261101/r16b-stopped-source-backup-pgdata'
        or scope['publication']['defer_source_acknowledgement'] is not True):
        raise base.StopSupervisor('R20 exact R16b stopped source and deferred-ACK scope required')
    r15_worker.validate_release_cohort(scope['release_cohort'],scope['runtime'])

base.validate_standing_authorization=standing
base.validate_release_scope=release_scope

if __name__=='__main__':
    # Keep supervisor-owned child logs private; the worker verifies this
    # supervisor-created directory before it touches the source clone.
    os.umask(0o077)
    raise SystemExit(base.main())
