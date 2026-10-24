#!/usr/bin/env python3
"""R13 bounded, preflight-only worker. It has no native execution mode."""
from __future__ import annotations
import argparse, hashlib, importlib.metadata as md, json, os, pathlib, subprocess, sys

HERE = pathlib.Path(__file__).resolve().parent
PREP = json.loads((HERE / 'preparation.json').read_bytes())
ROOT = pathlib.Path('/private/tmp/ashlar-truss050-current-cohort-9c745278-r13')
SOURCE = ROOT / 'source-inputs-9c745278'
VENV = ROOT / 'runtime/venv'
def sha(path): return hashlib.sha256(pathlib.Path(path).read_bytes()).hexdigest()
def need(ok, message):
    if not ok: raise RuntimeError(message)

def main():
    parser=argparse.ArgumentParser()
    parser.add_argument('--preflight', action='store_true', required=True)
    args=parser.parse_args()
    selected=PREP['selected_source']; source_commit=selected['commit']
    source_manifest=json.loads((HERE/'source-inputs-manifest.json').read_bytes())
    need(pathlib.Path(sys.prefix).resolve()==VENV.resolve(), 'wrong isolated runtime prefix')
    need(md.version('truss-toolkit')=='0.0.50', 'installed Truss is not official .50')
    need(sha(HERE/'runtime-receipt.json')==PREP['runtime']['receipt_sha256'], 'runtime receipt hash mismatch')
    need(sha(HERE/'bridge-readiness.json')==PREP['bridge_sha256'], 'bridge receipt hash mismatch')
    need(sha(HERE/'source-inputs-manifest.json')==PREP['selected_source']['manifest_sha256'], 'source manifest hash mismatch')
    need(sha(pathlib.Path(selected['archive_path']))==selected['archive_sha256'], 'source archive hash mismatch')
    need(SOURCE.is_dir(), 'selected extracted source missing')
    need(len(source_manifest['files'])==selected['file_count'], 'selected source inventory count mismatch')
    for row in source_manifest['files']:
        content=(SOURCE/row['path']).read_bytes()
        need(len(content)==row['bytes'] and hashlib.sha256(content).hexdigest()==row['sha256'], 'extracted source file mismatch: '+row['path'])
    # Bind selected source object tuple to the immutable Git object database, not this evidence commit.
    for name, oid in selected['objects'].items():
        observed=subprocess.check_output(['git','rev-parse',source_commit+':'+name],cwd=HERE.parents[5],text=True).strip()
        need(observed==oid, 'selected Git object mismatch: '+name)
    need(subprocess.check_output(['git','rev-parse',source_commit+'^{tree}'],cwd=HERE.parents[5],text=True).strip()==selected['tree'], 'selected Git tree mismatch')
    need(subprocess.run(['git','merge-base','--is-ancestor',source_commit,'HEAD'],cwd=HERE.parents[5],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0, 'selected source commit is not ancestor of packet branch')
    # Execute the retained .48 helper's import-only preflight; it opens no connection.
    old_runtime=pathlib.Path('/private/tmp/ashlar-truss048-weft052-umf054-runtime-20261022/venv')
    old_receipt=PREP['readonly_048_helper']['runtime_receipt_path']
    need(sha(old_receipt)==PREP['readonly_048_helper']['runtime_receipt_sha256'], '.48 runtime receipt changed')
    helper=subprocess.run([str(old_runtime/'bin/python3'),'-I','-B',str(HERE/'source_048_readonly_helper.py'),'--preflight'],capture_output=True,text=True,timeout=30,env={'PATH':str(old_runtime/'bin')+':/usr/bin:/bin','TMPDIR':'/private/tmp','PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1'})
    need(helper.returncode==0 and not helper.stderr.strip(), '.48 import-only helper preflight failed: '+helper.stderr)
    old=json.loads(helper.stdout)
    need(old.get('truss')=='0.0.48' and old.get('pg_started') is False and old.get('spark_started') is False, '.48 helper preflight result mismatch')
    # Pure UMF proposal path only; no Spark, DDL, filesystem table, or native connector.
    sys.path.insert(0,str(SOURCE/'src'))
    from ashlar_host.local_current_delta4_umf import LocalUmfDelta4Bridge, _pinned_resources
    manifest, _bundle, sources, _semantics = _pinned_resources()
    bridge=LocalUmfDelta4Bridge.open(); proposals=[]
    try:
        for role in ('object_current','edge_current','tombstone','whole_source_history','publication_attempt_phase','publication_manifest'):
            realized=bridge.realize(sources[role+'.umf.json'],'/r13-preflight/'+role,role)
            proposal=realized['proposal']
            proposals.append({'role':role,'schema_sha256':hashlib.sha256(proposal['schemaJson'].encode()).hexdigest(),'source_sha256':realized['source_bytes_sha256'],'sql_sha256':hashlib.sha256(proposal['sql'].encode()).hexdigest()})
    finally: bridge.close()
    # This static profile check is the admission gate: live A custody/checkpoint re-read remains deferred
    # to the existing .48 read-only execution gate, and cannot change this source-code profile allowlist.
    from ashlar_host.truss_current_catalog import installed_current_truss_profile
    try:
        installed_current_truss_profile()
        gate={'outcome':'passed_unexpectedly'}
    except ValueError as exc:
        gate={'outcome':'blocked','exception':type(exc).__name__,'message':str(exc),
              'source_literal':'CURRENT_TRUSS_049_VERSION = 0.0.49',
              'installed_version':md.version('truss-toolkit'),
              'installed_manifest_sha256':hashlib.sha256(__import__('importlib.resources').resources.files('truss').joinpath('_catalog_runtime/manifest.json').read_bytes()).hexdigest(),
              'live_A_gate':'deferred until separately authorized execution; live source cannot satisfy absent .50 source profile'}
    need(gate['outcome']=='blocked','expected exact-source .50 source admission blocker was absent')
    data=pathlib.Path(PREP['retained_pgdata']['path'])
    need((data/'PG_VERSION').read_text().strip()=='18','retained PGDATA version changed')
    need(not (data/'postmaster.pid').exists(),'retained PGDATA is no longer observed stopped')
    # Verify the A-scope immutable receipts; do not connect to source or inspect live state here.
    for key in ('source','origin','seed_request','existing_feed_registration_receipt','prior_public_source_reobservation_receipt'):
        item=PREP['immutable_inputs'][key]
        need(sha(item['path'])==item['sha256'],'immutable A input changed: '+key)
    result={'profile':'ashlar-truss050-r13-effect-free-preflight/0.1','status':'blocked_source_admission_profile_missing',
        'selected_source':{'commit':source_commit,'tree':selected['tree'],'objects':selected['objects'],'archive_sha256':selected['archive_sha256']},
        'runtime':{'truss_toolkit':md.version('truss-toolkit'),'prefix':str(VENV),'receipt_sha256':sha(HERE/'runtime-receipt.json')},
        'legacy_048_import_preflight':old,'bridge':{'profile':manifest['profile'],'sha256':manifest['bridge']['assetSha256'],'roles':proposals},
        'source_admission_gate':gate,'scope':'single retained A feed/consumer; B is out of scope',
        'lifecycle':'not executed; A custody and pending checkpoint are deferred to .48 read-only gate at separately authorized execution',
        'native_actions':{'postgres_started':False,'spark_started':False,'migration_invoked':False,'source_feed_publication_or_ack_mutated':False},
        'execution_authorization':{'authorized':False,'approval_or_start_marker_created':False},
        'retained_pgdata':{'path':str(data),'version':'18','postmaster_pid_absent':True,'modified':False}}
    print(json.dumps(result,sort_keys=True,separators=(',',':')))

if __name__=='__main__': main()
