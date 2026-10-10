import ast,base64,csv,hashlib,io,json,stat
from pathlib import Path
ROOT=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-e');P=Path('/private/tmp/astra-supplychain-compile-command-review-20261010-e')
def desc(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def check(d):
 assert desc(Path(d['path']))==d;assert Path(d['path']).is_file()
m=json.loads((ROOT/'manifest.json').read_bytes());assert desc(ROOT/'manifest.json')['sha256']=='46359bd4804f1a3e5e1ff3b6b998b057578d0ec6f11f714b2c20b8c671fac03f'
for d in m['files']:check(d)
c=json.loads((ROOT/'command.json').read_bytes());assert desc(ROOT/'command.json')['sha256']=='31008453efa145cbc7f42bf6363b2906a41ec7e180f07dd75af9304b4d129e08'
assert c['cwd']==str(ROOT) and c['environment']=={'PATH':'/usr/bin:/bin'}
assert c['argv'][1:]==['-I','-B',str(ROOT/'capture.py')]
assert c['cases']==['split-excursion','excursion','replay','lineage','sensor']
assert c['output']==str(ROOT/'candidate-output') and not Path(c['output']).exists()
assert len(c['resources'])==len({d['path'] for d in c['resources']})==125
for d in c['resources']:check(d)
paths={d['path'] for d in c['resources']};res=c['interpreter_resolution']
assert res['argv_path']==c['argv'][0] and str(Path(c['argv'][0]).resolve())==res['resolved_path']
assert {res['argv_path'],res['resolved_path']}<=paths
for case in c['cases']:
 a=ROOT/(case+'.request.json');b=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-b')/a.name
 assert a.read_bytes()==b.read_bytes()
# Exact reviewed loop/cleanup semantics retained; E adds only resolution guard to verify.
old=ast.parse(Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-c/capture.py').read_bytes());new=ast.parse((ROOT/'capture.py').read_bytes())
def others(t):return ast.dump(ast.Module(body=[n for n in t.body if not isinstance(n,ast.FunctionDef)],type_ignores=[]),include_attributes=False)
assert others(old)==others(new)
v=next(n for n in new.body if isinstance(n,ast.FunctionDef));exec(compile(ast.Module(body=[v],type_ignores=[]),'review-only-verify','exec'),env:={'Path':Path,'command':c,'hashlib':hashlib})
env['verify']()
# Confirm the additional resolution branch really refuses a changed selected target.
bad={**c,'interpreter_resolution':{**res,'resolved_path':'/not/the/selected/python'}};env['command']=bad
try:env['verify']()
except ValueError as e:assert str(e)=='candidate-runtime-drift'
else:raise AssertionError('resolution drift not refused')
env['command']=c
site=Path(c['installed_metadata_directory']).parent;metadata=Path(c['installed_metadata_directory'])
assert sorted(x.name for x in metadata.iterdir() if x.is_file())==sorted(c['installed_metadata_files'])
assert {'RECORD','METADATA','entry_points.txt'}<=set(c['installed_metadata_files'])
assert all(str(metadata/n) in paths for n in c['installed_metadata_files'])
record=[]
for row in csv.reader(io.StringIO((metadata/'RECORD').read_text())):
 assert len(row)==3
 path,digest,length=row
 if not digest:assert path.endswith('/RECORD') and not length;continue
 assert digest.startswith('sha256=');raw=(site/path).read_bytes()
 assert len(raw)==int(length) and base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode()==digest[7:]
 record.append(path)
custody=Path('/private/tmp/ashlar-paths-keys-native10-preparation-20261010-a/wheel/installed-custody.json');cust=json.loads(custody.read_bytes())
assert cust['sourceCommit']=='115cb2501d58b385c6550339d8df9d22a2502866'
for d in cust['sourceFiles']:check(d);assert d['path'] in paths
assert len(cust['sourceFiles'])==97
assert desc(Path(cust['record']['path']))['sha256']==cust['record']['sha256']
assert 'Version: 0.1.0.dev0\n' in (metadata/'METADATA').read_text()
installation=Path(c['installation']);ready=json.loads((installation/'ready.json').read_bytes())
assert ready['indexSha256']=='bd541aa8cb261376f160661d3df67065b36fcba4f6caf4c0f42e33d710c4b9dc'
assert desc(Path(c['index']))['sha256']==ready['indexSha256']
assert ready['realizationId']=='weft-3a2a79c-paths-keys-aarch64-apple-darwin-candidate'
for d in [ready['executable'],ready['provenance'],*ready['resources']]:
 p=installation/d['path'];actual=desc(p);assert actual['sha256']==d['sha256'] and actual['bytes']==d['bytes'];assert str(p) in paths
assert ready['executable']['sha256']=='471fc5dedc8f8eba3156444d19cc2161fac7a06833672dd5621ed49ead8b522c'
assert stat.S_IMODE((installation/'weft-paths-keys').stat().st_mode)==0o555
mechanics=site/'ashlar/_weft_installation_mechanics.py';source=mechanics.read_text();assert 'deadline=time.monotonic()+30' in source and "env={'PATH':'/usr/bin:/bin'}" in source and 'process.wait(timeout=2)' in source
assert 'PROTOCOL_LIMIT=16*1024*1024' in source and "else 4096" in source
for d in c['resources']:check(d)
assert not Path(c['output']).exists()
control=Path('/private/tmp/astra-supplychain-capture-controls-20261010-a.receipt.json')
out={'status':'approved-command-for-root-execution','command':desc(ROOT/'command.json'),'manifest':desc(ROOT/'manifest.json'),'capture':desc(ROOT/'capture.py'),'selected_resources_count':125,'selected_resource_bytes':sum(d['bytes'] for d in c['resources']),'opening_closing_current':True,'outputs_absent_at_review':True,'checks':{'five_requests_equal_independently_accepted_source_requests':True,'installed_original_source_files':97,'installed_record_hashed_entries':len(record),'metadata_files':len(c['installed_metadata_files']),'selected_interpreter_resolution_negative':True,'index_ready_backend_executable_six_schemas':True,'capture_loop_AST_equal_control_tested_C':True},'control_review':desc(control),'prior_installed_custody':desc(custody),'scope':['Root direct foreground env-i PATH-only invocation of exact argv/cwd, not outer forced-kill wrapper.','Five candidate compiler observations only; blocked responses remain unaccepted observations.','Installed public transport owns each compiler process group: 30-second exchange, 16MiB stdout, 4096-byte stderr, two-second reap; no shared whole-run wall deadline or abrupt-parent-death guarantee.','125 selected resources and underlying installed source/RECORD checked; stdlib and OS dynamic runtime remain trusted prerequisites, no hermetic closure.','Evidence files may remain partial after a failed write; nonzero terminal and later independent response review required.','No Spark, PostgreSQL, source ACK, current publication hold, native schema admission or native-result qualification.'], 'root_preconditions':['Recheck exact command/manifest and all125 current resource descriptors immediately before invocation, including interpreter resolution.','Fresh candidate-output directory must be absent.','Retain terminal status/stdout/stderr and independently compare closing125 resource vector on observed success or failure.'],'findings':[],'review_script':desc(Path(__file__))}
P.with_suffix('.json').write_text(json.dumps(out,indent=2)+'\n');print(json.dumps({'status':out['status'],'resources':125,'record_entries':len(record),'receipt':str(P.with_suffix('.json'))}))
