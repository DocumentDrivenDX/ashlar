import ast,hashlib,importlib.util,json,pathlib,subprocess,sys,tempfile
ROOT=pathlib.Path('/Users/erik/Projects/ashlar')
FILES=['tools/check_module_boundaries.py','tests/test_module_boundaries.py','tools/module_boundaries.json']
def desc(path):
 b=path.read_bytes();return {'path':str(path),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
opening=[desc(ROOT/p)for p in FILES]
old={p:subprocess.check_output(['git','show','HEAD:'+p],cwd=ROOT)for p in FILES}
new={p:(ROOT/p).read_bytes()for p in FILES}
assert new[FILES[0]]==old[FILES[0]].replace(b"source_state = {'source', 'apply', 'whole_entity', 'source_checkpoint'}",b"source_state = {'source', 'apply', 'whole_entity', 'source_checkpoint', 'commerce_evolution'}")
assert new[FILES[2]]==old[FILES[2]]
a=ast.parse(old[FILES[1]]);b=ast.parse(new[FILES[1]])
for tree in (a,b):
 for cls in tree.body:
  if isinstance(cls,ast.ClassDef):cls.body=[n for n in cls.body if not (isinstance(n,ast.FunctionDef)and n.name=='test_portable_evolution_role_allows_source_and_forbids_runtime')]
assert ast.dump(a)==ast.dump(b)
spec=importlib.util.spec_from_file_location('boundary',ROOT/FILES[0]);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
controls=[]
with tempfile.TemporaryDirectory(prefix='astra-evolution-boundary-')as tmp:
 r=pathlib.Path(tmp);(r/'src/ashlar').mkdir(parents=True);(r/'tools').mkdir()
 p=r/'src/ashlar/commerce_evolution.py'
 for target in ['source','apply','whole_entity','source_checkpoint','schema']:
  p.write_text('from .'+target+' import PublicName\n');assert module.scan(r)==[];controls.append({'target':target,'expect':'allowed'})
 for target in ['native','staging','attempt_store','schema_registry','pins','authority','retention_policy','weft_distribution','weft_paths_distribution','weft_paths_keys_distribution']:
  p.write_text('from .'+target+' import PublicName\n');rows=module.scan(r);assert len(rows)==1 and rows[0][3]=='portable-to-runtime';controls.append({'target':target,'expect':'portable-to-runtime'})
 for statement,reason in [('from ashlar_host import driver','core-to-host-composition'),('import pyspark','core-to-sdk'),('import opentelemetry.sdk','core-to-sdk'),('from tools import fixture_oracle','core-to-tools')]:
  p.write_text(statement+'\n');rows=module.scan(r);assert len(rows)==1 and rows[0][3]==reason;controls.append({'import':statement,'expect':reason})
assert module.check(ROOT,json.loads(new[FILES[2]]))==([],[])
assert opening==[desc(ROOT/p)for p in FILES]
out=pathlib.Path('/private/tmp/astra-evolution-boundary-role-review-20261010-a.json')
out.write_text(json.dumps({'format':'astra-boundary-role-source-review/0.1','verdict':'PASS','files':opening,'baselinePolicyUnchanged':True,'checkerOnlyAddition':'commerce_evolution to source_state','existingTestASTsUnchanged':True,'independentRealASTControls':controls,'actualScan':{'new':[],'stale':[]},'scope':'Static AST checker and stdlib temporary test trees only; no inspected source modules executed, no SDK/native/compiler/network or Git mutation.','runtime':sys.version},indent=2)+'\n')
print(json.dumps(desc(out)))
