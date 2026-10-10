from pathlib import Path
import ast,hashlib,importlib.util,json,sys,unittest
from unittest.mock import patch
ROOT=Path('/Users/erik/Projects/ashlar');sys.path.insert(0,str(ROOT/'src'))
import ashlar_host
rows=[]
for version in ('b','c'):
 base=Path('/private/tmp/ashlar-diagnostics-source-freeze-20261010-'+version)
 manifest=json.loads((base/'manifest.json').read_bytes())
 for f in manifest['files']:
  raw=(base/f['path']).read_bytes();assert len(raw)==f['bytes'] and hashlib.sha256(raw).hexdigest()==f['sha256']
 spec=importlib.util.spec_from_file_location('ashlar_host.diagnostics',base/'src/ashlar_host/diagnostics.py');m=importlib.util.module_from_spec(spec);sys.modules[spec.name]=m;setattr(ashlar_host,'diagnostics',m);spec.loader.exec_module(m)
 spec=importlib.util.spec_from_file_location('review_diagnostics_tests',base/'tests/test_host_diagnostics.py');test=importlib.util.module_from_spec(spec);sys.modules[spec.name]=test;spec.loader.exec_module(test)
 for name in ('lstat','mkdir','open','write'):
  for failure in (OSError(13,'private-sentinel','/private/sentinel/configuration'),KeyboardInterrupt(),SystemExit(),GeneratorExit()):
   t=test.DiagnosticsTests();t.setUp();sink=test.Sink()
   owner=m.Path if name in ('lstat','mkdir') else m.os
   try:
    with patch.object(owner,name,side_effect=failure):
     try:m.DiagnosticRun(t.config,sink.port())
     except BaseException as actual:
      if isinstance(failure,Exception):
       if version=='b':assert actual is failure and '/private/sentinel' in str(actual)
       else:assert type(actual) is m.DiagnosticsError and str(actual)=='diagnostics-configuration' and actual.__cause__ is None and actual.__suppress_context__
      else:assert actual is failure
     else:raise AssertionError('unexpected constructor success')
    assert sink.events==[] and sink.closed is False
    rows.append({'version':version,'port':name,'failure':type(failure).__name__,'pass':True})
   finally:t.doCleanups()
 if version=='c':
  # The two-file source freeze intentionally excludes governing schema fixtures.
  # Bind unchanged actual checked-out test bytes before resolving its original schemas.
  assert (ROOT/'tests/test_host_diagnostics.py').read_bytes()==(base/'tests/test_host_diagnostics.py').read_bytes()
  test.__file__=str(ROOT/'tests/test_host_diagnostics.py')
  result=unittest.TextTestRunner(verbosity=1).run(unittest.defaultTestLoader.loadTestsFromModule(test));assert result.wasSuccessful()
  count={'run':result.testsRun,'skipped':len(result.skipped),'failures':len(result.failures),'errors':len(result.errors)}
# All code outside constructor and one newly added test is AST-identical.
bases=[Path('/private/tmp/ashlar-diagnostics-source-freeze-20261010-'+v) for v in ('b','c')]
for rel,cls,method in [('src/ashlar_host/diagnostics.py','DiagnosticRun','__init__'),('tests/test_host_diagnostics.py','DiagnosticsTests','test_constructor_io_refusals_are_safe_and_cancellation_is_exact')]:
 trees=[ast.parse((b/rel).read_bytes()) for b in bases]
 for tree in trees:
  for n in tree.body:
   if isinstance(n,ast.ClassDef) and n.name==cls:n.body=[p for p in n.body if not isinstance(p,ast.FunctionDef) or p.name!=method]
 assert ast.dump(trees[0],include_attributes=False)==ast.dump(trees[1],include_attributes=False)
out=Path('/private/tmp/astra-diagnostics-constructor-controls-20261010-c-'+str(sys.version_info.major)+str(sys.version_info.minor)+'.json')
out.write_text(json.dumps({'rows':rows,'ownerTests':count,'scope':'Frozen B counterexamples and frozen C normalization/non-Exception exact identity; no SDK or operation ports. Sink cleanup remains composition owned.'},indent=2)+'\n')
print(str(out),hashlib.sha256(out.read_bytes()).hexdigest())
