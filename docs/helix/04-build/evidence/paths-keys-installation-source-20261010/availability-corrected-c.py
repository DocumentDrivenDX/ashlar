import importlib.util,sys,tempfile,json,hashlib
from pathlib import Path
from unittest.mock import patch
ROOT=Path('/private/tmp/ashlar-installation-mechanics-source-freeze-20261010-c');sys.path.insert(0,'/Users/erik/Projects/ashlar/src');import ashlar
spec=importlib.util.spec_from_file_location('ashlar._weft_installation_mechanics',ROOT/'src/ashlar/_weft_installation_mechanics.py');m=importlib.util.module_from_spec(spec);sys.modules[spec.name]=m;ashlar._weft_installation_mechanics=m;spec.loader.exec_module(m)
spec=importlib.util.spec_from_file_location('mechanics_tests',ROOT/'tests/test_weft_installation_mechanics.py');t=importlib.util.module_from_spec(spec);sys.modules[spec.name]=t;spec.loader.exec_module(t)
results=[]
for profile in ('paths','paths-keys'):
 case=t.MechanicsTests();case.setUp();primary=KeyboardInterrupt('injected after successful final link');real_link=m.os.link;observed=[]
 def link_then_interrupt(src,dst):
  real_link(src,dst);observed.append(Path(dst).read_bytes());raise primary
 try:
  with patch.object(m.os,'link',side_effect=link_then_interrupt):
   try:case.publish(profile)
   except BaseException as got:assert got is primary
   else:raise AssertionError('cancellation swallowed')
  results.append({'profile':profile,'readyAvailableBeforeInterrupt':bool(observed),'readyExistsAfterCleanup':(case.root/profile/'ready.json').exists(),'outputExistsAfterCleanup':(case.root/profile).exists(),'primaryPreserved':True})
 finally:case.doCleanups()
out=Path('/private/tmp/ashlar-root-mechanics-link-interruption-probe-20261010-c.json');out.write_text(json.dumps({'scope':'Inert file publication with fixture proof; no executable/compiler/native invocation','results':results},indent=2)+'\n');print(out.read_text())
