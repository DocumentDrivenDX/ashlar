from pathlib import Path
import hashlib,json,subprocess
R=Path('/Users/erik/Projects/ashlar');Q=Path('/private/tmp/ashlar-otel-adapter-source-20261010-b/qualification.json')
def pin(p):
 p=Path(p);b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def verify(d):
 p=Path(d['path']);assert p.is_file() and not p.is_symlink() and p.stat().st_size==d['bytes'] and d['bytes']<33554432
 h=hashlib.sha256();size=0
 with p.open('rb') as f:
  for b in iter(lambda:f.read(65536),b''):h.update(b);size+=len(b)
 assert size==d['bytes'] and h.hexdigest()==d['sha256']
v=json.loads(Q.read_text());assert v['openingSources']==v['closingSources'] and len(v['openingSources'])==8
for d in v['openingSources']:verify(d)
verify(v['sdkInstallationReceipt']);sdk=json.loads(Path(v['sdkInstallationReceipt']['path']).read_text());assert len(sdk['wheelOwnedFiles'])==698
for d in sdk['wheelOwnedFiles']:
 assert d['path'].startswith('/private/tmp/ashlar-otel-sdk-env-20261010-a/lib/python3.11/site-packages/')
 verify(d)
for command in v['commands']:
 assert command['exitCode']==0;verify(command['output'])
print('EIGHT SOURCE PINS / 698 WHEEL FILES / FOUR ROOT LOGS VERIFIED')
for name in ('astra-otel-supervision-ci-20261010-b','astra-otel-worker-ci-20261010-d'):
 print(name,Path('/private/tmp/'+name+'.log').read_text().strip())
adr='docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md'
before=subprocess.check_output(['git','show','HEAD:'+adr],cwd=R);after=(R/adr).read_bytes();assert before.split(b'---',2)[:2]==after.split(b'---',2)[:2]
wf='.github/workflows/module-boundaries.yml';old=subprocess.check_output(['git','show','HEAD:'+wf],cwd=R);new=(R/wf).read_bytes();added=new.split(b'jobs:\n',1)[1].split(b'  check:\n',1)[0];assert new.replace(added,b'',1)==old
assert b'python-version: \'3.11\'' in added and b'-S -W error::ResourceWarning' in added
result={'scope':'Selected source/evidence/SDK wheel-file custody verification; source test qualification remains finite and no installed host/receiver/native/full C006 claim','rootQualification':pin(Q),'rootSourceScript':pin(Q.parent/'qualify.py'),'exactSources':v['openingSources'],'sdkWheelFileCountRehashed':698,'sdkInstallationReceipt':v['sdkInstallationReceipt'],'rootLogs':[x['output'] for x in v['commands']],'independentCICommands':[{'argv':'PYTHONPATH=src:tests python3.11 -B -S -W error::ResourceWarning -m unittest discover -s tests -p test_otel_supervision.py','exitCode':0,'log':pin('/private/tmp/astra-otel-supervision-ci-20261010-b.log')},{'argv':'PYTHONPATH=src python3.11 -B -S -W error::ResourceWarning -m unittest discover -s tests -p test_otel_worker.py','exitCode':0,'log':pin('/private/tmp/astra-otel-worker-ci-20261010-d.log')}],'adrFrontmatterUnchanged':True,'oldCIBytesPreservedWithOneNewJob':True}
p=Path('/private/tmp/astra-otel-adapter-source-closure-20261010-b.json');p.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(pin(p)))
