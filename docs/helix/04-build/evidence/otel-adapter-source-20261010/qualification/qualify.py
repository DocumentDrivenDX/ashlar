import hashlib, json, os, subprocess, time
from pathlib import Path
root=Path('/Users/erik/Projects/ashlar')
out=Path('/private/tmp/ashlar-otel-adapter-source-20261010-b')
paths=['src/ashlar_host/config.py','src/ashlar_host/diagnostics.py','src/ashlar_host/otel.py','src/ashlar_host/_otel_worker.py','tests/test_otel_supervision.py','tests/test_otel_worker.py','.github/workflows/module-boundaries.yml','docs/helix/02-design/adr/ADR-001-delta-canonical-and-serving-layout.md']
def file(p):
    raw=p.read_bytes(); return {'path':str(p),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
opening=[file(root/p) for p in paths]
installation=Path('/private/tmp/ashlar-otel-sdk-installation-20261010-a/installation.json')
sdk=json.loads(installation.read_bytes())['wheelOwnedFiles']
assert all(file(Path(item['path']))==item for item in sdk)
python='/private/tmp/ashlar-otel-sdk-env-20261010-a/bin/python'
jobs=[('supervision',python,'test_otel_supervision.py',False),('worker',python,'test_otel_worker.py',False),('configuration','/usr/bin/python3','test_diagnostics_configuration.py',True),('capture','/usr/bin/python3','test_host_diagnostics.py',True)]
results=[]
for name, interpreter, pattern, isolated in jobs:
    argv=[interpreter,'-B']+(['-S'] if isolated else [])+['-W','error::ResourceWarning','-m','unittest','discover','-s','tests','-p',pattern]
    start=time.monotonic()
    result=subprocess.run(argv,cwd=root,env={'PATH':'/usr/bin:/bin','PYTHONPATH':'src:tests'},stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=10)
    assert len(result.stdout)<=131072
    (out/(name+'.log')).write_bytes(result.stdout)
    results.append({'name':name,'argv':argv,'environment':{'PATH':'/usr/bin:/bin','PYTHONPATH':'src:tests'},'exitCode':result.returncode,'durationSeconds':time.monotonic()-start,'output':file(out/(name+'.log'))})
    assert result.returncode==0
closing=[file(root/p) for p in paths]
assert opening==closing
assert all(file(Path(item['path']))==item for item in sdk)
receipt={'format':'ashlar-otel-adapter-source-qualification/0.1','scope':'Reviewed-source qualification only; worker tests use actual pinned SDK with mocked Ashlar package metadata and inert HTTP ports. Supervision uses actual private finite subprocesses and selected SDK parent context; no installed-host, network, receiver, native engine or full CONTRACT-006 support claim. SDK-free capture omits optional schema test explicitly.','openingSources':opening,'closingSources':closing,'sdkWheelOwnedFileCount':len(sdk),'sdkInstallationReceipt':file(installation),'commands':results}
(out/'qualification.json').write_text(json.dumps(receipt,indent=2)+'\n')
print(json.dumps({'tests':'all commands exited zero','sources':len(opening),'sdkFilesUnchanged':len(sdk)}))
