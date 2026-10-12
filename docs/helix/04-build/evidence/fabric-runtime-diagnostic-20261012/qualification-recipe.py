"""Runtime correspondence only; never starts Azure CLI, authentication or HTTP."""
import hashlib
import json
import os
from pathlib import Path
import signal
import sys
import time
sys.path.insert(0, '/Users/erik/Projects/ashlar/src')
from ashlar_host.fabric_cli_runtime import (FabricRuntimeCustody, FabricCliRuntimeSelection,
    FabricHttpRuntimeSelection, CA_BUNDLE)
from ashlar_host.lifecycle import finish

MESSAGES = {
    'Unselected runtime link':'runtime-link-refusal',
    'Selected native/venv link target differs':'native-link-refusal',
    'Unselected native runtime module':'native-inventory-refusal',
    'Selected operator-trusted installed profile differs':'provenance-or-native-fingerprint-refusal',
    'Ambient credential fallback is outside selected profile':'netrc-absence-refusal',
    'Runtime closure deadline exceeded':'deadline-refusal',
}
def classification(error):
    if str(error) in MESSAGES:return MESSAGES[str(error)]
    return 'runtime-correspondence-refusal' if isinstance(error,PermissionError) else 'deadline-refusal' if isinstance(error,TimeoutError) else 'runtime-failure'
def summary(owner):
    rows=owner.original
    files=[r for r in rows if r[0]=='file']
    # Whole inventory folded into digest; neither inventory nor paths exported.
    digest=hashlib.sha256()
    for row in rows:
        digest.update(json.dumps(row,separators=(',',':'),ensure_ascii=True).encode()+b'\n')
    return {'files':len(files),'bytes':sum(r[2][5]for r in files),
            'inventory_entries':len(rows),'inventory_sha256':digest.hexdigest()}
def qualify(cli,http,configuration,deadline, factory=FabricRuntimeCustody):
    owners=[];result={};primary=None;closing=[]
    try:
        for name,selection in (('cli',cli),('http',http)):
            owner=factory(selection,configuration=configuration if name=='cli' else None,deadline=deadline)
            owners.append((name,owner));result[name]=summary(owner)
        # Independent original resource correspondence, then independent closing.
        for name,owner in owners:owner.renew();result[name]['renewed']=True
    except BaseException as error:primary=error
    def close(name,owner):
        try:owner.close()
        except BaseException:
            closing.append({'owner':name,'closed':False});raise
        else:closing.append({'owner':name,'closed':True})
    try:finish(primary,[lambda name=n,owner=o:close(name,owner)for n,o in reversed(owners)])
    except BaseException as error:
        return {'status':'refused','failure':classification(error),'exception_type':type(error).__name__,
                'observations':result,'closing':closing},2
    return {'status':'correspondence-qualified','observations':result,'closing':closing},0

def main():
    if len(sys.argv)!=2:raise SystemExit('Explicit fresh canonical private output directory required')
    root=Path(sys.argv[1])
    if not root.is_absolute() or root.resolve()!=root or root.exists():raise SystemExit('Fresh canonical output required')
    os.mkdir(root,0o700)
    for name in ('configuration','temporary','extensions'):os.mkdir(root/name,0o700)
    started=time.monotonic();deadline=started+60
    cli=FabricCliRuntimeSelection('homebrew-azure-cli-2.90.0-python-3.14.8',50000,2*1024**3,60,root/'temporary',root/'extensions')
    http=FabricHttpRuntimeSelection('stdlib-python-3.14.8-fabric-http',50000,2*1024**3,60,CA_BUNDLE)
    def expired(signum,frame):raise TimeoutError('Runtime closure deadline exceeded')
    old=signal.signal(signal.SIGALRM,expired);signal.setitimer(signal.ITIMER_REAL,max(0.001,deadline-time.monotonic()))
    try:report,status=qualify(cli,http,root/'configuration',deadline)
    finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,old)
    report.update({'elapsed_seconds':time.monotonic()-started,'scope':'runtime-only correspondence; no credential-tool execution or remote permission claim',
        'selected_versions':{'azure_cli':'2.90.0','python':'3.14.8'},'versions_observed_by_this_probe':False,
        'assumptions':['genuine operator-admitted fixed Homebrew installation','trusted Darwin arm64 OS loader, system frameworks and host isolation'],
        'transient_mutation_exclusion_proven':False,'maximum_files':50000,'maximum_bytes':2*1024**3,'whole_probe_deadline_seconds':60})
    data=(json.dumps(report,sort_keys=True,indent=2)+'\n').encode()
    if len(data)>16384:raise RuntimeError('Summary capacity exceeded')
    fd=os.open(root/'observation.json',os.O_WRONLY|os.O_CREAT|os.O_EXCL,0o600)
    try:
        with os.fdopen(fd,'wb')as stream:stream.write(data)
    except BaseException:raise
    return status
if __name__=='__main__':raise SystemExit(main())
