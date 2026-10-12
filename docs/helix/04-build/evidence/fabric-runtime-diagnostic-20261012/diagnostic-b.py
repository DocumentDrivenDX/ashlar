"""Single bounded cProfile diagnostic; neither auth execution nor runtime admission."""
import cProfile
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import pstats
import signal
import sys
import time
PREP=Path('/private/tmp/ashlar-fabric-runtime-qualification-prep-a/qualify.py')
EXPECTED='da632235178546944d16c3627e8ec02bbf0dc8a3bce04d79e233463c78f077fc'
if hashlib.sha256(PREP.read_bytes()).hexdigest()!=EXPECTED:raise RuntimeError('Original recipe differs')
spec=importlib.util.spec_from_file_location('original_recipe',PREP);recipe=importlib.util.module_from_spec(spec);spec.loader.exec_module(recipe)

def category(key):
    filename,line,name=key
    if filename.endswith('fabric_cli_runtime.py'):
        return {'_scan':'owner_scan','__init__':'owner_constructor','timed':'deadline_checks','link_chain':'link_admission','renew':'renew','close':'closing','_private':'private_directory_admission','_host_identity':'host_identity','_ambient_netrc_paths':'netrc_metadata','<genexpr>':'owner_generator','<listcomp>':'owner_list_comprehension'}.get(name,'other_owner')
    if filename.endswith(('pathlib.py','posixpath.py','genericpath.py')):
        if name in ('resolve','realpath','_joinrealpath'):return 'path_resolution'
        if name in ('rglob','glob','_select_from','_iterate_directories','_make_selector','scandir'):return 'enumeration'
        return 'other_path_operations'
    if filename=='~':
        if 'scandir' in name:return 'enumeration'
        if 'readlink' in name:return 'readlink'
        if 'lstat' in name or 'stat' in name:return 'stat_metadata'
        if "posix.open" in name:return 'file_open'
        if "posix.read" in name:return 'file_read'
        if 'HASH' in name or 'openssl_sha256' in name:return 'hash'
        if 'monotonic' in name:return 'clock'
    return 'other'

def aggregate(profile):
    result={}
    for key,(primitive,total,self_seconds,cumulative,callers) in pstats.Stats(profile).stats.items():
        target=result.setdefault(category(key),{'primitive_calls':0,'total_calls':0,'self_seconds':0.0,'cumulative_seconds_overlapping':0.0})
        target['primitive_calls']+=primitive;target['total_calls']+=total
        target['self_seconds']+=self_seconds;target['cumulative_seconds_overlapping']+=cumulative
    for row in result.values():
        for name in ('self_seconds','cumulative_seconds_overlapping'):row[name]=round(row[name],6)
    return result

def profile_once(cli,http,configuration,deadline,factory=recipe.FabricRuntimeCustody):
    stage={'constructors_started':[],'constructors_returned':[]};profile=cProfile.Profile()
    def recorded(selection,**kwargs):
        name='cli'if not stage['constructors_started']else'http'
        stage['constructors_started'].append(name)
        owner=factory(selection,**kwargs);stage['constructors_returned'].append(name);return owner
    profile.enable()
    try:report,status=recipe.qualify(cli,http,configuration,deadline,recorded)
    finally:profile.disable()
    return {'original_outcome':report,'stage':stage,'costs':aggregate(profile)},status

def main():
    if len(sys.argv)!=2:raise SystemExit('Fresh explicit private output required')
    root=Path(sys.argv[1])
    if not root.is_absolute()or root.resolve()!=root or root.exists():raise SystemExit('Fresh canonical output required')
    os.mkdir(root,0o700)
    for name in ('configuration','temporary','extensions'):os.mkdir(root/name,0o700)
    cli=recipe.FabricCliRuntimeSelection('homebrew-azure-cli-2.90.0-python-3.14.8',50000,2*1024**3,60,root/'temporary',root/'extensions')
    http=recipe.FabricHttpRuntimeSelection('stdlib-python-3.14.8-fabric-http',50000,2*1024**3,60,recipe.CA_BUNDLE)
    started=time.monotonic();deadline=started+60
    def expired(signum,frame):raise TimeoutError('Runtime closure deadline exceeded')
    old=signal.signal(signal.SIGALRM,expired);signal.setitimer(signal.ITIMER_REAL,60)
    try:report,status=profile_once(cli,http,root/'configuration',deadline)
    finally:signal.setitimer(signal.ITIMER_REAL,0);signal.signal(signal.SIGALRM,old)
    report.update({'elapsed_seconds':time.monotonic()-started,'scope':'single instrumented runtime-only diagnostic; not runtime qualification, throughput benchmark or credential admission','profiling_overhead_present':True,'call_counts_are_not_bytes':True,'cumulative_category_seconds_overlap':True,'whole_probe_deadline_seconds':60,'maximum_files':50000,'maximum_bytes':2*1024**3,'assumptions':['genuine operator-admitted named Homebrew installation','trusted Darwin arm64 system loader/frameworks and host isolation'],'no_transient_mutation_exclusion':True})
    raw=(json.dumps(report,sort_keys=True,indent=2)+'\n').encode()
    if len(raw)>16384:raise RuntimeError('Private aggregate capacity exceeded')
    with open(root/'observation.json','xb')as stream:os.chmod(root/'observation.json',0o600);stream.write(raw)
    return status
if __name__=='__main__':raise SystemExit(main())
