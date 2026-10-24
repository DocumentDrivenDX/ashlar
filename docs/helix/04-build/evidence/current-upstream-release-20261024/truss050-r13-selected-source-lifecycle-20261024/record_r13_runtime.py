#!/usr/bin/env python3
"""Read-only R13 .50 runtime and RECORD verification; emits a JSON receipt."""
import base64, hashlib, importlib.metadata as md, json, os, pathlib, platform, subprocess, sys
ROOT = pathlib.Path('/private/tmp/ashlar-truss050-current-cohort-9c745278-r13')
VENV = ROOT / 'runtime/venv'
OLD_RECEIPT = pathlib.Path('/private/tmp/ashlar-truss049-current-cohort-e1001bd-r12/runtime-receipt.json')
OLD_RECEIPT_SHA = '517ac6892280e911060688afc75528907e62aa5d6d290abf6d9b166f5da5617a'
WHEEL = pathlib.Path('/private/tmp/truss050-qualify/truss_toolkit-0.0.50-py3-none-any.whl')
WHEEL_SHA = '7b0483bb0c624c98515cf38ec30bbbf1ca43c1250e8880e5d00d520b4b7eec00'
def sha(p):
    h=hashlib.sha256()
    with p.open('rb') as f:
        for chunk in iter(lambda:f.read(1024*1024),b''): h.update(chunk)
    return h.hexdigest()
def need(ok,msg):
    if not ok: raise SystemExit('FAIL: '+msg)
need(sha(OLD_RECEIPT)==OLD_RECEIPT_SHA,'immutable R12 source receipt changed')
need(sha(WHEEL)==WHEEL_SHA,'official .50 wheel hash mismatch')
need(sys.version_info[:3]==(3,12,14) and pathlib.Path(sys.prefix).resolve()==VENV.resolve(),'isolated CPython 3.12.14 prefix mismatch')
need(pathlib.Path(sys.prefix).resolve()!=pathlib.Path(sys.base_prefix).resolve(),'venv resolves to base interpreter')
site=VENV/'lib/python3.12/site-packages'
need([pathlib.Path(p).resolve() for p in sys.path if 'site-packages' in p]==[site.resolve()],'site-packages isolation mismatch')
versions={}
selected=[]
record_counts={}
record_payloads=0
errors=[]
for dist in md.distributions(path=[str(site)]):
    name=dist.metadata.get('Name','')
    version=dist.version
    versions[name]=version
    for item in dist.files or []:
        h=getattr(item,'hash',None)
        if h is None: continue
        p=pathlib.Path(dist.locate_file(item))
        if not p.is_file():
            errors.append('missing:'+name+':'+str(item)); continue
        raw=p.read_bytes(); expected=base64.urlsafe_b64encode(hashlib.new(h.mode,raw).digest()).decode().rstrip('=')
        if expected!=h.value or (item.size is not None and len(raw)!=item.size):
            errors.append('record:'+name+':'+str(item)); continue
        record_counts[name]=record_counts.get(name,0)+1
        record_payloads+=1
    if name.lower()=='pip': continue
    direct=dist.read_text('direct_url.json')
    need(bool(direct),'direct_url missing: '+name)
    origin=json.loads(direct)
    path=pathlib.Path(origin['url'].removeprefix('file://'))
    archive_hash=origin.get('archive_info',{}).get('hashes',{}).get('sha256')
    need(path.is_file() and not path.is_symlink(),'direct wheel unavailable: '+name)
    wheel_sha=sha(path)
    need(archive_hash==wheel_sha,'direct wheel hash differs: '+name)
    selected.append({'distribution':name,'version':version,'filename':path.name,'sha256':wheel_sha,'size_bytes':path.stat().st_size,'wheel_path':str(path),'direct_url_sha256':hashlib.sha256(direct.encode()).hexdigest()})
need(not errors,'installed RECORD payload errors: '+repr(errors[:8]))
need(len(selected)==27 and len(versions)==28,'unexpected installed distribution inventory')
required={'truss-toolkit':'0.0.50','umf-core':'0.10.1','umf_access':'0.1.0.dev7','weft-sql':'0.4.53','pyspark':'4.2.0','delta_spark':'4.4.1','pgserver-pg18':'18.6.1','psycopg':'3.3.6','psycopg-binary':'3.3.6'}
need(all(versions.get(k)==v for k,v in required.items()),'selected runtime cohort differs: '+repr({k:versions.get(k) for k in required}))
pip=subprocess.run([str(VENV/'bin/python'),'-I','-B','-m','pip','check'],capture_output=True,text=True,timeout=30,env={'PATH':str(VENV/'bin')+':/usr/bin:/bin','PYTHONNOUSERSITE':'1','PYTHONDONTWRITEBYTECODE':'1','PIP_NO_CACHE_DIR':'1','PIP_CACHE_DIR':'/dev/null'})
need(pip.returncode==0 and pip.stdout.strip()=='No broken requirements found.' and not pip.stderr.strip(),'pip check did not pass: '+pip.stdout+' '+pip.stderr)
truss=md.distribution('truss-toolkit')
truss_origin=json.loads(truss.read_text('direct_url.json'))
need(truss_origin['archive_info']['hashes']['sha256']==WHEEL_SHA,'Truss .50 direct wheel provenance mismatch')
need(pathlib.Path(truss.locate_file('')).resolve().is_relative_to(VENV.resolve()),'Truss .50 files outside selected venv')
# Count bytes without following links; the R13 retained root must stay below 1 GiB.
logical=allocated=0
for base,dirs,files in os.walk(ROOT,followlinks=False):
    for n in dirs+files:
        p=pathlib.Path(base)/n
        if p.is_symlink(): continue
        st=p.stat(); logical+=st.st_size; allocated+=getattr(st,'st_blocks',0)*512
need(logical<=1073741824 and allocated<=1073741824,'R13 retained runtime/source root exceeds 1 GiB')
print(json.dumps({'schema':'ashlar-r13-current-cohort-runtime-receipt/0.1','status':'passed_effect_free_runtime_install_and_record_checks','runtime':{'python':sys.version.split()[0],'platform':platform.platform(),'prefix':str(VENV),'include_system_site_packages':False,'site_packages_count_on_sys_path':1,'clone_method':'APFS copy-on-write clone of exact R12 venv; only Truss distribution force-reinstalled','source_runtime_receipt_sha256':OLD_RECEIPT_SHA},'install':{'mode':'--no-index --no-deps --force-reinstall from official local .50 wheel; no dependencies installed','truss_wheel_path':str(WHEEL),'truss_wheel_sha256':WHEEL_SHA,'pip_check':pip.stdout.strip(),'record_payloads_verified':record_payloads,'record_payload_counts':record_counts,'record_errors':len(errors),'installed_distribution_count':len(versions),'selected_wheel_count':len(selected),'selected_wheels':sorted(selected,key=lambda r:r['distribution']),'versions':dict(sorted(versions.items()))},'budgets':{'logical_bytes':logical,'allocated_bytes':allocated,'cap_bytes':1073741824},'native_actions':{'postgres_started':False,'spark_started':False,'migration_invoked':False,'source_feed_publication_or_ack_mutated':False}},indent=2,sort_keys=True))
