#!/usr/bin/env python3
"""Effect-free Weft 0.4.56 G03 `.4` request and page artifact preparation."""
from __future__ import annotations
import hashlib, json, os, subprocess, sys
from pathlib import Path
from urllib.parse import unquote, urlsplit

ROOT = Path(__file__).resolve().parents[6]
ATTEMPT = Path('/private/tmp/ashlar-g03-weft-native-attempt-r12-20261101')
SOURCE = Path('/private/tmp/ashlar-g03-sdk0101-lifecycle-r3-attempts-20261101/ashlar-g03-sdk0101-lifecycle-94c1991ea883463b88d938b126893d31')
CONT = Path('/private/tmp/ashlar-g03-sdk0101-r3-manifest-continuation-attempts-20261101/continuation-r5-d24785a0-e9a9-4607-a7cc-88ef672bc35c')
PYTHON = Path('/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/venv/bin/python3.12')
JAVA_HOME = Path('/opt/homebrew/Cellar/openjdk@21/21.0.13.0.2/libexec/openjdk.jdk/Contents/Home')
SPARK_HOME = PYTHON.parent.parent / 'lib/python3.12/site-packages/pyspark'
SPARK_LOCAL_IP = '127.0.0.1'
WHEEL_SHA = '68f9297b4b55df483b86d3041040991c4dadfb9b4d63afcff1420c43f93ae41d'
WHEEL = Path('/private/tmp/ashlar-r16-truss051-runtime-cohort-20261029/wheels/umf_weft-0.4.56-cp39-abi3-macosx_11_0_arm64.whl')
ROLES = ('object_current','edge_current','tombstone','whole_source_history')

def sha(b: bytes) -> str: return hashlib.sha256(b).hexdigest()
def native_alias_namespace(publication_id):
    if not isinstance(publication_id,str) or len(publication_id)<12: raise ValueError('invalid publication ID for alias namespace')
    return 'ashlar_g03_'+publication_id[-12:]

def assert_compiled_alias_binding(request,response,required_roles):
    target=request.get('target',request.get('request',{}).get('target',{}))
    binding=json.loads(target['bindingJson'])
    namespace=native_alias_namespace(binding['publication']['id'])
    expected=[['spark_catalog',namespace,role] for role in ROLES]
    actual=[x['name'] for x in binding['publication']['tables']]
    if actual!=expected: raise RuntimeError('compiler binding alias namespace differs from publication-derived Spark namespace')
    sql=response.get('sql',response.get('artifact',{}).get('sql',''))
    if not required_roles or any(role not in ROLES for role in required_roles): raise ValueError('explicit Weft table-role set required')
    present=[]
    for role in ROLES:
        ident='`spark_catalog`.`'+namespace+'`.`'+role+'`'
        if ident in sql: present.append(role)
    if set(present)!=set(required_roles): raise RuntimeError('compiled SQL table identifiers differ from exact publication binding and expected role subset')
    for other_namespace in __import__('re').findall(r'`spark_catalog`\.`([^`]+)`\.`[^`]+`',sql):
        if other_namespace!=namespace: raise RuntimeError('compiled SQL references a different Spark catalog namespace')
    return namespace

def canonical(v) -> bytes: return (json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False)+'\n').encode()
def write_new(path: Path, raw: bytes):
    path.parent.mkdir(parents=True, exist_ok=True)
    fd=os.open(path,os.O_WRONLY|os.O_CREAT|os.O_EXCL|os.O_NOFOLLOW,0o600)
    try:
        if os.write(fd,raw)!=len(raw): raise OSError('short write')
        os.fsync(fd)
    finally: os.close(fd)
def normalize_local_delta_location(value):
    if not isinstance(value,str) or not value: raise ValueError('Delta location must be a non-empty string')
    parsed=urlsplit(value)
    if parsed.scheme:
        if parsed.scheme!='file' or parsed.netloc not in ('','localhost') or parsed.query or parsed.fragment:
            raise ValueError('Delta location is not a local file URI')
        value=unquote(parsed.path)
    return Path(value).resolve()

def assert_delta_alias_identity(detail,expected_uuid,expected_path):
    actual_uuid=detail.get('id')
    raw_location=detail.get('location')
    expected_location=str(Path(expected_path).resolve())
    diagnostics={'actualUuid':actual_uuid,'expectedUuid':expected_uuid,'rawLocation':raw_location,'expectedLocation':expected_location}
    try: actual_location=normalize_local_delta_location(raw_location); diagnostics['normalizedLocation']=str(actual_location)
    except (TypeError,ValueError,OSError) as error:
        diagnostics['locationError']=str(error)[:512]
        actual_location=None
    if actual_uuid!=expected_uuid or actual_location!=Path(expected_location):
        error=RuntimeError('native alias identity mismatch: '+json.dumps(diagnostics,sort_keys=True,separators=(',',':')))
        error.diagnostics=diagnostics
        raise error
    return actual_location

def assert_page_one_lookahead(visible_rows, expected_page_one, expected_page_two, continuation_after):
    """Require the exact first page plus exactly one full source-oracle lookahead row."""
    if visible_rows[:len(expected_page_one)] != expected_page_one or len(visible_rows) != len(expected_page_one) + 1:
        raise RuntimeError('native Weft page 1 rows or lookahead cardinality differ from source oracle')
    if not expected_page_two or visible_rows[-1] != expected_page_two[0]:
        raise RuntimeError('native Weft page lookahead differs from source oracle')
    if not expected_page_one or expected_page_one[-1][0] != continuation_after[0]:
        raise RuntimeError('source-derived continuation key drift')

def assert_closing_snapshot(opening, closing, expected_roles, current_database, current_application_id):
    """Compare the six role heads separately from Spark session identity fields."""
    expected_role_heads={role:opening[role] for role in expected_roles}
    if closing != expected_role_heads:
        raise RuntimeError('opening/closing six-role Delta heads differ')
    if current_database != opening['database'] or current_application_id != opening['applicationId']:
        raise RuntimeError('Spark runtime identity drift')

def read(path: Path): return path.read_bytes()
def bounded_exception_details(error):
    import traceback
    message=str(error).encode('utf-8','replace')[:2048].decode('utf-8','replace')
    raw=''.join(traceback.format_exception(type(error),error,error.__traceback__)).encode('utf-8','replace')
    return type(error).__name__,message,raw[-16384:].decode('utf-8','replace'),len(raw)>16384
def main():
    if len(sys.argv)!=2 or sys.argv[1] not in ('prepare','preflight','run'): raise SystemExit('prepare.py prepare|preflight|run')
    ATTEMPT.mkdir(mode=0o700,exist_ok=True)
    if sys.argv[1]=='prepare': prepare(); return
    if sys.argv[1]=='preflight': preflight(); return
    run()
def preflight():
    import subprocess
    if not (ATTEMPT/'prepared.json').is_file() or (ATTEMPT/'worker-import-preflight.json').exists(): raise RuntimeError('fresh prepared attempt required for worker import preflight')
    code = r'''import importlib, importlib.metadata, json, os, pathlib, sys
mods=('pyspark.sql','pyspark.worker','pyspark.serializers','pyspark.cloudpickle','py4j','weft','ashlar_host.configured_local_runtime_profile','ashlar_host.managed_current_carriers')
loaded={}
for name in mods:
 m=importlib.import_module(name); loaded[name]=str(pathlib.Path(m.__file__).resolve())
assert importlib.metadata.version('pyspark')=='4.2.0'
assert hasattr(importlib.import_module('pyspark.worker'),'main')
assert os.environ.get('PYSPARK_PYTHON')==sys.executable
assert os.environ.get('PYSPARK_DRIVER_PYTHON')==sys.executable
assert os.environ.get('ASHLAR_WORKER_SRC') and str(pathlib.Path(os.environ['ASHLAR_WORKER_SRC']).resolve()) in sys.path
for name,path in loaded.items():
 p=pathlib.Path(path)
 if name.startswith('ashlar_host.'):
  assert pathlib.Path(os.environ['ASHLAR_WORKER_SRC']).resolve() in p.parents
 else:
  assert pathlib.Path(sys.prefix).resolve() in p.parents
print(json.dumps({'profile':'ashlar-g03-python-worker-imports/0.1','python':sys.executable,'pyspark':importlib.metadata.version('pyspark'),'modules':loaded,'workerMainImported':True,'sparkStarted':False,'deltaAccessed':False},sort_keys=True,separators=(',',':')))
'''
    prepared=json.loads(read(ATTEMPT/'prepared.json'))
    java=JAVA_HOME/'bin/java'
    if not java.is_file() or not os.access(java,os.X_OK): raise RuntimeError('pinned JAVA_HOME executable missing or not executable')
    env=os.environ.copy(); env.update({'JAVA_HOME':str(JAVA_HOME),'SPARK_HOME':str(SPARK_HOME),'SPARK_LOCAL_IP':SPARK_LOCAL_IP,'PYSPARK_PYTHON':str(PYTHON),'PYSPARK_DRIVER_PYTHON':str(PYTHON),'ASHLAR_WORKER_SRC':str(ROOT/'src')})
    env['PATH']=str(JAVA_HOME/'bin')+os.pathsep+env.get('PATH','')
    env['PYTHONPATH']=str(ROOT/'src')+os.pathsep+env.get('PYTHONPATH','')
    java_result=subprocess.run([str(java),'-version'],env=env,text=True,capture_output=True,timeout=10,check=True)
    java_version=java_result.stderr+java_result.stdout
    if '21.0.13' not in java_version: raise RuntimeError('pinned Java runtime version mismatch')
    submit=SPARK_HOME/'bin/spark-submit'
    if not submit.is_file() or not os.access(submit,os.X_OK) or not (SPARK_HOME/'jars').is_dir(): raise RuntimeError('pinned PySpark distribution is incomplete')
    r=subprocess.run([str(PYTHON),'-c',code],cwd=str(ROOT),env=env,text=True,capture_output=True,timeout=30,check=True)
    value=json.loads(r.stdout)
    if value.get('sparkStarted') is not False or value.get('deltaAccessed') is not False: raise RuntimeError('worker preflight crossed effect-free boundary')
    if sha(read(SOURCE/'inputs/typed-graph-source.jsonl'))!=prepared['sourceJsonlSha256'] or sha(read(SOURCE/'inputs/typed-graph-model.umf.json'))!=prepared['modelSha256'] or sha(read(SOURCE/'inputs/configured-typed-graph-source.json'))!=prepared['configurationSha256'] or sha(read(SOURCE/'inputs/typed-graph-binding.json'))!=prepared['bindingSha256']: raise RuntimeError('explicit configured source binding drift')
    value.update({'javaHome':str(JAVA_HOME),'javaExecutable':str(java),'javaVersion':java_version.strip(),'sparkHome':str(SPARK_HOME),'sparkSubmit':str(submit),'sparkLocalIp':SPARK_LOCAL_IP,'sourceBinding':{'root':str(SOURCE),'sourceCommit':prepared['sourceCommit'],'sourceJsonlSha256':prepared['sourceJsonlSha256'],'modelSha256':prepared['modelSha256'],'configurationSha256':prepared['configurationSha256'],'bindingSha256':prepared['bindingSha256']}})
    proof=canonical(value); write_new(ATTEMPT/'worker-import-preflight.json',proof)
    prepared=json.loads(read(ATTEMPT/'prepared.json')); prepared['workerPreflightSha256']=sha(proof)
    temp=ATTEMPT/'prepared.json.tmp'; write_new(temp,canonical(prepared)); os.replace(temp,ATTEMPT/'prepared.json')
    print(canonical(value).decode(),end='')

def prepare():
    if any((ATTEMPT/n).exists() for n in ('prepared.json','grouped.request.json','page.request.json','continuation.response.json')): raise RuntimeError('attempt already prepared')
    if sha(read(SOURCE/'transaction1-result.json'))!='f982e5e9ee5cb3297c4f459b1071f59d36e3ae2b140cc1f7ddf46d231e4c582a': raise RuntimeError('R3 transaction receipt drift')
    if sha(read(CONT/'continuation-result.json'))!='118eaabb135ae4e5a1ba8f491cab2ebdef40dfe502740c7c53a3f8a124895e0f': raise RuntimeError('R5 continuation receipt drift')
    retained=json.loads(read(CONT/'continuation-result.json')); vector=retained['realized_vector']
    if vector['carrierProfile']!='ashlar-delta/0.4-xxhash64' or retained['completion_receipt'].get('profile')!='ashlar-configured-local-delta4-output-receipt-ref/0.1': raise RuntimeError('vector/receipt profile')
    if vector['canonicalSha256']!=sha(json.dumps({k:v for k,v in vector.items() if k!='canonicalSha256'},sort_keys=True,separators=(',',':'),ensure_ascii=False).encode()): raise RuntimeError('canonical vector digest mismatch')
    inst=json.loads(read(SOURCE/'installation/installation.json'))
    target={x['table'].rsplit('.',1)[-1]:x for x in inst['targets']}
    if set(target)!=set(ROLES)|{'attempts','manifest'}: raise RuntimeError('six-role inventory mismatch')
    for role in ROLES:
        v=vector['dataRoles'][role]; t=target[role]
        if (v['uuid'],v['name'])!=(t['uuid'],t['table']): raise RuntimeError('realized vector inventory mismatch')
    if vector['installationInventory'] is None or vector['manifestUuid']!=target['manifest']['uuid']: raise RuntimeError('manifest identity mismatch')
    oracle_path=ROOT/'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/source-oracle.json'
    oracle=json.loads(read(oracle_path))
    model=json.loads(read(SOURCE/'inputs/typed-graph-model.umf.json'))
    if sha(read(SOURCE/'inputs/typed-graph-model.umf.json'))!=oracle['inputs']['modelSha256'] or sha(read(SOURCE/'inputs/typed-graph-source.jsonl'))!=oracle['inputs']['sourceJsonlSha256']: raise RuntimeError('source oracle inputs drift')
    sys.path.insert(0,str(ROOT/'src'))
    from ashlar_host.managed_current_carriers import load_current_managed_carriers
    import weft, importlib.metadata as metadata
    dist=metadata.distribution('umf-weft')
    if dist.version!='0.4.56' or not WHEEL.is_file() or sha(read(WHEEL))!=WHEEL_SHA or not dist.locate_file('weft/weft.abi3.so').is_file(): raise RuntimeError('Weft runtime origin unavailable')
    carrier=load_current_managed_carriers('ashlar-delta/0.4-xxhash64')
    binding_path=ROOT/'docs/helix/04-build/evidence/current-upstream-release-20261024/r02-native-execution-packet-20261024/positive-backend03-request.json'
    oldreq=json.loads(read(binding_path)); binding=json.loads(oldreq['target']['bindingJson'])
    binding.update(profile='ashlar-managed-current-core/0.1',layoutRevision='ashlar-delta/0.4-xxhash64',layoutSha256=sha(carrier.generated_bytes),lookupHashProfile=carrier.lookup_hash_profile)
    binding['publication']={'id':retained['manifest']['publication_id'],'manifestUuid':vector['manifestUuid'], 'tables':[{'name':['spark_catalog',native_alias_namespace(vector['publicationId']),role],'uuid':vector['dataRoles'][role]['uuid'],'version':vector['dataRoles'][role]['version']} for role in ROLES]}
    for row in binding['records']: row['table']=0
    for row in binding['relationships']: row['table']=1
    raw_binding=json.dumps(binding,sort_keys=True,separators=(',',':'),ensure_ascii=False)
    target03={'backendId':'ashlar.databricks.managed-uc-spark-xxhash64','backendVersion':'0.2.0-managed-current-core-xxhash64-candidate','targetProfile':'ashlar-managed-current-core/0.2-xxhash64'}
    group_sql=oldreq['sql'].replace('COUNT(*) > 1','COUNT(*) > 0')
    request={'interfaceVersion':'weft-compile/0.4.1','dialect':'weft-sql/0.4.1','sql':group_sql,'modules':oldreq['modules'],'options':{'allowCandidate':True},'target':{**target03,'bindingJson':raw_binding,'bindingSha256':sha(raw_binding.encode())}}
    response=json.loads(weft.compile_paths_keys_json(json.dumps(request,separators=(',',':'),ensure_ascii=False)))
    if response.get('status')!='compiled' or response.get('backend')!=dict(target03,interfaceVersion='weft-backend/0.3.0'): raise RuntimeError('fresh `.4` Backend03 request refused')
    alias_namespace=assert_compiled_alias_binding(request,response,('object_current','edge_current'))
    write_new(ATTEMPT/'grouped.request.json',canonical(request)); write_new(ATTEMPT/'grouped.response.json',canonical(response))
    positive_sql=oldreq['sql']; positive_req=dict(request); positive_req['sql']=positive_sql
    positive_response=json.loads(weft.compile_paths_keys_json(json.dumps(positive_req,separators=(',',':'),ensure_ascii=False)))
    if positive_response.get('status')!='compiled': raise RuntimeError('fresh `.4` positive Backend03 request refused')
    if assert_compiled_alias_binding(positive_req,positive_response,('object_current','edge_current'))!=alias_namespace: raise RuntimeError('positive query alias namespace drift')
    write_new(ATTEMPT/'grouped-positive.request.json',canonical(positive_req)); write_new(ATTEMPT/'grouped-positive.response.json',canonical(positive_response))
    # Page requests select only required STRING properties and preserve authored Keys.
    elements={(m['id'],e['id']):e for m in model['modules'] for e in m['elements']}
    page_binding=json.loads(raw_binding)
    for rec in page_binding['records']:
        rec['properties']=[p for p in rec['properties'] if elements[(p['logical']['module'],p['logical']['element'])].get('scalarType')=='string' and elements[(p['logical']['module'],p['logical']['element'])].get('nullability')=='required' and elements[(p['logical']['module'],p['logical']['element'])].get('cardinality')=='one' and not elements[(p['logical']['module'],p['logical']['element'])].get('facets',{})]
        fields={(p['logical']['module'],p['logical']['element']) for p in rec['properties']}
        el=elements[(rec['logical']['module'],rec['logical']['element'])]
        if not all((f['module'],f['element']) in fields for key in el.get('keys',[]) for f in key['fields']): raise RuntimeError('incomplete original page key binding')
    raw_page_binding=json.dumps(page_binding,sort_keys=True,separators=(',',':'),ensure_ascii=False)
    target02={'backendId':'ashlar.databricks.managed-uc-spark-xxhash64','backendVersion':'0.2.0-managed-current-core-xxhash64-pages-candidate','targetProfile':'ashlar-managed-current-core/0.2-xxhash64'}
    page_request={'interfaceVersion':'weft-compile/0.2.7','dialect':'weft-sql/0.2.0','timestampComparisonProfile':'umf-rfc3339-exact-instant/1.0.0','sql':'SELECT p.person_key,p.person_name FROM Person p','modules':oldreq['modules'],'parameters':{},'options':{'allowCandidate':True},'target':{**target02,'bindingJson':raw_page_binding,'bindingSha256':sha(raw_page_binding.encode())}}
    envelope={'version':'weft.engine-page/0.1','compiler':'ashlar-spark4-pages','request':page_request,'pageSize':2}
    page1=json.loads(weft.compile_engine_page_json(json.dumps(envelope,separators=(',',':'),ensure_ascii=False)))
    if page1.get('status')!='compiled' or page1.get('artifact',{}).get('backend',{}).get('targetProfile')!=target02['targetProfile']: raise RuntimeError('fresh `.4` Backend02 request refused')
    if assert_compiled_alias_binding(envelope,page1,('object_current',))!=alias_namespace: raise RuntimeError('page query alias namespace drift')
    after=json.dumps(['person-2'],separators=(',',':'),ensure_ascii=False)
    continuation=json.loads(weft.check_engine_page_json(json.dumps(page1,separators=(',',':'),ensure_ascii=False),json.dumps(page_request,separators=(',',':'),ensure_ascii=False),after))
    if continuation.get('status')!='compiled' or continuation.get('after')!=['person-2']: raise RuntimeError('source-derived page continuation refused')
    write_new(ATTEMPT/'page.request.json',canonical(envelope)); write_new(ATTEMPT/'page.response.json',canonical(page1)); write_new(ATTEMPT/'continuation.response.json',canonical(continuation))
    packet_commit=subprocess.run(['git','-C',str(ROOT),'rev-parse','HEAD'],text=True,capture_output=True,timeout=5,check=True).stdout.strip()
    manifest={'profile':'ashlar-g03-weft-native-prepared/0.1','sourceCommit':'4c92be7fdbade0d58a8eb18d83965af0c79c8c6d','packetCommit':packet_commit,'installationId':inst['scope']['installation_id'],'installationSha256':sha(read(SOURCE/'installation/installation.json')),'vector':vector,'vectorSha256':vector['canonicalSha256'],'manifestSha256':vector['manifestSha256'],'sourceOracleSha256':sha(read(oracle_path)),'sourceJsonlSha256':sha(read(SOURCE/'inputs/typed-graph-source.jsonl')),'modelSha256':sha(read(SOURCE/'inputs/typed-graph-model.umf.json')),'configurationSha256':sha(read(SOURCE/'inputs/configured-typed-graph-source.json')),'bindingSha256':sha(read(SOURCE/'inputs/typed-graph-binding.json')),'weftVersion':dist.version,'weftWheelSha256':WHEEL_SHA,'python':str(PYTHON),'javaHome':str(JAVA_HOME),'sparkHome':str(SPARK_HOME),'sparkLocalIp':SPARK_LOCAL_IP,'sourceBinding':{'root':str(SOURCE),'sourceCommit':'4c92be7fdbade0d58a8eb18d83965af0c79c8c6d','sourceJsonlSha256':sha(read(SOURCE/'inputs/typed-graph-source.jsonl')),'modelSha256':sha(read(SOURCE/'inputs/typed-graph-model.umf.json')),'configurationSha256':sha(read(SOURCE/'inputs/configured-typed-graph-source.json')),'bindingSha256':sha(read(SOURCE/'inputs/typed-graph-binding.json'))},'aliasNamespace':alias_namespace,'jarDir':'/private/tmp/ashlar-r16-delta4-native-smoke-20261029/attempt-84c7b6002712486495c5dd66bfa0b8d6/jars','groupsha':sha(read(ATTEMPT/'grouped.response.json')),'groupPositiveSha':sha(read(ATTEMPT/'grouped-positive.response.json')),'pagesha':sha(read(ATTEMPT/'page.response.json')),'continuationsha':sha(read(ATTEMPT/'continuation.response.json')),'state':'prepared-no-native-effects'}
    write_new(ATTEMPT/'prepared.json',canonical(manifest)); print(canonical(manifest).decode(),end='')

def run():
    import fcntl, signal, time
    if not (ATTEMPT/'prepared.json').is_file() or (ATTEMPT/'native-result.json').exists() or (ATTEMPT/'native-failure.json').exists(): raise RuntimeError('prepared unused one-shot native attempt required')
    prepared=json.loads(read(ATTEMPT/'prepared.json'))
    if not (ATTEMPT/'worker-import-preflight.json').is_file(): raise RuntimeError('effect-free Python worker import preflight required')
    pf=json.loads(read(ATTEMPT/'worker-import-preflight.json'))
    if pf.get('profile')!='ashlar-g03-python-worker-imports/0.1' or pf.get('python')!=str(PYTHON) or pf.get('pyspark')!='4.2.0' or sha(read(ATTEMPT/'worker-import-preflight.json'))!=prepared.get('workerPreflightSha256'): raise RuntimeError('Python worker import preflight identity/hash mismatch')
    if os.environ.get('PYSPARK_PYTHON')!=str(PYTHON) or os.environ.get('PYSPARK_DRIVER_PYTHON')!=str(PYTHON): raise RuntimeError('driver/worker interpreter differs from preflight')
    if os.environ.get('JAVA_HOME')!=str(JAVA_HOME) or os.environ.get('SPARK_HOME')!=str(SPARK_HOME) or os.environ.get('SPARK_LOCAL_IP')!=SPARK_LOCAL_IP: raise RuntimeError('Java/Spark environment differs from preflight')
    if prepared.get('javaHome')!=str(JAVA_HOME) or prepared.get('sparkHome')!=str(SPARK_HOME) or prepared.get('sourceBinding')!=pf.get('sourceBinding'): raise RuntimeError('prepared Java/Spark/source binding differs from preflight')
    if prepared.get('aliasNamespace')!=native_alias_namespace(prepared['vector']['publicationId']): raise RuntimeError('prepared Spark alias namespace differs from publication vector')
    import shutil
    if shutil.which('java')!=str(JAVA_HOME/'bin/java'): raise RuntimeError('PATH java differs from pinned JAVA_HOME')
    sb=prepared['sourceBinding']
    if sb.get('root')!=str(SOURCE) or sb.get('sourceCommit')!=prepared['sourceCommit'] or sha(read(SOURCE/'inputs/typed-graph-source.jsonl'))!=sb.get('sourceJsonlSha256') or sha(read(SOURCE/'inputs/typed-graph-model.umf.json'))!=sb.get('modelSha256') or sha(read(SOURCE/'inputs/configured-typed-graph-source.json'))!=sb.get('configurationSha256') or sha(read(SOURCE/'inputs/typed-graph-binding.json'))!=sb.get('bindingSha256'): raise RuntimeError('configured source binding drift before native call')
    sys.path.insert(0,str(ROOT/'src'))
    source_oracle=json.loads(read(ROOT/'docs/helix/04-build/evidence/current-upstream-release-20261029/g03-weft056-delta4-consumer-candidate/source-oracle.json'))
    vector=prepared['vector']; expected_vector=vector['canonicalSha256']
    vector_body={k:v for k,v in vector.items() if k!='canonicalSha256'}
    if sha(json.dumps(vector_body,sort_keys=True,separators=(',',':'),ensure_ascii=False).encode())!=expected_vector: raise RuntimeError('vector canonical digest mismatch')
    if sha(read(ATTEMPT/'grouped.response.json'))!=prepared['groupsha'] or sha(read(ATTEMPT/'grouped-positive.response.json'))!=prepared['groupPositiveSha'] or sha(read(ATTEMPT/'page.response.json'))!=prepared['pagesha'] or sha(read(ATTEMPT/'continuation.response.json'))!=prepared['continuationsha']: raise RuntimeError('frozen Weft compile artifact drift')
    if sha(read(CONT/'continuation-result.json'))!='118eaabb135ae4e5a1ba8f491cab2ebdef40dfe502740c7c53a3f8a124895e0f': raise RuntimeError('R5 continuation changed')
    inst=json.loads(read(SOURCE/'installation/installation.json')); inventory={x['table'].rsplit('.',1)[-1]:x for x in inst['targets']}
    expected_heads={'object_current':2,'edge_current':2,'tombstone':1,'whole_source_history':2,'attempts':3,'manifest':2}
    # Hold every existing local writer/installation lock throughout this read-only interval.
    lock_paths=[SOURCE/'installation/.finite-installation.lock',SOURCE/'installation/.configured-source-writer.lock',SOURCE/'installation/operations.sqlite.lock']
    lock_fds=[]; locks=[]; spark=None; aliases=[]; namespace=native_alias_namespace(vector['publicationId'])
    opening={}; primary=None; started=time.monotonic()
    previous_alarm_handler=signal.getsignal(signal.SIGALRM)
    def deadline(_signum, _frame): raise TimeoutError('bounded native read deadline reached')
    signal.signal(signal.SIGALRM,deadline); signal.alarm(360)
    try:
        for path in lock_paths:
            fd=os.open(path,os.O_RDONLY|os.O_NOFOLLOW|os.O_NONBLOCK); fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB); lock_fds.append(fd); locks.append((path.stat().st_ino,os.fstat(fd).st_ino))
        from pyspark.sql import SparkSession
        jar_dir=Path(prepared['jarDir']); jars=[jar_dir/n for n in ('delta-spark_4.2_2.13-4.4.1.jar','delta-storage-4.4.1.jar')]
        jar_hashes={p.name:sha(read(p)) for p in jars}
        from ashlar_host.configured_local_runtime_profile import JAR_SHA256, PARAMETER_SUBSTITUTION_SETTING
        if jar_hashes!=JAR_SHA256: raise RuntimeError('Delta jar identity mismatch')
        warehouse=ATTEMPT/'warehouse'; warehouse.mkdir(exist_ok=True)
        spark=(SparkSession.builder.master('local[1]').appName('Ashlar G03 Weft 0.4.56 native read')
            .config('spark.driver.memory','2g').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1')
            .config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC')
            .config(PARAMETER_SUBSTITUTION_SETTING,'true').config('spark.sql.ansi.enabled','true')
            .config('spark.sql.decimalOperations.allowPrecisionLoss','false').config('spark.jars',','.join(map(str,jars)))
            .config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension')
            .config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog')
            .config('spark.sql.warehouse.dir',str(warehouse)).getOrCreate())
        if spark.version!='4.2.0' or spark.sparkContext.master!='local[1]' or spark.conf.get('spark.driver.memory')!='2g' or spark.conf.get('spark.sql.ansi.enabled')!='true' or spark.conf.get('spark.sql.session.timeZone')!='UTC': raise RuntimeError('selected native runtime mismatch')
        if spark.conf.get('spark.sql.legacy.parameterSubstitution.constantsOnly')!='true': raise RuntimeError('named parameter substitution guard mismatch')
        role_paths={role:Path(inventory[role]['path']) for role in ('object_current','edge_current','tombstone','whole_source_history')}
        def head_and_id(role):
            lp=role_paths.get(role,Path(inventory[role]['path']))
            logs=sorted(p for p in (lp/'_delta_log').glob('*.json') if p.name[:20].isdigit())
            head=int(logs[-1].name[:20]); meta=None
            with (lp/'_delta_log'/'00000000000000000000.json').open('r',encoding='utf-8') as f:
                for line in f:
                    action=json.loads(line)
                    if 'metaData' in action: meta=action['metaData']; break
            if not meta: raise RuntimeError('Delta table metadata absent')
            if meta.get('id')!=inventory[role]['uuid'] or head!=expected_heads[role]: raise RuntimeError('six-role identity/head mismatch')
            return {'uuid':meta['id'],'version':head,'logHeadSha256':sha(read(logs[-1]))}
        opening={r:head_and_id(r) for r in expected_heads}
        for role in ROLES:
            v=vector['dataRoles'][role]
            if (v['uuid'],v['version'])!=(opening[role]['uuid'],opening[role]['version']): raise RuntimeError('realized four-role vector drift')
        # Read-only Delta snapshot proof, selecting the committed manifest and full vector before alias creation.
        manifest_path=Path(inventory['manifest']['path'])
        mrow=spark.read.format('delta').option('versionAsOf',expected_heads['manifest']).load(str(manifest_path)).select('publication_id','profile_version','table_versions_json','source_progress_json','schema_revisions_json','validation_report_json').collect()
        if len(mrow)!=1: raise RuntimeError('exact single retained manifest row required')
        manifest_row=mrow[0].asDict()
        expected_manifest=retained_manifest=json.loads(read(CONT/'continuation-result.json'))['manifest']
        if manifest_row['publication_id']!=expected_manifest['publication_id'] or manifest_row['profile_version']!='ashlar-delta/0.4-xxhash64' or json.loads(manifest_row['table_versions_json'])!={f'local.configured.{k}':v for k,v in (('object_current',2),('edge_current',2),('tombstone',1),('whole_source_history',2))} or json.loads(manifest_row['source_progress_json'])!=json.loads(expected_manifest['source_progress_json']) or json.loads(manifest_row['schema_revisions_json'])!={'typed-graph':'1'}: raise RuntimeError('manifest binding differs from R5 complete vector')
        spark.sql('CREATE DATABASE `spark_catalog`.`'+namespace+'`').collect()
        for role,path in role_paths.items():
            name='spark_catalog.'+namespace+'.'+role
            spark.sql('CREATE TABLE `'+name.replace('.','`.`')+'` USING DELTA LOCATION '+"'"+str(path)+"'").collect(); aliases.append(name)
            detail=spark.sql('DESCRIBE DETAIL `'+name.replace('.','`.`')+'`').first().asDict()
            assert_delta_alias_identity(detail,inventory[role]['uuid'],path)
            actual=spark.read.format('delta').option('versionAsOf',expected_heads[role]).load(str(path)).schema
            expected=[(x[0],x[1].upper()) for x in inst['columns'][role]]
            observed=[(f.name,f.dataType.simpleString().upper()) for f in actual.fields]
            if observed!=expected: raise RuntimeError('native Delta schema differs from pinned installation inventory')
        opening['database']=spark.catalog.currentDatabase(); opening['applicationId']=spark.sparkContext.applicationId
        # Singleton: exact retained payload text is read unchanged; numeric property remains JSON text.
        objects=spark.read.format('delta').option('versionAsOf',2).load(str(role_paths['object_current']))
        srows=objects.where("source_system='typed-graph-source' AND type_id=17 AND id=1").select('source_system','type_id','id','props_json','retained_json').collect()
        singleton=[] if not srows else [r.asDict() for r in srows]
        exp=source_oracle['queryExpected']['singletonPerson1']
        if len(singleton)!=1 or singleton[0]['source_system']!=exp['identity']['sourceSystem'] or singleton[0]['type_id']!=exp['identity']['typeId'] or singleton[0]['id']!=exp['identity']['entityId'] or singleton[0]['props_json']!=exp['propertyJsonText'] or singleton[0]['retained_json']!=exp['retainedJsonText']: raise RuntimeError('native singleton differs from exact source oracle')
        if '9007199254740993' not in singleton[0]['props_json']: raise RuntimeError('singleton exact INTEGER lexical token drift')
        def run_group(filename):
            req=json.loads(read(ATTEMPT/(filename+'.request.json'))); resp=json.loads(read(ATTEMPT/(filename+'.response.json'))); sql=resp['sql']
            if assert_compiled_alias_binding(req,resp,('object_current','edge_current'))!=namespace: raise RuntimeError('Weft grouped SQL alias differs from registered namespace')
            params={f'p{x["position"]}':x['value'] for x in resp['parameters']}
            rows=[list(r) for r in spark.sql(sql,args=params).collect()]
            expected=source_oracle['queryExpected']['backend03OriginalHavingGreaterThanZero' if filename=='grouped' else 'backend03PositiveHavingGreaterThanOne']
            normalized=[[r[0],int(r[1]),int(r[2])] for r in rows]
            if normalized!=expected: raise RuntimeError('native Weft grouped result differs from exact source oracle')
            return {'requestSha256':sha(read(ATTEMPT/(filename+'.request.json'))),'responseSha256':sha(read(ATTEMPT/(filename+'.response.json'))),'sqlSha256':sha(sql.encode()),'rows':normalized}
        groups={name:run_group(name) for name in ('grouped','grouped-positive')}
        page_req=json.loads(read(ATTEMPT/'page.request.json')); page1=json.loads(read(ATTEMPT/'page.response.json')); continuation=json.loads(read(ATTEMPT/'continuation.response.json'))
        if assert_compiled_alias_binding(page_req,page1,('object_current',))!=namespace: raise RuntimeError('Weft page SQL alias differs from registered namespace')
        def execute_page(program):
            artifact=program['artifact']; params={f'p{x["position"]}':x['value'] for x in artifact.get('parameters',[])}
            rows=[list(r) for r in spark.sql(artifact['sql'],args=params).collect()]
            visible=program['page']['visibleOutputs']; return rows, [r[:visible] for r in rows], params
        raw1, visible1, _=execute_page(page1); expected_page1=source_oracle['queryExpected']['backend02Page1']
        assert_page_one_lookahead(visible1,expected_page1,source_oracle['queryExpected']['backend02Page2'],source_oracle['queryExpected']['backend02ContinuationAfter'])
        raw2, visible2, _=execute_page(continuation)
        if visible2!=source_oracle['queryExpected']['backend02Page2']: raise RuntimeError('native Weft continuation differs from exact source oracle')
        # Exact current full vector and table heads after all buffered reads.
        closing={r:head_and_id(r) for r in expected_heads}
        assert_closing_snapshot(opening,closing,expected_heads,spark.catalog.currentDatabase(),spark.sparkContext.applicationId)
        if len(lock_fds)!=3 or any(os.fstat(fd).st_ino!=lock_paths[i].stat().st_ino or os.fstat(fd).st_ino!=locks[i][0] for i,fd in enumerate(lock_fds)): raise RuntimeError('local lock custody drift')
        result={'profile':'ashlar-g03-weft-native-consumer-result/0.1','status':'complete','sourceCommit':prepared['sourceCommit'],'packetCommit':prepared['packetCommit'],'sourceOracleSha256':prepared['sourceOracleSha256'],'vectorSha256':expected_vector,'publicationId':vector['publicationId'],'manifestSha256':vector['manifestSha256'],'openingSixRoleHeads':{r:opening[r] for r in expected_heads},'singleton':{'identity':exp['identity'],'propsJsonText':singleton[0]['props_json'],'retainedJsonText':singleton[0]['retained_json'],'quantityJsonText':exp['quantityJsonText']},'grouped':groups,'page1':{'requestSha256':sha(read(ATTEMPT/'page.request.json')),'responseSha256':prepared['pagesha'],'rows':visible1[:2],'lookahead':visible1[2:]},'continuation':{'responseSha256':prepared['continuationsha'],'after':source_oracle['queryExpected']['backend02ContinuationAfter'],'rows':visible2},'closingSixRoleHeads':closing,'runtime':{'spark':spark.version,'master':spark.sparkContext.master,'driverMemory':spark.conf.get('spark.driver.memory'),'database':opening['database'],'applicationId':spark.sparkContext.applicationId,'python':str(PYTHON),'weft':'0.4.56','weftWheelSha256':WHEEL_SHA,'deltaJars':jar_hashes,'elapsedSeconds':round(time.monotonic()-started,3)},'publicationMutation':False,'protectedAck':False,'cloud':False,'qualification':'One local Spark 4.2.0 / Delta 4.4.1 read-only native observation of this SDK 0.10.1 / Core 0.10.5 `.4-xxhash64` installation. It includes the local immutable-file typed-graph source and complete ordinal-1 manifest/vector; no Truss, protected ACK, managed UC or production-source authority is qualified.'}
    except BaseException as error: primary=error
    # Cleanup attempts run even on a primary failure; Delta table data and logs are never mutated.
    cleanup=[]
    if spark is not None:
        for name in reversed(aliases):
            try: spark.sql('DROP TABLE `'+name.replace('.','`.`')+'`').collect(); cleanup.append({'op':'drop-alias','status':'ok'})
            except BaseException: cleanup.append({'op':'drop-alias','status':'failed'})
        try: spark.sql('DROP DATABASE `spark_catalog`.`'+namespace+'`').collect(); cleanup.append({'op':'drop-database','status':'ok'})
        except BaseException: cleanup.append({'op':'drop-database','status':'failed'})
        try: spark.stop(); cleanup.append({'op':'spark-stop','status':'ok'})
        except BaseException: cleanup.append({'op':'spark-stop','status':'failed'})
    for fd in reversed(lock_fds):
        try: os.close(fd); cleanup.append({'op':'release-lock','status':'ok'})
        except BaseException: cleanup.append({'op':'release-lock','status':'failed'})
    signal.alarm(0); signal.signal(signal.SIGALRM,previous_alarm_handler)
    if primary is not None or any(x['status']!='ok' for x in cleanup):
        if primary is None:
            failure_type='CleanupError'; failure_message='one or more Spark/alias/lock cleanup actions failed'; trace=''; truncated=False
        else:
            failure_type,failure_message,trace,truncated=bounded_exception_details(primary)
        diagnostics=getattr(primary,'diagnostics',{}) if primary is not None else {}
        failure={'profile':'ashlar-g03-weft-native-failure/0.1','status':'failed-result-withheld','sourceCommit':prepared['sourceCommit'],'packetCommit':prepared['packetCommit'],'exceptionType':failure_type,'exceptionMessage':failure_message,'diagnostics':diagnostics,'tracebackUtf8Tail':trace,'tracebackTruncated':truncated,'cleanup':cleanup,'nativeResult':'withheld','publicationMutation':False,'protectedAck':False,'cloud':False}
        write_new(ATTEMPT/'native-failure.json',canonical(failure))
        raise RuntimeError(failure_type+': native result withheld; see bounded native-failure.json') from None
    result['cleanup']=cleanup; result['elapsedSeconds']=round(time.monotonic()-started,3)
    write_new(ATTEMPT/'native-result.json',canonical(result)); print(canonical(result).decode(),end='')

if __name__=='__main__': main()
