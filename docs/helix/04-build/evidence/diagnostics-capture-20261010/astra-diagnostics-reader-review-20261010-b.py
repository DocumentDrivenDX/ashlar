"""Independent frozen diagnostics readers: actual config, offline schemas, local snapshots."""
import copy
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import socket
import sys
import tempfile
import time
import types
from unittest.mock import patch
from uuid import uuid4
from jsonschema import Draft202012Validator
from referencing import Registry, Resource

REPO=Path('/Users/erik/Projects/ashlar')
FREEZE=Path('/private/tmp/ashlar-diagnostics-source-freeze-20261010-b')
BASE=Path(tempfile.mkdtemp(prefix='astra-diagnostics-reader-20261010-b-',dir='/private/tmp'));BASE.chmod(0o700)
ROWS=[]
def digest(path):
    raw=Path(path).read_bytes();return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
def row(test,outcome='pass',**detail):
    value={'test':test,'outcome':outcome,**detail};ROWS.append(value);print(json.dumps(value,sort_keys=True),flush=True)
def thrown(call):
    try:return None,call()
    except BaseException as error:return error,None
manifest=json.loads((FREEZE/'manifest.json').read_text())
assert digest(FREEZE/'manifest.json')['sha256']=='69b7188192715e5812a93b24d8f14e6e5500f7ac469c2b57334f6bc58bb76110'
for value in manifest['files']:
    actual=digest(FREEZE/value['path']);assert actual['bytes']==value['bytes'] and actual['sha256']==value['sha256']
for value in json.loads(Path('/private/tmp/ashlar-diagnostics-source-freeze-20261010-a/manifest.json').read_text())['governing']:assert digest(REPO/value['path'])['sha256']==value['sha256']
row('exact_freeze_source_contract_schema_config_hashes')
# Namespace packages avoid importing unrelated owners; real config and its real pure dependencies load normally.
for name in ['ashlar','ashlar_host']:
    package=types.ModuleType(name);package.__path__=[str(REPO/'src'/name)];sys.modules[name]=package
import ashlar_host.config as actual_config
spec=importlib.util.spec_from_file_location('ashlar_host.diagnostics',FREEZE/'src/ashlar_host/diagnostics.py')
d=importlib.util.module_from_spec(spec);sys.modules[spec.name]=d;spec.loader.exec_module(d)
assert d.DiagnosticsLimits is actual_config.DiagnosticsLimits
schemas=REPO/'docs/helix/02-design/contracts/schemas'
es=json.loads((schemas/'diagnostic-event-v0.1.schema.json').read_text());ms=json.loads((schemas/'diagnostic-run-v0.1.schema.json').read_text())
registry=Registry().with_resources([(es['$id'],Resource.from_contents(es)),(ms['$id'],Resource.from_contents(ms))])
ev=Draft202012Validator(es,registry=registry);mv=Draft202012Validator(ms,registry=registry)
RESOURCE={'service.name':'ashlar-host','service.version':'0.1.0.dev0','deployment.environment.name':'test','telemetry.sdk.name':'opentelemetry','telemetry.sdk.language':'python','telemetry.sdk.version':'1.45.1'}
LIMITS={'max_event_bytes':4096,'max_queue_records':128,'max_queue_bytes':524288,'max_segment_bytes':4096,'max_capture_bytes':16384,'max_emissions':10000,'max_attempts':64,'export_timeout_ms':100,'shutdown_timeout_ms':200,'retention_seconds':60}
ORIGINS={key:'explicit' for key in ['profile','capture_root','endpoint','headers','tls','ca_file','environment']+['limits.'+k for k in LIMITS]}
ATTEMPT='2'*32
NOW=time.time_ns()
def event(kind,seq,run_id='1'*32,attempt=ATTEMPT):
    name='ashlar.operation.'+kind
    attrs={'ashlar.run.id':run_id,'ashlar.attempt.id':attempt,'ashlar.emitter.id':'host','ashlar.sequence':seq,'ashlar.operation':'held-read'}
    if kind=='phase':attrs.update({'ashlar.phase':'guard','ashlar.phase.state':'completed'})
    if kind=='finished':attrs.update({'ashlar.outcome':'succeeded','ashlar.cleanup_failed':False})
    return {'schema_version':'ashlar.diagnostic.event/0.1','observed_timestamp_unix_nano':str(NOW),'severity_text':'INFO','severity_number':9,'event_name':name,'body':{'started':'Operation started','phase':'Operation phase observed','finished':'Operation finished'}[kind],'resource':copy.deepcopy(RESOURCE),'scope':{'name':'ashlar.host.diagnostics','version':'0.1.0'},'attributes':attrs}
def encode(value):return (json.dumps(value,separators=(',',':'),ensure_ascii=True)+'\n').encode()
def write(path,raw):path.write_bytes(raw);path.chmod(0o600)
class Snapshot:
    def __init__(self,events,*,complete=True,dropped=0,attempts=None):
        self.path=BASE/uuid4().hex;self.path.mkdir(mode=0o700)
        self.events=copy.deepcopy(events)
        for value in self.events:value['attributes']['ashlar.run.id']=self.path.name
        raw=b''.join(map(encode,self.events));self.raw=raw
        self.manifest={'schema_version':'ashlar.diagnostic.run/0.1','profile':'ashlar-host-otel-http/0.1','run_id':self.path.name,'state':'closed','started_unix_nano':str(NOW-1000),'ended_unix_nano':str(NOW),'expires_unix_nano':str(NOW+60000000000),'resource':copy.deepcopy(RESOURCE),'scope':{'name':'ashlar.host.diagnostics','version':'0.1.0'},'attempts':attempts if attempts is not None else ([{'attempt_id':ATTEMPT,'operation':'held-read'}] if events else []),'limits':copy.deepcopy(LIMITS),'configuration_origins':copy.deepcopy(ORIGINS),'sampling':{'logs':'all','new_spans':'always_on','metrics':'all'},'segments':[],'loss':{'local_attempted':len(events)+dropped,'local_written':len(events),'local_dropped':dropped,'local_unknown':False,**{signal:{'submitted':0,'handed_off':0,'dropped':0,'unknown':False,'flush':'complete'} for signal in ['logs','spans','metrics']}},'complete':complete}
        if raw:
            write(self.path/'events-0000.jsonl',raw)
            self.manifest['segments']=[{'source':'events-0000.jsonl','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),'records':len(events)}]
        self.save()
    def save(self):write(self.path/'run.json',encode(self.manifest))
    def read(self,**kwargs):return d.read_diagnostics(self.path,**kwargs)
    def replace_records(self,raw,records):
        write(self.path/'events-0000.jsonl',raw);self.manifest['segments'][0].update(bytes=len(raw),sha256=hashlib.sha256(raw).hexdigest(),records=records);self.save()
    def schema_valid(self):
        mv.validate(self.manifest)
        for value in self.events:ev.validate(value)

with patch.object(socket.socket,'connect',side_effect=AssertionError('NETWORK FORBIDDEN')),patch.object(socket.socket,'connect_ex',side_effect=AssertionError('NETWORK FORBIDDEN')),patch.object(socket,'create_connection',side_effect=AssertionError('NETWORK FORBIDDEN')):
    baseline=Snapshot([event('started',1),event('phase',2),event('finished',3)])
    baseline.schema_valid();d.validate_manifest(baseline.manifest)
    for value in baseline.events:d.validate_event(value)
    result=baseline.read(limit=2)
    assert result['matched']==3 and result['truncated'] and len(result['records'])==2 and result['capture_complete']
    filtered=baseline.read(event_name='ashlar.operation.finished',min_severity=13)
    assert filtered['matched']==0 and not filtered['truncated']
    row('baseline_real_schemas_and_bounded_conjunctive_filters')

    for complete in [True,False]:
        s=Snapshot([event('started',1),event('finished',2)],complete=complete,dropped=0 if complete else 1)
        write(s.path/'events-0001.jsonl',b'UNDECLARED PRIVATE SENTINEL AND PARTIAL JSON {')
        if complete:
            error,result=thrown(s.read);assert isinstance(error,d.DiagnosticsError) and str(error)=='diagnostics-snapshot-invalid' and result is None
            row('DR-001_complete_undeclared_partial_refuses')
        else:
            result=s.read();assert result['capture_complete'] is False and result['matched']==2
            assert 'SENTINEL' not in json.dumps(result)
            row('undeclared_partial_under_incomplete_loss_preserved')

    for complete in [True,False]:
        s=Snapshot([event('phase',1),event('started',2),event('finished',3)],complete=complete,dropped=0 if complete else 1)
        s.schema_valid();error,result=thrown(s.read)
        assert isinstance(error,d.DiagnosticsError) and str(error)=='diagnostics-snapshot-invalid' and result is None
        row('DR-002_phase_before_started_refuses_complete_'+str(complete))

    # Preserve valid loss: a missing started record can be explained by a dropped sequence.
    s=Snapshot([event('phase',2),event('finished',3)],complete=False,dropped=1);s.schema_valid()
    assert s.read()['matched']==2
    row('missing_start_with_declared_loss_is_admitted')

    mutations=[('unknown_event_key',lambda e:e.update(secret='SENTINEL')),
      ('unknown_attribute',lambda e:e['attributes'].update(secret='SENTINEL')),
      ('unknown_resource',lambda e:e['resource'].update(secret='SENTINEL')),
      ('unknown_scope',lambda e:e['scope'].update(secret='SENTINEL')),
      ('boolean_sequence',lambda e:e['attributes'].update({'ashlar.sequence':True})),
      ('wrong_body',lambda e:e.update(body='SENTINEL')),
      ('wrong_run',lambda e:e['attributes'].update({'ashlar.run.id':'f'*32})),
      ('wrong_attempt',lambda e:e['attributes'].update({'ashlar.attempt.id':'f'*32})),
      ('wrong_operation',lambda e:e['attributes'].update({'ashlar.operation':'publication'})),
      ('resource_drift',lambda e:e['resource'].update({'service.version':'2.0'})),
      ('duplicate_sequence',lambda e:e['attributes'].update({'ashlar.sequence':1}))]
    for name,mutate in mutations:
        s=Snapshot([event('started',1),event('finished',2)]);mutate(s.events[1]);s.replace_records(b''.join(map(encode,s.events)),2)
        error,result=thrown(s.read);assert isinstance(error,d.DiagnosticsError) and str(error)=='diagnostics-snapshot-invalid',(name,type(error),str(error))
        row('reject_'+name,fixedError=True)

    for name,raw in [('duplicate_members',b'{"x":1,"x":2}\n'),('integer_21_digits',b'{"x":123456789012345678901}\n'),('nesting_13',b'{"x":'+b'['*12+b'0'+b']'*12+b'}\n'),('invalid_utf8',b'{"x":"\xff"}\n'),('nonfinite',b'{"x":NaN}\n'),('float',b'{"x":1.2}\n')]:
        s=Snapshot([event('started',1)]);s.replace_records(raw,1)
        error,_=thrown(s.read);assert isinstance(error,d.DiagnosticsError) and str(error)=='diagnostics-snapshot-invalid'
        row('reject_'+name)

    for name in ['unexpected_file','undeclared_oversize','manifest_oversize','unsafe_permissions','symlink_segment']:
        s=Snapshot([event('started',1),event('finished',2)])
        if name=='unexpected_file':write(s.path/'foreign.txt',b'SENTINEL')
        if name=='undeclared_oversize':write(s.path/'events-0001.jsonl',b'x'*4097)
        if name=='manifest_oversize':write(s.path/'run.json',b' '*65537)
        if name=='unsafe_permissions':(s.path/'events-0000.jsonl').chmod(0o644)
        if name=='symlink_segment':
            target=BASE/(s.path.name+'.external');write(target,s.raw);(s.path/'events-0000.jsonl').unlink();(s.path/'events-0000.jsonl').symlink_to(target)
        error,_=thrown(s.read);assert isinstance(error,d.DiagnosticsError) and str(error)=='diagnostics-snapshot-invalid'
        row('reject_'+name)

    for name,mutate in [('local_sum',lambda m:m['loss'].update(local_attempted=3)),('written_count',lambda m:m['loss'].update(local_attempted=3,local_written=3)),('complete_with_loss',lambda m:m['loss'].update(local_attempted=3,local_dropped=1)),('export_sum',lambda m:m['loss']['logs'].update(submitted=2,handed_off=1)),('null_without_unknown',lambda m:m['loss'].update(local_attempted=None)),('unknown_manifest_key',lambda m:m.update(secret='SENTINEL')),('boolean_limit',lambda m:m['limits'].update(max_emissions=True)),('origin_private_payload',lambda m:m['configuration_origins'].update(endpoint='SENTINEL')),('duplicate_attempt_identity',lambda m:m['attempts'].append({'attempt_id':ATTEMPT,'operation':'publication'}))]:
        s=Snapshot([event('started',1),event('finished',2)]);mutate(s.manifest);s.save()
        error,_=thrown(s.read);assert isinstance(error,d.DiagnosticsError) and str(error)=='diagnostics-snapshot-invalid'
        row('reject_'+name)

    # Differential shape check of ordinary JSON values against the exact offline schemas.
    def leaf_paths(value,prefix=()):
        if type(value) is dict:
            for key,child in value.items():yield from leaf_paths(child,prefix+(key,))
        elif type(value) is list:
            for index,child in enumerate(value):yield from leaf_paths(child,prefix+(index,))
        else:yield prefix
    count=0;false_accept=[]
    candidates=[None,False,True,0,1,9,-1,1.5,'SENTINEL',[],{},['SENTINEL']]
    for kind,original,manual,schema in [('event',baseline.events[1],d.validate_event,ev),('manifest',baseline.manifest,d.validate_manifest,mv)]:
        for path in leaf_paths(original):
            for candidate in candidates:
                value=copy.deepcopy(original);cursor=value
                for key in path[:-1]:cursor=cursor[key]
                cursor[path[-1]]=candidate
                error,_=thrown(lambda:manual(value));shape=schema.is_valid(value);count+=1
                if error is None and not shape:false_accept.append({'kind':kind,'path':list(path),'candidate':candidate})
    assert false_accept==[],false_accept
    row('manual_validators_no_schema_false_accepts_for_json_primitive_mutations',mutations=count)

    # Filtered-out records still receive full validation; no partial result escapes.
    s=Snapshot([event('started',1),event('finished',2)]);s.events[1]['body']='SENTINEL';s.replace_records(b''.join(map(encode,s.events)),2)
    error,result=thrown(lambda:s.read(limit=1,event_name='ashlar.operation.started'))
    assert isinstance(error,d.DiagnosticsError) and result is None
    row('filtered_out_invalid_record_refuses_whole_response')

imports=[]
for name,module in sys.modules.copy().items():
    file=getattr(module,'__file__',None)
    if file and str(file).startswith(str(REPO/'src')):imports.append({'module':name,**digest(file)})
for value in json.loads(Path('/private/tmp/ashlar-diagnostics-source-freeze-20261010-a/manifest.json').read_text())['governing']:assert digest(REPO/value['path'])['sha256']==value['sha256']
assert digest(FREEZE/'src/ashlar_host/diagnostics.py')['sha256']=='29fabc05ad9537a852d470a7fe68d35e67fc95382244dab10c236fe09af59677'
result={'rows':ROWS,'fixtures':str(BASE),'realDependencies':imports,'source':digest(FREEZE/'src/ashlar_host/diagnostics.py'),'manifest':digest(FREEZE/'manifest.json'),'schemas':[digest(schemas/'diagnostic-event-v0.1.schema.json'),digest(schemas/'diagnostic-run-v0.1.schema.json')],'noSDK':not any(name.startswith(('opentelemetry','pyspark','pg8000')) for name in sys.modules)}
assert result['noSDK']
path=Path('/private/tmp/astra-diagnostics-reader-review-20261010-b-data.json')
with path.open('x') as stream:json.dump(result,stream,indent=2,sort_keys=True);stream.write('\n')
row('review_complete',cases=len(ROWS),falseSchemaAccepts=0,blockerReproductions=sum(value['outcome']=='blocker-reproduced' for value in ROWS),noSDK=True,fixtures=str(BASE),data=str(path))
