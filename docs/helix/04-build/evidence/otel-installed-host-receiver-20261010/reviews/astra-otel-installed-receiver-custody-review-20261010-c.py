from pathlib import Path
import hashlib,json,stat,sys
from importlib import metadata
from jsonschema import Draft202012Validator
from referencing import Registry,Resource
ROOT=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-c')
B=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-b')
REPO=Path('/Users/erik/Projects/ashlar')
def sha(x):return hashlib.sha256(x).hexdigest()
def d(p):
 p=Path(p);x=p.read_bytes();return {'path':str(p),'bytes':len(x),'sha256':sha(x)}
def block(event,args):
 if event in ('subprocess.Popen','socket.connect','socket.bind','socket.getaddrinfo','os.system'):raise AssertionError('unexpected effect:'+event)
sys.addaudithook(block)
opening=(ROOT/'opening-inputs.json').read_bytes();assert (ROOT/'closing-inputs.json').read_bytes()==opening
pins=json.loads(opening)['files'];assert len(pins)==900 and len({r['path'] for r in pins})==900
for row in pins:assert d(row['path'])==row
process=json.loads((ROOT/'process.json').read_bytes());assert process['exitCode']==0 and process['closingCustodyPassed'] is True and process['selectedInputCount']==900
assert process['execution']=='direct-foreground-selected-interpreter-no-outer-wrapper'
assert process['commandSha256']==d(ROOT/'command.json')['sha256']=='63dcbd893077164759ab3a9074d24ca6d01e531c0dcdace78c00ace642cccae8'
assert (ROOT/'process.stdout').read_bytes()==b''
assert (ROOT/'process.stderr').read_bytes()==b'Operation started\nOperation phase observed\nOperation finished\n'*2
result=json.loads((ROOT/'result.json').read_bytes());assert result['outcome']=='passed' and result['receiverThreadClosed'] is True and result['durationSeconds']<2
observations=json.loads((ROOT/'receiver-observations.json').read_bytes());assert observations['state']=='provisional' and observations['receiverFailures']==[] and observations['requests']==result['requests']
requests=result['requests'];assert len(requests)==9
assert [sum(r['path']==p for r in requests) for p in ('/v1/logs','/v1/traces','/v1/metrics')]==[6,2,1]
for n,row in enumerate(requests):
 assert row['file']=='request-%02d.pb'%n
 assert d(ROOT/row['file'])=={'path':str(ROOT/row['file']),'bytes':row['bytes'],'sha256':row['sha256']}
 assert 0<row['bytes']<=65536 and stat.S_IMODE((ROOT/row['file']).stat().st_mode)==0o600
assert sum(r['bytes'] for r in requests)<=1048576
folders=list((ROOT/'capture').iterdir());assert len(folders)==1 and folders[0].is_dir();run_dir=folders[0]
assert all(stat.S_IMODE(p.stat().st_mode)==0o700 for p in (ROOT/'capture',run_dir))
manifest=json.loads((run_dir/'run.json').read_bytes());expected=json.loads((ROOT/'expected.json').read_bytes())
assert manifest['state']=='closed' and manifest['complete'] is True and manifest['run_id']==run_dir.name
assert set(p.name for p in run_dir.iterdir())=={'run.json','events-0000.jsonl'}
assert manifest['segments']==[{'source':'events-0000.jsonl','bytes':5005,'records':6,'sha256':d(run_dir/'events-0000.jsonl')['sha256']}]
events=[json.loads(x) for x in (run_dir/'events-0000.jsonl').read_bytes().splitlines()]
assert len(events)==6 and events==expected['events']
assert (run_dir/'events-0000.jsonl').read_bytes().endswith(b'\n')
assert all(len(x)+1<=4096 for x in (run_dir/'events-0000.jsonl').read_bytes().splitlines())
for p in run_dir.iterdir():assert stat.S_IMODE(p.stat().st_mode)==0o600 and not p.is_symlink()
resource={'service.name':'ashlar-host','service.version':'0.1.0.dev0','deployment.environment.name':'test','telemetry.sdk.name':'opentelemetry','telemetry.sdk.language':'python','telemetry.sdk.version':'1.45.1'}
assert manifest['resource']==expected['resource']==resource
assert manifest['scope']=={'name':'ashlar.host.diagnostics','version':'0.1.0'}
assert manifest['loss']['local_attempted']==manifest['loss']['local_written']==6 and manifest['loss']['local_dropped']==0 and manifest['loss']['local_unknown'] is False
for signal,count in (('logs',6),('spans',2),('metrics',2)):
 assert manifest['loss'][signal]=={'submitted':count,'handed_off':count,'dropped':0,'unknown':False,'flush':'complete'}
assert int(manifest['expires_unix_nano'])-int(manifest['ended_unix_nano'])==3600*10**9
assert len(manifest['attempts'])==2 and len({a['attempt_id'] for a in manifest['attempts']})==2
for i,event in enumerate(events):
 assert event['resource']==resource and event['scope']==manifest['scope']
 attrs=event['attributes'];attempt=manifest['attempts'][i//3]
 assert attrs['ashlar.sequence']==i+1 and attrs['ashlar.run.id']==manifest['run_id']
 assert attrs['ashlar.attempt.id']==attempt['attempt_id'] and attrs['ashlar.operation']==attempt['operation'] and attrs['ashlar.emitter.id']=='host'
 assert event['event_name']==['ashlar.operation.started','ashlar.operation.phase','ashlar.operation.finished'][i%3]
 assert int(manifest['started_unix_nano'])<=int(event['observed_timestamp_unix_nano'])<=int(manifest['ended_unix_nano'])
 assert event['trace']==events[i//3*3]['trace'] and event['trace']['trace_flags']==3
 assert 'timestamp_unix_nano' not in event
assert events[2]['attributes']['ashlar.outcome']=='succeeded' and events[5]['attributes']['ashlar.outcome']=='refused'
assert events[5]['attributes']['ashlar.error.category']=='guard'
assert events[0]['trace']['trace_id']==expected['parent']['trace_id'] and events[0]['trace']['span_id']!=expected['parent']['span_id']
assert events[3]['trace']['trace_id'] not in (expected['parent']['trace_id'],expected['retry_link']['trace_id'])
assert events[3]['trace']['span_id'] not in (events[0]['trace']['span_id'],expected['retry_link']['span_id'])
schemas=[]
for name in ('event','run'):
 p=REPO/('docs/helix/02-design/contracts/schemas/diagnostic-'+name+'-v0.1.schema.json');schemas.append((p,json.loads(p.read_bytes())))
def retrieve(uri):raise AssertionError('network/schema retrieval forbidden')
registry=Registry(retrieve=retrieve).with_resources((schema['$id'],Resource.from_contents(schema)) for p,schema in schemas)
for p,schema in schemas:Draft202012Validator.check_schema(schema)
for event in events:Draft202012Validator(schemas[0][1],registry=registry).validate(event)
Draft202012Validator(schemas[1][1],registry=registry).validate(manifest)
expected_files={'candidate.py','command.json','opening-inputs.json','closing-inputs.json','root-runner.py','process.json','process.stdout','process.stderr','expected.json','receiver-observations.json','result.json'}|{r['file'] for r in requests}|{'capture/'+run_dir.name+'/'+n for n in ('run.json','events-0000.jsonl')}
actual_files={str(p.relative_to(ROOT)) for p in ROOT.rglob('*') if p.is_file()};assert actual_files==expected_files,(actual_files-expected_files,expected_files-actual_files)
assert not list(ROOT.rglob('*.tmp')) and not (ROOT/'.result-stage.json').exists()
inventory=[d(ROOT/name) for name in sorted(actual_files)]
prior=json.loads((B/'process.json').read_bytes());assert prior['exitCode']==1 and prior['closingCustodyPassed'] is True
assert json.loads((B/'receiver-observations.json').read_bytes())=={'state':'provisional','requests':[],'receiverFailures':[]}
stderr=(B/'process.stderr').read_text();assert 'server = Server' in stderr and 'self.socket.bind' in stderr and 'PermissionError: [Errno 1] Operation not permitted' in stderr
assert not (B/'capture').exists() and not (B/'result.json').exists() and not list(B.glob('request-*.pb'))
for row in pins:assert d(row['path'])==row
for row in inventory:assert d(row['path'])==row
receipt={'verdict':'approved-actual-custody-and-local-capture','scope':'Actual2synthetic sequential installed host invocations; no receiver/worker rerun by reviewer. Independent protobuf wire review is separate.','selectedInputCount':900,'selectedOpeningClosingEqual':True,'process':d(ROOT/'process.json'),'stdout':d(ROOT/'process.stdout'),'stderr':d(ROOT/'process.stderr'),'result':d(ROOT/'result.json'),'outputFiles':len(inventory),'outputInventory':inventory,'requestCount':9,'requestBodyBytes':sum(r['bytes'] for r in requests),'capture':{'manifest':d(run_dir/'run.json'),'segment':d(run_dir/'events-0000.jsonl'),'events':6,'attempts':2,'originalExpectedEquality':True,'closedComplete':True,'schemaChecks':7,'schemas':[d(p) for p,s in schemas],'validatorVersions':{'jsonschema':metadata.version('jsonschema'),'referencing':metadata.version('referencing')}} ,'priorSandboxRefusal':{'process':d(B/'process.json'),'stderr':d(B/'process.stderr'),'observations':d(B/'receiver-observations.json'),'beforeWorkerOrCapture':True},'limitations':['Synthetic held-read/publication labels do not execute native query/publication or exercise source/ACK authority.','Handed_off counts are local transport completion; this finite review also has exact receiver bytes but makes no durable remote delivery guarantee.','No concurrent/spanless/outage/deadline/privacy-sentinel or investigator pilot claim.','No hostile local peer/abrupt host death/global runtime closure claim; process census was not performed by reviewer.'],'script':d(__file__)}
out=Path('/private/tmp/astra-otel-installed-receiver-custody-review-20261010-c.json');out.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(d(out)))
