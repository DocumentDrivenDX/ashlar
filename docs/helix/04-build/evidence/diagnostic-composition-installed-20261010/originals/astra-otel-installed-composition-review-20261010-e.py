from pathlib import Path
from importlib import metadata
import hashlib,json,stat,sys
from opentelemetry.proto.collector.logs.v1.logs_service_pb2 import ExportLogsServiceRequest
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import ExportTraceServiceRequest
from opentelemetry.proto.collector.metrics.v1.metrics_service_pb2 import ExportMetricsServiceRequest
R=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-e')
def d(p):
 b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def verify(row):
 actual=d(Path(row['path']));assert actual==row;return actual
def load(n):return json.loads((R/n).read_text())
opening=load('opening-inputs.json');closing=load('closing-inputs.json');assert opening==closing
assert len(opening['files'])==925 and len({r['path'] for r in opening['files']})==925
for row in opening['files']:verify(row)
process=load('process.json');assert process['exitCode']==0 and process['terminalChunk']=='8a3da4'
for key in ('command','stdout','stderr','opening','closing','candidateResult'):verify(process[key])
assert (R/'process.stdout').read_bytes()==b''
assert (R/'process.stderr').read_bytes()==b'Operation started\nOperation phase observed\nOperation finished\n'*2
assert d(R/'candidate.py')['sha256']=='6c9413bd8dab57fa9447474fd10103cb342e665a4609e92aa7de7a79d6494cad'
result=load('result.json');observed=load('receiver-observations.json');assert result['outcome']=='passed' and result['receiverThreadClosed'] is True
assert observed['requests']==result['requests'] and observed['receiverFailures']==[]
assert len(result['requests'])==9 and sum(v['bytes'] for v in result['requests'])==5974
assert {p.name for p in R.glob('request-*.pb')}=={v['file'] for v in result['requests']}
assert not (R/'.result-stage.json').exists()
runs=list((R/'capture').iterdir());assert len(runs)==1 and runs[0].is_dir();run=runs[0]
manifest=json.loads((run/'run.json').read_text());assert manifest['state']=='closed' and manifest['complete'] is True
assert manifest['profile']=='ashlar-host-otel-http/0.1' and manifest['run_id']==run.name
assert len(manifest['attempts'])==2 and len(manifest['configuration_origins'])==17 and set(manifest['configuration_origins'].values())=={'explicit'}
assert {p.name for p in run.iterdir()}=={'run.json',*[v['source'] for v in manifest['segments']]}
events=[];records=[]
for segment in manifest['segments']:
 p=run/segment['source'];raw=p.read_bytes();assert len(raw)==segment['bytes'] and d(p)['sha256']==segment['sha256'] and len(raw)<=16384
 lines=raw.splitlines();assert len(lines)==segment['records']
 for index,line in enumerate(lines,1):
  assert len(line)+1<=4096;e=json.loads(line);events.append(e);records.append({'source':p.name,'line':index,'event':e})
assert len(events)==6 and [e['attributes']['ashlar.sequence'] for e in events]==list(range(1,7))
assert [e['event_name'] for e in events]==['ashlar.operation.started','ashlar.operation.phase','ashlar.operation.finished']*2
assert load('expected.json')=={'events':events,'resource':events[0]['resource']}
resource={'service.name':'ashlar-host','service.version':metadata.version('ashlar-graph-toolkit'),'deployment.environment.name':'test','telemetry.sdk.name':'opentelemetry','telemetry.sdk.language':'python','telemetry.sdk.version':'1.45.1'}
assert resource['service.version']=='0.1.0.dev0'
for e in events:
 assert e['resource']==resource and e['scope']=={'name':'ashlar.host.diagnostics','version':'0.1.0'}
 assert e['attributes']['ashlar.run.id']==run.name and e['trace']['trace_flags']==3
 assert len(e['trace']['trace_id'])==32 and int(e['trace']['trace_id'],16)>0 and len(e['trace']['span_id'])==16 and int(e['trace']['span_id'],16)>0
assert len({e['trace']['trace_id'] for e in events})==2 and len({e['trace']['span_id'] for e in events})==2
snapshot={'schema_version':'ashlar.diagnostic.query/0.1','run_id':run.name,'manifest_sha256':d(run/'run.json')['sha256'],'capture_complete':True,'loss':manifest['loss'],'matched':6,'truncated':False,'records':records}
assert (R/'cli-output.json').read_bytes()==(json.dumps(snapshot,ensure_ascii=True,separators=(',',':'),allow_nan=False)+'\n').encode()
def attrs(items,expected):
 actual={}
 for item in items:
  assert item.key not in actual
  value=expected[item.key];kind={str:'string_value',int:'int_value',bool:'bool_value'}[type(value)]
  assert item.value.WhichOneof('value')==kind;actual[item.key]=getattr(item.value,kind)
 assert actual==expected

def group_scope(group,groups):
 attrs(group.resource.attributes,resource);assert group.resource.dropped_attributes_count==0
 assert group.schema_url=='https://opentelemetry.io/schemas/1.44.0' and len(groups)==1
 scope=groups[0];assert scope.scope.name=='ashlar.host.diagnostics' and scope.scope.version=='0.1.0'
 assert not scope.scope.attributes and scope.scope.dropped_attributes_count==0 and not scope.schema_url
 return scope
logs=[];spans=[];points=[];routes=[]
for row in result['requests']:
 p=R/row['file'];raw=p.read_bytes();assert len(raw)==row['bytes']<=65536 and d(p)['sha256']==row['sha256'];routes.append(row['path'])
 if row['path']=='/v1/logs':
  m=ExportLogsServiceRequest.FromString(raw);assert len(m.resource_logs)==1;g=m.resource_logs[0];logs.extend(group_scope(g,g.scope_logs).log_records)
 elif row['path']=='/v1/traces':
  m=ExportTraceServiceRequest.FromString(raw);assert len(m.resource_spans)==1;g=m.resource_spans[0];spans.extend(group_scope(g,g.scope_spans).spans)
 else:
  assert row['path']=='/v1/metrics';m=ExportMetricsServiceRequest.FromString(raw);assert len(m.resource_metrics)==1;g=m.resource_metrics[0];metrics=group_scope(g,g.scope_metrics).metrics;assert len(metrics)==1
  metric=metrics[0];assert metric.name=='ashlar.operation.completed' and metric.unit=='{operation}' and metric.WhichOneof('data')=='sum'
  assert metric.sum.aggregation_temporality==2 and metric.sum.is_monotonic;points.extend(metric.sum.data_points)
assert routes==['/v1/logs']*6+['/v1/traces']*2+['/v1/metrics']
assert len(logs)==6 and len(spans)==2 and len(points)==2
for e,l in zip(events,logs):
 assert l.time_unix_nano==0 and l.observed_time_unix_nano==int(e['observed_timestamp_unix_nano'])
 assert (l.severity_number,l.severity_text,l.event_name)==(e['severity_number'],e['severity_text'],e['event_name'])
 assert l.body.WhichOneof('value')=='string_value' and l.body.string_value==e['body']
 attrs(l.attributes,{**e['attributes'],'ashlar.diagnostic.schema_version':e['schema_version']})
 assert l.trace_id.hex()==e['trace']['trace_id'] and l.span_id.hex()==e['trace']['span_id'] and l.flags==3 and l.dropped_attributes_count==0
for i,s in enumerate(spans):
 start,phase,end=events[i*3:i*3+3];a=start['attributes'];t=start['trace'];assert phase['trace']==t==end['trace']
 expected={k:a[k] for k in ('ashlar.run.id','ashlar.attempt.id','ashlar.operation')};expected.update({k:v for k,v in end['attributes'].items() if k in ('ashlar.outcome','ashlar.cleanup_failed','ashlar.error.category')});attrs(s.attributes,expected)
 assert s.trace_id.hex()==t['trace_id'] and s.span_id.hex()==t['span_id'] and s.flags==259
 assert s.name=='ashlar.'+a['ashlar.operation'] and s.kind==1 and not s.parent_span_id and not s.links and not s.events and not s.trace_state
 assert 0<int(start['observed_timestamp_unix_nano'])<=s.start_time_unix_nano<=int(phase['observed_timestamp_unix_nano'])<=s.end_time_unix_nano==int(end['observed_timestamp_unix_nano'])
 assert s.status.code==(0 if i==0 else 2) and not s.status.message
 assert s.dropped_attributes_count==s.dropped_events_count==s.dropped_links_count==0
bags=[]
for p in points:
 assert p.WhichOneof('value')=='as_int' and p.as_int==1 and p.flags==0 and not p.exemplars
 assert 0<p.start_time_unix_nano<=p.time_unix_nano and p.time_unix_nano>=int(events[-1]['observed_timestamp_unix_nano'])
 expected={a.key:a.value.string_value for a in p.attributes};attrs(p.attributes,expected);bags.append(expected)
assert sorted(bags,key=lambda a:a['ashlar.operation'])==[{'ashlar.operation':'held-read','ashlar.outcome':'succeeded'},{'ashlar.operation':'publication','ashlar.outcome':'refused'}]
for signal,count in (('logs',6),('spans',2),('metrics',2)):
 assert manifest['loss'][signal]=={'submitted':count,'handed_off':count,'dropped':0,'unknown':False,'flush':'complete'}
assert {k:manifest['loss'][k] for k in ('local_attempted','local_written','local_dropped','local_unknown')}=={'local_attempted':6,'local_written':6,'local_dropped':0,'local_unknown':False}
assert stat.S_IMODE(run.stat().st_mode)==0o700
for p in list(run.iterdir())+list(R.glob('request-*.pb'))+[R/'expected.json',R/'receiver-observations.json',R/'cli-output.json',R/'result.json']:assert stat.S_IMODE(p.stat().st_mode)==0o600
assert not any(name in sys.modules for name in ('opentelemetry.sdk','pyspark','delta','psycopg'))
for row in closing['files']:verify(row)
report={'reviewer':'/root/astra_plan_review','verdict':'approve-exact-actual-installed-composition-receiver-subset','scope':'Actual current-main installed diagnostic_run success path for two synthetic traced lifecycle attempts, closed capture and installed CLI main retrieval. No native/business workflow or startup-failure qualification.','sourceCommit':opening['sourceCommit'],'process':d(R/'process.json'),'command':d(R/'command.json'),'candidate':d(R/'candidate.py'),'result':d(R/'result.json'),'openingClosing':{'files':925,'identical':True,'currentRehashUnchanged':True,'opening':d(R/'opening-inputs.json'),'closing':d(R/'closing-inputs.json')},'actual':{'terminalExit':0,'terminalChunk':'8a3da4','httpRequests':9,'rawProtobufBytes':5974,'logs':6,'rootSpans':2,'cumulativeIntegerCounterPoints':2,'localRecords':6,'signalLossKnownComplete':True,'stdoutBytes':0,'stderrBytes':124,'stderrMeaning':'Six exact fixed catalog milestones','sdkLogTraceFlags':3,'otlpRootSpanFlags':259,'cliSnapshotBytes':5883,'cliExactIndependentSerialization':True},'checks':['Independently decoded all nine retained protobuf requests and compared actual field types, resource/schema URL/scope, body/time/severity/events/attributes/full log flags to retained local events.','Both spans have real nonzero per-attempt context, exact low8 flags plus native root upper flag, exact attrs/status/time, no parent/links/events or drops.','One monotonic cumulative integer Sum has two distinct operation/outcome series, each1; no exemplar or extra point.','Closed manifest, segment sizes/digests/record inventory, actual loss counters and local event sequence are consistent; expected.json equals original segment events.','Installed CLI main output equals independently serialized public snapshot including original manifest hash/records/loss; no prefix or extra stdout.','No stale result stage or extra protobuf/capture files; private modes hold; actual candidate receiver reports thread closure. No independent process census inferred.','925 selected custody pins opening=closing and current rehash exact; interpreter/preparation/package source remains previously approved.'],'artifacts':[d(R/n) for n in ('receiver-observations.json','expected.json','cli-output.json','process.stdout','process.stderr')]+[d(R/v['file']) for v in result['requests']]+[d(p) for p in sorted(run.iterdir())],'limitations':result['gaps']+['Reviewer imported only installed protobuf decoders and metadata; did not rerun receiver, worker or native operation.','Synthetic publication/held-read labels do not qualify real publication/query/source/ACK effects.','This success run does not establish dependency failure/fallback, outage, privacy sentinel, concurrency or full C006 assurance.'],'findings':[]}
out=Path('/private/tmp/astra-otel-installed-composition-review-20261010-e.json');out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(d(out)))
