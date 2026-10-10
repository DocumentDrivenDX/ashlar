from pathlib import Path
import ast,contextlib,copy,hashlib,io,json,subprocess,sys
R=Path('/Users/erik/Projects/ashlar');sys.path[:0]=[str(R/'src'),str(R/'tests')]
from ashlar_host.config import with_diagnostics_transport,HostError
from ashlar_host.diagnostics import decode_event,validate_signal_loss,DiagnosticsError
from test_diagnostics_configuration import DiagnosticsConfigurationTests
checks=[]
def yes(name,fn):fn();checks.append(name)
def refuses(name,fn):
 try:fn()
 except (DiagnosticsError,HostError):checks.append(name);return
 raise AssertionError(name)
config=DiagnosticsConfigurationTests().configuration()
values=[]
assert with_diagnostics_transport(config,lambda *args:values.append(args) or 'owned')=='owned'
assert values==[('http://127.0.0.1:4318/base/',(),'loopback-test',None)]
checks.append('explicit transport snapshot callback result')
for bad in (None,{},object()):refuses('config type refusal',lambda:with_diagnostics_transport(bad,lambda *a:(_ for _ in ()).throw(AssertionError())))
for exc in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
 try:with_diagnostics_transport(config,lambda *a:(_ for _ in ()).throw(exc))
 except BaseException as got:assert got is exc;checks.append('trusted callback original cancellation')
event={'schema_version':'ashlar.diagnostic.event/0.1','observed_timestamp_unix_nano':'1','severity_text':'INFO','severity_number':9,'event_name':'ashlar.operation.started','body':'Operation started','resource':{'service.name':'ashlar-host','service.version':'0.1.0.dev0','deployment.environment.name':'test','telemetry.sdk.name':'opentelemetry','telemetry.sdk.language':'python','telemetry.sdk.version':'1.45.1'},'scope':{'name':'ashlar.host.diagnostics','version':'0.1.0'},'attributes':{'ashlar.run.id':'a'*32,'ashlar.attempt.id':'b'*32,'ashlar.emitter.id':'host','ashlar.sequence':1,'ashlar.operation':'held-read'}}
raw=(json.dumps(event,separators=(',',':'))+'\n').encode()
assert decode_event(raw)==event;checks.append('closed exact event admitted')
for bad in (raw[:-1],raw+b'\n',raw+b'x'*4096,raw.decode(),b'\xff\n',b'{}\n'):
 refuses('bounded event refusal',lambda:decode_event(bad))
unknown=copy.deepcopy(event);unknown['private']='sentinel';refuses('unknown event member refuses',lambda:decode_event((json.dumps(unknown)+'\n').encode()))
a=decode_event(raw);a['attributes']['ashlar.sequence']=999;assert decode_event(raw)==event;checks.append('owned fresh event result snapshot')
known={'submitted':2,'handed_off':1,'dropped':1,'unknown':False,'flush':'complete'}
assert validate_signal_loss(known) is None;checks.append('known exact loss accepted')
unknown={'submitted':2,'handed_off':None,'dropped':None,'unknown':True,'flush':'failed'}
assert validate_signal_loss(unknown) is None;checks.append('unknown loss accepted')
for change in ({'handed_off':3},{'dropped':None},{'submitted':True},{'private':'sentinel'},{'flush':'private'}):
 bad={**known,**change};refuses('loss refusal',lambda:validate_signal_loss(bad))
unchanged=[]
for relative in ('src/ashlar_host/config.py','src/ashlar_host/diagnostics.py'):
 before=subprocess.check_output(['git','show','HEAD:'+relative],cwd=R)
 after=(R/relative).read_bytes()
 old={n.name:ast.dump(n,include_attributes=False) for n in ast.parse(before).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
 new={n.name:ast.dump(n,include_attributes=False) for n in ast.parse(after).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
 assert all(new[k]==v for k,v in old.items());unchanged.append({'path':relative,'existingFunctionClassBodies':len(old)})
receipt={'scope':'New public configuration/diagnostic seams only; source reads and stdlib controls; no SDK/provider/process worker/network/native/secret input','checks':checks,'count':len(checks),'preservedOldDefinitions':unchanged,'source':[{'path':str(R/x),'sha256':hashlib.sha256((R/x).read_bytes()).hexdigest()} for x in ('src/ashlar_host/config.py','src/ashlar_host/diagnostics.py')]}
p=Path('/private/tmp/astra-otel-public-ports-review-20261010-a.json');p.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(receipt,indent=2))
