from pathlib import Path
from unittest.mock import patch
import ast,hashlib,json,os,tempfile,time,types
P=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-b/candidate.py')
raw=P.read_bytes();assert hashlib.sha256(raw).hexdigest()=='d6b4eaf033ae4c4df23faaa25be5b2b723287d53195586bf1cc1abb4821f3fbf'
tree=ast.parse(raw);functions={n.name:n for n in tree.body if isinstance(n,ast.FunctionDef)}
def desc(p):
 r=Path(p).read_bytes();return {'path':str(p),'bytes':len(r),'sha256':hashlib.sha256(r).hexdigest()}
def compile_nodes(nodes,space):exec(compile(ast.Module(body=nodes,type_ignores=[]),str(P),'exec'),space)
helper=[functions[n] for n in ('write','mark','cleanup_failure')]
checks=[]
class FixedCancel(KeyboardInterrupt):
 def __setattr__(self,n,v):
  if n=='cleanup_failed':raise TypeError('read-only marker')
  super().__setattr__(n,v)
def normal(root):
 space={'ROOT':root,'os':os,'time':time,'json':json,'hashlib':hashlib,'started':time.monotonic(),'inventory':[]}
 compile_nodes(helper,space);return space
with tempfile.TemporaryDirectory(prefix='astra-receiver-b-inert-') as temp:
 base=Path(temp)
 # Execute actual final-publication statements, not a rewritten model.
 tail=functions['main'].body[-3:]
 assert isinstance(tail[0],ast.Expr) and isinstance(tail[1],ast.Expr) and isinstance(tail[2],ast.Try)
 for kind in ('success','write-cancel-close-error','close-cancel','existing-final','postcommit-unlink-error','postcommit-unlink-cancel'):
  root=base/kind;root.mkdir();space=normal(root);primary=FixedCancel('review cancellation');calls=[]
  if kind=='write-cancel-close-error':
   def write(fd,data):os.write(fd,data[:10]);raise primary
   def close(fd):os.close(fd);raise OSError('review close')
  else:
   write=os.write
   def close(fd):
    os.close(fd)
    if kind=='close-cancel':raise primary
  def link(a,b):calls.append('link');return os.link(a,b)
  space['os']=types.SimpleNamespace(open=os.open,write=write,close=close,link=link,O_WRONLY=os.O_WRONLY,O_CREAT=os.O_CREAT,O_EXCL=os.O_EXCL,O_NOFOLLOW=os.O_NOFOLLOW)
  if kind=='existing-final':(root/'result.json').write_bytes(b'foreign final')
  original_unlink=Path.unlink
  def unlink(p,*a,**kw):
   if p==root/'.result-stage.json':
    if kind=='postcommit-unlink-error':raise OSError('maintenance')
    if kind=='postcommit-unlink-cancel':raise primary
   return original_unlink(p,*a,**kw)
  error=None
  try:
   with patch.object(Path,'unlink',unlink):compile_nodes(tail,space)
  except BaseException as exc:error=exc
  if kind in ('write-cancel-close-error','close-cancel'):
   assert error is primary and not (root/'result.json').exists() and not calls
  elif kind=='existing-final':assert isinstance(error,FileExistsError) and (root/'result.json').read_bytes()==b'foreign final'
  else:
   assert (root/'result.json').exists() and json.loads((root/'result.json').read_bytes())['outcome']=='passed'
   if kind=='postcommit-unlink-cancel':assert error is primary
   else:assert error is None
   if kind=='success':assert not (root/'.result-stage.json').exists()
  checks.append({'control':kind,'passed':True})
 # Fail before thread construction completes: socket must close, raw observations survive.
 for kind in ('ordinary-setup','cancel-setup','cancel-setup-and-retention'):
  root=base/kind;root.mkdir();space=normal(root);state={};primary=FixedCancel('setup cancel') if kind!='ordinary-setup' else RuntimeError('setup')
  class Server:
   def __init__(self,*args):state['acquired']=True
   def serve_forever(self,**kwargs):raise AssertionError('no receiver allowed')
   def server_close(self):state['closed']=True
  def Thread(*args,**kwargs):raise primary
  space.update({'Server':Server,'Receiver':object,'threading':types.SimpleNamespace(Thread=Thread),'SpanContext':lambda *a:a,'TraceFlags':lambda x:x,'TraceState':dict,'received':[('/v1/logs',b'original-inert-protobuf-bytes')],'failures':[]})
  if kind=='cancel-setup-and-retention':
   def refused(*a):raise OSError('retention')
   space['write']=refused
  compile_nodes([functions['main']],space)
  try:space['main']()
  except BaseException as error:assert error is primary
  else:raise AssertionError('setup did not refuse')
  assert state=={'acquired':True,'closed':True} and not (root/'result.json').exists()
  if kind!='cancel-setup-and-retention':
   assert (root/'request-00.pb').read_bytes()==b'original-inert-protobuf-bytes'
   assert json.loads((root/'receiver-observations.json').read_bytes())['state']=='provisional'
  checks.append({'control':kind,'passed':True})
 # Exact accepted-socket method, fake inherited accept only, no socket creation.
 for kind in ('cancel-timeout-close-error','ordinary-timeout-close-cancel'):
  primary=FixedCancel('socket cancel');ordinary=OSError('settimeout');state={}
  class Connection:
   def settimeout(self,n):assert n==.5;raise primary if kind.startswith('cancel') else ordinary
   def close(self):state['closed']=True;raise OSError('close') if kind.startswith('cancel') else primary
  class HTTPServer:
   def get_request(self):return Connection(),('127.0.0.1',1)
  space=normal(base);space['HTTPServer']=HTTPServer
  compile_nodes([next(n for n in tree.body if isinstance(n,ast.ClassDef) and n.name=='Server')],space)
  try:space['Server']().get_request()
  except BaseException as error:assert error is primary
  else:raise AssertionError('missing socket refusal')
  assert state['closed'];checks.append({'control':kind,'passed':True})
command_path=P.with_name('command.json');command=json.loads(command_path.read_bytes());assert desc(P)==command['source']
assert desc(command_path)['sha256']=='37941ff0cffd879c5b9b07dfad4cec2279c32a79fe17ad5bb53dc4373c3d7f50'
assert command['argv']==['/private/tmp/ashlar-otel-installed-preparation-20261010-a/environment/bin/python','-I','-B',str(P)]
assert command['env']=={'PATH':'/usr/bin:/bin'} and command['cwd']==str(P.parent)
assert command['bounds']=={'attempts':2,'expectedHttpRequests':9,'maxHttpRequests':19,'maxReceivedBodyBytesEach':65536,'maxReceivedBodyBytesTotal':1048576,'acceptedSocketTimeoutSeconds':0.5,'receiverJoinSeconds':1,'sdkExportTimeoutMs':1000,'sdkShutdownTimeoutMs':2000}
assert not any((P.parent/name).exists() for name in ('capture','result.json','.result-stage.json','expected.json','receiver-observations.json'))
assert not list(P.parent.glob('request-*.pb'))
assert desc('/private/tmp/astra-otel-installed-preparation-review-20261010-b.json')['sha256']=='acbe1d9869254dc49369c3de0134139e5790058859b10fcc5b4e018d0b095464'
receipt={'verdict':'approved-command-conditional-opening-and-closing-custody','source':desc(P),'command':desc(command_path),'checks':checks,'checkCount':len(checks),'priorFindingsResolved':['9requests6logs2spans1metrics-snapshot,2metriclogicalpoints','Exclusive staged result write/close/readback precedes no-clobber availability; committed housekeeping differs from cancellation.','Bounded raw protobuf and provisional receipt preserved before semantic checks and on lifecycle failure; persistence cannot replace original cancellation.','Protected receiver/thread acquisition, accepted-socket primary preservation, all acquired cleanup actions attempted.'],'executionConditions':['Root rehash exact source+command, approved preparation freeze/input pins, wheel, all installed source/dependency/RECORD content and interpreter/pyvenv before and after; current inventories are selected custody, not hermetic closure.','Fresh capture/result/stage/expected/provisional/request paths immediately before invocation.','Execute exact direct foreground installed Python-I-B command with only stated environment; no outer abrupt-termination/globaldeadline assertion.','Two synthetic sequential attempts only; fixed incoming parent/retry contexts, genuine current worker SDK span identities. No native/source/publication/ACK operation.','Retain raw process stdout/stderr, terminal outcome and bounded capture/protobuf artifacts even when semantic assertions fail.'],'qualificationScope':'Candidate command only. Actual installed public owner -> SDK worker -> custom real loopback OTLP receiver qualification is pending. No concurrent/untraced/credential-sentinel/outage/pilot/native/fullC006 support claim. The accepted-socket inactivity timeout and finite cooperative local sender are not hostile-sender/global wallclock guarantees.','inertControlScope':'AST-extracted original helpers/main/final statements with fake receiver/thread/accept and owned temporary files only; no receiver/network/worker/provider execution.','script':desc(__file__)}
out=Path('/private/tmp/astra-otel-installed-receiver-command-review-20261010-b.json');out.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(desc(out)))
