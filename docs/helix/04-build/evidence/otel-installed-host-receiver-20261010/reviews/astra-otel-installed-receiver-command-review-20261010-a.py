from pathlib import Path
import ast,hashlib,json,os,tempfile,types
P=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-a/candidate.py')
raw=P.read_bytes();assert hashlib.sha256(raw).hexdigest()=='9a8cdad16cc967f85bd7532a835a07e687d5738bb017b7c29f744979d56eed1a'
tree=ast.parse(raw);names={'write','mark','main'}
nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
assert {n.name for n in nodes}==names
def desc(p):
 r=Path(p).read_bytes();return {'path':str(p),'bytes':len(r),'sha256':hashlib.sha256(r).hexdigest()}
checks=[]
with tempfile.TemporaryDirectory(prefix='astra-probe-inert-') as temp:
 root=Path(temp);primary=KeyboardInterrupt('review-only');closed=[]
 def close(fd):os.close(fd);closed.append(fd);raise primary
 space={'os':types.SimpleNamespace(open=os.open,write=os.write,close=close,O_WRONLY=os.O_WRONLY,O_CREAT=os.O_CREAT,O_EXCL=os.O_EXCL,O_NOFOLLOW=os.O_NOFOLLOW)}
 exec(compile(ast.Module(body=nodes[:2],type_ignores=[]),str(P),'exec'),space)
 path=root/'result.json';payload=b'{"outcome":"passed"}\n'
 try:space['write'](path,payload)
 except BaseException as error:assert error is primary
 else:raise AssertionError('missing cancellation')
 assert closed and path.read_bytes()==payload
 checks.append({'id':'IR-002','control':'close interruption after complete write','observed':'exact KeyboardInterrupt retained but final result.json containing outcome=passed remains available','confirmed':True})
 state={'closed':False}
 class FakeServer:
  def __init__(self,*args):state['acquired']=True
  def serve_forever(self,**kwargs):raise AssertionError('must not run')
  def server_close(self):state['closed']=True
 failure=RuntimeError('thread construction failed')
 def thread(*args,**kwargs):raise failure
 space={'ROOT':root,'Server':FakeServer,'Receiver':object,'threading':types.SimpleNamespace(Thread=thread)}
 # Separate fresh output absence is a main precondition.
 path.unlink()
 exec(compile(ast.Module(body=[next(n for n in nodes if n.name=='main')],type_ignores=[]),str(P),'exec'),space)
 try:space['main']()
 except BaseException as error:assert error is failure
 else:raise AssertionError('missing setup failure')
 assert state['acquired'] and not state['closed']
 checks.append({'id':'IR-004','control':'thread construction failure after receiver acquisition','observed':'bound receiver owner is not closed; no try/finally entered','confirmed':True})
receipt={'verdict':'corrections-required-unexecuted-candidate','source':desc(P),'command':desc(P.with_name('command.json')),'independentChecks':checks,'findings':[{'id':'IR-001','severity':'medium','finding':'Expected HTTP count10 conflates two metric logical points with one metric snapshot. Six logs+two spans+one snapshot gives9; assert 6/2/1 endpoint request counts.'},{'id':'IR-002','severity':'medium','finding':'Direct final-name result write permits complete or partial success-named artifact after write/close failure. Stage exclusively, finish close/readback, then no-clobber link final availability; preserve primary and postcommit maintenance semantics.'},{'id':'IR-003','severity':'medium','finding':'Receiver raw payload retention follows semantic assertions; semantic failure loses observed protobuf evidence. Retain bounded provisional raw evidence on every outcome, without success claim.'},{'id':'IR-004','severity':'medium','finding':'Receiver acquired before protected cleanup; thread construction failure leaks owner. Accepted socket close can replace settimeout failure. Protect acquisition-to-cleanup and original primary.'}],'scope':'AST-extracted write and lifecycle controls only, fake receiver/thread and owned temporary file. No installed worker, receiver socket, SDK provider, network, native or secret execution.','script':desc(__file__)}
out=Path('/private/tmp/astra-otel-installed-receiver-command-review-20261010-a.json');out.write_text(json.dumps(receipt,indent=2)+'\n');print(json.dumps(desc(out)))
