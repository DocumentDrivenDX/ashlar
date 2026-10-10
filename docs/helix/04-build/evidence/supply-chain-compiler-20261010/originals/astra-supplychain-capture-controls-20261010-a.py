import ast,hashlib,json
from pathlib import Path
source=Path('/private/tmp/ashlar-supply-chain-compile-candidate-20261010-c/capture.py');raw=source.read_bytes();pin=hashlib.sha256(raw).hexdigest()
tree=ast.parse(raw);body=[n for n in tree.body if isinstance(n,ast.Try)][0];fragment=ast.fix_missing_locations(ast.Module(body=[body],type_ignores=[]));code=compile(fragment,str(source),'exec')
observations=[]
classes=(OSError,KeyboardInterrupt,SystemExit,GeneratorExit)
cases=[(a('write'),b('close'),c('custody'),None)for a in classes for b in classes for c in classes]
cases += [(None,None,None,value)for value in (0,2,True,None)]
cases += [(None,c('close'),None,3)for c in classes]
cases += [(None,None,c('custody'),3)for c in classes]
cases += [(None,None,None,3)]
for write_error,close_error,custody_error,written in cases:
 retained=bytearray();calls=[]
 class Stream:
  def write(self,response):
   retained.extend(response[:1] if write_error is not None or written!=3 else response)
   if write_error is not None:raise write_error
   return written
  def close(self):
   calls.append('close')
   if close_error is not None:raise close_error
 class FakePath:
  def __init__(self,*args):pass
  def __truediv__(self,other):return self
  def mkdir(self):calls.append('mkdir')
  def read_bytes(self):return b'{}'
  def open(self,mode):assert mode=='xb';return Stream()
 def verify():
  calls.append('verify')
  if custody_error is not None:raise custody_error
 namespace={'Path':FakePath,'ROOT':FakePath(),'command':{'output':'synthetic','index':'synthetic','installation':'synthetic','cases':['synthetic']},'primary':None,'verify':verify,'PathsKeysDistributionPaths':lambda *args:object(),'compile_paths_keys_distribution':lambda *args:b'abc'}
 actual=None
 try:exec(code,namespace)
 except BaseException as error:actual=error
 if write_error is not None:assert actual is write_error
 elif type(written)is not int or written!=3:assert type(actual)is ValueError and str(actual)=='candidate-short-write'
 elif close_error is not None:assert actual is close_error
 elif custody_error is not None:assert actual is custody_error
 else:assert actual is None
 assert calls.count('close')==1 and calls.count('verify')==1
 assert retained==(b'abc' if write_error is None and written==3 else b'a')
 observations.append({'write':type(write_error).__name__,'close':type(close_error).__name__,'custody':type(custody_error).__name__,'writeReturnType':type(written).__name__,'returned':type(actual).__name__,'originalIdentityPreserved':True,'retainedBytes':len(retained),'closeOnce':True,'success':actual is None})
assert hashlib.sha256(source.read_bytes()).hexdigest()==pin
value={'source':str(source),'sha256':pin,'cases':len(observations),'scope':'Actual top-level capture try AST with inert facade/path/stream/custody ports; no compiler/process/native/resource runtime execution. Existing business-primary preservation policy verified; no diagnostic priority claim.','observations':observations}
Path('/private/tmp/astra-supplychain-capture-controls-20261010-a.json').write_text(json.dumps(value,indent=2)+'\n');print(json.dumps(value))
