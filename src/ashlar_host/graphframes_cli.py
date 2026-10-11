"""Explicit installed GraphFrames operator boundary; code hashes grant no authority.

The selected provider supplies actual live custody or retained protected-ACK export
admission and original source semantics. The engine runs in an isolated JVM.
"""
from dataclasses import dataclass
import hashlib,json,re
from pathlib import Path
from types import ModuleType
from .evolution_cli import InvocationFile,MAX_CONFIG,MAX_PROVIDER
from .puppy_release_cli import original_file,directory_identity,retain
from .graphframes_release import GraphFramesRuntimeConfig,GraphFramesOfflineSession,GraphFramesPublicationSession,execute_graphframes_release
from .private_graph_custody import load_private_graph_release
from ashlar.graph_release import GraphRelease
from .lifecycle import owned_context

PROFILE='ashlar-graphframes-invocation/0.1'
def _digest(value):
 if type(value)is not str or not re.fullmatch('[0-9a-f]{64}',value):raise ValueError('Exact externally selected digest required')
 return value

def _path(value):
 if type(value)is not str or not 0<len(value)<=4096 or '\x00'in value or not Path(value).is_absolute():raise ValueError('Exact absolute operator paths required')
 return Path(value)

def _pairs(pairs):
 value={}
 for k,v in pairs:
  if k in value:raise ValueError('Duplicate operator configuration member')
  value[k]=v
 return value

def _integer(token):
 if len(token)>8 or not re.fullmatch('0|[1-9][0-9]*',token):raise ValueError('Finite canonical configuration integer required')
 return int(token)

@dataclass(frozen=True)
class GraphFramesInvocation:
 mode:str
 pack:Path
 output:Path
 runtime:GraphFramesRuntimeConfig
 release:Path=None
 release_sha256:str=None
 custody:Path=None
 custody_sha256:str=None
 def __post_init__(self):
  if type(self.mode)is not str or self.mode not in ('live-publication','protected-ack-export'):raise ValueError('Explicit GraphFrames custody mode required')
  if any(type(p)is not type(Path())or not p.is_absolute()for p in (self.pack,self.output)):raise ValueError('Exact original pack/output paths required')
  if type(self.runtime)is not GraphFramesRuntimeConfig or self.runtime.work!=self.output/'engine':raise ValueError('Separate explicitly selected engine output required')
  self.runtime.__post_init__()
  if self.mode=='protected-ack-export':
   if any(type(p)is not type(Path())or not p.is_absolute()for p in (self.release,self.custody)):raise ValueError('Original protected export paths required')
   _digest(self.release_sha256);_digest(self.custody_sha256)
  elif any(v is not None for v in (self.release,self.release_sha256,self.custody,self.custody_sha256)):raise ValueError('Live mode cannot acquire retained export fallback')

def parse_graphframes_invocation(raw):
 if type(raw)is not bytes or not 0<len(raw)<=MAX_CONFIG:raise ValueError('Bounded exact invocation bytes required')
 value=json.loads(raw,object_pairs_hook=_pairs,parse_int=_integer,parse_float=lambda _:(_ for _ in ()).throw(ValueError('Float configuration refused')),parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite configuration refused')))
 common={'profile','mode','pack','output','runtime'}
 if type(value)is not dict or value.get('profile')!=PROFILE or type(value.get('mode'))is not str:raise ValueError('Closed selected GraphFrames invocation required')
 expected=common|({'release','release_sha256','custody','custody_sha256'}if value['mode']=='protected-ack-export'else set())
 if set(value)!=expected:raise ValueError('Closed mode-specific invocation required')
 runtime=value['runtime'];keys={'python','python_sha256','java_home','java_sha256','worker_revision','worker_inventory_sha256','sdk_inventory_sha256','jars','timeout_seconds','maximum_stream_bytes','maximum_evidence_bytes'}
 if type(runtime)is not dict or set(runtime)!=keys:raise ValueError('Exact isolated runtime configuration required')
 output=_path(value['output']);config=GraphFramesRuntimeConfig(_path(runtime['python']),_digest(runtime['python_sha256']),_path(runtime['java_home']),_digest(runtime['java_sha256']),runtime['worker_revision'],_digest(runtime['worker_inventory_sha256']),_digest(runtime['sdk_inventory_sha256']),_path(runtime['jars']),output/'engine',runtime['timeout_seconds'],runtime['maximum_stream_bytes'],runtime['maximum_evidence_bytes'])
 return GraphFramesInvocation(value['mode'],_path(value['pack']),output,config,*((_path(value['release']),_digest(value['release_sha256']),_path(value['custody']),_digest(value['custody_sha256']))if value['mode']=='protected-ack-export'else (None,None,None,None)))

def run_graphframes_command(configuration,provider_path,provider_sha256):
 config=InvocationFile.read(configuration,MAX_CONFIG);invocation=parse_graphframes_invocation(config.raw)
 provider=InvocationFile.read(provider_path,MAX_PROVIDER)
 if hashlib.sha256(provider.raw).hexdigest()!=_digest(provider_sha256):raise ValueError('Explicit provider bytes changed')
 pack,pack_identity=original_file(invocation.pack,1048576);watched=[(invocation.pack,pack,pack_identity,1048576)]
 release=None;custody=None
 if invocation.mode=='protected-ack-export':
  release_raw,release_identity=original_file(invocation.release,16777216);custody,custody_identity=original_file(invocation.custody,16777216)
  load_private_graph_release(release_raw,invocation.release_sha256,custody,invocation.custody_sha256)
  release=GraphRelease(release_raw,invocation.release_sha256)
  watched.extend([(invocation.release,release_raw,release_identity,16777216),(invocation.custody,custody,custody_identity,16777216)])
 def renew():
  config.renew(MAX_CONFIG);provider.renew(MAX_PROVIDER)
  for path,raw,identity,maximum in watched:
   current,current_identity=original_file(path,maximum)
   if current!=raw or current_identity!=identity:raise ValueError('Original invocation source/export changed')
 renew()
 if invocation.output.exists():raise ValueError('Fresh operator output required')
 invocation.output.mkdir(mode=0o700);output_identity=directory_identity(invocation.output)
 # Provider is trusted explicitly selected operator code, not a sandbox/plugin.
 module=ModuleType('ashlar_operator_graphframes_'+provider_sha256);module.__file__=str(provider.path)
 exec(compile(provider.raw,str(provider.path),'exec'),module.__dict__)
 if not callable(getattr(module,'open_graphframes',None)):raise ValueError('Actual GraphFrames host provider required')
 count=0;total=0
 def observe(value):
  nonlocal count,total
  raw=(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode();total+=len(raw)
  if total>invocation.runtime.maximum_evidence_bytes or count>=128:raise ValueError('Complete raw observation budget exceeded')
  retain(invocation.output,'observation-'+str(count)+'.json',raw,output_identity);count+=1
 with owned_context(module.open_graphframes(invocation,release,custody))as session:
  expected=GraphFramesOfflineSession if invocation.mode=='protected-ack-export'else GraphFramesPublicationSession
  if type(session)is not expected:raise ValueError('Provider cannot substitute authority mode')
  if expected is GraphFramesOfflineSession and(session.release!=release or session.custody_bytes!=custody or session.custody_sha256!=invocation.custody_sha256):raise ValueError('Provider retained export correspondence differs')
  result=execute_graphframes_release(session,invocation.runtime,pack,observe)
  renew()
 renew()
 report={'format':'ashlar-installed-graphframes/0.1','mode':invocation.mode,'observations':count,'result':result,'scope':'All original pack cases and common topology under the explicitly admitted host mode; retained export mode does not assert current canonical availability.'}
 raw=(json.dumps(report,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
 if len(raw)>invocation.runtime.maximum_evidence_bytes:raise ValueError('Complete report exceeds byte budget')
 retain(invocation.output,'report.json',raw,output_identity)
 return report
