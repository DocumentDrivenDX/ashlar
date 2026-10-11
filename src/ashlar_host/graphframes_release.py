"""Separately admitted GraphFrames execution over immutable original graph bytes.

Live complete-vector publication custody and retained protected-ACK export custody
are distinct host modes. Carrier validation and engine reports grant no authority.
The GraphFrames JVM is always a separate owned process from a canonical reader.
"""
from contextlib import contextmanager
from dataclasses import dataclass
import hashlib,json,os,re,selectors,signal,subprocess,time
from pathlib import Path
from types import MappingProxyType
from ashlar.graph_release import GraphRelease,read_graph_release
from ashlar.pins import PinVector
from .finite_dataset import FinitePackSource
from .finite_pack import FinitePackDefinition
from .private_graph_custody import load_private_graph_release
from .lifecycle import owned_context,finish
from .evolution_producer import snapshot
from .supply_chain_request import source_snapshot

VERSIONS={'pyspark':'3.5.3','delta-spark':'3.2.1','graphframes-py':'0.12.3'}
SDK_INVENTORIES=tuple(VERSIONS)+('transitive-imports',)
JARS={
 'io.delta_delta-spark_2.12-3.2.1.jar':'088e187da689a347a6a8556dcb22318e3dfcfb995d807f5e2c19b4d0a7ee9499',
 'io.delta_delta-storage-3.2.1.jar':'4dcc179fc4076bda5060a4038f979c53e1f5916cf04971e28f9441db390763c7',
 'io.graphframes_graphframes-graphx-spark3_2.12-0.12.3.jar':'8bbd2bbb4c7a3b137e51b4f32f49a30a2ccf70eeb07b452cffa20090dd481b96',
 'io.graphframes_graphframes-spark3_2.12-0.12.3.jar':'69d7909628caf42bcbe1c7e6e5a5a0901040d00583738d931af41bd72af1d3dc'}

def _source(source):
 if type(source)is not FinitePackSource or type(source.definition)is not FinitePackDefinition or source.definition.name not in ('archaeology','ecology') or any(type(v)is not bytes for v in (source.model,source.graph,source.raw)):raise ValueError('Original typed source required')
 source.definition.inputs(source.model,source.graph);source.metadata()
 return source.definition.name,source.model,source.graph,source.raw

def _renew(source,original):
 if _source(source)!=original:raise ValueError('Original whole source changed')

def _runtime_port(policy):
 if not all(callable(getattr(policy,name,None))for name in ('admit_graphframes_runtime','renew_graphframes_runtime')):raise ValueError('Actual installed worker and complete SDK runtime custody required')

@dataclass(frozen=True)
class GraphFramesRuntimeFile:
 path:Path
 sha256:str
 size:int
 def __post_init__(self):
  if type(self.path)is not type(Path())or not self.path.is_absolute()or type(self.sha256)is not str or not re.fullmatch('[0-9a-f]{64}',self.sha256)or type(self.size)is not int or not 0<=self.size<=134217728:raise ValueError('Exact bounded selected runtime file required')

@dataclass(frozen=True)
class GraphFramesRuntimeInventory:
 worker_revision:str
 worker:tuple
 sdks:tuple
 def __post_init__(self):
  if type(self.worker_revision)is not str or not re.fullmatch('[0-9a-f]{40}',self.worker_revision)or type(self.worker)is not tuple or not 0<len(self.worker)<=8192 or type(self.sdks)is not tuple or len(self.sdks)!=4:raise ValueError('Complete selected installed runtime inventory required')
  names=[];files=list(self.worker)
  for entry in self.sdks:
   if type(entry)is not tuple or len(entry)!=2 or type(entry[0])is not str or type(entry[1])is not tuple or not 0<len(entry[1])<=8192:raise ValueError('Complete exact SDK inventories required')
   names.append(entry[0]);files.extend(entry[1])
  if set(names)!=set(SDK_INVENTORIES)or len(set(names))!=len(names)or len(files)>8192:raise ValueError('Every selected SDK exactly once required')
  if any(type(f)is not GraphFramesRuntimeFile for f in files):raise ValueError('Exact passive runtime file records required')
  for f in files:f.__post_init__()
  if sum(f.size for f in files)>1073741824 or len({str(f.path)for f in files})!=len(files):raise ValueError('Bounded injective complete runtime files required')
 def verify(self,config):
  """Correspondence only: current independent policy admits actual resolution."""
  if type(config)is not GraphFramesRuntimeConfig:raise ValueError('Exact selected runtime configuration required')
  config.__post_init__();self.__post_init__()
  def descriptors(files):return [{'path':str(f.path),'sha256':f.sha256,'size':f.size}for f in files]
  def digest(value):return hashlib.sha256(json.dumps(value,sort_keys=True,separators=(',',':')).encode()).hexdigest()
  if self.worker_revision!=config.worker_revision or digest(descriptors(self.worker))!=config.worker_inventory_sha256 or digest({name:descriptors(files)for name,files in self.sdks})!=config.sdk_inventory_sha256:raise ValueError('Selected complete installed worker/SDK inventory differs')
  for f in self.worker+tuple(f for _,files in self.sdks for f in files):
   raw=snapshot(f.path,max(1,f.size))
   if len(raw)!=f.size or hashlib.sha256(raw).hexdigest()!=f.sha256:raise ValueError('Actual installed runtime file changed')

@dataclass(frozen=True)
class GraphFramesOfflineSession:
 release:GraphRelease
 custody_bytes:bytes
 custody_sha256:str
 source:FinitePackSource
 policy:object
 context:object
 runtime_policy:object
 runtime_context:object
 def __post_init__(self):
  if type(self.release)is not GraphRelease:raise ValueError('Exact retained graph release required')
  load_private_graph_release(self.release.payload,self.release.sha256,self.custody_bytes,self.custody_sha256)
  if not all(callable(getattr(self.policy,n,None))for n in ('writer','admit_original_export')):raise ValueError('Independent retained export policy required')
  _runtime_port(self.runtime_policy)
  _source(self.source)

@dataclass(frozen=True)
class GraphFramesPublicationSession:
 executor:object
 backend:object
 pins:object
 vector:PinVector
 graph_policy:object
 source:FinitePackSource
 context:object
 publication_id:str
 node_table:str
 edge_table:str
 outer_hold:object
 supported_profiles:tuple
 supported_revisions:object
 max_nodes:int
 max_edges:int
 runtime_policy:object
 runtime_context:object
 def __post_init__(self):
  if type(self.vector)is not PinVector or not callable(self.outer_hold):raise ValueError('Actual complete-vector outer custody required')
  if any(type(v)is not str or not v for v in (self.publication_id,self.node_table,self.edge_table)):raise ValueError('Exact publication roles required')
  if type(self.supported_profiles)is not tuple or not 0<len(self.supported_profiles)<=32 or any(type(t)is not str or not 0<len(t)<=1024 for t in self.supported_profiles):raise ValueError('Exact selected profiles required')
  revisions=self.supported_revisions
  if type(revisions)not in (dict,type(MappingProxyType({})))or not 0<len(revisions)<=128:raise ValueError('Exact source revision mapping required')
  owned={}
  for source,values in revisions.items():
   if type(source)is not str or not 0<len(source)<=1024 or type(values)not in (tuple,list)or not 0<len(values)<=64 or any(type(v)is not str or not 0<len(v)<=1024 for v in values):raise ValueError('Passive original source revision inventory required')
   owned[source]=tuple(values)
  object.__setattr__(self,'supported_revisions',MappingProxyType(owned))
  if any(type(v)is not int or not 0<v<=10000 for v in (self.max_nodes,self.max_edges)):raise ValueError('Bounded original graph required')
  _runtime_port(self.runtime_policy)
  _source(self.source)

@contextmanager
def hold_graphframes_session(session):
 """Hold original host authority across worker shutdown and closing renewal."""
 if type(session)not in (GraphFramesOfflineSession,GraphFramesPublicationSession):raise ValueError('Explicit host custody mode required')
 session.__post_init__();original=_source(session.source)
 if type(session)is GraphFramesOfflineSession:
  with owned_context(session.policy.writer(session.context)):
   def renew():
    _renew(session.source,original)
    load_private_graph_release(session.release.payload,session.release.sha256,session.custody_bytes,session.custody_sha256)
    if session.policy.admit_original_export(session.release,session.custody_bytes,session.source,session.context)is not None:raise PermissionError('Current independent export admission required')
   renew();yield session.release,renew;renew()
 else:
  with owned_context(session.outer_hold(session.context)):
   with owned_context(session.pins.hold(session.vector,context=session.context)):
    release=read_graph_release(session.executor,session.backend,session.pins,session.vector,session.graph_policy,publication_id=session.publication_id,node_table=session.node_table,edge_table=session.edge_table,context=session.context,supported_profiles=session.supported_profiles,supported_revisions=session.supported_revisions,max_nodes=session.max_nodes,max_edges=session.max_edges)
    def renew():
     _renew(session.source,original)
     current=read_graph_release(session.executor,session.backend,session.pins,session.vector,session.graph_policy,publication_id=session.publication_id,node_table=session.node_table,edge_table=session.edge_table,context=session.context,supported_profiles=session.supported_profiles,supported_revisions=session.supported_revisions,max_nodes=session.max_nodes,max_edges=session.max_edges)
     if current!=release:raise ValueError('Original complete publication release changed')
    renew();yield release,renew;renew()

@dataclass(frozen=True)
class GraphFramesRuntimeConfig:
 python:Path
 python_sha256:str
 java_home:Path
 java_sha256:str
 worker_revision:str
 worker_inventory_sha256:str
 sdk_inventory_sha256:str
 jars:Path
 work:Path
 timeout_seconds:int=300
 maximum_stream_bytes:int=1048576
 maximum_evidence_bytes:int=16777216
 def __post_init__(self):
  if any(type(v)is not type(Path()) for v in (self.python,self.java_home,self.jars,self.work)):raise ValueError('Explicit runtime paths required')
  if any(not v.is_absolute()for v in (self.python,self.java_home,self.jars,self.work)):raise ValueError('Absolute runtime paths required')
  if any(type(v)is not str or not re.fullmatch('[0-9a-f]{64}',v)for v in (self.python_sha256,self.java_sha256)):raise ValueError('Exact selected interpreter/Java pins required')
  if type(self.worker_revision)is not str or not re.fullmatch('[0-9a-f]{40}',self.worker_revision)or any(type(v)is not str or not re.fullmatch('[0-9a-f]{64}',v)for v in (self.worker_inventory_sha256,self.sdk_inventory_sha256)):raise ValueError('Explicit installed worker/SDK closure pins required')
  for value,bound in ((self.timeout_seconds,300),(self.maximum_stream_bytes,1048576),(self.maximum_evidence_bytes,16777216)):
   if type(value)is not int or not 0<value<=bound:raise ValueError('Finite runtime capacity required')
 def renew(self):
  self.__post_init__()
  if hashlib.sha256(snapshot(self.python,64*1024*1024)).hexdigest()!=self.python_sha256:raise ValueError('Selected interpreter bytes changed')
  if hashlib.sha256(snapshot(self.java_home/'bin/java',64*1024*1024)).hexdigest()!=self.java_sha256:raise ValueError('Selected Java bytes changed')
  if set(p.name for p in self.jars.iterdir())!=set(JARS):raise ValueError('Exact four runtime JARs required')
  for name,digest in JARS.items():
   if hashlib.sha256(snapshot(self.jars/name,16*1024*1024)).hexdigest()!=digest:raise ValueError('Selected runtime JAR bytes changed')

def renew_graphframes_runtime(runtime,inventory,policy,context):
 """Independent selected bytes and current whole import-closure admission."""
 if type(runtime)is not GraphFramesRuntimeConfig or type(inventory)is not GraphFramesRuntimeInventory:raise ValueError('Exact selected runtime and inventory required')
 _runtime_port(policy);runtime.renew();inventory.verify(runtime)
 if policy.renew_graphframes_runtime(inventory,runtime,context)is not None:raise PermissionError('Current actual installed runtime renewal required')


# Fixed transport program. Caller argv is passed as arguments, never code or shell.
# The supervisor holds the session leader alive until the owner kills the group.
_SUPERVISOR = """import os,signal,subprocess,sys
fd=int(sys.argv[1])
child=subprocess.Popen(sys.argv[2:],stdin=subprocess.DEVNULL,close_fds=True)
status=child.wait()
os.close(1)
os.close(2)
os.write(fd,(str(status)+'\\n').encode('ascii'))
os.close(fd)
while True: signal.pause()
"""

def capture_graphframes_worker(argv, *, cwd, environment, renew,
            timeout_seconds, maximum_output_bytes, retain_transport=None) -> tuple[bytes, bytes]:
    """Own one session, cap both streams and deadline, kill descendants on every outcome."""
    if type(argv)not in (list,tuple)or not argv or len(argv)>16 or any(type(v)is not str or not v or '\x00'in v or len(v.encode())>4096 for v in argv):raise ValueError('Exact bounded child argv required')
    if type(cwd)is not type(Path())or not cwd.is_absolute()or type(environment)is not dict or any(type(k)is not str or type(v)is not str or '\x00'in k or '\x00'in v for k,v in environment.items())or sum(len(k.encode())+len(v.encode())for k,v in environment.items())>65536:raise ValueError('Explicit bounded child environment required')
    if (type(timeout_seconds) is not int or not 0 < timeout_seconds <= 300 or
            type(maximum_output_bytes) is not int or not 0 < maximum_output_bytes <= 1048576 or
            not argv or not Path(argv[0]).is_absolute()):
        raise ValueError('graphframes-worker-refused')
    if not callable(renew)or retain_transport is not None and not callable(retain_transport):raise ValueError('Actual held authority renewal and owned transport sink required')
    renew()
    next_renewal=time.monotonic()
    process = None
    selector = None
    streams = []
    primary = None
    cleanup = None
    output = [bytearray(), bytearray(), bytearray()]
    control_read = None
    control_write = None
    producer_status = None
    deadline = time.monotonic() + timeout_seconds
    try:
        control_read, control_write = os.pipe()
        process = subprocess.Popen([str(argv[0]), '-I', '-B', '-c', _SUPERVISOR, str(control_write), *argv], cwd=cwd, env=dict(environment),
                                   stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE, start_new_session=True, pass_fds=(control_write,))
        streams = [process.stdout, process.stderr]
        os.close(control_write)
        control_write = None
        control_stream = os.fdopen(control_read, 'rb', buffering=0)
        control_read = None
        streams.append(control_stream)
        selector = selectors.DefaultSelector()
        for index, stream in enumerate(streams):
            os.set_blocking(stream.fileno(), False)
            selector.register(stream, selectors.EVENT_READ, index)
        while selector.get_map():
            if time.monotonic()>=next_renewal:
                renew();next_renewal=time.monotonic()+.5
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise ValueError('graphframes-worker-refused')
            for key, _ in selector.select(min(remaining, .05)):
                bound = 16 if key.data == 2 else maximum_output_bytes
                raw = os.read(key.fileobj.fileno(), min(65536, bound - len(output[key.data]) + 1))
                if not raw:
                    selector.unregister(key.fileobj)
                    continue
                if len(output[key.data]) + len(raw) > bound:
                    raise ValueError('graphframes-worker-refused')
                output[key.data].extend(raw)
        status_bytes = bytes(output[2])
        producer_status = int(status_bytes)
        if status_bytes != (str(producer_status) + '\n').encode('ascii') or not -255 <= producer_status <= 255:
            raise ValueError('graphframes-worker-refused')
    except BaseException as exc:
        primary = exc
    finally:
        actions = []
        if process is not None:
            def kill():
                try:
                    os.killpg(process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            actions.extend((kill, lambda: process.wait(timeout=1)))
        for fd in (control_read, control_write):
            if fd is not None:
                actions.append(lambda fd=fd: os.close(fd))
        if selector is not None:
            actions.append(selector.close)
        actions.extend(stream.close for stream in streams)
        if retain_transport is not None:actions.append(lambda:retain_transport({'stdoutHex':bytes(output[0]).hex(),'stderrHex':bytes(output[1]).hex(),'exit':producer_status,'complete':producer_status is not None and primary is None and cleanup is None,'scope':'bounded owned stream prefixes'}))
        for action in actions:
            try:
                action()
            except BaseException as exc:
                if cleanup is None or isinstance(cleanup, Exception) and not isinstance(exc, Exception):
                    cleanup = exc
        if primary is not None:
            if cleanup is not None:
                try:
                    primary.cleanup_failed = True
                except BaseException:
                    pass
            if not isinstance(primary, Exception):
                raise primary
            if cleanup is not None and not isinstance(cleanup, Exception):
                raise cleanup
            raise ValueError('graphframes-worker-refused') from None
        if cleanup is not None:
            if not isinstance(cleanup, Exception):
                raise cleanup
            raise ValueError('graphframes-worker-refused') from None
    if producer_status != 0:
        raise ValueError('graphframes-worker-refused')
    renew()
    return bytes(output[0]), bytes(output[1])

def _write_owned(path,raw):
 if type(raw)is not bytes or not 0<len(raw)<=16777216:raise ValueError('Bounded owned bytes required')
 stream=path.open('xb');primary=None
 try:stream.write(raw);stream.flush()
 except BaseException as error:primary=error
 finish(primary,[stream.close])


def execute_graphframes_release(session,runtime,pack_bytes,observe):
 """Original seventeen cases under the unchanged exact pack source profile."""
 from .graphframes_pack_scenarios import prepare_scenarios
 if type(runtime)is not GraphFramesRuntimeConfig or not callable(observe):raise ValueError('Exact runtime and owned sink required')
 runtime.renew()
 with hold_graphframes_session(session)as(release,renew_authority):
  original=_source(session.source)
  profile=prepare_scenarios(session.source.definition,original[1],original[2],pack_bytes,json.loads(release.payload))
  result=_execute_prepared_graphframes(runtime,session.runtime_policy,session.runtime_context,release,profile,renew_authority,observe)
 return result


def execute_commerce_graphframes_release(session,runtime,pack_bytes,observe):
 """Closed public commerce dispatch sharing the single owned worker lifetime."""
 from .commerce_graphframes_release import CommerceGraphFramesSession,hold_commerce_graphframes_session
 if type(session)is not CommerceGraphFramesSession or type(runtime)is not GraphFramesRuntimeConfig or not callable(observe):raise ValueError('Exact commerce session/runtime and owned sink required')
 runtime.renew()
 with hold_commerce_graphframes_session(session,pack_bytes)as(release,profile,renew):
  result=_execute_prepared_graphframes(runtime,session.runtime_policy,session.runtime_context,release,profile,renew,observe)
 return result


def _execute_prepared_graphframes(runtime,runtime_policy,runtime_context,release,profile,renew_authority,observe):
 """Internal closed-profile seam; public owning sessions supply current renewal."""
 if type(runtime)is not GraphFramesRuntimeConfig or type(release)is not GraphRelease or not callable(observe):raise ValueError('Exact runtime/release and owned sink required')
 profile=source_snapshot(profile)
 if profile.get('pack')not in ('archaeology','ecology','commerce')or(profile['pack']=='commerce'and profile.get('profile')!='original-commerce/0.1'):raise ValueError('Closed packaged GraphFrames profile required')
 runtime.renew();renew_authority()
 if runtime.work.exists():raise ValueError('Fresh owned GraphFrames output required')
 runtime.work.mkdir(mode=0o700)
 identity=(runtime.work.stat().st_dev,runtime.work.stat().st_ino)
 inventory=runtime_policy.admit_graphframes_runtime(runtime,runtime_context)
 if type(inventory)is not GraphFramesRuntimeInventory:raise PermissionError('Actual complete installed runtime inventory admission required')
 inventory.verify(runtime)
 request={'format':'ashlar-graphframes-worker/0.1','profile':profile,'release':json.loads(release.payload),'versions':VERSIONS,'maximum_evidence_bytes':runtime.maximum_evidence_bytes,'jars':{name:str(runtime.jars/name)for name in JARS}}
 raw=(json.dumps(request,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
 input_path=runtime.work/'input.json';_write_owned(input_path,raw);os.chmod(input_path,0o400)
 def renew():
  renew_graphframes_runtime(runtime,inventory,runtime_policy,runtime_context);renew_authority()
  if runtime.work.is_symlink()or(runtime.work.stat().st_dev,runtime.work.stat().st_ino)!=identity or snapshot(input_path,16777216)!=raw:raise ValueError('Owned worker correspondence changed')
 environment={'PATH':str(runtime.java_home/'bin')+':/usr/bin:/bin','JAVA_HOME':str(runtime.java_home),'TMPDIR':str(runtime.work),'PYSPARK_PYTHON':str(runtime.python),'PYSPARK_DRIVER_PYTHON':str(runtime.python)}
 primary=None;observed_records=[]
 try:
  capture_graphframes_worker([str(runtime.python),'-I','-B','-m','ashlar_host.graphframes_release','--worker',str(input_path),str(runtime.work)],cwd=runtime.work,environment=environment,renew=renew,timeout_seconds=runtime.timeout_seconds,maximum_output_bytes=runtime.maximum_stream_bytes,retain_transport=lambda value:_write_owned(runtime.work/'transport.json',(json.dumps(value,sort_keys=True,separators=(',',':'))+'\n').encode()))
 except BaseException as error:primary=error
 def retain():
  path=runtime.work/'observations.jsonl'
  if path.exists():
   observations=snapshot(path,runtime.maximum_evidence_bytes)
   for line in observations.splitlines():
    value=json.loads(line);observed_records.append(source_snapshot(value))
    if observe(source_snapshot(value))is not None:raise ValueError('Raw observation sink refused')
 finish(primary,[retain,renew])
 result=json.loads(snapshot(runtime.work/'result.json',runtime.maximum_evidence_bytes))
 from .graphframes_pack_scenarios import validate_case,bag,common_oracle
 import base64
 if type(result.get('cases'))is not list or [r.get('original_scenario')for r in result['cases']]!=[c['original_scenario']for c in profile['cases']]:raise ValueError('Complete unchanged original case order required')
 for record in result['cases']:
  raw_witness=[v for v in observed_records if v.get('kind')=='original-scenario'and v.get('case')==record['original_scenario']['id']]
  if len(raw_witness)!=1 or raw_witness[0].get('schema')!=record['schema']or raw_witness[0].get('rows')!=record['rows']or raw_witness[0].get('original_scenario')!=record['original_scenario']:raise ValueError('Complete actual raw scenario witness differs')
  validate_scenario_evidence(profile,record)
 expected_common=common_oracle(json.loads(base64.b64decode(profile['source_admission']['original_graph_base64'],validate=True)))
 if type(result.get('common'))is not dict:raise ValueError('Complete common topology evidence required')
 for name in ('one-hop','two-hop','grouped-count','singleton','filtered-limit','isolates'):
  if bag(result['common'].get(name,[]))!=bag(expected_common[name]):raise ValueError('Parent original common occurrence bag differs')
 if result['common'].get('filtered-total')!=expected_common['filtered-total']:raise ValueError('Parent original filtered membership differs')
 if bag(result['common'].get('all-objects',[]))!=bag(request['release']['nodes'])or bag(result['common'].get('all-edges',[]))!=bag(request['release']['edges']):raise ValueError('Parent complete original canonical carrier bags differ')
 if type(result)is not dict or result.get('format')!='ashlar-original-pack-graphframes/0.1' or result.get('pack')!=profile['pack']or result.get('all_original_cases_passed')!=len(profile['cases']):raise ValueError('Complete original GraphFrames evidence required')
 inventory.verify(runtime)
 validate_derived_evidence(result,request['release'],observed_records,runtime.work)
 validate_common_evidence(result,request['release'],profile,observed_records)
 validate_projected_carrier_evidence(profile,observed_records)
 if profile['pack']=='commerce':
  from .commerce_graphframes_scenarios import validate_arithmetic_evidence
  validate_arithmetic_evidence(profile,observed_records)
 renew()
 runtime.renew()
 return source_snapshot(result)


def graphframes_worker(input_path,output):
 """Isolated engine correspondence only; authority stays in the parent host."""
 import sys,importlib.metadata
 from .graphframes_pack_scenarios import execute_scenarios,bag,bounded_frame_rows,common_oracle
 from ashlar.graph_release import release_columns
 from pyspark.sql import SparkSession,functions as F
 from pyspark.sql.types import StructType,StructField,StringType,LongType
 from graphframes import GraphFrame
 if sys.version_info[:2]!=(3,11)or {name:importlib.metadata.version(name)for name in VERSIONS}!=VERSIONS:raise ValueError('Exact selected GraphFrames runtime required')
 request=json.loads(snapshot(input_path,16777216));profile=request['profile'];release=request['release']
 if request['format']!='ashlar-graphframes-worker/0.1' or request['versions']!=VERSIONS or set(request['jars'])!=set(JARS):raise ValueError('Closed selected worker profile required')
 for name,path in request['jars'].items():
  if hashlib.sha256(snapshot(Path(path),16777216)).hexdigest()!=JARS[name]:raise ValueError('Worker JAR custody differs')
 def observe(value):
  path=output/'observations.jsonl';raw=(json.dumps(value,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode()
  if len(raw)+(path.stat().st_size if path.exists()else 0)>request['maximum_evidence_bytes']:raise ValueError('Complete observation byte budget exceeded')
  stream=path.open('ab');primary=None
  try:stream.write(raw);stream.flush()
  except BaseException as error:primary=error
  finish(primary,[stream.close])
 spark=(SparkSession.builder.master('local[1]').appName('Ashlar installed original GraphFrames').config('spark.driver.memory','512m').config('spark.ui.enabled','false').config('spark.sql.shuffle.partitions','1').config('spark.databricks.delta.snapshotPartitions','1').config('spark.sql.session.timeZone','UTC').config('spark.sql.ansi.enabled','true').config('spark.jars',','.join(request['jars'].values())).config('spark.sql.extensions','io.delta.sql.DeltaSparkSessionExtension').config('spark.sql.catalog.spark_catalog','org.apache.spark.sql.delta.catalog.DeltaCatalog').config('spark.sql.warehouse.dir',str(output/'warehouse')).getOrCreate())
 primary=None;result=None
 try:
  frames={};derived={};paths={};integers={f['signed64_column']for f in profile['fields']if f['signed64_column']}
  for role,rows in [('nodes',profile['nodes']),('edges',profile['edges'])]:
   names=list(rows[0]);schema=StructType([StructField(name,LongType()if name in integers else StringType(),True)for name in names])
   path=output/role;paths[role]=path
   spark.createDataFrame([[row[n]for n in names]for row in rows],schema).write.format('delta').save(str(path))
   detail=spark.sql('DESCRIBE DETAIL delta.`'+str(path).replace('`','``')+'`').first().asDict()
   derived[role]={'uuid':detail['id'],'version':0,'location':detail['location'],'minReaderVersion':detail['minReaderVersion'],'minWriterVersion':detail['minWriterVersion'],'tableFeatures':detail.get('tableFeatures')}
   observe({'kind':'derived-snapshot','phase':'opening','role':role,'detail':derived[role],'history':bounded_frame_rows(spark.sql('DESCRIBE HISTORY delta.`'+str(path).replace('`','``')+'`').select('version'),2)})
   frames[role]=spark.read.format('delta').option('versionAsOf',0).load(str(path))
  def parity(phase):
   for role in ('nodes','edges'):
    frame=frames[role];names=frame.columns;raw=bounded_frame_rows(frame,10000)
    observe({'kind':'full-carrier','phase':phase,'role':role,'schema':[{'name':f.name,'type':f.dataType.simpleString()}for f in frame.schema.fields],'rows':raw})
    if bag(dict(zip(names,row))for row in raw)!=bag(profile[role]):raise ValueError('Every original projected carrier cell differs')
    detail=spark.sql('DESCRIBE DETAIL delta.`'+str(paths[role]).replace('`','``')+'`').first().asDict()
    history=spark.sql('DESCRIBE HISTORY delta.`'+str(paths[role]).replace('`','``')+'`').select('version')
    witness={'uuid':detail['id'],'version':0,'location':detail['location'],'minReaderVersion':detail['minReaderVersion'],'minWriterVersion':detail['minWriterVersion'],'tableFeatures':detail.get('tableFeatures')}
    versions=bounded_frame_rows(history,2);observe({'kind':'derived-snapshot','phase':phase,'role':role,'detail':witness,'history':versions})
    if witness!=derived[role]or versions!=[[0]]:raise ValueError('Distinct immutable derived Delta vector changed')
  parity('before-query')
  vertices=frames['nodes'].withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id');edges=frames['edges'].withColumnRenamed('id','carrier_id').withColumnRenamed('graph_id','id');graphframe=GraphFrame(vertices,edges)
  if profile.get('pack')=='commerce':
   from .commerce_graphframes_scenarios import execute_commerce_graphframes_scenarios
   cases=execute_commerce_graphframes_scenarios(graphframe,profile,observe)
  else:cases=execute_scenarios(graphframe,profile,observe)
  common=execute_common_graphframes(graphframe,profile,release,observe)
  parity('closing');result={'format':'ashlar-original-pack-graphframes/0.1','pack':profile['pack'],'cases':cases,'common':common,'derived_snapshots':derived,'original_snapshots':release['snapshots'],'all_original_cases_passed':len(cases),'full_carrier_parity':True,'authority_scope':'Parent-selected live hold or separately admitted retained protected-ACK export; this worker establishes no authority.'}
 except BaseException as error:primary=error
 finish(primary,[spark.stop])
 _write_owned(output/'result.json',(json.dumps(result,ensure_ascii=False,sort_keys=True,separators=(',',':'),allow_nan=False)+'\n').encode())

def execute_common_graphframes(frame,profile,release,observe):
 """Common seven original bag operations plus independent isolated-node control."""
 import base64
 from pyspark.sql import functions as F
 from ashlar.graph_release import release_columns
 from .graphframes_pack_scenarios import common_oracle,bounded_frame_rows,bag
 graph=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64'],validate=True));expected=common_oracle(graph)
 nodes={row['graph_id']:row['original_key']for row in profile['nodes']}
 edges={row['graph_id']:json.loads(row['retained_json'])['original']['key']for row in profile['edges']}
 relationships={row['rel_type_id']:json.dumps(json.loads(row['retained_json'])['original']['relationship'],sort_keys=True,separators=(',',':'))for row in profile['edges']}
 def capture(name,data):
  rows=bounded_frame_rows(data,10000);schema=[{'name':f.name,'type':f.dataType.simpleString()}for f in data.schema.fields]
  if observe(source_snapshot({'kind':'common-query','case':name,'schema':schema,'rows':rows}))is not None:raise ValueError('Common observation sink refused')
  return rows
 def canonical(kind,data,name):
  names=release_columns(kind)
  expressions=[F.col('carrier_id'if n=='id'else'id'if n=='graph_id'else n).alias(n)for n in names]
  raw=capture(name,data.select(*expressions))
  actual=[dict(zip(names,row))for row in raw]
  return actual
 actual={}
 actual['all-objects']=canonical('node',frame.vertices,'all-objects')
 actual['all-edges']=canonical('edge',frame.edges,'all-edges')
 if bag(actual['all-objects'])!=bag(release['nodes'])or bag(actual['all-edges'])!=bag(release['edges']):raise ValueError('Common complete original carriers differ')
 raw=capture('one-hop',frame.find('(a)-[e]->(b)').select('a.id','e.id','b.id'))
 actual['one-hop']=[[nodes[a],edges[e],nodes[b]]for a,e,b in raw]
 raw=capture('two-hop',frame.find('(a)-[e]->(b); (b)-[f]->(c)').select('a.id','e.id','b.id','f.id','c.id'))
 actual['two-hop']=[[nodes[a],edges[e],nodes[b],edges[f],nodes[c]]for a,e,b,f,c in raw]
 grouped=frame.edges.groupBy('src','rel_type_id').agg(F.count('*').alias('occurrences'),F.countDistinct('dst').alias('destinations'))
 actual['grouped-count']=[[nodes[s],relationships[t],count,distinct]for s,t,count,distinct in capture('grouped-count',grouped)]
 by_key={key:gid for gid,key in nodes.items()}
 singleton=frame.vertices.where(F.col('id')==by_key[expected['singleton'][0]])
 singleton_values=canonical('node',singleton,'singleton');actual['singleton']=[nodes[row['graph_id']]for row in singleton_values]
 filtered=frame.vertices.where(F.col('original_type')==expected['filtered-type'])
 filtered_raw=capture('filtered-total',filtered.select('id'));actual['filtered-total']=len(filtered_raw)
 limited=filtered.orderBy('original_key').limit(1)
 limited_values=canonical('node',limited,'filtered-limit');actual['filtered-limit']=[nodes[row['graph_id']]for row in limited_values]
 endpoints=frame.edges.select(F.col('src').alias('id')).union(frame.edges.select(F.col('dst').alias('id'))).distinct()
 isolated=frame.vertices.join(endpoints,'id','left_anti');isolated_values=canonical('node',isolated,'isolates')
 actual['isolates']=sorted(nodes[row['graph_id']]for row in isolated_values)
 original={row['graph_id']:row for row in release['nodes']}
 if any(row!=original[row['graph_id']]for row in singleton_values+limited_values+isolated_values):raise ValueError('Complete selected or isolated carrier differs')
 for name in ('one-hop','two-hop','grouped-count','singleton','filtered-limit','isolates'):
  if bag(actual[name])!=bag(expected[name]):raise ValueError('Complete original common occurrence bag differs '+name)
 if actual['filtered-total']!=expected['filtered-total']:raise ValueError('Original complete filtered membership differs')
 return actual

def validate_projected_carrier_evidence(profile,observations):
 """Bind every projected cell, including unused fields, to original source bags."""
 from .graphframes_pack_scenarios import bag
 integers={f['signed64_column']for f in profile['fields']if f['signed64_column']}
 selected=[v for v in observations if v.get('kind')=='full-carrier']
 if len(selected)!=4:raise ValueError('Complete before-query and closing projected witnesses required')
 for phase in ('before-query','closing'):
  for role in ('nodes','edges'):
   found=[v for v in selected if v.get('phase')==phase and v.get('role')==role]
   names=sorted(profile[role][0]);types=['bigint'if n in integers else 'string'for n in names]
   if len(found)!=1 or set(found[0])!={'kind','phase','role','schema','rows'}or found[0]['schema']!=[{'name':n,'type':t}for n,t in zip(names,types)]:raise ValueError('Exact complete projected schema and phase required')
   rows=found[0]['rows']
   if type(rows)is not list or len(rows)>10000 or any(type(r)is not list or len(r)!=len(names)for r in rows):raise ValueError('Bounded complete projected rows required')
   for row in rows:
    for value,kind in zip(row,types):
     if value is not None and (kind=='string'and type(value)is not str or kind=='bigint'and (type(value)is not int or not -(2**63)<=value<2**63)):raise ValueError('Exact projected native cell type required')
   if bag(dict(zip(names,row))for row in rows)!=bag(profile[role]):raise ValueError('Complete original projected carrier bag differs')

def validate_scenario_evidence(profile,record):
 """Keep reported logical cells and independent oracle bound to original facts."""
 if profile.get('pack')=='commerce':
  from .commerce_graphframes_scenarios import validate_case
 else:
  from .graphframes_pack_scenarios import validate_case
 record=source_snapshot(record)
 if type(record)is not dict or set(record)!={'original_scenario','schema','rows','logical_rows','independent_expected'}or type(record['original_scenario'])is not dict or set(record['original_scenario'])!={'id','sql'}or any(type(v)is not str for v in record['original_scenario'].values()):raise ValueError('Closed original scenario evidence required')
 originals=[c for c in profile['cases']if c['original_scenario']==record['original_scenario']]
 if len(originals)!=1:raise ValueError('Exact original scenario identity and SQL required')
 actual=validate_case(profile,record['original_scenario']['id'],record['schema'],record['rows'])
 if record['logical_rows']!=actual or record['independent_expected']!=originals[0]['original_graph_expected']:raise ValueError('Reported logical/oracle values differ from original facts')
 return actual

def validate_common_evidence(result,release,profile,observations):
 """Bind complete common claims to raw native cells, not report assertions."""
 from ashlar.graph_release import release_columns
 from .graphframes_pack_scenarios import bag,common_oracle
 import base64
 graph=json.loads(base64.b64decode(profile['source_admission']['original_graph_base64'],validate=True))
 expected=common_oracle(graph);nodes={row['graph_id']:row['original_key']for row in profile['nodes']}
 edges={row['graph_id']:json.loads(row['retained_json'])['original']['key']for row in profile['edges']}
 relations={row['rel_type_id']:json.dumps(json.loads(row['retained_json'])['original']['relationship'],sort_keys=True,separators=(',',':'))for row in profile['edges']}
 def raw(name,names,types):
  found=[v for v in observations if v.get('kind')=='common-query'and v.get('case')==name]
  if len(found)!=1 or set(found[0])!={'kind','case','schema','rows'}or found[0]['schema']!=[{'name':n,'type':t}for n,t in zip(names,types)]:raise ValueError('Complete exact common raw schema witness required')
  rows=found[0]['rows']
  if type(rows)is not list or len(rows)>10000 or any(type(r)is not list or len(r)!=len(names)for r in rows):raise ValueError('Complete bounded common raw row witness required')
  for r in rows:
   for value,kind in zip(r,types):
    if kind=='bigint'and type(value)is not int or kind=='string'and value is not None and type(value)is not str:raise ValueError('Exact common native cell type required')
  return rows
 actual={}
 for kind,role,name in [('node','nodes','all-objects'),('edge','edges','all-edges')]:
  names=release_columns(kind);actual[name]=[dict(zip(names,r))for r in raw(name,names,['string']*len(names))]
  if bag(actual[name])!=bag(release[role]):raise ValueError('Raw complete canonical carrier witness differs')
 actual['one-hop']=[[nodes[a],edges[e],nodes[b]]for a,e,b in raw('one-hop',['id']*3,['string']*3)]
 actual['two-hop']=[[nodes[a],edges[e],nodes[b],edges[f],nodes[c]]for a,e,b,f,c in raw('two-hop',['id']*5,['string']*5)]
 actual['grouped-count']=[[nodes[a],relations[t],n,d]for a,t,n,d in raw('grouped-count',['src','rel_type_id','occurrences','destinations'],['string','string','bigint','bigint'])]
 for name in ('singleton','filtered-limit','isolates'):
  names=release_columns('node');rows=[dict(zip(names,r))for r in raw(name,names,['string']*len(names))]
  originals={r['graph_id']:r for r in release['nodes']}
  if any(row!=originals.get(row['graph_id'])for row in rows):raise ValueError('Raw complete selected canonical carrier differs')
  actual[name]=[nodes[row['graph_id']]for row in rows]
 filtered=raw('filtered-total',['id'],['string']);actual['filtered-total']=len(filtered)
 if bag([nodes[row[0]]for row in filtered])!=bag([obj['key']for obj in graph['objects']if json.dumps(obj['type'],sort_keys=True,separators=(',',':'))==expected['filtered-type']]):raise ValueError('Raw complete filtered membership differs')
 if set(result['common'])!=set(actual):raise ValueError('Closed complete common result required')
 for name,rows in actual.items():
  if name=='filtered-total':
   if type(result['common'][name])is not int or result['common'][name]!=rows:raise ValueError('Raw common count differs')
  elif bag(result['common'][name])!=bag(rows):raise ValueError('Common result is not original raw witness')

def validate_derived_evidence(result,release,observations,work):
 """Bind final engine claims to complete raw opening/closing derived witnesses."""
 import uuid
 from urllib.parse import urlsplit,unquote
 keys={'format','pack','cases','common','derived_snapshots','original_snapshots','all_original_cases_passed','full_carrier_parity','authority_scope'}
 if type(result)is not dict or set(result)!=keys or result['format']!='ashlar-original-pack-graphframes/0.1'or type(result['pack'])is not str or result['pack']not in ('archaeology','ecology','commerce')or type(result['cases'])is not list or type(result['common'])is not dict or type(result['all_original_cases_passed'])is not int or result['all_original_cases_passed']!=({'archaeology':8,'ecology':9,'commerce':4}[result['pack']])or len(result['cases'])!=result['all_original_cases_passed']or type(result['authority_scope'])is not str or not 0<len(result['authority_scope'])<=1024:raise ValueError('Closed original GraphFrames result required')
 for case in result['cases']:
  if type(case)is not dict or set(case)!={'original_scenario','schema','rows','logical_rows','independent_expected'}or type(case['original_scenario'])is not dict or set(case['original_scenario'])!={'id','sql'}or any(type(v)is not str for v in case['original_scenario'].values()):raise ValueError('Closed unchanged original case evidence required')
 if result.get('original_snapshots')!=release['snapshots']:raise ValueError('Original complete publication vector differs')
 if type(result.get('original_snapshots'))is not dict or any(type(k)is not str or type(v)is not dict or set(v)!={'uuid','version'}or type(v['uuid'])is not str or type(v['version'])is not int for k,v in result['original_snapshots'].items()):raise ValueError('Exact original snapshot carriers required')
 if result.get('full_carrier_parity')is not True:raise ValueError('Full native carrier parity required')
 derived=result.get('derived_snapshots')
 if type(derived)is not dict or set(derived)!={'nodes','edges'}:raise ValueError('Complete separate derived vector required')
 keys={'uuid','version','location','minReaderVersion','minWriterVersion','tableFeatures'}
 identifiers=[]
 for role,detail in derived.items():
  if type(detail)is not dict or set(detail)!=keys:raise ValueError('Closed native derived detail required')
  if type(detail['uuid'])is not str or str(uuid.UUID(detail['uuid']))!=detail['uuid']:raise ValueError('Actual canonical derived UUID required')
  identifiers.append(detail['uuid'])
  if type(detail['version'])is not int or detail['version']!=0 or type(detail['minReaderVersion'])is not int or detail['minReaderVersion']!=1 or type(detail['minWriterVersion'])is not int or detail['minWriterVersion']!=2:raise ValueError('Selected immutable Delta0 protocol required')
  features=detail['tableFeatures']
  if features is not None and(type(features)is not list or any(type(v)is not str for v in features)or len(set(features))!=len(features)or not set(features)<= {'appendOnly','invariants'}):raise ValueError('Unknown native Delta feature refused')
  if type(detail['location'])is not str:raise ValueError('Actual derived location required')
  location=urlsplit(detail['location'])
  if location.scheme!='file'or location.netloc not in ('','localhost')or location.query or location.fragment or Path(unquote(location.path))!=work/role:raise ValueError('Actual separately owned derived location differs')
  for phase in ('opening','before-query','closing'):
   found=[v for v in observations if v.get('kind')=='derived-snapshot'and v.get('role')==role and v.get('phase')==phase]
   if len(found)!=1 or set(found[0])!={'kind','phase','role','detail','history'}or found[0]['detail']!=detail or found[0]['history']!=[[0]]or any(type(row[0])is not int for row in found[0]['history']):raise ValueError('Complete exact native derived closing witness differs')
 if len(set(identifiers))!=2 or set(identifiers)&{v['uuid']for v in release['snapshots'].values()}:raise ValueError('Derived UUIDs must remain distinct from original canonical vector')

if __name__=='__main__':
 import argparse
 parser=argparse.ArgumentParser(description=__doc__)
 parser.add_argument('--worker',action='store_true',required=True)
 parser.add_argument('input',type=Path);parser.add_argument('output',type=Path)
 args=parser.parse_args();graphframes_worker(args.input,args.output)
