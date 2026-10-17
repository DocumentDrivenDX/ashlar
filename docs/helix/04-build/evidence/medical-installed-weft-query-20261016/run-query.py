from pathlib import Path
import hashlib,json,os,time,signal,stat
from ashlar_host.original_medical_local_configuration import parse_original_medical_local
from ashlar_host.original_medical_runtime import OriginalMedicalRuntimeCustody
from ashlar_host.medical_worker_capture import capture_original_medical_worker
from ashlar_host.lifecycle import owned_context,finish
def cancelled(signum,frame):raise KeyboardInterrupt('outer termination')
signal.signal(signal.SIGTERM,cancelled)
r=Path(__file__).resolve().parent
config=r/'read-query-configuration.json';worker=r/'query-worker.py'
original=config.read_bytes();assert hashlib.sha256(original).hexdigest()=='407eaabbbd80b7864948217522a2d599752485f5ea0b7e7a157a7bf5af610635'
assert hashlib.sha256(worker.read_bytes()).hexdigest()=='b0177a6fc189ab7de17fa2e42caa38a48608ddfccfbb4d36258232200c7b08d7'
selected=parse_original_medical_local(original)
def native_files():
 root=selected.root;targets={t.path for t in selected.targets};entries=[];total=0
 if len(targets)!=6 or any(t.parent!=root for t in targets):raise PermissionError('Exact six original native target directories required')
 for entry in sorted(root.iterdir()):
  if entry.is_symlink():raise PermissionError('Original native closure symlink refused')
  paths=sorted(entry.rglob('*'))if entry in targets else[entry]
  if entry not in targets and entry.is_dir():raise PermissionError('Unexpected original native control directory')
  for path in paths:
   st=path.lstat()
   if stat.S_ISDIR(st.st_mode):continue
   if not stat.S_ISREG(st.st_mode)or path.is_symlink():raise PermissionError('Original native closure requires regular files')
   if len(entries)>=4096 or st.st_size>134217728-total:raise ValueError('Native closure count/byte capacity exceeded')
   digest=hashlib.sha256();size=0
   with path.open('rb')as stream:
    while True:
     part=stream.read(min(65536,134217729-total-size))
     if not part:break
     size+=len(part)
     if size>st.st_size or total+size>134217728:raise PermissionError('Native closure changed or exceeded bytebound')
     digest.update(part)
   end=path.lstat()
   if size!=st.st_size or(st.st_dev,st.st_ino,st.st_size,st.st_mtime_ns,st.st_ctime_ns)!=(end.st_dev,end.st_ino,end.st_size,end.st_mtime_ns,end.st_ctime_ns):raise PermissionError('Native closure file changed during hashing')
   total+=size;entries.append({'path':str(path.relative_to(root)),'bytes':size,'sha256':digest.hexdigest()})
 return {'profile':'ashlar-original-medical-native-byte-closure/0.1','root':str(root),'maximum_files':4096,'maximum_bytes':134217728,'files':entries,'total_bytes':total,'scope':'Six selected native Delta directories and immediate original journal/control files only; content/inventory correspondence, no source or OS authority. SQLite SHM coordination compared separately.'}
def retain(name,value):
 fd=os.open(r/name,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 with os.fdopen(fd,'w')as out:json.dump(value,out,indent=2);out.write('\n');out.flush();os.fsync(out.fileno())
before=native_files();retain('native-files-before.json',before)
started=time.monotonic();primary=None;stdout=stderr=b'';state='failed';failure=None
try:
 with owned_context(OriginalMedicalRuntimeCustody.open(selected.runtime,selected.inputs,selected.bounds,selected.producer))as custody:
  def renew():
   if config.read_bytes()!=original or hashlib.sha256(worker.read_bytes()).hexdigest()!='b0177a6fc189ab7de17fa2e42caa38a48608ddfccfbb4d36258232200c7b08d7':raise PermissionError('Original SDK worker selection differs')
   custody.renew()
  stdout,stderr=capture_original_medical_worker([str(selected.runtime.python.path),'-I','-B',str(worker)],cwd=r,environment=custody.native_environment(),renew=renew,bounds=selected.bounds)
 state='terminal';code=0
except BaseException as error:
 primary=error;captured=BaseException.__dict__['__dict__'].__get__(error,type(error)).get('medical_capture')
 if type(captured)is dict:
  stdout=captured['stdout'];stderr=captured['stderr'];failure=captured['failure']
 code=2
after=native_files();retain('native-files-after.json',after)
def data_inventory(value):return [row for row in value['files']if row['path']!='operations.sqlite-shm']
closure_equal=data_inventory(before)==data_inventory(after)
if not closure_equal:code=2;state='native-byte-closure-refusal'
for name,raw in [('query.stdout',stdout),('query.stderr',stderr)]:
 fd=os.open(r/name,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
 with os.fdopen(fd,'wb')as out:out.write(raw);out.flush();os.fsync(out.fileno())
record={'profile':'ashlar-original-medical-sdk-query-attempt/0.1','state':state,'exit_code':code,'elapsed_seconds':time.monotonic()-started,'failure':failure,'native_byte_closure_equal':closure_equal,'sqlite_shm_equal':[v for v in before['files']if v['path']=='operations.sqlite-shm']==[v for v in after['files']if v['path']=='operations.sqlite-shm'],'capture_prefixes':{name:{'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}for name,raw in [('stdout',stdout),('stderr',stderr)]},'worker_sha256':'b0177a6fc189ab7de17fa2e42caa38a48608ddfccfbb4d36258232200c7b08d7','configuration_sha256':'407eaabbbd80b7864948217522a2d599752485f5ea0b7e7a157a7bf5af610635','scope':'Actual fixed installed SDK and owned medical worker supervision only; source/native/query success requires closed final result.'}
fd=os.open(r/'query-attempt.json',os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600)
with os.fdopen(fd,'w')as out:json.dump(record,out,indent=2);out.write('\n');out.flush();os.fsync(out.fileno())
print(json.dumps(record));raise SystemExit(code)
