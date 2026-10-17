"""Outer finite observation only; actual installed public CLI owns all query authority."""
from pathlib import Path
import json,os,signal,subprocess,sys,time,hashlib,selectors
R=Path(__file__).resolve().parent
sys.path.insert(0,str(R))
from native_inventory import native_files
from ashlar_host.medical_query_configuration import parse_original_medical_query
from ashlar_host.lifecycle import finish
from ashlar_host.evolution_producer import capture
os.umask(0o077)
def cancelled(signum,frame):raise KeyboardInterrupt('Outer cancellation')
signal.signal(signal.SIGTERM,cancelled)
selected_raw=(R/'query-configuration.json').read_bytes();selected=parse_original_medical_query(selected_raw)
def retain(name,value):
 with (R/name).open('x')as stream:json.dump(value,stream,indent=2);stream.write('\n');stream.flush();os.fsync(stream.fileno())
before=native_files(selected.read);retain('native-files-before.json',before)
argv=[str(R/'environment/bin/python3'),'-I','-B','-m','ashlar','query-original-medical','--configuration',str(R/'query-configuration.json')]
p=None;primary=None;started=time.monotonic();groups=set();out=bytearray();err=bytearray();selector=selectors.DefaultSelector();last_group_sample=0.0
def observe_groups():
 global last_group_sample
 if p is None or time.monotonic()-last_group_sample<5:return
 last_group_sample=time.monotonic()
 # Best-effort owned-descendant cleanup supplement, not global process absence.
 raw,_=capture(['/bin/ps','-axo','pid=,ppid=,pgid='],cwd=R,environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'},timeout_seconds=2,maximum_output_bytes=1048576)
 rows=raw.decode('ascii').splitlines()
 if len(rows)>8192:raise ValueError('Outer process inventory row capacity exceeded')
 triples=[tuple(map(int,line.split()))for line in rows];owned={p.pid};changed=True
 while changed:
  changed=False
  for pid,parent,group in triples:
   if parent in owned and pid not in owned:owned.add(pid);changed=True
 for pid,parent,group in triples:
  if pid in owned and group!=os.getpgrp():groups.add(group)
def kill_groups(sig):
 for group in sorted(groups):
  try:os.killpg(group,sig)
  except ProcessLookupError:pass
def cleanup():
 if p is None:return
 # SIGINT lets the installed Python CLI unwind its retained child-session owner.
 kill_groups(signal.SIGINT)
 try:p.wait(timeout=10)
 except subprocess.TimeoutExpired:pass
 kill_groups(signal.SIGKILL)
def reap():
 if p is not None:p.wait(timeout=10)
def closed_inventory():
 after=native_files(selected.read);retain('native-files-after.json',after)
 def data(value):return [row for row in value['files']if row['path']!='operations.sqlite-shm']
 if data(before)!=data(after):raise PermissionError('Original native byte closure differs')
 if (R/'query-configuration.json').read_bytes()!=selected_raw:raise PermissionError('Original configuration changed')
def streams():
 for name,data in [('cli.stdout',out),('cli.stderr',err)]:
  with (R/name).open('xb')as stream:stream.write(data);stream.flush();os.fsync(stream.fileno())
def terminal():retain('outer-attempt.json',{'argv':argv,'exit_code':None if p is None else p.returncode,'elapsed_seconds':time.monotonic()-started,'maximum_seconds':990,'primary_type':None if primary is None else type(primary).__name__,'owned_groups':sorted(groups),'stdout_bytes':len(out),'stderr_bytes':len(err)})
try:
 p=subprocess.Popen(argv,cwd=R,stdout=subprocess.PIPE,stderr=subprocess.PIPE,start_new_session=True);groups.add(p.pid)
 for stream,label in [(p.stdout,'stdout'),(p.stderr,'stderr')]:os.set_blocking(stream.fileno(),False);selector.register(stream,selectors.EVENT_READ,label)
 while selector.get_map():
  if time.monotonic()-started>970:raise TimeoutError('Outer970second work bound')
  observe_groups()
  for key,_ in selector.select(0.25):
   block=os.read(key.fileobj.fileno(),65536)
   if not block:selector.unregister(key.fileobj);continue
   target=out if key.data=='stdout'else err;limit=selected.bounds.receipt_bytes if key.data=='stdout'else selected.bounds.stream_bytes
   if len(target)+len(block)>limit:raise ValueError('Outer bounded CLI stream exceeded')
   target.extend(block)
 p.wait(timeout=1)
 if p.returncode!=0:raise RuntimeError('Installed public CLI refused')
 json.loads(bytes(out))
except BaseException as error:primary=error
finish(primary,[cleanup,reap,selector.close]+([]if p is None else[p.stdout.close,p.stderr.close])+[closed_inventory,streams,terminal])
