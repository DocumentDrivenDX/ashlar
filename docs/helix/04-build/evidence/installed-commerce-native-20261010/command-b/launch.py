"""Bounded installed host phase launcher; exclusive native grant is external."""
import argparse,hashlib,json,importlib.util,pathlib,sys

def descriptor(path):
 p=pathlib.Path(path);h=hashlib.sha256();size=0
 with p.open('rb') as f:
  for b in iter(lambda:f.read(1024*1024),b''):h.update(b);size+=len(b)
 return {'path':str(p),'sha256':h.hexdigest(),'bytes':size}
def verify(items):
 drift=[]
 for d in items:
  actual=descriptor(d['path'])
  if actual!=d:drift.append(actual)
 return drift
def native_files(root):
 root=pathlib.Path(root);files=[];total=0
 if not root.exists():return files
 for p in sorted(root.rglob('*')):
  if p.is_symlink():raise ValueError('publication symlink')
  if p.is_file():
   total+=p.stat().st_size
   if len(files)>=256 or p.stat().st_size>16*1024*1024 or total>64*1024*1024:raise ValueError('tiny publication custody bound')
   files.append(descriptor(p))
 return files
def main():
 p=argparse.ArgumentParser();p.add_argument('--command',required=True);p.add_argument('--sha256',required=True);p.add_argument('--phase',required=True,choices=['publish','query']);a=p.parse_args()
 with open(a.command,'rb') as f:raw=f.read(2000001)
 if len(raw)>2000000 or hashlib.sha256(raw).hexdigest()!=a.sha256:raise ValueError('command-pin')
 plan=json.loads(raw);c=next(c for c in plan['phases']if c['phase']==a.phase)
 if verify(plan['resources']):raise ValueError('opening custody')
 own=pathlib.Path(plan['capture']['path']);spec=importlib.util.spec_from_file_location('reviewed_bounded_capture',own);m=importlib.util.module_from_spec(spec);spec.loader.exec_module(m)
 before=native_files(plan['publication']);primary=None;result=None;drift=[];after=[]
 try:result=m.capture(c['argv'],c['cwd'],c['environment'],c['stdout'],c['stderr'],180,16*1024*1024)
 except BaseException as e:primary=e
 finally:
  try:
   drift=verify(plan['resources']);after=native_files(plan['publication'])
   if drift:raise ValueError('closing custody')
  except BaseException as e:
   if primary is None:primary=e
  try:
   payload=(json.dumps({'phase':a.phase,'commandSha256':a.sha256,'process':result,'closingResourceDrift':drift,'openingPublicationFiles':before,'closingPublicationFiles':after,'bodyFailed':primary is not None,'scope':'Installed host selected local process/custody observation. Semantic rows, ACK and cleanup receipts require independent review; no blanket native inventory equality claim.'},indent=2)+'\n').encode()
   with pathlib.Path(c['receipt']).open('xb')as f:f.write(payload)
  except BaseException as e:
   if primary is None:primary=e
   else:
    try:primary.cleanup_failed=True
    except BaseException:pass
 if primary is not None:raise primary
 if result['exitCode']:raise SystemExit(result['exitCode'])
if __name__=='__main__':main()
