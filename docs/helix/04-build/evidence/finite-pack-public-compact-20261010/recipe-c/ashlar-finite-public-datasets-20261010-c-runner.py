"""Two reviewed public dataset operations, full opening/closing custody and bounded capture."""
import hashlib,importlib.util,json,os,sys
from pathlib import Path
CLOSURE=Path('/private/tmp/ashlar-finite-public-datasets-20261010-c-closure.json')
CLOSURE_SHA='89648f3aca73904c9eeca564d7218356b464b7f70fb0816ff02dc9095f5f97eb'
ROOT=Path('/private/tmp/ashlar-finite-public-datasets-20261010-c-source')
OUTPUT=Path('/private/tmp/ashlar-finite-public-datasets-20261010-c')
RECEIPT=Path('/private/tmp/ashlar-finite-public-datasets-20261010-c.receipt.json')
CAPTURE=Path('/private/tmp/ashlar-count-star-installed-cli-capture-a.py')
PYTHON='/usr/bin/python3'
ENV={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C','TZ':'UTC','PYTHONDONTWRITEBYTECODE':'1','PYTHONPATH':str(ROOT/'src'),'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_TERMINAL_PROMPT':'0'}
COMMAND=[PYTHON,'-B','-S','/private/tmp/ashlar-finite-public-datasets-20261010-c-probe.py']
def digest(p):
 h=hashlib.sha256()
 with p.open('rb')as f:
  for block in iter(lambda:f.read(262144),b''):h.update(block)
 return h.hexdigest()
def verify():
 if digest(CLOSURE)!=CLOSURE_SHA:raise ValueError('closure pin')
 plan=json.loads(CLOSURE.read_bytes())
 for name in plan['absent_startup_paths']:
  if Path(name).exists():raise ValueError('Selected absent Python startup path changed')
 if len(plan['files'])>plan['maximum_files']or sum(r['bytes']for r in plan['files'].values())>plan['maximum_total_bytes']:raise ValueError('closed custody capacity')
 for link in plan['symlinks']:
  p=Path(link['path'])
  if not p.is_symlink()or os.readlink(p)!=link['target']or str(p.resolve())!=link['resolved']:raise ValueError('dependency selection link changed')
 for root in plan['roots']:
  actual={str(p.resolve())for p in Path(root).rglob('*')if p.is_file()and '__pycache__'not in p.parts and p.name!='.DS_Store'and p.suffix!='.pyc'}
  expected={n for n in plan['files']if n.startswith(str(Path(root).resolve())+'/')}
  if actual!=expected:raise ValueError('complete selected custody inventory changed')
 for name,r in plan['files'].items():
  p=Path(name)
  if p.is_symlink()or not p.is_file()or p.stat().st_size!=r['bytes']or digest(p)!=r['sha256']:raise ValueError('selected custody bytes changed')
def write_receipt(path,value,primary,select_failure):
 stream=None
 try:
  stream=path.open('x');json.dump(value,stream,indent=2);stream.write('\n')
 except BaseException as error:primary=select_failure(primary,error)
 finally:
  if stream is not None:
   try:stream.close()
   except BaseException as error:primary=select_failure(primary,error)
 return primary
def main():
 if len(sys.argv)!=2 or digest(Path(__file__))!=sys.argv[1]:raise ValueError('reviewed runner pin')
 verify()
 if OUTPUT.exists()or RECEIPT.exists():raise ValueError('fresh native outputs required')
 spec=importlib.util.spec_from_file_location('capture',CAPTURE);capture=importlib.util.module_from_spec(spec);spec.loader.exec_module(capture)
 primary=None;result=None;closing=True
 try:result=capture.capture(COMMAND,ROOT,ENV,Path('/private/tmp/ashlar-finite-public-datasets-20261010-c.stdout'),Path('/private/tmp/ashlar-finite-public-datasets-20261010-c.stderr'),120,2*1024*1024)
 except BaseException as error:primary=error
 finally:
  try:verify()
  except BaseException as error:closing=False;primary=capture.select_failure(primary,error)
 primary=write_receipt(RECEIPT,{'result':result,'openingClosingCustody':closing,'failureType':None if primary is None else type(primary).__name__,'sourceRevision':'83528d6249cb86a883ed2b36652fc546ae6eae85','qualification':'Actual public UMF finite dataset operations only; no Spark/Delta/query/native/source authority/installedwheel/PG/ACK/cloud claim.'},primary,capture.select_failure)
 if primary is not None:raise primary
 if result['exitCode']:raise SystemExit(result['exitCode'])
if __name__=='__main__':main()
