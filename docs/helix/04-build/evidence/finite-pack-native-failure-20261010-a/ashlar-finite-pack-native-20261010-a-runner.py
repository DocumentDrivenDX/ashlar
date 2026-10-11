"""One reviewed finite native process, full opening/closing custody and bounded capture."""
import hashlib,importlib.util,json,os,sys
from pathlib import Path
CLOSURE=Path('/private/tmp/ashlar-finite-pack-native-20261010-a-closure.json')
CLOSURE_SHA='c5274c3857ee5f083863c87490c99b10f3391143cf9a552e4c9320370692be81'
ROOT=Path('/private/tmp/ashlar-finite-pack-native-20261010-a-source')
if len(sys.argv)!=3 or sys.argv[2]not in ('archaeology','ecology'):raise ValueError('Closed pack runner required')
NAME=sys.argv[2]
OUTPUT=Path('/private/tmp/ashlar-finite-pack-native-20261010-a-'+NAME)
RECEIPT=Path('/private/tmp/ashlar-finite-pack-native-20261010-a-'+NAME+'.receipt.json')
CAPTURE=Path('/private/tmp/ashlar-count-star-installed-cli-capture-a.py')
PYTHON='/Users/erik/.local/share/uv/python/cpython-3.11.17-macos-aarch64-none/bin/python3.11'
JAVA='/opt/homebrew/Cellar/openjdk@21/21.0.12.1/libexec/openjdk.jdk/Contents/Home'
ENV={'PATH':JAVA+'/bin:/usr/bin:/bin','JAVA_HOME':JAVA,'LANG':'C','LC_ALL':'C','TZ':'UTC','PYTHONDONTWRITEBYTECODE':'1',
 'PYTHONPATH':str(ROOT/'src')+':/private/tmp/ashlar-indexed-delta4-python-20261010-a:/private/tmp/ashlar-spark4-python39-bridge:/private/tmp/truss-python-report-schema-env/lib/python3.11/site-packages',
 'PYSPARK_PYTHON':PYTHON,'PYSPARK_DRIVER_PYTHON':PYTHON,'SPARK_LOCAL_IP':'127.0.0.1','SPARK_LOCAL_HOSTNAME':'localhost',
 'GIT_CONFIG_NOSYSTEM':'1','GIT_CONFIG_GLOBAL':'/dev/null','GIT_CONFIG_SYSTEM':'/dev/null','GIT_TERMINAL_PROMPT':'0'}
COMMAND=[PYTHON,'-B','-S','/private/tmp/ashlar-finite-pack-native-probe-20261010-a.py',NAME]
def digest(p):
 h=hashlib.sha256()
 with p.open('rb')as f:
  for block in iter(lambda:f.read(262144),b''):h.update(block)
 return h.hexdigest()
def verify():
 if digest(CLOSURE)!=CLOSURE_SHA:raise ValueError('closure pin')
 plan=json.loads(CLOSURE.read_bytes())
 for absent in plan['absent_startup_paths']:
  if Path(absent).exists():raise ValueError('Selected Python startup path changed')
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
 if len(sys.argv)!=3 or digest(Path(__file__))!=sys.argv[1]:raise ValueError('reviewed runner pin')
 verify()
 if OUTPUT.exists()or RECEIPT.exists():raise ValueError('fresh native outputs required')
 spec=importlib.util.spec_from_file_location('capture',CAPTURE);capture=importlib.util.module_from_spec(spec);spec.loader.exec_module(capture)
 primary=None;result=None;closing=True
 try:result=capture.capture(COMMAND,ROOT,ENV,Path('/private/tmp/ashlar-finite-pack-native-20261010-a-'+NAME+'.stdout'),Path('/private/tmp/ashlar-finite-pack-native-20261010-a-'+NAME+'.stderr'),300,16*1024*1024)
 except BaseException as error:primary=error
 finally:
  try:verify()
  except BaseException as error:closing=False;primary=capture.select_failure(primary,error)
 primary=write_receipt(RECEIPT,{'result':result,'openingClosingCustody':closing,'failureType':None if primary is None else type(primary).__name__,'sourceRevision':'b50f791d99691650e4f1921347c5f02eed3b935c','qualification':'One exact committed-source finite native run; no installed wheel, PostgreSQL, protected ACK, Unity Catalog or paid cloud claim.'},primary,capture.select_failure)
 if primary is not None:raise primary
 if result['exitCode']:raise SystemExit(result['exitCode'])
if __name__=='__main__':main()
