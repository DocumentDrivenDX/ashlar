from pathlib import Path
import ast,base64,csv,hashlib,io,json,os,selectors,signal,stat,subprocess,tarfile,time,zipfile
ROOT=Path('/private/tmp/ashlar-otel-installed-preparation-20261010-c')
REPO=Path('/Users/erik/Projects/ashlar')
COMMIT='02136b9e68afe0f0b175186c6444662347fc463a'
BUILD='/private/tmp/ashlar-host-wheel-env-20261010-a/bin/python'
WHEEL_ROOT=Path('/private/tmp/ashlar-otel-wheel-inputs-20261010-c')
def sha(raw):return hashlib.sha256(raw).hexdigest()
def record(path):
 p=Path(path);raw=p.read_bytes();return {'path':str(p),'bytes':len(raw),'sha256':sha(raw)}
def save(name,value):
 with (ROOT/name).open('x')as out:json.dump(value,out,sort_keys=True,indent=2);out.write('\n')
env={'PATH':'/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin','PYTHONDONTWRITEBYTECODE':'1','TMPDIR':str(ROOT/'tmp'),'PIP_CONFIG_FILE':'/dev/null'}
(ROOT/'tmp').mkdir();(ROOT/'source').mkdir()
# Reuse the prior independently reviewed bounded packaging phase implementation.
launcher=Path('/private/tmp/ashlar-paths-wheel-command-20261010-a/root-runner.py')
original=launcher.read_bytes();tree=ast.parse(original)
function=next(node for node in tree.body if isinstance(node,ast.FunctionDef)and node.name=='phase')
exec(compile(ast.Module(body=[function],type_ignores=[]),str(launcher),'exec'),globals())
listing=subprocess.run(['/usr/bin/git','ls-tree','-rz',COMMIT,'src','README.md','pyproject.toml'],cwd=REPO,stdout=subprocess.PIPE,check=True).stdout
rows=[]
for entry in listing.split(b'\0'):
 if not entry:continue
 head,path=entry.split(b'\t');mode,kind,blob=head.decode().split();name=path.decode()
 assert kind=='blob'and mode in ('100644','100755')and len(rows)<2048
 raw=subprocess.run(['/usr/bin/git','cat-file','blob',blob],cwd=REPO,stdout=subprocess.PIPE,check=True).stdout
 assert len(raw)<=16*1024*1024
 target=ROOT/'source'/name;target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(raw);target.chmod(int(mode[-3:],8))
 assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==blob
 rows.append({'path':name,'mode':mode,'gitBlob':blob,'bytes':len(raw),'sha256':sha(raw)})
assert sum(row['bytes']for row in rows)<=32*1024*1024
save('source-inventory.json',{'sourceCommit':COMMIT,'scope':'Exact selected Git blob export, no working tree, tests, .git or native SDK source','files':rows})
download=json.loads((WHEEL_ROOT/'download.json').read_bytes());assert len(download['files'])==16
for item in download['files']:
 p=WHEEL_ROOT/item['filename'];assert record(p)['sha256']==item['sha256']and p.stat().st_size==item['bytes']
inputs=[record(launcher),record(BUILD),record(Path(BUILD).resolve()),record(WHEEL_ROOT/'download.json')]+[record(WHEEL_ROOT/item['filename'])for item in download['files']]
# Executable and all selected source/wheel inputs remain unchanged through phases.
def verify():
 for item in inputs:
  assert record(item['path'])==item
 for item in rows:
  p=ROOT/'source'/item['path'];assert p.read_bytes()and sha(p.read_bytes())==item['sha256']
commands=[{'phase':'fresh-environment','argv':[BUILD,'-I','-B','-m','venv','--without-pip',str(ROOT/'environment')],'cwd':str(ROOT)},
 {'phase':'wheel-build','argv':[BUILD,'-I','-B','-m','pip','--isolated','wheel','--no-deps','--no-index','--no-cache-dir','--no-build-isolation','--wheel-dir',str(ROOT/'wheels'),'.'],'cwd':str(ROOT/'source')},
 {'phase':'wheel-install','argv':[BUILD,'-I','-B','-m','pip','--isolated','--python',str(ROOT/'environment'),'install','--no-index','--no-deps','--no-compile','--no-cache-dir',str(ROOT/'wheels/ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl')]+[str(WHEEL_ROOT/item['filename'])for item in download['files']],'cwd':str(ROOT)},
 {'phase':'installed-custody','argv':[str(ROOT/'environment/bin/python'),'-I','-B',str(ROOT/'verify_installed.py')],'cwd':str(ROOT)}]
config={'sourceCommit':COMMIT,'commands':commands,'environment':env,'limits':{'phaseSeconds':60,'stdoutBytes':262144,'stderrBytes':262144},'phaseImplementation':record(launcher),'scope':'Offline package preparation only; no worker startup, receiver, network or native operation; dependency/runtime closure is selected custody, not hermetic reproducibility.'}
save('command.json',config);save('inputs.json',inputs)
primary=None
try:
 verify()
 for command in commands:phase(command,config)
except BaseException as exc:primary=exc
finally:
 try:verify()
 except BaseException as exc:
  if primary is None:primary=exc
  else:
   try:primary.cleanup_failed=True
   except BaseException:pass
if primary is not None:raise primary
save('preparation.json',{'status':'passed','sourceCommit':COMMIT,'wheel':record(ROOT/'wheels/ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl'),'sourceFiles':len(rows),'closingSelectedInputsUnchanged':True,'scope':config['scope']})
