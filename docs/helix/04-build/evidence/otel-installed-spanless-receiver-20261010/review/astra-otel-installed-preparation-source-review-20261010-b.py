from pathlib import Path
import hashlib,json,os,stat,subprocess,sys,zipfile
ROOT=Path('/private/tmp/ashlar-otel-installed-preparation-20261010-b')
REPO=Path('/Users/erik/Projects/ashlar')
OUT=Path('/private/tmp/astra-otel-installed-preparation-source-review-20261010-b.json')
COMMIT='ea4b4ff433edfe096ea40290f19cf1439c5fae33'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def descriptor(path):
 p=Path(path);raw=p.read_bytes();return {'path':str(p),'bytes':len(raw),'sha256':sha(raw)}
def check(item):
 got=descriptor(item['path']);assert got==item,(got,item)
def git(*args):return subprocess.run(['/usr/bin/git',*args],cwd=REPO,check=True,stdout=subprocess.PIPE,stderr=subprocess.PIPE).stdout
freeze=ROOT/'freeze.json';assert sha(freeze.read_bytes())=='42aa7c1f88287b1d4704c8da10eee8f4c100da5287dbe2536d631047735b42aa'
f=json.loads(freeze.read_bytes());assert f['sourceCommit']==COMMIT
wheel=json.loads((ROOT/'preparation.json').read_bytes())['wheel']
assert wheel['bytes']==273263 and wheel['sha256']=='52e166683404a8f88dd76d54d96f388fdea9f50f5a2891dd537374f2c306fbbe'
assert 'inherited environment' in f['launch']['scope'] and f['launch']['exitCode']==0
for item in f['files']+[wheel]:check(item)
inputs=json.loads((ROOT/'inputs.json').read_bytes())
for item in inputs:check(item)
inventory=json.loads((ROOT/'source-inventory.json').read_bytes());rows=inventory['files'];assert inventory['sourceCommit']==COMMIT
listing=git('ls-tree','-rz',COMMIT,'src','README.md','pyproject.toml')
actual={}
for entry in listing.split(b'\0'):
 if not entry:continue
 head,name=entry.split(b'\t');mode,kind,blob=head.decode().split();assert kind=='blob'
 actual[name.decode()]=(mode,blob)
assert set(actual)=={r['path'] for r in rows} and len(rows)==len(actual)==103
for row in rows:
 name=row['path'];mode,blob=actual[name];raw=git('cat-file','blob',blob)
 assert (mode,blob)==(row['mode'],row['gitBlob'])
 assert len(raw)==row['bytes'] and sha(raw)==row['sha256']
 assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==blob
 p=ROOT/'source'/name;assert not p.is_symlink() and stat.S_ISREG(p.stat().st_mode)
 assert p.read_bytes()==raw and stat.S_IMODE(p.stat().st_mode)==int(mode[-3:],8)
approved=json.loads(Path('/private/tmp/astra-otel-spanless-source-review-20261010-d.json').read_bytes())
for item in approved['exactSourceFiles']+approved['exactGoverningDocuments']:
 rel=str(Path(item['path']).relative_to(REPO));raw=git('show',COMMIT+':'+rel)
 assert len(raw)==item['bytes'] and sha(raw)==item['sha256'],rel
command=json.loads((ROOT/'command.json').read_bytes());assert command['sourceCommit']==COMMIT
assert command['environment']=={'PATH':'/opt/homebrew/bin:/usr/local/bin:/usr/bin:/bin','PIP_CONFIG_FILE':'/dev/null','PYTHONDONTWRITEBYTECODE':'1','TMPDIR':str(ROOT/'tmp')}
assert command['limits']=={'phaseSeconds':60,'stdoutBytes':262144,'stderrBytes':262144}
assert len(command['commands'])==4
processes=[]
for c in command['commands']:
 phase=c['phase'];rec=json.loads((ROOT/(phase+'-process.json')).read_bytes())
 assert rec['phase']==phase and rec['exitCode']==0 and 0<=rec['durationSeconds']<60
 assert rec['environmentKeys']==sorted(command['environment'])
 for stream in ('stdout','stderr'):
  raw=(ROOT/(phase+'.'+stream)).read_bytes();assert len(raw)==rec['captureBytes'][stream]<=262144
  if stream=='stderr':assert raw==b''
 processes.append({'phase':phase,'record':descriptor(ROOT/(phase+'-process.json')),'stdout':descriptor(ROOT/(phase+'.stdout')),'stderr':descriptor(ROOT/(phase+'.stderr'))})
assert all('-I' in c['argv'] and '-B' in c['argv'] for c in command['commands'])
assert '--without-pip' in command['commands'][0]['argv']
for c in command['commands'][1:3]:assert all(v in c['argv'] for v in ('--isolated','--no-deps','--no-index','--no-cache-dir'))
assert '--no-build-isolation' in command['commands'][1]['argv'] and '--no-compile' in command['commands'][2]['argv']
prep=json.loads((ROOT/'preparation.json').read_bytes());assert prep['status']=='passed' and prep['closingSelectedInputsUnchanged'] is True and prep['sourceFiles']==103
assert prep['wheel']==wheel
with zipfile.ZipFile(wheel['path']) as z:
 expected={r['path'][4:]:r for r in rows if r['path'].startswith('src/') and not r['path'].endswith('README.md')}
 package={n for n in z.namelist() if n.startswith(('ashlar/','ashlar_host/')) and not n.endswith('/')}
 assert package==set(expected) and len(package)==100
 for n,r in expected.items():assert sha(z.read(n))==r['sha256'] and len(z.read(n))==r['bytes']
 assert not any(n.startswith(('tests/','tools/')) for n in z.namelist())
source_files={str(p.relative_to(ROOT/'source')) for p in (ROOT/'source').rglob('*') if p.is_file()}
generated=sorted(source_files-set(actual))
assert all(n.startswith(('build/','src/ashlar_graph_toolkit.egg-info/')) for n in generated)
# Inert installed imports only: deny effects before importing reviewed modules.
probe="""import sys,json,os
from pathlib import Path
from importlib import metadata
def deny(event,args):
 if event in ('subprocess.Popen','socket.connect','socket.bind','socket.getaddrinfo','os.system'):raise AssertionError('unexpected-effect:'+event)
sys.addaudithook(deny)
import ashlar,ashlar_host.otel,ashlar_host._otel_worker
assert not any(n in sys.modules for n in ('pyspark','delta','psycopg','opentelemetry.sdk'))
site=Path(metadata.distribution('ashlar-graph-toolkit').locate_file('')).resolve()
modules={n:str(Path(sys.modules[n].__file__).resolve()) for n in ('ashlar','ashlar_host.otel','ashlar_host._otel_worker')}
assert all(Path(v).is_relative_to(site) for v in modules.values())
print(json.dumps({'version':metadata.version('ashlar-graph-toolkit'),'python':sys.version,'executable':sys.executable,'site':str(site),'modules':modules,'sysPath':sys.path,'effectAudit':'no subprocess, network, DNS or native SDK startup'}))
"""
argv=[str(ROOT/'environment/bin/python'),'-I','-B','-c',probe]
env={'PATH':'/usr/bin:/bin'}
r=subprocess.run(argv,cwd='/private/tmp',env=env,stdout=subprocess.PIPE,stderr=subprocess.PIPE,timeout=15)
assert r.returncode==0 and r.stderr==b'',r.stderr
observation=json.loads(r.stdout);assert observation['version']=='0.1.0.dev0' and observation['python'].startswith('3.11.17 ')
assert not any('/Users/erik/Projects/' in p or '/source' in p for p in observation['sysPath'])
for item in f['files']+[wheel]+inputs:check(item)
receipt={'verdict':'approved-selected-source-and-preparation-custody','freeze':descriptor(freeze),'sourceCommit':COMMIT,'selectedGitFiles':len(rows),'selectedGitBytes':sum(r['bytes'] for r in rows),'allFourApprovedSourceAndThreeDocumentPinsPresentInCommit':True,'actualParentEnvironment':f['launch']['scope'],'wheelSourceMembers':len(expected),'foreignSourceOverlays':False,'generatedBuildFilesClassifiedSeparately':len(generated),'inputsRehashed':len(inputs),'freezeArtifactsRehashed':len(f['files']),'processes':processes,'inertInstalledObservation':observation,'independentProbe':{'argv':argv,'cwd':'/private/tmp','environment':env,'exitCode':r.returncode,'stdoutBytes':len(r.stdout),'stderrBytes':0},'limitations':['Actual preparation parent inherited shell environment under -I -B; preparation-command.json describes a proposed cleared environment, not an observed one. The four child phases use command.json explicit environment. No parent hermeticity claim.','Original 103 inputs match committed Git exactly; generated build/egg-info products are separately classified.','Selected interpreter/wheel/source inputs only; preparation did not bind complete pip/setuptools/host runtime dependency closure. No hermetic or deterministic build claim.','No worker/provider/receiver/network/native execution. Installed RECORD and dependency payload review is separate.'],'script':descriptor(__file__)}
with OUT.open('x') as stream: stream.write(json.dumps(receipt,indent=2,sort_keys=True)+'\n')
print(json.dumps(descriptor(OUT),sort_keys=True))
