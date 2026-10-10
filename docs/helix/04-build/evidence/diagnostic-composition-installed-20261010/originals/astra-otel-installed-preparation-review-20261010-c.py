from pathlib import Path
import base64,csv,email,hashlib,io,json,stat,subprocess,zipfile
R=Path('/private/tmp/ashlar-otel-installed-preparation-20261010-c');REPO=Path('/Users/erik/Projects/ashlar');COMMIT='02136b9e68afe0f0b175186c6444662347fc463a'
def d(p):
 p=Path(p);b=p.read_bytes();return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest()}
def verify(row):
 a=d(row['path']);assert a['bytes']==row['bytes'] and a['sha256']==row['sha256'];return a
freeze=json.loads((R/'freeze-b.json').read_text());assert d(R/'freeze-b.json')['sha256']=='1a2ccfebe9992d87f4c9e28d5c249da1d96a20137443242126b7a5682f0b3ae2'
for row in freeze['artifacts']:verify(row)
inv=json.loads((R/'source-inventory.json').read_text());assert inv['sourceCommit']==COMMIT
listing=subprocess.check_output(['/usr/bin/git','ls-tree','-rz',COMMIT,'src','README.md','pyproject.toml'],cwd=REPO)
gitrows={}
for value in listing.split(b'\0'):
 if not value:continue
 h,name=value.split(b'\t');mode,kind,blob=h.decode().split();assert kind=='blob';gitrows[name.decode()]=(mode,blob)
assert set(gitrows)=={v['path'] for v in inv['files']} and len(gitrows)==105
for item in inv['files']:
 mode,blob=gitrows[item['path']];p=R/'source'/item['path'];raw=p.read_bytes();assert mode==item['mode'] and blob==item['gitBlob']
 assert hashlib.sha1(b'blob '+str(len(raw)).encode()+b'\0'+raw).hexdigest()==blob
 assert hashlib.sha256(raw).hexdigest()==item['sha256'] and len(raw)==item['bytes']
 assert stat.S_IMODE(p.stat().st_mode)==int(mode[-3:],8)
for row in json.loads((R/'inputs.json').read_text()):verify(row)
command=json.loads((R/'command.json').read_text());assert command['sourceCommit']==COMMIT and len(command['commands'])==4
processes=[]
for item in command['commands']:
 n=item['phase'];rec=json.loads((R/(n+'-process.json')).read_text());assert rec['exitCode']==0
 for stream in ('stdout','stderr'):
  assert (R/(n+'.'+stream)).stat().st_size==rec['captureBytes'][stream] <= command['limits'][stream+'Bytes']
 processes.append({'phase':n,'exitCode':0,'durationSeconds':rec['durationSeconds'],'captureBytes':rec['captureBytes']})
installed=json.loads((R/'installed-custody.json').read_text());site=Path(installed['site']);assert installed['sourceCommit']==COMMIT and installed['installedVersion']=='0.1.0.dev0'
wheel=R/'wheels/ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl';verify(freeze['wheel'])
expected={v['path'][4:]:v for v in inv['files'] if v['path'].startswith('src/') and not v['path'].endswith('README.md')}
assert len(expected)==102 and len(installed['installedSourceFiles'])==102
with zipfile.ZipFile(wheel) as z:
 names=z.namelist();assert len(names)==len(set(names))
 assert {n for n in names if n.startswith(('ashlar/','ashlar_host/')) and not n.endswith('/')}==set(expected)
 for name,v in expected.items():
  raw=z.read(name);assert raw==(site/name).read_bytes()==(R/'source'/v['path']).read_bytes()
 metadata_name=next(n for n in names if n.endswith('.dist-info/METADATA'));metadata=email.message_from_bytes(z.read(metadata_name))
 assert metadata['Name']=='ashlar-graph-toolkit' and metadata['Version']=='0.1.0.dev0'
 requires=metadata.get_all('Requires-Dist',[]);assert len(requires)==16 and all('extra == "diagnostics"' in v for v in requires)
 assert all("python_version == \"3.11\"" in v for v in requires)
 ep=z.read(next(n for n in names if n.endswith('.dist-info/entry_points.txt')));assert b'ashlar = ashlar.cli:main' in ep
for row in installed['installedSourceFiles']+installed['recordFiles']:verify(row)
custody=json.loads((R/'dependency-payload-custody.json').read_text());assert len(custody['installedSDKWheelMemberCopies'])==698 and len(custody['installedRECORDs'])==17
for row in custody['installedRECORDs']:
 verify(row);record=Path(row['path']);local_site=record.parent.parent
 for name,digest,size in csv.reader(io.StringIO(record.read_text())):
  p=(local_site/name).resolve();assert p.is_relative_to(R/'environment')
  if not digest:assert name.endswith('.dist-info/RECORD');continue
  algorithm,encoded=digest.split('=',1);raw=p.read_bytes();assert algorithm=='sha256' and len(raw)==int(size)
  assert base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode()==encoded
wheels=Path('/private/tmp/ashlar-otel-wheel-inputs-20261010-c');download=json.loads((wheels/'download.json').read_text());assert len(download['files'])==16
bywheel={}
for row in custody['installedSDKWheelMemberCopies']:bywheel.setdefault(row['wheel'],[]).append(row)
for item in download['files']:
 p=wheels/item['filename'];a=d(p);assert a['sha256']==item['sha256'] and a['bytes']==item['bytes']
 with zipfile.ZipFile(p) as z:
  members={n for n in z.namelist() if not n.endswith('/') and not n.endswith('.dist-info/RECORD')}
  assert members=={v['member'] for v in bywheel[item['filename']]}
  for row in bywheel[item['filename']]:assert Path(row['path']).read_bytes()==z.read(row['member']);verify(row)
  m=email.message_from_bytes(z.read(next(n for n in z.namelist() if n.endswith('.dist-info/METADATA'))))
  assert m['Version']==item['version']==installed['dependencyVersions'][item['name']]
assert len(installed['recordFiles'])==857 and len({v['path'] for v in installed['recordFiles']})==857
assert all(str(site) in p for p in installed['modulePaths'].values())
for row in freeze['artifacts']:verify(row)
report={'reviewer':'/root/astra_plan_review','verdict':'approve-scoped-actual-offline-installed-preparation','sourceCommit':COMMIT,'freeze':d(R/'freeze-b.json'),'wheel':d(wheel),'checks':{'selectedGitFiles':105,'exactGitModesBlobShaAndSha256':True,'installedSourceMembers':102,'uniqueHashedRECORDPayloads':857,'installedRECORDs':17,'SDKWheels':16,'SDKOriginalMemberCopies':698,'selectedPackagingInputs':20,'wheelEmbeddedOptionalDependencies':16,'coreDependencyFree':True,'installedEntryPoint':'ashlar.cli:main'},'processes':processes,'scope':'Pure independent Git/file/ZIP/CSV/metadata reads. Actual preparation records show offline package build/install; no reviewer installed import, worker/provider/receiver/native/network execution.','command':d(R/'command.json'),'sourceInventory':d(R/'source-inventory.json'),'installedCustody':d(R/'installed-custody.json'),'dependencyPayloadCustody':d(R/'dependency-payload-custody.json'),'executionQualification':'Preparation parent used inherited shell environment with -I/-B as explicitly recorded; four child packaging commands used the declared explicit environment. Selected custody only, no hermetic/toolchain-complete claim.','findings':[],'historicalCorrection':'Original freeze artifacts=[] preserved; successor freeze-b binds the actual selected artifacts. No implied pre-execution complete-freeze claim.','limitations':['Metadata version/module path observations retained from inert installed verifier; reader review did not initialize SDK.','No actual current-main receiver/composition qualification until separately reviewed direct command runs.','Source export is selected src/README/pyproject, not full repository; build-generated files are separate from original selected inputs.']}
out=Path('/private/tmp/astra-otel-installed-preparation-review-20261010-c.json');out.write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(d(out)))
