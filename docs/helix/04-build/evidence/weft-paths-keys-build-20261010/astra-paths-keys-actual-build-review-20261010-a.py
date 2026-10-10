import hashlib,json,os,stat,struct
from pathlib import Path
ROOT=Path('/private/tmp/weft-paths-keys-3a2a79c-build-20261010-a')
OUT=Path('/private/tmp/astra-paths-keys-actual-build-checks-20261010-a.json')
def desc(path):
 p=Path(path); h=hashlib.sha256(); n=0
 with p.open('rb') as f:
  for b in iter(lambda:f.read(65536),b''): h.update(b); n+=len(b)
 return {'path':str(p),'sha256':h.hexdigest(),'bytes':n}
def pinned(path,sha):
 d=desc(path);assert d['sha256']==sha,(str(path),'pin');return d
cmd=pinned(ROOT/'command-b.json','1ad88380c1c0ece419c7054c6a8d474aaaa425fdef1027798848df1f9e17ac90')
plan=json.loads((ROOT/'command-b.json').read_bytes())
assert plan['sourceCommit']=='3a2a79ccc19d636f20d31116662d61365c1b4ff7'
inv=json.loads(Path(plan['sourceInventory']['path']).read_bytes())
assert inv['sourceCommit']==plan['sourceCommit']
root=Path(plan['sourceExport']); expected={d['path'] for d in inv['files']}
assert len(expected)==len(inv['files'])==1987
actual=set(); total=0
for parent,dirs,files in os.walk(root,followlinks=False):
 for d in dirs:assert not (Path(parent)/d).is_symlink()
 for f in files: actual.add((Path(parent)/f).relative_to(root).as_posix())
assert actual==expected
for d in inv['files']:
 p=root/d['path']; s=p.lstat(); assert stat.S_ISREG(s.st_mode)
 assert bool(s.st_mode&0o111)==(d['mode']=='100755')
 h=hashlib.sha256(); g=hashlib.sha1(('blob '+str(d['bytes'])+'\0').encode()); n=0
 with p.open('rb') as f:
  for b in iter(lambda:f.read(65536),b''):h.update(b);g.update(b);n+=len(b)
 assert (h.hexdigest(),g.hexdigest(),n)==(d['sha256'],d['gitBlob'],d['bytes']),d['path']
 total+=n
assert total==plan['sourceBytes']==312239178
resources=[]
for d in [*plan['resources'],plan['launcher']]:
 observed=desc(d['path']); assert (observed['sha256'],observed['bytes'])==(d['sha256'],d['bytes']);resources.append(observed)
phases=[]
for phase,outcome_name,stdout_name,stderr_name in [('cli-build','cli-build-outcome.json','cli-build.log','cli-build.log.stderr'),('source-metadata','metadata-build-outcome.json','backend-manifest.json','metadata-build.log')]:
 o=json.loads((ROOT/outcome_name).read_bytes());assert o['phase']==phase and o['exitCode']==0
 assert o['commandSha256']==cmd['sha256'] and o['openingClosingCustody'] is True and o['environmentInherited'] is False
 std=desc(ROOT/stdout_name);err=desc(ROOT/stderr_name)
 assert o['stdoutBytes']==std['bytes'] and o['stderrBytes']==err['bytes']
 assert max(std['bytes'],err['bytes'])<plan['launcherLimits']['maximumStreamBytes']
 assert b'Finished `release` profile' in (ROOT/stderr_name).read_bytes()
 phases.append({'phase':phase,'outcome':desc(ROOT/outcome_name),'stdout':std,'stderr':err,'recordedExitCode':0,'recordedOpeningClosingCustody':True})
binary=pinned(ROOT/'weft-paths-keys','471fc5dedc8f8eba3156444d19cc2161fac7a06833672dd5621ed49ead8b522c')
assert binary['bytes']==8330656
s=(ROOT/'weft-paths-keys').lstat();assert stat.S_ISREG(s.st_mode) and stat.S_IMODE(s.st_mode)==0o555
with (ROOT/'weft-paths-keys').open('rb') as f:header=f.read(32)
magic,cpu,sub,filetype,ncmds,szcmds,flags,reserved=struct.unpack('<8I',header)
assert magic==0xfeedfacf and cpu==0x0100000c and filetype==2
built=desc(plan['commands'][0]['builtBinary']);assert (built['sha256'],built['bytes'])==(binary['sha256'],binary['bytes'])
assert 'HOME' not in plan['environment'] and plan['environment']['CARGO_BUILD_JOBS']=='1'
result={'format':'independent-built-custody-check/0.1','command':cmd,'sourceCommit':plan['sourceCommit'],'sourceFiles':len(expected),'sourceBytes':total,'sourceFullVectorNowMatchesReviewedGitInventory':True,'sourceInventory':desc(ROOT/'source-inventory.json'),'resourcesNowMatch':resources,'phases':phases,'binary':binary,'binaryFormat':'little-endian Mach-O64 arm64 MH_EXECUTE','currentTargetBinaryEqualsRetained':True,'actualExecutionObservedBy':'Root-owned terminal sessions39793 and15392; reviewed persisted outcomes/logs here, no rebuild by reviewer','limitations':['Cached offline build with allowlisted environment; complete Cargo cache/SDK/dynamic-library closure is not inventoried.','These checks verify retained build artifacts and metadata custody; no compiler corpus, installation, package/index, native query or deterministic-build qualification.'],'passed':True}
OUT.write_text(json.dumps(result,sort_keys=True,indent=2)+'\n')
print(json.dumps({'passed':True,'sources':len(expected),'sourceBytes':total,'selectedResourcesIncludingLauncher':len(resources),'binarySha256':binary['sha256'],'receipt':desc(OUT)},sort_keys=True))
