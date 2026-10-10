"""Pure accepted-package inspection and repinned fixture-authority counterexamples."""
import copy,hashlib,json,shutil,tempfile,time
from pathlib import Path
from unittest.mock import patch
from ashlar import weft_paths_keys_package as p
SOURCE=Path('/private/tmp/weft-paths-keys-package-assembly-20261010-a/package')
INDEX=Path('/private/tmp/weft-paths-keys-index-candidate-20261010-a/index.json')
sha=lambda raw:hashlib.sha256(raw).hexdigest()
results={}
with tempfile.TemporaryDirectory() as temporary:
 root=Path(temporary).resolve(); package=root/'package';shutil.copytree(SOURCE,package); index=root/'index.json'
 original_index=json.loads(INDEX.read_bytes());original_proof=json.loads((package/'assembly-custody.json').read_bytes());original_manifest=json.loads((package/'manifest.json').read_bytes())
 originals={name:(package/name).read_bytes() for name in ['producer/corpus-harness.py','evidence/full-receipt.json.gz','source-subset/docs/helix/02-design/contracts/logical-plan-v0.4.schema.json','source-subset/distributions/realizations/weft-530ae35-paths-aarch64-apple-darwin-candidate/manifest.json']}
 def rewrite(name,raw):
  target=package/name
  if target.exists():target.chmod(0o644)
  target.write_bytes(raw);target.chmod(0o444)
 def repin(name,raw):
  rewrite(name,raw); proof=copy.deepcopy(original_proof);manifest=copy.deepcopy(original_manifest)
  d={'path':name,'sha256':sha(raw),'bytes':len(raw)}
  def replace(value):
   if type(value)is dict:
    if set(value)=={'path','sha256','bytes'} and value['path']==name: value.update(d)
    else:
     for v in value.values():replace(v)
   elif type(value)is list:
    for v in value:replace(v)
  replace(manifest);m=p.encode_document(manifest)+b'\n';rewrite('manifest.json',m)
  replace(proof)
  for item in proof['artifacts']:
   if item['path']=='manifest.json':item.update(path='manifest.json',sha256=sha(m),bytes=len(m))
  custody=p.encode_document(proof)+b'\n';rewrite('assembly-custody.json',custody)
  ix=copy.deepcopy(original_index);entry=ix['entries'][-1];entry['manifest'].update(sha256=sha(m),bytes=len(m));entry['assemblyCustody'].update(sha256=sha(custody),bytes=len(custody));index.write_bytes(p.encode_document(ix))
 def reset():
  for name,raw in originals.items():rewrite(name,raw)
  rewrite('manifest.json',p.encode_document(original_manifest)+b'\n');rewrite('assembly-custody.json',p.encode_document(original_proof)+b'\n');index.write_bytes(INDEX.read_bytes())
 def config():return p.PathsKeysInstallationConfig(index,'a'*40,sha(index.read_bytes()),package,original_manifest['realizationId'],root/'never-installed','aarch64-apple-darwin','27.0.1')
 reset()
 with patch('subprocess.Popen',side_effect=AssertionError('must remain inert')):
  before=time.monotonic();v=p.inspect_package(config());results['actualPositive']={'files':127,'bytes':47778857,'resources':len(v.resources),'executableBytes':len(v.executable_bytes),'seconds':time.monotonic()-before,'fixtureIndexAuthorityOnly':True}
  def refuse(name):
   try:p.inspect_package(config())
   except p.PathsKeysInstallationError:results[name]='refused';return
   raise AssertionError(name+' accepted')
  for name in originals:
   reset();repin(name,originals[name]+b'\n');refuse('repinned:'+name)
  reset();(package/'producer/corpus-harness.py').unlink();refuse('missingH');rewrite('producer/corpus-harness.py',originals['producer/corpus-harness.py'])
  reset();(package/'extra').mkdir();refuse('unindexed-directory');(package/'extra').rmdir()
  reset();(package/'bin/weft-paths-keys').chmod(0o444);refuse('executable-mode');(package/'bin/weft-paths-keys').chmod(0o555)
  reset();original=p.read_snapshot;calls={}
  def drift(path,*args):
   raw=original(path,*args);calls[path]=calls.get(path,0)+1
   if path==package/'producer/corpus-harness.py' and calls[path]>1:return raw+b'drift'
   return raw
  with patch.object(p,'read_snapshot',side_effect=drift):refuse('closing-H-drift')
  reset();c=config();c= p.PathsKeysInstallationConfig(c.index_path,c.index_revision,c.index_sha256,c.package,c.realization_id,c.output,c.observed_target,'27.0.2')
  try:p.inspect_package(c)
  except p.PathsKeysInstallationError:results['actual-observedOS-mismatch']='refused'
  else:raise AssertionError('OS mismatch accepted')
  assert not (root/'never-installed').exists()
receipt={'format':'paths-keys-package-pure-controls/0.1','sourcePackage':str(SOURCE),'index':str(INDEX),'indexSha256':sha(INDEX.read_bytes()),'results':results,'scope':'No installation/process/native execution; repinned copies use explicit fixture index authority only.'}
out=Path('/private/tmp/ashlar-paths-keys-package-actual-controls-20261010-a.json');out.write_text(json.dumps(receipt,indent=2)+'\n');print(out);print(sha(out.read_bytes()));print(json.dumps(results,indent=2))
