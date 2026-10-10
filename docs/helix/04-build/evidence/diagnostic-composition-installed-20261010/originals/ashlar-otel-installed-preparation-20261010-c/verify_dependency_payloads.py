from importlib import metadata
from pathlib import Path
import hashlib,json,zipfile
ROOT=Path('/private/tmp/ashlar-otel-installed-preparation-20261010-c')
WHEELS=Path('/private/tmp/ashlar-otel-wheel-inputs-20261010-c')
def descriptor(path):
 raw=path.read_bytes();return {'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
download=json.loads((WHEELS/'download.json').read_bytes());copies=[];records=[]
for item in download['files']:
 dist=metadata.distribution(item['name']);site=Path(dist.locate_file('')).resolve()
 assert dist.version==item['version']
 wheel=WHEELS/item['filename'];assert descriptor(wheel)['sha256']==item['sha256']
 with zipfile.ZipFile(wheel)as z:
  for name in z.namelist():
   if name.endswith('/')or name.endswith('.dist-info/RECORD'):continue
   assert '.data/'not in name
   original=z.read(name);installed=Path(dist.locate_file(name)).resolve()
   assert installed.is_relative_to(site)and installed.read_bytes()==original
   copies.append({'path':str(installed),'wheel':item['filename'],'member':name,'bytes':len(original),'sha256':hashlib.sha256(original).hexdigest()})
 for path in dist.files:
  if str(path).endswith('.dist-info/RECORD'):records.append(descriptor(Path(dist.locate_file(path)).resolve()))
core=metadata.distribution('ashlar-graph-toolkit')
for path in core.files:
 if str(path).endswith('.dist-info/RECORD'):records.append(descriptor(Path(core.locate_file(path)).resolve()))
assert len(records)==17
with (ROOT/'dependency-payload-custody.json').open('x')as out:json.dump({'installedSDKWheelMemberCopies':copies,'installedRECORDs':records,'scope':'Exact16 pinned wheel member bytes compared to installed files; generated RECORD files retained independently; no SDK runtime initialization.'},out,sort_keys=True,indent=2);out.write('\n')
print(json.dumps({'wheelMemberCopies':len(copies),'RECORDs':len(records)}))
