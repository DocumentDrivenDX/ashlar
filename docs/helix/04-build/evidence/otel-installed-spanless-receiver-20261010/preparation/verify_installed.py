from pathlib import Path
from importlib import metadata
import base64,csv,hashlib,io,json,sys,zipfile
ROOT=Path('/private/tmp/ashlar-otel-installed-preparation-20261010-b')
def sha(raw):return hashlib.sha256(raw).hexdigest()
source=json.loads((ROOT/'source-inventory.json').read_bytes())
expected={item['path'][4:]:item for item in source['files']if item['path'].startswith('src/')}
excluded={name:item for name,item in expected.items()if name.endswith('README.md')}
expected={name:item for name,item in expected.items()if name not in excluded}
dist=metadata.distribution('ashlar-graph-toolkit');site=Path(dist.locate_file('')).resolve()
wheel=ROOT/'wheels/ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl';installed=[]
with zipfile.ZipFile(wheel)as archive:
 members={name for name in archive.namelist()if name.startswith(('ashlar/','ashlar_host/'))and not name.endswith('/')}
 assert members==set(expected),(sorted(members-set(expected)),sorted(set(expected)-members))
 for name,item in expected.items():
  raw=archive.read(name);path=site/name
  assert len(raw)==item['bytes']and sha(raw)==item['sha256']and path.read_bytes()==raw
  installed.append({'path':str(path),'sourcePath':item['path'],'bytes':len(raw),'sha256':sha(raw)})
record_checks=[]
for distribution in [dist]+[metadata.distribution(item['name'])for item in json.loads((Path('/private/tmp/ashlar-otel-wheel-inputs-20261010-c')/'download.json').read_bytes())['files']]:
 text=distribution.read_text('RECORD');assert text is not None
 for name,digest,size in csv.reader(io.StringIO(text)):
  if not digest:continue
  algorithm,encoded=digest.split('=',1);assert algorithm=='sha256'
  path=Path(distribution.locate_file(name)).resolve();raw=path.read_bytes()
  assert len(raw)==int(size)and base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode()==encoded
  record_checks.append({'path':str(path),'bytes':len(raw),'sha256':sha(raw)})
import ashlar,ashlar_host.otel,ashlar_host._otel_worker
assert metadata.version('ashlar-graph-toolkit')=='0.1.0.dev0'
assert not any(name in sys.modules for name in ('pyspark','delta','psycopg','opentelemetry.sdk'))
modules={name:str(Path(module.__file__).resolve())for name,module in [('ashlar',ashlar),('ashlar_host.otel',ashlar_host.otel),('ashlar_host._otel_worker',ashlar_host._otel_worker)]}
assert all(str(site)in path for path in modules.values())
versions={item['name']:metadata.version(item['name'])for item in json.loads((Path('/private/tmp/ashlar-otel-wheel-inputs-20261010-c')/'download.json').read_bytes())['files']}
receipt={'sourceCommit':source['sourceCommit'],'python':sys.version,'interpreter':sys.executable,'installedVersion':metadata.version('ashlar-graph-toolkit'),'modulePaths':modules,'site':str(site),'installedSourceFiles':installed,'recordFiles':record_checks,'developmentOnlyExcluded':list(excluded.values()),'dependencyVersions':versions,'scope':'Installed metadata and inert facade/worker module paths only; no SDK worker/provider startup, receiver/network/native execution. Exact RECORD content checked; no deterministic whole-system closure claim.'}
with (ROOT/'installed-custody.json').open('x')as out:json.dump(receipt,out,indent=2,sort_keys=True);out.write('\n')
print(json.dumps({'installedSourceFiles':len(installed),'recordFiles':len(record_checks),'SDKDistributions':len(versions),'modulePaths':modules}))
