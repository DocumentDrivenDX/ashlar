"""Offline installed byte/RECORD checks, SDK-free help and portable core tests."""
import base64,csv,hashlib,importlib.metadata,io,json,os,subprocess,sys,zipfile
from pathlib import Path
ROOT=Path(__file__).resolve().parent
SOURCE=ROOT/'source'

def sha(raw):return hashlib.sha256(raw).hexdigest()
def main():
    inventory=json.loads((ROOT/'source-inventory.json').read_bytes())
    for item in inventory['files']:
        raw=(SOURCE/item['path']).read_bytes()
        if len(raw)!=item['bytes']or sha(raw)!=item['sha256']:raise ValueError('closing-source')
    wheel=ROOT/'wheels/ashlar_graph_toolkit-0.1.0.dev0-py3-none-any.whl'
    if wheel.stat().st_size>4*1024*1024:raise ValueError('wheel-bound')
    distribution=importlib.metadata.distribution('ashlar-graph-toolkit');site=Path(distribution.locate_file('')).resolve()
    if not site.is_relative_to(ROOT/'environment'):raise ValueError('installed-location')
    expected={}
    for item in inventory['files']:
        name=item['path']
        if name.startswith('src/')and(name.endswith('.py')or name.startswith('src/ashlar_host/resources/')or name=='src/ashlar_host/source-correspondence.json'):
            expected[name[4:]]=(SOURCE/name).read_bytes()
    with zipfile.ZipFile(wheel)as archive:
        actual={n for n in archive.namelist()if n.startswith(('ashlar/','ashlar_host/'))and not n.endswith('/')}
        if actual!=set(expected):raise ValueError('wheel-package-inventory')
        for name,raw in expected.items():
            if archive.read(name)!=raw or (site/name).read_bytes()!=raw:raise ValueError('source-wheel-installed-parity')
    record=next(p for p in distribution.files if str(p).endswith('.dist-info/RECORD'))
    rows=list(csv.reader(io.StringIO(Path(distribution.locate_file(record)).read_text())))
    seen=set()
    for name,digest,size in rows:
        if name in seen:raise ValueError('duplicate-record')
        seen.add(name);path=Path(distribution.locate_file(name)).resolve()
        if not path.is_relative_to(ROOT/'environment')or path.is_symlink()or not path.is_file():raise ValueError('record-containment')
        if digest:
            raw=path.read_bytes();expected_digest='sha256='+base64.urlsafe_b64encode(hashlib.sha256(raw).digest()).rstrip(b'=').decode()
            if expected_digest!=digest or len(raw)!=int(size):raise ValueError('record-hash')
        elif not name.endswith('/RECORD'):raise ValueError('unhashed-record')
    # Import help in this SDK-free fresh environment; no checkout/tools sys.path.
    from ashlar.cli import main as cli_main
    for args in (['--help'],['publish-commerce','--help'],['query-commerce','--help'],['install-weft-paths','--help'],['compile-weft-paths','--help']):
        saved=sys.argv;sys.argv=['ashlar']+args
        try:
            try:cli_main()
            except SystemExit as exc:
                if exc.code not in (0,None):raise
        finally:sys.argv=saved
    if any(n in sys.modules for n in ('pyspark','delta','graphframes','psycopg')):raise ValueError('native-help-import')
    result={'sourceCommit':inventory['commit'],'wheelSha256':sha(wheel.read_bytes()),'installedPackageFiles':len(expected),'recordRows':len(rows),'sourceWheelInstalledParity':True,'recordHashes':True,'helpWithoutSDK':True,'compilerExecuted':False,'nativeExecuted':False,'scope':'Offline wheel packaging and portable core only; no installed compiler/native query qualification.'}
    payload=(json.dumps(result,sort_keys=True,indent=2)+'\n').encode()
    with (ROOT/'installed-checks.json').open('xb')as stream:stream.write(payload)
    print(json.dumps(result))
if __name__=='__main__':main()
