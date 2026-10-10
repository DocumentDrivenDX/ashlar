"""Root-run core-only qualification; no engine or native support inference."""
import hashlib
import json
import platform
import sys
from pathlib import Path
from ashlar.weft_paths_installation import PathsInstallationConfig,install,open_installation,compile_request,installed_schema_bundle
from ashlar.weft_paths_package import read_snapshot,JSON_LIMIT,FILE_LIMIT

ROOT=Path(__file__).resolve().parent
INDEX=Path('/private/tmp/ashlar-weft-distribution-d2d/distributions/index.json')
PACKAGE=Path('/private/tmp/ashlar-weft-distribution-d2d/distributions/realizations/weft-530ae35-paths-aarch64-apple-darwin-candidate')
REVISION='c8d825808a27e15517c1fc93c3fba0faffc2406e'
INDEX_SHA='a7571d2bd141bf182ce171acc16f64f1276cca29a855dbf4b49a92283fc71864'
REALIZATION='weft-530ae35-paths-aarch64-apple-darwin-candidate'
OUTPUT=ROOT/'installation'

def sha(raw):return hashlib.sha256(raw).hexdigest()
def write(path,raw):
    with path.open('xb')as stream:stream.write(raw)

def main():
    phase=sys.argv[1]
    if phase not in ('install','reopen'):raise ValueError('phase')
    if sys.platform!='darwin' or platform.machine()!='arm64' or platform.mac_ver()[0]!='27.0.1':raise ValueError('observed-platform')
    resources=json.loads(read_snapshot(ROOT/'resources.json',JSON_LIMIT))
    def custody():
        result=[]
        for item in resources:
            raw=read_snapshot(Path(item['path']),FILE_LIMIT)
            if len(raw)!=item['bytes'] or sha(raw)!=item['sha256']:raise ValueError('resource-drift')
            result.append(item)
        return result
    opening=custody()
    report_path=ROOT/(phase+'-report.json')
    if report_path.exists()or report_path.is_symlink():raise ValueError('fresh-report')
    config=PathsInstallationConfig(INDEX,REVISION,INDEX_SHA,PACKAGE if phase=='install' else None,REALIZATION,OUTPUT,'aarch64-apple-darwin',platform.mac_ver()[0])
    installed=install(config)if phase=='install' else open_installation(config)
    results=[]
    if phase=='reopen':
        for case in json.loads(read_snapshot(ROOT/'cases.json',JSON_LIMIT)):
            request=read_snapshot(ROOT/'inputs'/case['request']);expected=read_snapshot(ROOT/'inputs'/case['expected'])
            actual=compile_request(installed,request)
            if actual!=expected:raise ValueError('exact-response')
            write(ROOT/(case['id']+'.actual-response'),actual)
            results.append({'id':case['id'],'requestSha256':sha(request),'responseSha256':sha(actual),'responseBytes':len(actual),'exactExpected':True})
    schemas=[]
    for name,raw in installed_schema_bundle(installed):
        expected=read_snapshot(PACKAGE/'source-subset/docs/helix/02-design/contracts'/name,JSON_LIMIT)
        if raw!=expected:raise ValueError('installed-schema')
        schemas.append({'name':name,'sha256':sha(raw),'bytes':len(raw)})
    # Restart and schema access use package=None, not a fabricated package path.
    restart=PathsInstallationConfig(INDEX,REVISION,INDEX_SHA,None,REALIZATION,OUTPUT,'aarch64-apple-darwin',platform.mac_ver()[0])
    if open_installation(restart).ready_bytes!=installed.ready_bytes:raise ValueError('restart')
    installed_files=[]
    for name in ['weft-paths','provenance.json','ready.json','backend-manifest.json']+['schemas/'+item['name']for item in schemas]:
        raw=read_snapshot(OUTPUT/name)
        installed_files.append({'path':name,'sha256':sha(raw),'bytes':len(raw),'mode':oct((OUTPUT/name).stat().st_mode&0o777)})
    closing=custody()
    if closing!=opening:raise ValueError('closing-drift')
    result={'phase':phase,'indexRevision':REVISION,'indexSha256':INDEX_SHA,'realizationId':REALIZATION,'observedTarget':'aarch64-apple-darwin','observedOS':platform.mac_ver()[0],'packageRequiredForRestart':False,'readySha256':sha(installed.ready_bytes),'cleanupPending':installed.cleanup_pending,'cases':results,'schemaBundle':schemas,'openingClosingCustody':True,'resources':closing,'installedFiles':installed_files,'scope':'Actual indexed core install/string compiler only; candidate metadata and host obligations remain. No native engine/source/ACK or production cloud qualification.'}
    payload=(json.dumps(result,sort_keys=True,indent=2)+'\n').encode()
    if len(payload)>JSON_LIMIT:raise ValueError('report-bound')
    write(report_path,payload)
    print(json.dumps({'phase':phase,'status':'completed','cases':len(results)}))

if __name__=='__main__':main()
