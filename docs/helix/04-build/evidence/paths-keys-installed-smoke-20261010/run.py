"""Bounded actual indexed installation/restart and two original-byte transports.
No Spark/native/publication/ACK qualification is asserted.
"""
import hashlib,json,platform,time,signal
from pathlib import Path
from ashlar.weft_paths_keys_package import read_snapshot
from ashlar import _weft_installation_mechanics as mechanics
from ashlar.weft_paths_keys_distribution import (PathsKeysDistributionPaths,
 install_paths_keys_distribution,open_paths_keys_distribution,
 installed_paths_keys_schema_bundle,compile_paths_keys_distribution)
def terminate(signum,frame):raise KeyboardInterrupt('bounded-smoke-cancelled')
signal.signal(signal.SIGTERM,terminate)
root=Path('/private/tmp/ashlar-paths-keys-installed-smoke-20261010-d')
weft=Path('/private/tmp/ashlar-weft-distribution-d2d')
paths=PathsKeysDistributionPaths(weft/'distributions/index.json',root/'installation',
 weft/'distributions/realizations/weft-3a2a79c-paths-keys-aarch64-apple-darwin-candidate')
corpus_path=Path('/private/tmp/weft-paths-keys-candidate-corpus-20261010-a/candidate-cases.json')
raw=read_snapshot(corpus_path,32*1024*1024);assert hashlib.sha256(raw).hexdigest()=='b9661c7ff024e9696417ef5bce3ca70a28d8cd8b93b109835ab0517ededf63d0'
corpus=json.loads(raw)
started=time.monotonic();installed=install_paths_keys_distribution(paths)
restart=PathsKeysDistributionPaths(paths.index,paths.output)
assert open_paths_keys_distribution(restart).ready_bytes==installed.ready_bytes
schemas=installed_paths_keys_schema_bundle(restart);assert len(schemas)==6
cases=[]
for case in (corpus['paths'][1],corpus['controls'][0]):
 request=bytes.fromhex(case['requestHex']);expected=bytes.fromhex(case['responseHex'])
 response=compile_paths_keys_distribution(restart,request)
 assert response==expected
 cases.append({'id':case['id'],'requestSha256':hashlib.sha256(request).hexdigest(),
 'responseSha256':hashlib.sha256(response).hexdigest(),'status':json.loads(response)['status']})
assert open_paths_keys_distribution(restart).ready_bytes==installed.ready_bytes
result={'format':'ashlar-paths-keys-installed-smoke/0.1','observedPlatform':platform.platform(),
 'elapsedSeconds':time.monotonic()-started,'cases':cases,
 'schemas':[{'name':name,'sha256':hashlib.sha256(data).hexdigest()}for name,data in schemas],
 'readySha256':hashlib.sha256(installed.ready_bytes).hexdigest(),
 'scope':'Actual installation, restart and original compiler transport only; no native query/publication/source/ACK proof.'}
assert read_snapshot(corpus_path,32*1024*1024)==raw
payload=(json.dumps(result,indent=2)+'\n').encode('utf8')
assert len(payload)<64*1024
def refuse():raise ValueError('result-publication-refused')
mechanics.write_owned(root/'provisional-result.json',payload,refuse=refuse)
print(json.dumps({'cases':len(cases),'schemas':len(schemas),'elapsedSeconds':result['elapsedSeconds']}))
