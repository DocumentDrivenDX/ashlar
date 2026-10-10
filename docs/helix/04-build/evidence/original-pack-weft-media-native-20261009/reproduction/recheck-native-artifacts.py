import json,subprocess,hashlib
from pathlib import Path
out=Path('/Users/erik/Projects/ashlar/docs/helix/04-build/evidence/original-pack-weft-media-native-20261009')
binary=Path('/private/tmp/ashlar-weft-having-order-collation-freeze-20261009-a/weft-runtime')
rows=[]
for request in sorted((out/'native').glob('*-request.json')):
 artifact=request.with_name(request.name.replace('-request','-artifact'));actual=artifact.read_bytes();response=subprocess.run([str(binary)],input=request.read_bytes(),stdout=subprocess.PIPE,check=True).stdout
 if json.loads(response)!=json.loads(actual):raise RuntimeError(request.name+' differs')
 rows.append({'case':request.name.removesuffix('-request.json'),'requestSha256':hashlib.sha256(request.read_bytes()).hexdigest(),'artifactSha256':hashlib.sha256(actual).hexdigest(),'recompileJsonIdentical':True})
(out/'native/recompile-receipts.json').write_text(json.dumps({'compilerSha256':hashlib.sha256(binary.read_bytes()).hexdigest(),'cases':rows},indent=2,sort_keys=True)+'\n')
print('six exact native request/artifact JSON recompiles pass')
