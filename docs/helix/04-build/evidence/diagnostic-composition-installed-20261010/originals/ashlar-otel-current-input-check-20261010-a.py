from pathlib import Path
import json,hashlib,sys
base=Path('/private/tmp/ashlar-otel-installed-preparation-20261010-c')
out=Path('/private/tmp/ashlar-otel-installed-receiver-probe-20261010-e')
records={}
def add(item):
 path=Path(item['path']);raw=path.read_bytes()
 actual={'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()}
 assert actual=={k:item[k] for k in ('path','bytes','sha256')}, str(path)
 if str(path) in records:assert records[str(path)]==actual
 records[str(path)]=actual
def own(path):
 raw=path.read_bytes();add({'path':str(path),'bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest()})
freeze=json.loads((base/'freeze-b.json').read_bytes());own(base/'freeze-b.json')
for item in freeze['artifacts']:add(item)
for item in json.loads((base/'inputs.json').read_bytes()):add(item)
installed=json.loads((base/'installed-custody.json').read_bytes())
for item in installed['recordFiles']:add(item)
for item in json.loads((base/'dependency-payload-custody.json').read_bytes())['installedRECORDs']:add(item)
for path in [out/'candidate.py',out/'command.json']:own(path)
add(freeze['wheel'])
value={'scope':'Selected Git-export/preparation/wheel/install/RECORD/interpreter/command custody; not complete OS closure or hermetic environment.','sourceCommit':freeze['sourceCommit'],'files':sorted(records.values(),key=lambda x:x['path'])}
if sys.argv[1]=='opening':
 assert not any((out/n).exists() for n in ['result.json','capture','receiver-observations.json','request-00.pb'])
else:
 assert value==json.loads((out/'opening-inputs.json').read_bytes())
with (out/(sys.argv[1]+'-inputs.json')).open('x') as handle:json.dump(value,handle,indent=2);handle.write('\n')
print(len(records),'selected input pins verified')
