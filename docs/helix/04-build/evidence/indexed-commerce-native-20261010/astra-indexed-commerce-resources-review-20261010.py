from pathlib import Path
import json,hashlib,subprocess
sha=lambda b:hashlib.sha256(b).hexdigest();C=Path('/private/tmp/ashlar-indexed-commerce-native-command-20261010-c.json');c=json.loads(C.read_bytes());resources={}
for ref in c['references']:
 raw=Path(ref['path']).read_bytes();assert len(raw)==ref['bytes']and sha(raw)==ref['sha256'];value=json.loads(raw)
 for key in ['files','selected_runtime_resources','additional_resources','selected_files','setup_jars','query_jars','new_delta4_resources','typing_resources','loaded_modules']:
  for item in value.get(key,[]):
   path=item['path'];path=path if path.startswith('/')else str(Path(value['root'])/path)
   if path in resources:assert resources[path]['sha256']==item['sha256']
   resources[path]=item
   if 'original'in item:assert sha(Path(item['original']).read_bytes())==item['sha256']and Path(path).stat().st_mode&0o777==int(item['mode'],8)
for path,item in resources.items():
 raw=Path(path).read_bytes();assert sha(raw)==item['sha256'],path
 if 'bytes'in item:assert len(raw)==item['bytes'],path
result={'scope':'Independent post-terminal selected-file hash closure; no broad external build reproducibility claim','command_sha256':sha(C.read_bytes()),'references':len(c['references']),'unique_resources':len(resources),'bytes':sum(Path(p).stat().st_size for p in resources),'all_equal':True}
p=Path('/private/tmp/astra-indexed-commerce-resources-review-20261010.json');p.write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({'receipt':str(p),'sha256':sha(p.read_bytes()),**result}))
