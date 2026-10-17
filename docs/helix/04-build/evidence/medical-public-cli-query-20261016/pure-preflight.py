from pathlib import Path
import ast,json,hashlib
from ashlar_host.medical_query_configuration import parse_original_medical_query,retain_medical_query_selection
from ashlar_host.original_medical_runtime import OriginalMedicalRuntimeCustody
r=Path(__file__).resolve().parent
config=parse_original_medical_query((r/'query-configuration.json').read_bytes())
assert config.cases==('filtered-limit',)
assert config.bounds.stream_bytes>0 and config.bounds.receipt_bytes>0
assert config.bounds.native_seconds==900 and config.bounds.whole_seconds==960
renew=retain_medical_query_selection(config);renew()
with OriginalMedicalRuntimeCustody.open(config.runtime,config.inputs,config.bounds,config.producer)as custody:custody.renew()
for name in ['run-public-cli.py','native_inventory.py']:ast.parse((r/name).read_bytes())
installed=json.loads((r/'installed.json').read_bytes());assert installed['revision']=='ef92d5e047959be342c5db23b2bc470dbec7d553'
site=r/'environment/lib/python3.12/site-packages'
for pin in installed['files']:
 raw=(site/pin['path']).read_bytes();assert len(raw)==pin['bytes']and hashlib.sha256(raw).hexdigest()==pin['sha256']
record={'profile':'ashlar-public-medical-query-preparation/0.1','revision':installed['revision'],'cases':list(config.cases),'public_argv':[str(r/'environment/bin/python3'),'-I','-B','-m','ashlar','query-original-medical','--configuration',str(r/'query-configuration.json')],'installed_files':len(installed['files']),'compiler_ready_renewed':True,'runtime_custody_opened_closed':True,'qualification':'Pure parse/indexedcompilerready/runtime/installedfile verification only; no Spark/source/ACK/native queries'}
(r/'pure-preflight.json').write_text(json.dumps(record,indent=2)+'\n');print(json.dumps(record))
