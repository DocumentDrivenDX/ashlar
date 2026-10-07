import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_apply_r231';s=json.loads((O/'summary.json').read_text());rs=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];failed=rs[-1];assert failed['label']=='append-property_journal';c=Client(O/'audit');native=c.w.api_client.do('GET','/api/2.0/sql/statements/'+failed['statement_id']);assert native['status']['state']=='FAILED';audit={'failed_query_id':failed['statement_id'],'native_state':'FAILED','heads':{}}
for role,table in s['tables'].items():
 if role=='object_current':continue
 h=c.sql('history-'+role,'DESCRIBE HISTORY '+table['table']+' LIMIT 1');audit['heads'][role]=int(h[0][0])
assert audit['heads']=={'edge_current':0,'source_record':1,'property_journal':0,'adjacency_forward':0,'tombstone':0}
assert c.sql('empty-manifest','SELECT count(*) FROM client_dev.ashlar_entropy_20261006_r86.mixed_manifest_r231')==[['0']];audit['manifest_rows']=0;audit['qualification']='Raw append committed; failed journal/current0/adjacency0/tombstone0 and empty private manifest. Do not replay on these targets.';(O/'failure-audit.json').write_text(json.dumps(audit,indent=2)+'\n');print(json.dumps(audit))
