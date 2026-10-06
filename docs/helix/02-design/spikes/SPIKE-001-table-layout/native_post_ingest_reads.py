"""Pinned mixed changed/unchanged singleton control after scattered ingest."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent
summary=json.loads((B/'out/native/ashlar_scheduled_broadcast_20261005_n5/summary.json').read_text());assert summary['state']=='completed'
v=summary['batches'][-1]['versions']['object_current'];out=B/'out/native/ashlar_post_ingest_reads_20261005_n6';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');expected={};kinds={}
for phase in ['prime','repeat','repeat2']:
 for rep in range(51):
  key=1+(rep*196613)%10000000;selection=((key-1)*104729)%10000000
  pos=1+selection//200000 if selection<600000 else 4+(selection-600000)//300000 if selection<1800000 else 8+(selection-1800000)//250000 if selection<2800000 else None
  rows=c.sql(f'{phase}-{rep}',f"SELECT id,props_json,retained_json,logical_key_json,entity_version,source_position FROM client_dev.ashlar_scattered_20261005_n1.object_current VERSION AS OF {v} WHERE source_system='pilot' AND type_id=1 AND id={key}")
  assert len(rows)==1 and rows[0][0]==str(key) and rows[0][3]=='['+str(key)+']'
  if pos is not None:assert rows[0][4:]==[str(pos),str(pos)]
  if phase=='prime':expected[key]=rows;kinds[key]='changed' if pos is not None else 'unchanged'
  else:assert rows==expected[key]
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps(dict(state='completed',object_version=v,nodes=10000000,changed_keys=sum(x=='changed' for x in kinds.values()),unchanged_keys=sum(x=='unchanged' for x in kinds.values()),scope='fixed snapshot after writes; exact repeated carrier equality and changed fixture ordering; inspect remote/cache metrics for warm classification; no concurrent-write or billion-scale claim'),indent=2)+'\n')
