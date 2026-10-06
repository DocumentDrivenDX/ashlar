"""Pinned mixed changed/unchanged singleton control after scattered ingest."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent
summary=json.loads((B/'out/native/ashlar_composite_publication_20261005_p2/summary.json').read_text());assert summary['state']=='completed'
v=summary['versions']['object_current'];out=B/'out/native/ashlar_composite_post_reads_20261005_p3';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');expected={};kinds={}
for phase in ['prime','repeat','repeat2']:
 for rep in range(51):
  domain=rep%10;source='pilot:'+str(domain//2);typ=1+domain%2;key=1+(rep*196613)%1000000
  import hashlib
  hash=hashlib.sha256(json.dumps(dict(source_system=source,type_id=typ,id=key),separators=(',',':')).encode()).hexdigest()
  rows=c.sql(f'{phase}-{rep}',f"SELECT source_system,type_id,id,props_json,retained_json,logical_key_json,lookup_hash FROM client_dev.ashlar_composite_20261005_p1r1.object_current VERSION AS OF {v} WHERE lookup_hash='{hash}' AND source_system='{source}' AND type_id={typ} AND id={key}")
  assert len(rows)==1 and rows[0][:3]==[source,str(typ),str(key)] and rows[0][5:]==['['+str(key)+']',hash]
  pos=1 if ((key-1)*104729)%1000000<20000 else None
  key=(source,typ,key)
  if phase=='prime':expected[key]=rows;kinds[key]='changed' if pos is not None else 'unchanged'
  else:assert rows==expected[key]
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps(dict(state='completed',object_version=v,nodes=10000000,changed_keys=sum(x=='changed' for x in kinds.values()),unchanged_keys=sum(x=='unchanged' for x in kinds.values()),scope='fixed snapshot after writes; exact repeated carrier equality and changed fixture ordering; inspect remote/cache metrics for warm classification; no concurrent-write or billion-scale claim'),indent=2)+'\n')
