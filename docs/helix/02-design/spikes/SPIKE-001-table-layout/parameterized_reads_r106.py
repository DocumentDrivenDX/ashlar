"""Alternating uncached literal/parameter singleton reads against published E16."""
import json, math
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_parameterized_reads_r106'
assert not (O/'statements.jsonl').exists(), 'Inspect existing statement IDs before continuing'
s=json.loads((B/'out/native/ashlar_maintained_contention_20261007_r103/summary.json').read_text())
batch=s['batches'][0]
E='client_dev.ashlar_entropy_20261006_r86.edge_current'
cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision','entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position','published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
c=DriverClient(O)
c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
rows=c.sql('expected',f"SELECT {','.join(cols)} FROM {batch['source_stage']} VERSION AS OF 0 ORDER BY id LIMIT 30")
assert len(rows)==30
base=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF {batch['versions'][E]} WHERE "
parameter_sql=base+'lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)'
def lit(v):return "decode(unhex('"+v.encode().hex()+"'),'UTF-8')"
for round in range(2):
 for i,row in enumerate(rows):
  modes=('literal','parameter') if (i+round)%2==0 else ('parameter','literal')
  for mode in modes:
   if mode=='literal':q=base+f'lookup_hash={lit(row[16])} AND source_system={lit(row[0])} AND rel_type_id={int(row[1])} AND id={int(row[2])}';params=None
   else:q=parameter_sql;params={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]}
   assert c.sql(f'{mode}-{round}-{i}',q,parameters=params,tag=False)==[row]
(O/'summary.json').write_text(json.dumps({'state':'120 exact full-field reads completed; history audit pending','pinned_version':batch['versions'][E],'keys':30,'rounds':2,'reads_per_mode':60,'qualification':'Alternating modes on same warm large-token keys, one persistent connection; stable SQL text and no comments for both. No concurrent publisher or cold/scale SLA.'},indent=2)+'\n')
c.history();c.close()
print('Completed 120 exact full-field reads')
