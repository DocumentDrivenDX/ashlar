"""Read pruning screen on the still-live matched-row-tracking scratch tables."""
import json
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import lit
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_reads_r138'
assert not (O/'statements.jsonl').exists(),'Inspect saved IDs before repeating'
parent=json.loads((B/'out/native/ashlar_append_layout_r137/audited-summary.json').read_text())
F='client_dev.ashlar_entropy_20261006_r86';c=DriverClient(O)
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
sources={'raw':(F+'.source_record_r89',16),'journal':(F+'.property_journal_r89',18)}
for role,(source,version) in sources.items():
 rows=c.sql(role+'-oracle',f"SELECT * FROM {source} VERSION AS OF {version} WHERE apply_batch_id='r133-b1' ORDER BY sha2(cast({'delivery_id' if role=='raw' else 'id'} AS STRING),256) LIMIT 5")
 names=[col['name'] for col in c.records[-1]['response']['manifest']['schema']['columns']]
 assert len(rows)==5
 for i,expected in enumerate(rows):
  fields=dict(zip(names,expected))
  predicates=[f'source_feed={lit(fields["source_feed"])}',f'source_epoch={lit(fields["source_epoch"])}']
  if role=='raw':predicates.append('delivery_id='+lit(fields['delivery_id']))
  else:
   predicates += ['id='+str(int(fields['id'])),'property_id=105','event_ordinal=0','source_position IS NULL']
  for mode in (('clustered','unclustered') if i%2==0 else ('unclustered','clustered')):
   run=next(r for r in parent['runs'] if r['mode']==mode)
   assert c.sql(f'{role}-{mode}-{i}',f"SELECT * FROM {run['targets'][role]} WHERE "+' AND '.join(predicates))==[expected]
c.history();c.close()
(O/'summary.json').write_text(json.dumps({'state':'20 exact full-row raw/history reads passed across both tracked layouts; metrics pending','scope':'Five hash-ranked immutable keys per role, alternating layouts, one pass each.100k-row scratch tables and already-stored clustered source replay may retain source ordering. No realistic growing-history, repeat p95, general history index or canonical singleton admission.'},indent=2)+'\n')
print('Twenty full-row raw/history reads passed')
