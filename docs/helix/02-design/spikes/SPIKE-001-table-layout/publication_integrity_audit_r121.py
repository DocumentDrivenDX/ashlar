"""Final-history audit of post-publication custody, history and schema checks."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;P=B/'out/native/ashlar_isolation_r121';costs={}
for name in ('integrity','integrity-custody','schema'):
 o=P/name;c=Client(o);c.records=[json.loads(l) for l in (o/'statements.jsonl').read_text().splitlines()]
 h={q['query_id']:q for q in c.history()}
 assert all(h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
 if name=='integrity':
  assert c.records[-1]['response']['status']['state']=='FAILED'
  continue
 assert all(r['response']['status']['state']=='SUCCEEDED' for r in c.records)
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records)
 costs[name]={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records}
s=json.loads((P/'integrity-custody/summary.json').read_text());schemas=json.loads((P/'schema/summary.json').read_text())
s.update(state='Post-publication19.9M immutable physical-row custody, exhaustive inherited raw/journal parity and stable3-table schemas audited; all successful histories final uncached',schema_evidence=schemas,costs=costs,failed_attempt='integrity/failure.json',qualification='Changed100k full20-field carriers verified in publication harness. Untouched19.9M logical-key/file-path/physical-row-index custody relies on Delta immutable-file semantics and stable schemas. Full unchanged-payload EXCEPT ALL timed out; do not claim independently re-read payload equality. Complete inherited raw and journal EXCEPT ALL parity passed. All post-publication checks exclude recorded freshness clock.')
(P/'integrity-custody/audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print({k:round(v['caller_ms']/1000,3) for k,v in costs['integrity-custody'].items()})
