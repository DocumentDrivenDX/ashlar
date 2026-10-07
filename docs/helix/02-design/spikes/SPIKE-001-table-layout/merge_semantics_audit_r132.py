"""Saved-ID final execution audit for the bounded native eligibility control."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_merge_semantics_r132'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
labels={'control-exact','candidate-exact','control-counts','candidate-counts','paired-parity'}
checks=[r for r in c.records if r['label'] in labels]
assert len(checks)==5
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in checks)
s.update(state='Five exact native mixed-eligibility controls audited; all queries successful/final and controls result-uncached',controls={r['label']:{'query_id':r['statement_id'],'result':r['response']['result']['data_array'],'caller_ms':r['wall_ms']} for r in checks})
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(s['state'])
