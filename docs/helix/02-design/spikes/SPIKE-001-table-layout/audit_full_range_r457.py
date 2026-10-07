"""Offline exact SQL/finality/result and complete-range cost audit."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent;d=B/'out/native/ashlar_full_range_sweep_r456';s=json.loads((d/'summary.json').read_text());assert s['state']=='Complete64ranges/all100k predecessor and42field source checks pass';h={q['query_id']:q for q in json.loads((d/'shared-history.json').read_text())['queries']};records=[json.loads(x) for p in d.glob('*/statements.jsonl') for x in p.read_text().splitlines()];assert len(records)==72
for p in d.glob('*/statements.jsonl'):
 for line in p.read_text().splitlines():
  r=json.loads(line);q=h[r['statement_id']];assert q['query_text']=='/* ashlar '+p.parent.name+' '+r['label']+' */ '+r['sql'] and q['is_final'] and q['status']=='FINISHED'
assert sorted(r['range'] for r in s['reads'])==list(range(64)) and sum(s['expected_counts'])==100000
for r in s['reads']:
 j=r['range'];assert r['result']==[[str(s['expected_counts'][j]),'0',s['expected_digests'][j]]] and not h[r['statement_id']]['metrics'].get('result_from_cache')
costs={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in s['costs']};assert costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items());assert costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
out={'state':'Complete64range terminal SQL/results/digest bindings/cost audit passes','statements':len(records),'rows':100000,'costs':costs,'summary_sha256':hashlib.sha256((d/'summary.json').read_bytes()).hexdigest(),'code_sha256':hashlib.sha256((B/'full_range_sweep_r456.py').read_bytes()).hexdigest(),'qualification':'Native results bind independent generated per-range42field source digests; read-only/fixed-cache-state observations do not prove MERGE or source freshness.'};(B/'out/full-range-audit-r457.json').write_text(json.dumps(out,indent=2)+'\n');print(out)
