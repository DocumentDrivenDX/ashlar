"""Compute range/phase-specific small-cohort maintenance read metrics."""
import json,hashlib,math,statistics
from pathlib import Path
from seventh_changes_r589 import SeventhChanges
B=Path(__file__).resolve().parent;p=B/'out/native/ashlar_sparse_post_reads_r613';a=json.loads((p/'audited-summary.json').read_text());assert a['audit']['matched_full_carriers']==32;r={x['statement_id']:x for x in map(json.loads,(p/'statements.jsonl').read_text().splitlines())};h={q['query_id']:q for q in json.loads((p/'shared-history.json').read_text())['queries']};w=SeventhChanges();groups={};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
for family in ['before','after']:
 for phase in [0,1]:
  for scope in ['inside','outside']:
   reads=[]
   for x in a['reads']:
    key=w.change(x['index'])['before']['lookup_hash'];inside='08'+'0'*62<=key<'0c'+'0'*62
    if x['family']==family and x['phase']==phase and inside==(scope=='inside'):reads.append(x)
   assert len(reads)==4 and sum(w.change(x['index'])['after'] is None for x in reads)==1
   metrics=[h[x['statement_id']]['metrics'] for x in reads];groups[family+'-'+str(phase)+'-'+scope]={'queries':4,'caller_p95_ms':p95([r[x['statement_id']]['wall_ms'] for x in reads]),'engine_p95_ms':p95([x['execution_time_ms'] for x in metrics]),'read_bytes':sum(x['read_bytes'] for x in metrics),'median_files':statistics.median(x['read_files_count'] for x in metrics),'remote_bytes':sum(x.get('read_remote_bytes',0) for x in metrics)}
result={'state':'Audited maintenance read responses split by exact inside/outside hash predicate','source_sha256':hashlib.sha256((p/'audited-summary.json').read_bytes()).hexdigest(),'groups':groups,'qualification':'Four queries per subgroup means nearest-rank95th is maximum; tiny serial paired sample only, one delete/threeupdates each. No servicewidep95/cold/concurrency/causal/performance admission.'};(B/'out/sparse-read-groups-r615.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
