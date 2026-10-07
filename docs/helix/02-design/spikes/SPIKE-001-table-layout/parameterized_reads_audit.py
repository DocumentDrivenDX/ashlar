"""Final-history audit; never re-executes measured queries."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent
O=B/'out/native/ashlar_parameterized_reads_r106'
s=json.loads((O/'summary.json').read_text())
records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
c=Client(O);c.records=records
h={q['query_id']:q for q in c.history()}
reads=[r for r in records if r['label'].startswith(('literal-','parameter-'))]
assert len(reads)==120
assert all(r['response']['status']['state']=='SUCCEEDED' for r in reads)
assert all(h[r['statement_id']]['is_final'] for r in reads),'Refresh history only'
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in reads)
def p95(v):return sorted(v)[math.ceil(.95*len(v))-1]
summary={}
for mode in ('literal','parameter'):
 group=[r for r in reads if r['label'].startswith(mode+'-')]
 def values(key):return [h[r['statement_id']]['metrics'][key] for r in group]
 summary[mode]={'n':len(group),'caller_p95_ms':p95([r['wall_ms'] for r in group]),
   'engine_p95_ms':p95(values('execution_time_ms')),'compile_p95_ms':p95(values('compilation_time_ms')),
   'total_server_p95_ms':p95(values('total_time_ms')),
   'remote_queries':sum(v>0 for v in values('read_remote_bytes')),
   'file_reads_p95':p95(values('read_files_count')),
   'unique_query_texts':len({r['sql'] for r in group})}
s.update(state='audited 120 exact full-field reads; all final uncached',comparison=summary)
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps(summary,indent=2))
