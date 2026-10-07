"""Audit exact native IDs for the bounded wire screen; no data mutation."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_wire_validation_r150'
s=json.loads((O/'summary.json').read_text());assert s['state'].startswith('Three balanced pairs')
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records),'Inspect same saved IDs; no SQL replay'
s['costs']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records}
for mode in ('control','stored'):
 rs=[r for r in c.records if r['label'].startswith(mode+'-')];assert len(rs)==3
 assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
 s[mode]={'caller_ms':[r['wall_ms'] for r in rs],'engine_ms':[h[r['statement_id']]['metrics']['execution_time_ms'] for r in rs],'files':[h[r['statement_id']]['metrics']['read_files_count'] for r in rs],'remote_bytes':[h[r['statement_id']]['metrics']['read_remote_bytes'] for r in rs]}
build=s['costs']['materialize']['caller_ms'];oracle=s['costs']['witness-exact']['caller_ms']
s['single_use_sensitivity']={'build_plus_stored_ms':[build+x for x in s['stored']['caller_ms']], 'build_plus_exact_witness_oracle_plus_stored_ms':[build+oracle+x for x in s['stored']['caller_ms']], 'qualification':'Separate sequential samples summed for screening only, not observed whole publication; the independent exact witness oracle must remain counted if required for production proof.'}
s['cost_totals']={'read_bytes':sum(v['metrics'].get('read_bytes',0) for v in s['costs'].values()),'write_remote_bytes':sum(v['metrics'].get('write_remote_bytes',0) for v in s['costs'].values())}
s['state']='Balanced exact raw validation and independent witness equality finalized; no publication admission'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n')
print(json.dumps({k:s[k] for k in ['control','stored','single_use_sensitivity','cost_totals']},indent=2))
