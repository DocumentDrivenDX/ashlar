"""Saved-ID audit of canonical structural reuse and projection build costs."""
import json,math
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_canonical_adjacency_r120'
s=json.loads((O/'summary.json').read_text());c=Client(O)
c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in c.history()}
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
reads=[r for r in c.records if r['label'].startswith('page-')];assert len(reads)==30
assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in reads)
def p95(v):return sorted(v)[math.ceil(.95*len(v))-1]
s['pages']={'n':30,'caller_p95_ms':p95([r['wall_ms'] for r in reads]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in reads]),'files_p95':p95([h[r['statement_id']]['metrics']['read_files_count'] for r in reads]),'bytes_p95':p95([h[r['statement_id']]['metrics']['read_bytes'] for r in reads]),'remote_queries':sum(h[r['statement_id']]['metrics']['read_remote_bytes']>0 for r in reads),'scope':'Ten lexicographically first endpoint keys,3repetitions; sparse neighborhood screen only, not singleton or general adjacency SLO'}
s['costs']={r['label']:{'caller_ms':r['wall_ms'],'metrics':h[r['statement_id']]['metrics']} for r in c.records if r['label'] in ('create','build','structural-reuse-proof','full-parity','identities','closure')}
s['state']='20M canonical full projection parity, identity, typed closure, E13/E16 structural equivalence and30 pages audited; all page histories final uncached'
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps({'pages':s['pages'],'physical':{f:s['physical_detail'][f] for f in ('numFiles','sizeInBytes')},'cost_seconds':{k:round(v['caller_ms']/1000,3) for k,v in s['costs'].items()}},indent=2))
