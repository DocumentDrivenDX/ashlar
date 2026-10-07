"""Finalize stopped preflight IDs; no query/read workload replay."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_publication_reads_r201';c=Client(O);c.records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];h={x['query_id']:x for x in c.history()};assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
r=next(r for r in c.records if r['label']=='overlap-version');names=[p['name'] for p in r['response']['manifest']['schema']['columns']];head=dict(zip(names,r['response']['result']['data_array'][0]));assert head['version']=='3' and head['operation']=='OPTIMIZE'
assert 'Predictive Optimization' in json.loads(head['job'])['jobName']
s={'state':'Controller preflight stopped on obsolete latest-head==published-version assumption; all submitted metadata queries succeeded','observed_head':head,'costs':{k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']},'qualification':'No singleton or oracle query was submitted by r201. Physical maintenance head is not a new logical publication; r202 separately verifies/read-pins exact published version1. No native failure or write replay.'}
(O/'audited-summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s['costs']))
