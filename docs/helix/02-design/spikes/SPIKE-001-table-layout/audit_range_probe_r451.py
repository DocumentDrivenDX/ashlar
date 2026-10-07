"""Independent local range count and terminal native receipt audit."""
import json,hashlib
from pathlib import Path
from fourth_changes_r427 import FourthChanges
B=Path(__file__).resolve().parent;d=B/'out/native/ashlar_range_probe_r450';s=json.loads((d/'summary.json').read_text());records=[json.loads(x) for x in (d/'statements.jsonl').read_text().splitlines()];h={q['query_id']:q for q in json.loads((d/'shared-history.json').read_text())['queries']};assert len(records)==2
w=FourthChanges();n=sum('00'<=w.change(i)['before']['lookup_hash']<'04' for i in range(100000));assert n==1554
costs={k:0 for k in s['costs']}
for r in records:
 q=h[r['statement_id']];assert q['query_text']==r['sql'] and q['status']=='FINISHED' and q['is_final'];assert r['sql'].startswith('/* ashlar range_probe_r450 ') and 'VERSION AS OF 0' in r['sql'] and 'VERSION AS OF 6' in r['sql'];assert r['response']['result']['data_array']==[[str(n),'0']] and not q['metrics'].get('result_from_cache')
 for k in costs:costs[k]+=q['metrics'].get(k,0)
assert costs==s['costs'] and costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
out={'state':'Exact range count/full20field native results, terminal SQL receipts and costs pass','rows':n,'costs':costs,'summary_sha256':hashlib.sha256((d/'summary.json').read_bytes()).hexdigest(),'runner_sha256':hashlib.sha256((B/'native_range_probe_r450.py').read_bytes()).hexdigest(),'qualification':'Read-only E6/prepared0; two fixed-order observations, not actual MERGE or production throughput.'};(B/'out/range-probe-audit-r451.json').write_text(json.dumps(out,indent=2)+'\n');print(out)
