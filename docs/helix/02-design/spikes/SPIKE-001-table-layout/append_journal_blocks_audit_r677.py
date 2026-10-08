"""Audit bounded complete journal parity without native SQL submission."""
import json,hashlib
from pathlib import Path
from append_edge_blocks_r672 import coverage_query,block_query
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_journal_blocks_r676'
s=json.loads((O/'summary.json').read_text());r=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];history=json.loads((O/'shared-history.json').read_text());h={q['query_id']:q for q in history['queries']}
assert len(r)==len(h)==6 and len({x['statement_id'] for x in r})==6 and not history['missing_ids']
assert not (O/'live-statement.json').exists()
for rec in r:
 q=h[rec['statement_id']];assert q['query_text']==rec['sql'] and q['is_final'] and q['status']=='FINISHED' and rec['response']['status']['state']=='SUCCEEDED'
t=s['pin'];oracle_path=B/'out/append-edge-oracle-r656.json';oracle=json.loads(oracle_path.read_text());fields=oracle['chunks'][0]['roles']['property_journal']['fields']
assert r[3]['sql']==coverage_query(t['table'],t['version'],'property_journal',48000000)
assert r[3]['response']['result']['data_array']==s['coverage']==[['32000000','0']]
for i,low in enumerate([40000000,44000000]):
 rec=r[4+i];assert rec['sql']==block_query(t['table'],t['version'],'property_journal',fields,low,low+4000000,48000000)
 expected=[[str(j),str(oracle['chunks'][j]['roles']['property_journal']['rows']),oracle['chunks'][j]['roles']['property_journal']['digest']] for j in range(i*40,(i+1)*40)]
 assert rec['response']['result']['data_array']==expected==s['blocks'][i]['groups']
 assert h[rec['statement_id']]['metrics']['result_from_cache'] is False
assert sum(int(row[1]) for block in s['blocks'] for row in block['groups'])==32000000
costs={k:sum(q['metrics'][k] for q in h.values()) for k in s['costs']};assert costs==s['costs']
a={'state':'All6 exact final statements,80 independent digest groups and whole32M coverage audited','costs':costs,'wall_s':s['wall_s'],'blocks':[{k:h[rec['statement_id']]['metrics'].get(k) for k in ['execution_time_ms','total_time_ms','read_bytes','read_files_count','pruned_files_count']} for rec in r[4:]],'sources':{str(p.relative_to(B)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [oracle_path,O/'summary.json',O/'statements.jsonl',O/'shared-history.json']},'qualification':s['qualification']+' Only property-journal blocks have native execution evidence here. General carrier/raw/adjacency block branches are not promoted from this result. Prior setup cancellation costs remain unknown until final metrics arrive.'}
(O/'audited-summary-r677.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps(a,indent=2))
