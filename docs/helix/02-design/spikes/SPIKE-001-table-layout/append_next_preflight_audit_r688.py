"""Exact final audit of fresh owned heads and maintenance-preserved role content."""
import json,hashlib
from pathlib import Path
from append_edge_blocks_r681 import coverage_query,block_query
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_next_preflight_r687'
def run():
 s=json.loads((O/'summary.json').read_text());records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];history=json.loads((O/'shared-history.json').read_text());h={q['query_id']:q for q in history['queries']}
 assert len(records)==len(h)==30 and len({r['statement_id'] for r in records})==30 and not history['missing_ids']
 assert not (O/'live-statement.json').exists()
 for r in records:
  q=h[r['statement_id']];assert q['query_text']==r['sql'] and q['is_final'] and q['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED'
 oracle_path=B/'out/append-edge-oracle-r656.json';oracle=json.loads(oracle_path.read_text())
 for role,t in s['tables'].items():
  assert sorted(int(x['version']) for x in t['maintenance_interval'])==list(range(t['prior_pin']+1,t['version']+1))
  assert all(x['operation']=='OPTIMIZE' for x in t['maintenance_interval'])
  assert t['head']['version']==t['closing_head']['version'] and t['head']['queryHistoryStatementId']==t['closing_head']['queryHistoryStatementId']
  assert json.loads(t['detail']['properties'])['delta.targetFileSize']=='67108864'
  fields=oracle['chunks'][0]['roles'][role]['fields'];total=32000000 if role=='property_journal' else 8000000
  r=next(r for r in records if r['label']=='coverage-'+role)
  assert r['sql']==coverage_query(t['table'],t['version'],role,48000000) and r['response']['result']['data_array']==[[str(total),'0']]
  for i,low in enumerate([40000000,44000000]):
   r=next(r for r in records if r['label']==role+'-'+str(low))
   assert r['sql']==block_query(t['table'],t['version'],role,fields,low,low+4000000,48000000)
   expected=[[str(j),str(oracle['chunks'][j]['roles'][role]['rows']),oracle['chunks'][j]['roles'][role]['digest']] for j in range(i*40,(i+1)*40)]
   assert r['response']['result']['data_array']==expected==s['checks'][role]['blocks'][i]['groups']
   assert h[r['statement_id']]['metrics']['result_from_cache'] is False
  assert sum(int(row[1]) for block in s['checks'][role]['blocks'] for row in block['groups'])==total
 costs={k:sum(q['metrics'][k] for q in h.values()) for k in s['costs']};assert costs==s['costs']
 a={'state':'All30 exact final statements and320 full-field groups qualify observed maintenance heads','heads':{k:t['version'] for k,t in s['tables'].items()},'maintenance_commits':{k:len(t['maintenance_interval']) for k,t in s['tables'].items()},'costs':costs,'wall_s':s['wall_s'],'sources':{str(p.relative_to(B)):hashlib.sha256(p.read_bytes()).hexdigest() for p in [oracle_path,O/'summary.json',O/'statements.jsonl',O/'shared-history.json']},'qualification':s['qualification']+' Complete live content compared with independent oracle; raw Delta-log/DV actions and retention remain unqualified. Next writer must recheck heads; this is not a future-head guarantee.'}
 (O/'audited-summary-r688.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps(a,indent=2))
if __name__=='__main__':run()
