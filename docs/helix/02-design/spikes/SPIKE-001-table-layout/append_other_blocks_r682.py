"""Read-only complete remaining verifier branches with explicit direct ID pruning."""
import json,time
from pathlib import Path
from append_edge_blocks_r681 import coverage_query,block_query
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_other_blocks_r682';assert not O.exists()
prior=json.loads((B/'out/native/ashlar_append_edge_growth_r665/audited-summary-r669.json').read_text());oracle=json.loads((B/'out/append-edge-oracle-r656.json').read_text())
c=Client(O,observation_timeout=240,cancel_after=60);start=time.monotonic()
c.sql('timeout','SET STATEMENT_TIMEOUT=180');c.sql('cache','SET use_cached_result=false');c.cancel_after=180
result={'state':'Checking all remaining block branches','roles':{},'qualification':'Complete independent current8M extent for carrier/raw/adjacency branches. Direct ID pruning adds no new logical allocation. Whole-table count/invalid membership and sum-of-block counts required. Not next16M growth, publication or performance admission.'}
for role in ['edge_current','source_record','adjacency_forward']:
 t=prior['profiles'][role];fields=oracle['chunks'][0]['roles'][role]['fields']
 rows=c.sql('identity-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id']
 coverage=c.sql('coverage-'+role,coverage_query(t['table'],t['version'],role,48000000));assert coverage==[['8000000','0']]
 blocks=[]
 for low in [40000000,44000000]:
  rows=c.sql(role+'-'+str(low),block_query(t['table'],t['version'],role,fields,low,low+4000000,48000000))
  first=(low-40000000)//100000;expected=[[str(i),str(oracle['chunks'][i]['roles'][role]['rows']),oracle['chunks'][i]['roles'][role]['digest']] for i in range(first,first+40)]
  assert rows==expected;blocks.append({'start':low,'end':low+4000000,'groups':rows})
 assert sum(int(r[1]) for block in blocks for r in block['groups'])==8000000
 result['roles'][role]={'pin':t,'coverage':coverage,'blocks':blocks}
for attempt in range(12):
 try:h=collect_history(c.w,c.records,O/'shared-history.json');break
 except HistoryPending:
  if attempt==11:raise
  time.sleep(2)
costs={k:sum(q['metrics'][k] for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
assert costs['read_bytes']<=30000000000 and costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
result.update(state='Complete independent8M parity for all three remaining block branches',costs=costs,wall_s=time.monotonic()-start)
(O/'summary.json').write_text(json.dumps(result,indent=2)+'\n');(O/'live-statement.json').unlink(missing_ok=True)
print(json.dumps({'state':result['state'],'costs':costs,'wall_s':result['wall_s']},indent=2))
