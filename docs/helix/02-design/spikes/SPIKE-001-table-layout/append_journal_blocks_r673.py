"""Read-only bounded journal verification against complete independent oracle."""
import json,time
from append_native_r637 import BASE
from append_edge_blocks_r672 import coverage_query,block_query
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
OUT=BASE/'out/native/ashlar_append_journal_blocks_r673'
assert not OUT.exists()
prior=json.loads((BASE/'out/native/ashlar_append_edge_growth_r665/audited-summary-r669.json').read_text())
t=prior['profiles']['property_journal'];oracle=json.loads((BASE/'out/append-edge-oracle-r656.json').read_text())
c=Client(OUT,observation_timeout=240,cancel_after=180);start=time.monotonic()
c.sql('timeout','SET STATEMENT_TIMEOUT=180');c.sql('cache','SET use_cached_result=false')
d=c.sql('identity','DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,d[0]))['id']==t['id']
coverage=c.sql('coverage',coverage_query(t['table'],t['version'],'property_journal',48000000));assert coverage==[['32000000','0']]
fields=oracle['chunks'][0]['roles']['property_journal']['fields'];blocks=[]
for low in [40000000,44000000]:
 rows=c.sql('block-'+str(low),block_query(t['table'],t['version'],'property_journal',fields,low,low+4000000,48000000))
 first=(low-40000000)//100000
 expected=[[str(i),str(oracle['chunks'][i]['roles']['property_journal']['rows']),oracle['chunks'][i]['roles']['property_journal']['digest']] for i in range(first,first+40)]
 assert rows==expected;blocks.append({'start':low,'end':low+4000000,'groups':rows})
assert sum(int(row[1]) for b in blocks for row in b['groups'])==32000000
for attempt in range(12):
 try:h=collect_history(c.w,c.records,OUT/'shared-history.json');break
 except HistoryPending:
  if attempt==11:raise
  time.sleep(2)
costs={k:sum(q['metrics'][k] for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
assert costs['read_bytes']<=20000000000 and costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
result={'state':'Complete32M journal field parity in two4M-entity blocks with whole-table coverage','pin':t,'coverage':coverage,'blocks':blocks,'costs':costs,'wall_s':time.monotonic()-start,'qualification':'Independent full local oracle; all80 groups and32M property rows covered. Whole-table count/invalid membership plus sum-of-block counts prevents filtered corrupt rows disappearing. Current8M edge extent only; not16M-stage admission, real source fidelity, ingest or singleton gate evidence.'}
(OUT/'summary.json').write_text(json.dumps(result,indent=2)+'\n');(OUT/'live-statement.json').unlink(missing_ok=True)
print(json.dumps({'state':result['state'],'costs':costs,'wall_s':result['wall_s']},indent=2))
