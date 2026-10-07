"""Qualify pure-edge block pruning against saved complete8M-node/4M-edge digests."""
import json,time
from pathlib import Path
from mixed_grouped_pruning_r253 import pruned_mixed_query
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_pruning_r253';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_edges_r251/audited-summary.json').read_text());node=json.loads((B/'out/native/ashlar_scale_growth_r248/audited-summary.json').read_text());a={'state':'Running equivalent pure-edge block verification','checks':{},'bounds':{'read_bytes':15000000000,'write_bytes':0,'statement_s':180}};c=Client(O,observation_timeout=200,cancel_after=180);started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 for role in ['source_record','property_journal']:
  t=prior['tables'][role];rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[f['name'] for f in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id'];expected=prior['checks']['complete-'+role]['groups'][100:]
  actual=c.sql('pruned-'+role,pruned_mixed_query(t['table'],t['version'],role,node['oracle'][0]['roles'][role]['fields'],8000000,4000000,100000,100,100));assert actual==expected;assert c.sql('full-count-'+role,f"SELECT count(*) FROM {t['table']} VERSION AS OF {t['version']}")==[[str(prior['tables'][role]['rows'])]];a['checks'][role]={'table':t,'groups':actual,'qualification':'Pure-edge final20 groups match independent complete prior oracle at same snapshot; prior first100 groups preserved by unchanged query shape. Whole-role count unchanged.'};save()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=15000000000 and a['costs']['write_remote_bytes']==0;a['metrics']={r['label']:dict(caller_ms=r['wall_ms'],**h[r['statement_id']]['metrics']) for r in c.records if r['label'].startswith('pruned-')};assert all(not q.get('result_from_cache') for q in a['metrics'].values());a['state']='Both uncached pruned pure-edge blocks preserve every field digest and complete counts';a['wall_s']=time.monotonic()-started;save();print(json.dumps({k:a[k] for k in ['state','costs','wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles before admission',error=str(e));save();raise
