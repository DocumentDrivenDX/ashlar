"""Read-only full-slice bootstrap role reuse differential against independent oracle."""
import json,time,hashlib
from pathlib import Path
from scale_mixed_roles_sql_r224 import role_sql,role_from_pinned_carrier
from scale_mixed_r219 import Workload
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_carrier_reuse_r238';assert not O.exists();s=json.loads((B/'out/native/ashlar_scale_slice_r236/summary.json').read_text());t=s['tables']['object_current'];c=Client(O,observation_timeout=200,cancel_after=180);a={'state':'running','source':t,'checks':{}}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180');d=c.sql('detail','DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,d[0]))['id']==t['id']
 defaults=json.loads((B/'out/role-generator-defaults-r238.json').read_text());assert all(hashlib.sha256(role_sql(role,'node',8000000,40000000,0,200000).encode()).hexdigest()==digest for role,digest in defaults.items())
 w=Workload(8000000,40000000);samples=dict(w.roles('node',0))
 for role in ['source_record','property_journal']:
  fields=list(samples[role]);q=role_from_pinned_carrier(role,'node',8000000,40000000,0,200000,t['table'],0)
  actual=c.sql('digest-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(fields)}))),256) FROM ({q})");expected=s['checks'][role];assert actual==[[str(expected['rows']),expected['independent_all_field_digest']]],role;a['checks'][role]=expected;save()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=2000000000 and a['costs']['write_remote_bytes']==0;a['state']='All200k raw records and800k bootstrap events reused from native carrier match independent oracle';a['qualification']='Pinned verified synthetic bootstrap only. Default range SQL fingerprints unchanged. No mutation, source-authority/raw reconstruction claim, general property-event extraction, throughput improvement or scale admission; exact cryptographic field preservation across all slice rows.';save();print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect same native handles',error=str(e));save();raise
