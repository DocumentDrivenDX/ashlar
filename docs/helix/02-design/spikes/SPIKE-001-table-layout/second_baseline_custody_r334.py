"""Supplemental read-only full first-batch interval custody audit after r332."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from normalized_apply_sql_r276 import pin
from mixed_change_queries_r230 import row_hash_sql
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent
def main():
 p=B/'out/native/ashlar_incremental_publish_r332/summary.json';a=json.loads(p.read_text());assert a['state']=='Private second100k range32 publication passes complete change-image custody checks'
 first=json.loads((B/'out/native/ashlar_normalized_delta_stage_r275/audited-summary.json').read_text());oracle=json.loads((B/'out/cdf-image-oracle-r288.json').read_text());o=B/'out/native/ashlar_second_baseline_custody_r334';assert not o.exists();o.mkdir();c=Client(o,observation_timeout=200,cancel_after=180);start=time.monotonic();out={'state':'Auditing reconstructed first batch intervals','publisher_summary_sha256':hashlib.sha256(p.read_bytes()).hexdigest(),'checks':{}}
 def save():(o/'summary.json').write_text(json.dumps(out,indent=2)+'\n')
 save()
 try:
  for role in ['source_record','property_journal','tombstone','adjacency_forward']:
   t=a['tables'][role];e=next(e for e in a['commit_events'] if e['role']==role and e['statement_id']==next(r['statement_id'] for r in [json.loads(x) for x in (p.parent/'statements.jsonl').read_text().splitlines()] if r['label']=='prepare-first-'+('forward' if role=='adjacency_forward' else role)));v=e['version']
   if role=='adjacency_forward':
    f=oracle['roles'][role];q=f"SELECT _change_type,count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(f['fields'])}))),256) FROM table_changes('{t['table']}',{v},{v}) GROUP BY _change_type ORDER BY _change_type";want=[[k,str(x['rows']),x['digest']] for k,x in sorted(f['images'].items())]
   else:
    f=first['checks'][role];rel=pin(t['table'],v) if role=='tombstone' else f"table_changes('{t['table']}',{v},{v})";q=f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(f['fields'])}))),256) FROM {rel}";want=[[str(f['rows']),f['all_known_field_digest']]]
   assert c.sql('first-custody-'+role,q)==want;out['checks'][role]={'version':v,'expected':want};save()
  for i in range(20):
   try:h=collect_history(c.w,c.records,o/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  out['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert out['costs']['read_bytes']<10000000000 and out['costs']['write_remote_bytes']==0 and out['costs']['spill_to_disk_bytes']==0;assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records);out['wall_s']=time.monotonic()-start;out['state']='All reconstructed first-batch role intervals match complete independent digests';out['qualification']='Supplemental read-only baseline qualification after experimental descriptor; excluded from r332 processing clock and explicitly charged separately. No production pointer/ACK or general writer fence.';save();print(json.dumps(out,indent=2))
 except Exception as e:out.update(state='Stopped; inspect same read-only handles',error=str(e));save();raise
if __name__=='__main__':main()
