"""Bounded matched full-carrier predecessor reads; no writes or MERGE execution."""
import json,time,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from normalized_apply_sql_r276 import mutation_source,pin
from mixed_change_queries_r230 import row_hash_sql
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_merge_join_read_r296';assert not p.exists();p.mkdir();start=time.monotonic()
 source=B/'out/native/ashlar_merge_plan_r295/summary.json';prior=json.loads(source.read_text());assert prior['native_history_final'];e=prior['base'];inputs=prior['inputs'];raw=inputs['source_record'];cur=inputs['current_replacement'];oracle=json.loads((B/'out/cdf-image-oracle-r288.json').read_text())['roles']['edge_current'];src=mutation_source(raw['table'],raw['version'],cur['table'],cur['version']);digest=row_hash_sql(['b.'+f for f in oracle['fields']]);c=BoundedReads(p,socket_timeout=60)
 a={'state':'Running two matched full-carrier predecessor reads','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'table':e,'inputs':inputs,'bounds':{'read_bytes':100000000000,'write_remote_bytes':0,'spill_to_disk_bytes':5000000000,'wall_s':300,'statement_timeout_s':60},'reads':[]}
 def save():(p/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(15):
   try:h=collect_history(c.w,c.records,p/'shared-history.json');break
   except HistoryPending:
    if i==14:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items());return h
 try:
  save();c.sql('timeout','SET STATEMENT_TIMEOUT=60');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  expected=[[False,'90000',oracle['images']['update_preimage']['digest']],[True,'10000',oracle['images']['delete']['digest']]]
  for label,hint in [('default',''),('broadcast-source','/*+ BROADCAST(s) */')]:
   if a.get('costs'):assert a['costs']['read_bytes']+40000000000<=a['bounds']['read_bytes']
   q=f"SELECT {hint} s.is_delete,count(*),sha2(concat_ws('',sort_array(collect_list({digest}))),256) FROM {pin(e['table'],e['version'])} b INNER JOIN ({src}) s ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id WHERE b.entity_version=1 AND b.apply_batch_id='mixed-bootstrap' GROUP BY s.is_delete ORDER BY s.is_delete"
   rows=c.sql(label,q);assert [[str(x[0]).lower(),str(x[1]),x[2]] for x in rows]==[[str(x[0]).lower(),x[1],x[2]] for x in expected];h=metrics();r=c.records[-1];m=h[r['statement_id']]['metrics'];assert not m.get('result_from_cache');a['reads'].append({'label':label,'statement_id':r['statement_id'],'caller_ms':r['wall_ms'],'metrics':m,'rows':rows});save();assert time.monotonic()-start<300
  a['state']='Both matched100k predecessor joins preserve complete20-field digests';a['wall_s']=time.monotonic()-start;a['qualification']='Two ordered uncached read-only join samples on one pinned40M-edge snapshot and identical100k scattered keys. All20carrier fields and separate90k-update/10k-delete predecessor multisets match independent local oracle under SHA256 collision resistance. No causality/repeated service-tail claim; second query may benefit from first. Read aggregation is not MERGE, CDF capture or publication freshness; broad hash distribution may still touch all files. No source ACK, compute change or billion admission.';save();(p/'inflight-request.json').unlink();print(json.dumps({'state':a['state'],'costs':a['costs'],'reads':[{k:r[k] for k in ['label','caller_ms','metrics']} for r in a['reads']]},indent=2))
 except Exception as exc:a.update(state='Stopped; inspect same native handles before admission',error=str(exc));save();raise
 finally:c.close()
if __name__=='__main__':main()
