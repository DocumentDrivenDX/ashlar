"""Read-only exact pinned singleton plan inspection; no cohort replay."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
from normalized_apply_sql_r276 import mutation_source
B=Path(__file__).resolve().parent

def main():
 out=B/'out/native/mutation_source_probe_r562';assert not out.exists();start=time.monotonic();c=BoundedReads(out,socket_timeout=30);a={'state':'checking full normalized mutation source','code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'sources':{},'plans':{},'bounds':{'wall_s':180,'read_bytes':5000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0}}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  p=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json';pub=json.loads(p.read_text());a['sources']['sixth_publication']=hashlib.sha256(p.read_bytes()).hexdigest();target=pub['tables']['edge_current']['table'];raw=pub['inputs']['source_record'];current=pub['inputs']['current_replacement']
  oracle_path=B/'out/sixth-cdf-image-oracle-r529.json';oracle=json.loads(oracle_path.read_text())['roles']['edge_current'];a['sources']['cdf_oracle']=hashlib.sha256(oracle_path.read_bytes()).hexdigest();fields=oracle['fields'];relation=mutation_source(raw['table'],raw['version'],current['table'],current['version']);before=row_hash_sql(fields);after=row_hash_sql(['after_'+f for f in fields]);q=f"SELECT is_delete,count(*),count(DISTINCT struct(source_system,rel_type_id,id)),sha2(concat_ws('',sort_array(collect_list({before}))),256),sha2(concat_ws('',sort_array(collect_list(CASE WHEN NOT is_delete THEN {after} END))),256) FROM ({relation}) GROUP BY is_delete ORDER BY is_delete"
  wanted=[['false','90000','90000',oracle['images']['update_preimage']['digest'],oracle['images']['update_postimage']['digest']],['true','10000','10000',oracle['images']['delete']['digest'],hashlib.sha256(b'').hexdigest()]]
  rows=c.sql('source-plan','EXPLAIN FORMATTED '+q);text='\n'.join(str(r[0]) for r in rows)+'\n';(out/'source-plan.txt').write_text(text);a['plans']['source-plan']={'query':q,'statement_id':c.records[-1]['statement_id'],'sha256':hashlib.sha256(text.encode()).hexdigest()}
  a['expected']=wanted;a['query']=q;a['reads']=[]
  for label in ['first','repeat']:
   rows=c.sql(label,q);assert rows==wanted;a['reads'].append({'label':label,'statement_id':c.records[-1]['statement_id'],'result':rows,'caller_ms':c.records[-1]['wall_ms']});save()
  a['qualification']='Full20field before and after source digest checks; pinned sixth inputs only, no current target scan or MERGE execution. Grouping/hashing/sorting adds oracle validation work; not a naked source materialization clock, producer arrival, or causal decomposition of historical MERGE.'
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=180;a['state']='Complete normalized mutation source before/after digests pass twice';save();(out/'inflight-request.json').rename(out/'completed-last-request.json');print(json.dumps({k:a[k] for k in ['state','costs','wall_s','reads']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles without replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
