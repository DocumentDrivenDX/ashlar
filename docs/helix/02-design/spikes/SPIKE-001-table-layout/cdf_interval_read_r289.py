"""Read-only full-field automatic-CDF qualification for existing owned intervals."""
import json,time,hashlib
from pathlib import Path
from normalized_apply_sql_r276 import pin
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 paths={'publisher':'out/native/ashlar_mixed_publish_r281/audited-summary.json','oracle':'out/cdf-image-oracle-r288.json'};sources={k:json.loads((B/p).read_text()) for k,p in paths.items()};O=B/'out/native/ashlar_cdf_interval_read_r289';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=200,cancel_after=180);a={'state':'Reading exact owned mutation intervals without change-set filters','source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'checks':{},'bounds':{'read_bytes':100000000000,'write_remote_bytes':0,'spill_to_disk_bytes':20000000000,'wall_s':600,'statement_s':180}}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  for role in ['edge_current','adjacency_forward']:
   t=sources['publisher']['tables'][role];v=t['version'];assert v==1;pin(t['table'],v);d=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(names,d[0]));assert detail['id']==t['id'];assert json.loads(detail['properties'])['delta.enableRowTracking']=='true';assert not json.loads(detail['properties']).get('delta.enableChangeDataFeed')=='true'
   expected=sources['oracle']['roles'][role];q=f"SELECT _change_type,count(*),min(_commit_version),max(_commit_version),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(expected['fields'])}))),256) FROM table_changes('{t['table']}',{v},{v}) GROUP BY _change_type ORDER BY _change_type"
   result=[[kind,str(x['rows']),str(v),str(v),x['digest']] for kind,x in sorted(expected['images'].items())];assert c.sql('all-images-'+role,q)==result;a['checks'][role]={'table':t,'rows':190000,'groups':result,'all_fields':'Complete expected pre/post/delete multisets; no additional classes/versions/rows'};save()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'].startswith('all-images-'));a['wall_s']=time.monotonic()-start;assert a['wall_s']<600;a['state']='Every native CDF image matches independent complete-field oracle';a['qualification']='Two owned post-clone version1 MERGE intervals only, Runtime/SQL channel retained in final history. Automatic row-tracking CDF, no legacy enablement or schema/retention mutation. Read-only full190k-image field digests per role match independently qualified full-sweep publication. Not permanent semantic history, real source/writer fencing, complete incremental publisher, latency/SLO or billion admission.';save();print(json.dumps({'state':a['state'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same read-only native CDF handles; do not infer support',error=str(e));save();raise
if __name__=='__main__':main()
