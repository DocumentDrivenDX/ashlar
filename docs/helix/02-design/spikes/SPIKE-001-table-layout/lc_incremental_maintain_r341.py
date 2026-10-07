"""Ordinary LC OPTIMIZE with complete before/after carrier preservation."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent

def main():
 source_path=B/'out/native/ashlar_lc_second_apply_r337/audited-summary.json';s=json.loads(source_path.read_text());assert s['state']=='Liquid-clustered second-batch MERGE preserves complete190k edge images';t=s['tables']['edge_current'];assert t['version']==3
 p=B/'out/native/ashlar_lc_incremental_maintain_r341';assert not p.exists();p.mkdir();c=Client(p,observation_timeout=200,cancel_after=90);start=time.monotonic();a={'state':'Preflighting ordinary incremental LC maintenance','source_sha256':hashlib.sha256(source_path.read_bytes()).hexdigest(),'table':t['table'],'uuid':t['id'],'before_version':3,'checks':{},'bounds':{'read_bytes':200000000000,'write_remote_bytes':40000000000,'spill_to_disk_bytes':5000000000,'wall_s':900,'statement_cancel_after_s':90},'basis':'Full before/after scans approximately32GB each plus ordinary optimizer rewrite up to full32GB;200GB read40GB write bounds include headroom. Native counters measured, no assumed small incremental rewrite or cleanup.'}
 def save():(p/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics(reserve=0):
  for n in range(20):
   try:h=collect_history(c.w,c.records,p/'shared-history.json');break
   except HistoryPending:
    if n==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=a['bounds']['read_bytes'];assert all(v<=a['bounds'][k] for k,v in a['costs'].items());assert time.monotonic()-start<900;return h
 def history(label):
  rows=c.sql(label,'DESCRIBE HISTORY '+t['table']+' LIMIT 100');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(names,r)) for r in rows]
 def detail(label):
  rows=c.sql(label,'DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,rows[0]))
 def digest(label,version):
  fields=json.loads((B/'out/second-cdf-image-oracle-r327.json').read_text())['roles']['edge_current']['fields'];hs=row_hash_sql(fields);q=f"SELECT CAST(floor((id-8000001)/100000) AS BIGINT) group_id,count(*),sha2(concat_ws('',sort_array(collect_list({hs}))),256) FROM {t['table']} VERSION AS OF {version} GROUP BY group_id ORDER BY group_id";rows=c.sql(label,q);assert len(rows)==400 and [int(r[0]) for r in rows]==list(range(400)) and sum(int(r[1]) for r in rows)==39980000;return rows
 save()
 try:
  a['before_detail']=detail('detail-before');assert a['before_detail']['id']==t['id'] and json.loads(a['before_detail']['partitionColumns'])==[] and json.loads(a['before_detail']['clusteringColumns'])==['lookup_hash'];h=history('history-before');assert [int(x['version']) for x in h]==[3,2,1,0];approved={x['statement_id'] for x in s['commits']};assert all(x['queryHistoryStatementId'] in approved for x in h);a['before_history']=h
  a['checks']['before']=digest('before-all',3);metrics(80000000000);a['state']='Submitting one ordinary OPTIMIZE, no FULL or range predicate';save();st=time.monotonic();c.sql('optimize','OPTIMIZE '+t['table']);a['optimize_caller_s']=time.monotonic()-st;sid=c.records[-1]['statement_id'];a['optimize_statement_id']=sid;h=history('history-after');new=[x for x in h if int(x['version'])>3];assert all(x['operation']=='OPTIMIZE' and x['queryHistoryStatementId']==sid for x in new);a['commits']=new;a['after_version']=int(h[0]['version']);assert [int(x['version']) for x in new]==list(range(a['after_version'],3,-1));a['state']='Checking every carrier after maintenance';save();metrics()
  a['checks']['after']=digest('after-all',a['after_version']);assert a['checks']['after']==a['checks']['before'];a['after_detail']=detail('detail-after');assert a['after_detail']['id']==t['id'];h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'] in ['before-all','after-all']);a['optimize_metrics']=h[sid]['metrics'];a['wall_s']=time.monotonic()-start;a['state']='Ordinary LC maintenance preserves all39.98M complete carriers';a['qualification']='Complete400 identity-group20-field multisets before3/after actual selected head, equality assumesSHA256collision resistance. Exact original controlled clone/apply custody and all maintenance commits accounted bySID, including zero-file or multi-commit outcomes. No independent source/fence proof, production descriptor/ACK, retained-file inventory/VACUUM, controlled cold/sustained rate or billion admission. Whole verification costs separated from optimizer cost.';save();print(json.dumps({'state':a['state'],'optimize_caller_s':a['optimize_caller_s'],'costs':a['costs'],'wall_s':a['wall_s'],'after_version':a['after_version']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native handles and commits, no replay',error=str(e),wall_s=time.monotonic()-start);save();raise
if __name__=='__main__':main()
