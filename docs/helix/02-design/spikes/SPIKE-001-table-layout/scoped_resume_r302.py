"""Same-SID recovery of multi-commit scoped OPTIMIZE; never resubmits mutation."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_scoped_maintenance_r300';a=json.loads((p/'summary.json').read_text());assert a['state']=='Stopped; inspect original handles without replay';stopped=p/'stopped-summary.json';assert not stopped.exists();stopped.write_text(json.dumps(a,indent=2)+'\n');c=Client(p,observation_timeout=200,cancel_after=90);c.records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];opt=next(r for r in c.records if r['label']=='optimize');assert c.w.api_client.do('GET','/api/2.0/sql/statements/'+opt['statement_id'])['status']['state']=='SUCCEEDED';t=a['table'];start=time.monotonic()
 def save():(p/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics(reserve=0):
  for n in range(15):
   try:h=collect_history(c.w,c.records,p/'shared-history.json');break
   except HistoryPending:
    if n==14:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=320000000000 and a['costs']['write_remote_bytes']<=5000000000 and a['costs']['spill_to_disk_bytes']<=5000000000;assert time.time()-c.records[0]['start_epoch']<1200;return h
 try:
  rows=c.sql('resume-history','DESCRIBE HISTORY '+t+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];hist=[dict(zip(names,r)) for r in rows];events=[x for x in hist if int(x['version'])>2];assert [int(x['version']) for x in events]==[4,3] and all(x['operation']=='OPTIMIZE' and x['queryHistoryStatementId']==opt['statement_id'] for x in events);assert [x for x in hist if int(x['version'])<=2]==a['before_history'];a.update(state='Read-only recovery audit after multi-commit maintenance',optimize_statement_id=opt['statement_id'],commits=events,after_version=4,recovery={'stopped_summary_sha256':hashlib.sha256(stopped.read_bytes()).hexdigest(),'reason':'Single OPTIMIZE produced data rewrite3 and zero-file commit4; both same actual SID. No mutation replay.'});save();metrics()
  fields=json.loads((B/'out/cdf-image-oracle-r288.json').read_text())['roles']['edge_current']['fields'];hs=row_hash_sql(fields);allrows=[]
  for first in range(0,400,100):
   metrics(40000000000);lo=8000001+first*100000;hi=lo+10000000;rows=c.sql('after-'+str(first),f"SELECT CAST(floor((id-8000001)/100000) AS BIGINT) group_id,count(*),sha2(concat_ws('',sort_array(collect_list({hs}))),256) FROM {t} VERSION AS OF 4 WHERE id>={lo} AND id<{hi} GROUP BY group_id ORDER BY group_id");assert len(rows)==100;allrows.extend(rows);save()
  assert allrows==a['checks']['before'];a['checks']['after']=allrows;assert sum(int(r[1]) for r in allrows)==39990000;assert c.sql('final-count',f'SELECT count(*) FROM {t} VERSION AS OF 4')==[['39990000']];rows=c.sql('detail-after','DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];a['after_detail']=dict(zip(names,rows[0]));assert a['after_detail']['id']==a['uuid'];metrics();a.pop('error',None);a.update(state='Scoped maintenance commits with all39.99M complete carriers preserved',wall_s=time.time()-c.records[0]['start_epoch'],qualification='Full20-field before2/after4multiset digests for400groups cover39.99M rows underSHA256collision resistance. Same OPTIMIZE SID accounts for both data rewrite3 and zero-file4commit, no replay. Maintenance equivalence to version2 only; source/global identity/deletion/endpoints and singleton/ingest candidate admission remain separate. No full-table maintenance, sourceACK, retained-storage inventory or billion admission.');save();(p/'live-statement.json').unlink();print(a['state'],a['costs'])
 except Exception as exc:a.update(state='Recovery stopped; inspect same handles',error=str(exc));save();raise
if __name__=='__main__':main()
