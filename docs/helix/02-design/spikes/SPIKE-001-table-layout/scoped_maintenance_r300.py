"""Owned scoped liquid maintenance with whole-carrier before/after digests."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent

def main():
 pre=B/'out/native/ashlar_scoped_maintenance_preflight_r299/summary.json';s=json.loads(pre.read_text());assert s['state']=='Existing candidate passes scoped maintenance preflight' and s['overlapping_bytes']<=2500000000
 bound=json.loads((B/'out/scoped-maintenance-selection-r298.json').read_text());p=B/'out/native/ashlar_scoped_maintenance_r300';assert not p.exists();p.mkdir();c=Client(p,observation_timeout=200,cancel_after=90);t=bound['candidate_existing_table'];start=time.monotonic();a={'state':'Collecting complete before-carrier digests','source_sha256':hashlib.sha256(pre.read_bytes()).hexdigest(),'bounds':bound['bounds'],'table':t,'before_version':2,'checks':{}}
 def save():(p/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics(reserve=0):
  for n in range(15):
   try:h=collect_history(c.w,c.records,p/'shared-history.json');break
   except HistoryPending:
    if n==14:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=a['bounds']['whole_read_bytes'];assert a['costs']['write_remote_bytes']<=5000000000 and a['costs']['spill_to_disk_bytes']<=5000000000;assert time.monotonic()-start<1200;return h
 def history(label):
  rows=c.sql(label,'DESCRIBE HISTORY '+t+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(names,r)) for r in rows]
 def digest(phase,version):
  fields=json.loads((B/'out/cdf-image-oracle-r288.json').read_text())['roles']['edge_current']['fields'];hashsql=row_hash_sql(fields);allrows=[]
  for first in range(0,400,100):
   metrics(40000000000);lo=8000001+first*100000;hi=lo+10000000;q=f"SELECT CAST(floor((id-8000001)/100000) AS BIGINT) group_id,count(*),sha2(concat_ws('',sort_array(collect_list({hashsql}))),256) FROM {t} VERSION AS OF {version} WHERE id>={lo} AND id<{hi} GROUP BY group_id ORDER BY group_id";rows=c.sql(phase+'-'+str(first),q);assert len(rows)==100 and [int(r[0]) for r in rows]==list(range(first,first+100));allrows.extend(rows);save()
  assert sum(int(r[1]) for r in allrows)==39990000;return allrows
 try:
  save();detail=c.sql('detail-before','DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,detail[0]));assert d['id']==bound['expected_candidate_uuid'];a['uuid']=d['id'];h=history('history-before');assert int(h[0]['version'])==2 and not any(x['operation']=='OPTIMIZE' for x in h);a['before_history']=h
  a['checks']['before']=digest('before',2);a['state']='Submitting bounded range-scoped FULL maintenance';save();metrics(15000000000)
  c.sql('optimize',f"OPTIMIZE {t} FULL WHERE {bound['predicate']}");sid=c.records[-1]['statement_id'];a['optimize_statement_id']=sid;h=history('history-after');commit=[x for x in h if x.get('queryHistoryStatementId')==sid];assert len(commit)==1 and commit[0]['operation']=='OPTIMIZE' and int(commit[0]['version'])==3 and int(h[0]['version'])==3;a['commit']=commit[0];a['after_version']=3;a['state']='Auditing complete after-carrier digests';save();metrics()
  a['checks']['after']=digest('after',3);assert a['checks']['after']==a['checks']['before'];assert c.sql('final-count',f'SELECT count(*) FROM {t} VERSION AS OF 3')==[['39990000']]
  rows=c.sql('detail-after','DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];a['after_detail']=dict(zip(names,rows[0]));assert a['after_detail']['id']==a['uuid'];metrics();a['state']='Scoped maintenance commits with all39.99M complete carriers preserved';a['wall_s']=time.monotonic()-start;a['qualification']='All20-field before/after multiset SHA256 digests in400bounded100k-identity groups cover39.99M rows; collision-resistance assumption. One actual OPTIMIZE commit bound by SID/version, UUID stable. This proves maintenance equivalence to preflight version2, not independently all original Truss input correctness. Scoped rewrite may move outside-range rows; singleton/ingest comparison and full uniqueness/deletion/typed endpoints remain required before candidate admission. No production descriptor, ACK, retention cleanup, compute change, sustained service or billion admission.';save();(p/'live-statement.json').unlink();print(a['state'],a['costs'])
 except Exception as exc:a.update(state='Stopped; inspect original handles without replay',error=str(exc),wall_s=time.monotonic()-start);save();raise
if __name__=='__main__':main()
