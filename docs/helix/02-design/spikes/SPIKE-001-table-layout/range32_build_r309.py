"""Full39.99M range-partition candidate; staged native writes and full-carrier checks."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent

def main():
 budget_path=B/'out/range32-comparison-budget-r308.json';budget=json.loads(budget_path.read_text());source_path=B/'out/native/ashlar_scoped_maintenance_r300/audited-summary.json';source=json.loads(source_path.read_text());assert hashlib.sha256(source_path.read_bytes()).hexdigest()==budget['source_sha256'];p=B/'out/native/ashlar_range32_build_r309';assert not p.exists();p.mkdir();start=time.monotonic();c=Client(p,observation_timeout=200,cancel_after=180);target=budget['candidate_table'];parent=budget['parent_table'];fields=json.loads((B/'out/cdf-image-oracle-r288.json').read_text())['roles']['edge_current']['fields'];a={'state':'Preflighting full range-partition candidate','source_sha256':budget['source_sha256'],'budget_sha256':hashlib.sha256(budget_path.read_bytes()).hexdigest(),'table':target,'parent':{'table':parent,'version':4,'id':source['uuid']},'bounds':budget['bounds'],'commits':[],'checks':{}}
 def save():(p/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics(read_reserve=0,write_reserve=0):
  for n in range(15):
   try:h=collect_history(c.w,c.records,p/'shared-history.json');break
   except HistoryPending:
    if n==14:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+read_reserve<=400000000000 and a['costs']['write_remote_bytes']+write_reserve<=80000000000 and a['costs']['spill_to_disk_bytes']<=10000000000;assert time.monotonic()-start<2400;return h
 def detail(table,label):
  rows=c.sql(label,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,rows[0]))
 def history(label):
  rows=c.sql(label,'DESCRIBE HISTORY '+target+' LIMIT 100');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(names,r)) for r in rows]
 def bind(sid,label):
  rows=history('history-'+label);new=[r for r in rows if r.get('queryHistoryStatementId')==sid];assert new and all(int(r['version']) not in {int(e['version']) for e in a['commits']} for r in new);a['commits'].extend(new);assert {int(r['version']) for r in rows}=={int(r['version']) for r in a['commits']}==set(range(int(rows[0]['version'])+1));a['version']=int(rows[0]['version']);save()
 def digests(phase,version):
  actual=[];hs=row_hash_sql(fields)
  for first in range(0,400,100):
   metrics(40000000000);lo=8000001+first*100000;hi=lo+10000000;rows=c.sql(phase+'-'+str(first),f"SELECT CAST(floor((id-8000001)/100000) AS BIGINT) group_id,count(*),sha2(concat_ws('',sort_array(collect_list({hs}))),256) FROM {target} VERSION AS OF {version} WHERE id>={lo} AND id<{hi} GROUP BY group_id ORDER BY group_id");assert len(rows)==100;actual.extend(rows)
  assert actual==source['checks']['after'];a['checks'][phase]=actual;save()
 try:
  save();d=detail(parent,'parent-detail');assert d['id']==source['uuid'];schema=c.sql('parent-schema','DESCRIBE TABLE '+parent);assert [r[0] for r in schema[:20]]==fields
  q=f"CREATE TABLE {target} USING DELTA PARTITIONED BY (lookup_partition) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd','delta.enableDeletionVectors'='true') AS SELECT {','.join(fields)},{budget['partition_expression']} AS lookup_partition FROM {parent} VERSION AS OF 4";metrics(40000000000,40000000000);a['state']='Building full39.99M partitioned current candidate';save();c.sql('create',q);bind(c.records[-1]['statement_id'],'create');c.sql('disable-maintenance','ALTER TABLE '+target+' DISABLE PREDICTIVE OPTIMIZATION');settings=c.sql('maintenance-setting','DESCRIBE TABLE EXTENDED '+target);assert [r[1] for r in settings if r[0]=='Predictive Optimization']==['DISABLE'];a['initial_detail']=detail(target,'initial-detail');a['uuid']=a['initial_detail']['id'];assert json.loads(a['initial_detail']['partitionColumns'])==['lookup_partition'] and json.loads(a['initial_detail']['clusteringColumns'])==[];a['state']='Auditing full candidate copy before maintenance';save();digests('copied',a['version'])
  distribution=c.sql('partition-coverage',f"SELECT lookup_partition,count(*),count_if(lookup_partition<>{budget['partition_expression']}) FROM {target} VERSION AS OF {a['version']} GROUP BY lookup_partition ORDER BY lookup_partition");assert [int(r[0]) for r in distribution]==list(range(32)) and sum(int(r[1]) for r in distribution)==39990000 and all(int(r[2])==0 for r in distribution);a['checks']['partition_coverage']=distribution;save();metrics()
  for bucket in range(32):
   metrics(8000000000,2500000000);a['state']='Maintaining partition '+str(bucket)+' of32';save();c.sql('optimize-'+str(bucket),f'OPTIMIZE {target} WHERE lookup_partition={bucket} ZORDER BY (lookup_hash)');bind(c.records[-1]['statement_id'],'optimize-'+str(bucket));metrics()
  a['state']='Auditing whole candidate after all32partition maintenance stages';save();digests('maintained',a['version']);assert c.sql('final-count',f"SELECT count(*) FROM {target} VERSION AS OF {a['version']}")==[['39990000']];a['final_detail']=detail(target,'final-detail');assert a['final_detail']['id']==a['uuid'];metrics();a.update(state='Full39.99M range32 current candidate preserves all20carrier fields',wall_s=time.monotonic()-start,qualification='Full independent CTAS and all32partition ZORDER stages. Every20field multiset digest matches immutable qualified parent across400groups and39.99Mrows underSHA256collision resistance. Physical extra lookup_partition exact-derived and32partitions covered. Actual all commit versions bound bySID; unknownwriter refused. No independent real-source/ACK/fencing proof, direct graph feature admission, mixed ingest publication/cold/concurrency or billion admission. Singleton benchmark remains pending.');save();(p/'live-statement.json').unlink();print(a['state'],a['costs'])
 except Exception as exc:a.update(state='Stopped; inspect original native handles without replay',error=str(exc),wall_s=time.monotonic()-start);save();raise
if __name__=='__main__':main()
