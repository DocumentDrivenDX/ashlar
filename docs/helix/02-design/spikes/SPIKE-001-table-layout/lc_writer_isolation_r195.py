"""Sequential two-stage writer comparison on fresh owned E23 shallow clones."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import PropertyApply
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_lc_writer_isolation_r195';assert not O.exists(),'Inspect prior native write IDs'
old=json.loads((B/'out/native/ashlar_bucket_update_r176/audited-summary.json').read_text());fresh=json.loads((B/'out/native/ashlar_queue_resume_r192/audited-summary.json').read_text());E=old['source'];F=E.rsplit('.',1)[0]
arms=[('fresh',F+'.schedule_r189_1',fresh['owned_identity'][F+'.schedule_r189_1']['id'],'r189-b1','schedule-r189',9007199254741101),('prior',old['stage'],old['stage_id'],'r176-b1','bucket-r176',9007199254742501)]
c=BoundedReads(O,socket_timeout=60);deadline=time.monotonic()+600;s={'source':E,'source_version':23,'arms':{},'state':'In progress, unpublished'}
def save():(O/'summary.json').write_text(json.dumps(s,indent=2)+'\n')
def sql(label,q):
 assert time.monotonic()<deadline,'Controller admission deadline'
 return c.sql(label,q)
def detail(label,t):
 a=sql(label,'DESCRIBE DETAIL '+t);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,a[0]))
def budget(reserve=0):
 c.cursor.close();c.cursor=c.connection.cursor()
 for attempt in range(10):
  h={x['query_id']:x for x in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  time.sleep(2)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records)
 s['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save()
 assert s['costs']['read_bytes']+reserve<=25000000000 and s['costs']['write_remote_bytes']<=2000000000,'Stop admission: budget exceeded'
 return h
try:
 sql('timeout','SET STATEMENT_TIMEOUT=90');s['source_id']=detail('source-detail',E)['id'];assert s['source_id']==json.loads((B/'out/native/ashlar_lc_range_update_r187/audited-summary.json').read_text())['source_id']
 assert sql('absence',f"SHOW TABLES IN {F} LIKE 'lc_writer_r195_*'")==[]
 for name,U,uid,batch,epoch,xid in arms:
  budget(8000000000);T=F+'.lc_writer_r195_'+name;d=detail(name+'-stage-detail',U);assert d['id']==uid;assert sql(name+'-stage-version','DESCRIBE HISTORY '+U+' LIMIT 1')[0][0]=='0'
  sql(name+'-clone',f'CREATE TABLE {T} SHALLOW CLONE {E} VERSION AS OF 23');d0=detail(name+'-clone-detail',T);assert json.loads(d0['clusteringColumns'])==['lookup_hash'];assert sql(name+'-clone-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='0'
  x={'table':T,'id':d0['id'],'stage':U,'stage_id':uid,'stage_detail':d,'version':0};s['arms'][name]=x;save()
  q=PropertyApply(T,U,0,15,batch,'r139-b1',epoch,xid,eligibility_placement='on',input_ranges=6,scope_predecessor=True)
  assert sql(name+'-members',q.membership())==[['100000','100000']];assert sql(name+'-intended',q.intended())==[['0']];budget(8000000000)
  sql(name+'-apply',q.apply());x['apply_query_id']=c.records[-1]['statement_id'];assert sql(name+'-version','DESCRIBE HISTORY '+T+' LIMIT 1')[0][0]=='1';x['version']=1;save()
  a=sql(name+'-history','DESCRIBE HISTORY '+T);names=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,r)) for r in a];assert [int(r['version']) for r in history]==[1,0];assert history[0]['queryHistoryStatementId']==x['apply_query_id'];m=json.loads(history[0]['operationMetrics']);assert m['numTargetRowsUpdated']=='100000' and m['numTargetRowsCopied']=='0' and m['numTargetRowsInserted']=='0' and m['numTargetRowsDeleted']=='0';x['history']=history
  assert sql(name+'-output',q.output(1))==[['0']];budget(4000000000)
  def untouched(v):return f"SELECT b.source_system,b.rel_type_id,b.id,b._metadata.file_path,b._metadata.row_index FROM {T} VERSION AS OF {v} b LEFT ANTI JOIN (SELECT source_system,rel_type_id,id FROM {U} VERSION AS OF 0) k ON b.source_system=k.source_system AND b.rel_type_id=k.rel_type_id AND b.id=k.id"
  a,b=untouched(0),untouched(1)
  assert sql(name+'-untouched-count',f'SELECT count(*) FROM ({b})')==[['19900000']];assert sql(name+'-custody',f'SELECT count(*) FROM (({a} EXCEPT ALL {b}) UNION ALL ({b} EXCEPT ALL {a}))')==[['0']]
  assert sql(name+'-identities',f'SELECT count(*),count(DISTINCT id) FROM {T} VERSION AS OF 1')==[['20000000','20000000']]
  rows=sql(name+'-hot-ranges',f"SELECT _metadata.file_path,max(_metadata.file_size),count(*),min(lookup_hash),max(lookup_hash) FROM {T} VERSION AS OF 1 WHERE entity_version=16 AND apply_batch_id='{batch}' GROUP BY _metadata.file_path");assert sum(int(r[2]) for r in rows)==100000;x['hot_files']=rows;x['domain_spans']=[(int(r[4],16)-int(r[3],16))/2**256 for r in rows];h=budget();x['apply_metrics']={'caller_ms':next(r['wall_ms'] for r in c.records if r['statement_id']==x['apply_query_id']),'metrics':h[x['apply_query_id']]['metrics']};save();print(name,'files',len(rows),'spans',x['domain_spans'],flush=True)
 pins=sql('canonical-publication',f"SELECT table_versions_json FROM {F}.publication_manifest_r89 WHERE publication_id='r139-b1'");assert json.loads(pins[0][0])[E]==23;budget();s['state']='Both sequential stage writer arms exact, custody and full20M IDs passed';s['qualification']='Fresh clones of same immutable E23, same session/6-range hint/eligibility, sequential writes with no raw/journal interference. Sources have same synthetic hot identities but different values/epochs/delivery/timestamps and physical file layouts. Not causal isolation of stage file count, not task-plan proof or latency/publisher/billion admission. Exact changed20 fields and19.9M immutable custody in closed zero-copy lineage. Canonical publication unchanged.';save();print(json.dumps(s['costs']))
except Exception as e:s['state']='Stopped; inspect same native IDs and owned commits';s['error']=str(e);save();raise
finally:c.close()
