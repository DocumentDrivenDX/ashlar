"""Resume r332 validation only after authoritative tombstone-CDF failure; no mutation replay."""
import json,time,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from normalized_apply_sql_r276 import pin
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent
F='client_dev.ashlar_entropy_20261006_r86'
def main():
 O=B/'out/native/ashlar_incremental_publish_r332';a=json.loads((O/'summary.json').read_text());assert a['state']=='Stopped; inspect same handles and partial commits; no blind replay or descriptor advance'
 records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];bad=[r for r in records if r['response']['status']['state']!='SUCCEEDED'];assert len(bad)==1 and bad[0]['label']=='digest-tombstone' and bad[0]['response']['status']['state']=='FAILED';assert not any(r['label']=='publish' for r in records)
 c=Client(O,observation_timeout=200,cancel_after=180);c.records=records
 final=c.w.api_client.do('GET','/api/2.0/sql/statements/'+bad[0]['statement_id']);assert final['status']['state']=='FAILED'
 history=c.history();failed=next(q for q in history if q['query_id']==bad[0]['statement_id']);assert failed['status']=='FAILED' and failed['is_final'];(O/'failed-tombstone-query.json').write_text(json.dumps({'client':final,'native':failed},indent=2)+'\n')
 c.records=[r for r in records if r['response']['status']['state']=='SUCCEEDED']
 budget=json.loads((B/'out/incremental-publisher-budget-r332.json').read_text());inputs=json.loads((B/'out/native/ashlar_second_delta_stage_r324/audited-summary.json').read_text())['tables'];processing=time.monotonic()-(time.time()-a['processing_start_epoch']);start=processing-a['preparation_s'];manifest=a['manifest']['table']
 source={'canonical_tomb':json.loads((B/'out/second-canonical-tomb-r332.json').read_text())}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics(reserve=0):
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:failed['metrics'].get(k,0)+sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
  for k,v in a['costs'].items():assert v+reserve<=a['bounds'][k],k
  assert time.monotonic()-start<a['bounds']['wall_s'];save();return h
 def detail(table,label):
  r=c.sql('detail-'+label,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,r[0]))
 def bind(role,sid):
  t=a['tables'][role];r=c.sql('history-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];found=[dict(zip(names,row)) for row in r if dict(zip(names,row)).get('queryHistoryStatementId')==sid];assert len(found)==1
  t['version']=int(found[0]['version']);a['commit_events'].append({'role':role,'statement_id':sid,'version':t['version'],'history':found[0]});save()
 def write(role,label,q):
  metrics(100000000);c.sql(label,q);sid=c.records[-1]['statement_id'];bind(role,sid)
 def digest(role,q,rows,expected):
  assert c.sql('digest-'+role,q)==[[str(rows),expected]];a['checks'][role]={'rows':rows,'all_fields_digest':expected};save();metrics()
 save()
 try:
  assert c.sql('empty-descriptor-before-recovery','SELECT count(*) FROM '+manifest)==[['0']]
  t=a['tables']['tombstone'];v=t['version'];expected=source['canonical_tomb'];keys=inputs['tombstone'];rel=pin(t['table'],v);selection=pin(keys['table'],keys['version']);fields=['t.'+f for f in expected['fields']];q=f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(fields)}))),256) FROM {rel} t INNER JOIN {selection} s ON t.source_feed=s.source_feed AND t.source_epoch=s.source_epoch AND t.source_delivery_id=s.source_delivery_id";digest('tombstone',q,10000,expected['digest'])
  a['recovery']='One authoritative FAILED tombstone CDF read, no mutation replay. Direct pinned full canonical-field comparison by exact delivery keys; all failed-query cost and repair delay charged in processing clock.';a.pop('error',None)
  # Refuse any unexplained commit through selected versions; no concurrent-writer fence claimed.
  for role in ['edge_current','source_record','property_journal','adjacency_forward','tombstone']:
   t=a['tables'][role];r=c.sql('custody-history-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 100');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,x)) for x in r];versions=[int(x['version']) for x in history];assert versions==list(range(t['version'],-1,-1));approved={e['statement_id'] for e in a['commit_events'] if e['role']==role};assert all(x.get('queryHistoryStatementId') in approved for x in history),role
  a['checks']['custody']='Qualified immutable baseline and complete change intervals; every clone/configuration/mutation version accounted by actual statement ID; no schema/protocol/unknown writer gaps'
  edge=pin(a['tables']['edge_current']['table'],a['tables']['edge_current']['version']);node=pin(a['tables']['object_current']['table'],a['tables']['object_current']['version']);tomb=pin(a['tables']['tombstone']['table'],a['tables']['tombstone']['version'])
  assert c.sql('unique-edges',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {edge}')==[['39980000','39980000']]
  assert c.sql('deleted-absent',f'SELECT count(*) FROM {edge} e INNER JOIN {tomb} t ON e.source_system=t.source_system AND e.rel_type_id=t.type_id AND e.id=t.id')==[['0']]
  assert c.sql('typed-endpoints',f'SELECT count(*),count_if(s.id IS NULL OR t.id IS NULL) FROM {edge} e LEFT JOIN {node} s ON e.source_system=s.source_system AND e.source_type=s.type_id AND e.source_id=s.id LEFT JOIN {node} t ON e.source_system=t.source_system AND e.target_type=t.type_id AND e.target_id=t.id')==[['39980000','0']]
  metrics();a['checks']['global_integrity']='Full counts, unique typed edge identities, all deletions absent, both typed endpoints closed';vector={t['table']:t['version'] for t in a['tables'].values()};values=['incremental-r332','ashlar-delta/0.3-synthetic-mixed',json.dumps(vector,sort_keys=True,separators=(',',':')),json.dumps({'inputs':inputs,'members':100000,'profile':'synthetic-mixed-change/1','real_source_ack':False},sort_keys=True),json.dumps(a['schema_revisions'],sort_keys=True),json.dumps(a['checks'],sort_keys=True)]
  def lit(x):return "decode(unhex('"+x.encode().hex()+"'),'UTF-8')"
  cols='publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json';c.sql('publish',f'INSERT INTO {manifest} ({cols},recorded_at) SELECT '+','.join(lit(x) for x in values)+',current_timestamp()');publish_sid=c.records[-1]['statement_id'];assert c.sql('descriptor-readback',f'SELECT {cols} FROM {manifest}')==[values];metrics();a['processing_s']=time.monotonic()-processing;a['publication_vector']=vector;a['publish_statement_id']=publish_sid;a['active_details']={role:detail(t['table'],'final-'+role) for role,t in a['tables'].items()};metrics();a['processing_s']=time.monotonic()-processing;a['wall_s']=time.monotonic()-start;a['state']='Private second100k range32 publication passes complete change-image custody checks';a['qualification']='Second distinct synthetic90k-update/10k-delete batch over8M/39.99M, original baseline R/J/A plus explicitly replayed first input as charged preparation; range32 E parent already contains first batch. Canonical tombstone10fields appended; extra batch metadata retained in immutable input_json and permanent raw source delivery linkage.  immutable exact descriptor vector with actual revision map and canonical recorded_at. Full changed-image and added-row digests assume SHA256 collision resistance and preserve multiplicity; immutable qualified baseline custody replaces repeated inherited-row scans in this controlled fixture. Full r281 sweep remains independent qualification; this run still includes final global identity/deletion/endpoints. Not a general unknown-writer or concurrent-fencing proof. No real producer fencing/ACK, complete canonical CTAS constraints, retained-storage inventory, sustained10k/s or100k/s burst, caller/cold/concurrency or billion admission. Latency misses tune owner-selected UC Delta architecture, not architecture veto.';save();print(json.dumps({'state':a['state'],'mutation_s':a['mutation_s'],'processing_s':a['processing_s'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped during validation recovery; inspect same handles',error=str(e));save();raise
if __name__=='__main__':main()
