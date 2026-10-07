"""Private100k synthetic publisher over fully qualified8M/40M Delta graph."""
import json,time,hashlib
from pathlib import Path
from normalized_apply_sql_r276 import pin,append,current_merge,adjacency_merge
from mixed_preservation_sql_r280 import unchanged_groups,changed_digest,bootstrap_groups,added_digest
from normalized_input_sql_r264 import role_row
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;F='client_dev.ashlar_entropy_20261006_r86'

def main():
 paths={'base':'out/native/ashlar_scale_edges_r274/audited-summary.json','input':'out/native/ashlar_normalized_delta_stage_r275/audited-summary.json','predecessor':'out/native/ashlar_normalized_predecessor_r279/audited-summary.json','changed':'out/mixed-changed-oracle-r277.json','cdf':'out/cdf-image-oracle-r288.json','legacy':'out/native/ashlar_legacy_cdf_compare_r290/audited-summary.json','full_sweep':'out/native/ashlar_mixed_publish_r281/audited-summary.json'}
 source={k:json.loads((B/p).read_text()) for k,p in paths.items()};base=source['base']['tables'];inputs=source['input']['tables'];raw=inputs['source_record'];current=inputs['current_replacement']
 assert source['predecessor']['state']=='Full100k predecessor and prepared mutation-source checks pass'
 budget_path=B/'out/incremental-publisher-budget-r292.json';budget=json.loads(budget_path.read_text())
 for k,p in paths.items():assert hashlib.sha256((B/p).read_bytes()).hexdigest()==budget['source_sha256'][k]
 O=B/'out/native/ashlar_incremental_publish_r292';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=200,cancel_after=180)
 a={'state':'Preparing private incremental-custody publisher','source_sha256':budget['source_sha256'],'budget_sha256':hashlib.sha256(budget_path.read_bytes()).hexdigest(),'tables':{},'checks':{},'commit_events':[],'bounds':budget['bounds'],'clock':'Whole run includes clone/reference preparation. Ready-input processing starts after preflight and includes all mutations, complete change-image custody audit, exact descriptor validation/readback and telemetry. Separate completed input transfer357.215s, staging92.411s, predecessor44.096s are not hidden from end-to-end source preparation cost. No actual arrival/freshness claim.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics(reserve=0):
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
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
  for role,t in inputs.items():assert detail(t['table'],'input-'+role)['id']==t['id']
  for role,t in base.items():assert detail(t['table'],'base-'+role)['id']==t['id']
  a['tables']['object_current']=dict(base['object_current'])
  for role in ['edge_current','source_record','property_journal','adjacency_forward']:
   table=F+'.mixed_'+role+'_r292';c.sql('clone-'+role,f"CREATE TABLE {table} SHALLOW CLONE {pin(base[role]['table'],base[role]['version'])}");sid=c.records[-1]['statement_id'];d=detail(table,role);a['tables'][role]={'table':table,'id':d['id']};bind(role,sid)
   c.sql('disable-'+role,'ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');settings=c.sql('maintenance-'+role,'DESCRIBE TABLE EXTENDED '+table);assert [r[1] for r in settings if r[0]=='Predictive Optimization']==['DISABLE'];save();metrics()
  for role in ['edge_current','adjacency_forward']:
   table=a['tables'][role]['table'];c.sql('enable-cdf-'+role,"ALTER TABLE "+table+" SET TBLPROPERTIES ('delta.enableChangeDataFeed'='true')");sid=c.records[-1]['statement_id'];bind(role,sid);assert json.loads(detail(table,'cdf-'+role)['properties'])['delta.enableChangeDataFeed']=='true'
  table=F+'.mixed_tombstone_r292';cols=','.join(role_row('tombstone'));c.sql('create-tombstone',f"CREATE TABLE {table} USING DELTA AS SELECT {cols} FROM {pin(inputs['tombstone']['table'],inputs['tombstone']['version'])} WHERE false");sid=c.records[-1]['statement_id'];a['tables']['tombstone']={'table':table,'id':detail(table,'tombstone')['id']};bind('tombstone',sid);c.sql('disable-tombstone','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION')
  manifest=F+'.mixed_manifest_r292';c.sql('create-manifest',f'CREATE TABLE {manifest} (publication_id STRING NOT NULL,profile_version STRING NOT NULL,table_versions_json STRING NOT NULL,source_progress_json STRING NOT NULL,schema_revisions_json STRING NOT NULL,validation_report_json STRING NOT NULL,recorded_at TIMESTAMP NOT NULL) USING DELTA');a['manifest']={'table':manifest,'id':detail(manifest,'manifest')['id']};save()
  revisions=c.sql('schema-revisions',f"SELECT DISTINCT source_feed,schema_revision FROM {pin(base['source_record']['table'],base['source_record']['version'])} UNION SELECT DISTINCT source_feed,schema_revision FROM {pin(raw['table'],raw['version'])}");assert revisions==[['synthetic-scale-mixed','synthetic-mixed/1']];a['schema_revisions']=dict(revisions);metrics();a['preparation_s']=time.monotonic()-start;processing=time.monotonic();a['processing_start_epoch']=time.time();a['state']='Applying private100k mixed changes';save()
  for role in ['source_record','property_journal','tombstone']:
   t=inputs[role];write(role,'append-'+role,append(a['tables'][role]['table'],role,t['table'],t['version']))
  write('edge_current','merge-current',current_merge(a['tables']['edge_current']['table'],raw['table'],raw['version'],current['table'],current['version']))
  write('adjacency_forward','merge-forward',adjacency_merge(a['tables']['adjacency_forward']['table'],raw['table'],raw['version'],current['table'],current['version']))
  a['mutation_s']=time.monotonic()-processing;a['state']='Auditing every resulting role before descriptor';save()
  expected_totals={'object_current':8000000,'edge_current':39990000,'adjacency_forward':39990000,'source_record':48100000,'property_journal':192216667,'tombstone':10000}
  for role,t in a['tables'].items():assert c.sql('final-count-'+role,'SELECT count(*) FROM '+pin(t['table'],t['version']))==[[str(expected_totals[role])]]
  for role in ['edge_current','adjacency_forward']:
   t=a['tables'][role];v=t['version'];expected=source['cdf']['roles'][role];q=f"SELECT _change_type,count(*),min(_commit_version),max(_commit_version),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(expected['fields'])}))),256) FROM table_changes('{t['table']}',{v},{v}) GROUP BY _change_type ORDER BY _change_type";want=[[kind,str(x['rows']),str(v),str(v),x['digest']] for kind,x in sorted(expected['images'].items())];assert c.sql('cdf-'+role,q)==want;a['checks'][role]={'rows':190000,'groups':want,'proof':'Complete change images match independent pre/post/delete oracle and full-sweep reference'};save();metrics()
  for role in ['source_record','property_journal']:
   t=a['tables'][role];v=t['version'];expected=source['input']['checks'][role];q=f"SELECT _change_type,count(*),min(_commit_version),max(_commit_version),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(expected['fields'])}))),256) FROM table_changes('{t['table']}',{v},{v}) GROUP BY _change_type ORDER BY _change_type";assert c.sql('cdf-'+role,q)==[['insert',str(expected['rows']),str(v),str(v),expected['all_known_field_digest']]];a['checks'][role]={'rows':expected['rows'],'digest':expected['all_known_field_digest'],'proof':'Complete CDF interval contains only exact new input rows; inherited immutable clone baseline qualified'};save();metrics()
  t=a['tables']['tombstone'];expected=source['input']['checks']['tombstone'];digest('tombstone',added_digest(t['table'],t['version'],expected['fields'],True),10000,expected['all_known_field_digest'])
  # Refuse any unexplained commit through selected versions; no concurrent-writer fence claimed.
  for role in ['edge_current','source_record','property_journal','adjacency_forward','tombstone']:
   t=a['tables'][role];r=c.sql('custody-history-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 100');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,x)) for x in r];versions=[int(x['version']) for x in history];assert versions==list(range(t['version'],-1,-1));approved={e['statement_id'] for e in a['commit_events'] if e['role']==role};assert all(x.get('queryHistoryStatementId') in approved for x in history),role
  a['checks']['custody']='Qualified immutable baseline and complete change intervals; every clone/configuration/mutation version accounted by actual statement ID; no schema/protocol/unknown writer gaps'
  edge=pin(a['tables']['edge_current']['table'],a['tables']['edge_current']['version']);node=pin(a['tables']['object_current']['table'],a['tables']['object_current']['version']);tomb=pin(a['tables']['tombstone']['table'],a['tables']['tombstone']['version'])
  assert c.sql('unique-edges',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {edge}')==[['39990000','39990000']]
  assert c.sql('deleted-absent',f'SELECT count(*) FROM {edge} e INNER JOIN {tomb} t ON e.source_system=t.source_system AND e.rel_type_id=t.type_id AND e.id=t.id')==[['0']]
  assert c.sql('typed-endpoints',f'SELECT count(*),count_if(s.id IS NULL OR t.id IS NULL) FROM {edge} e LEFT JOIN {node} s ON e.source_system=s.source_system AND e.source_type=s.type_id AND e.source_id=s.id LEFT JOIN {node} t ON e.source_system=t.source_system AND e.target_type=t.type_id AND e.target_id=t.id')==[['39990000','0']]
  metrics();a['checks']['global_integrity']='Full counts, unique typed edge identities, all deletions absent, both typed endpoints closed';vector={t['table']:t['version'] for t in a['tables'].values()};values=['incremental-r292','ashlar-delta/0.3-synthetic-mixed',json.dumps(vector,sort_keys=True,separators=(',',':')),json.dumps({'inputs':inputs,'members':100000,'profile':'synthetic-mixed-change/1','real_source_ack':False},sort_keys=True),json.dumps(a['schema_revisions'],sort_keys=True),json.dumps(a['checks'],sort_keys=True)]
  def lit(x):return "decode(unhex('"+x.encode().hex()+"'),'UTF-8')"
  cols='publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json';c.sql('publish',f'INSERT INTO {manifest} ({cols},recorded_at) SELECT '+','.join(lit(x) for x in values)+',current_timestamp()');publish_sid=c.records[-1]['statement_id'];assert c.sql('descriptor-readback',f'SELECT {cols} FROM {manifest}')==[values];metrics();a['processing_s']=time.monotonic()-processing;a['publication_vector']=vector;a['publish_statement_id']=publish_sid;a['active_details']={role:detail(t['table'],'final-'+role) for role,t in a['tables'].items()};metrics();a['wall_s']=time.monotonic()-start;a['state']='Private100k incremental publication passes complete change-image custody checks';a['qualification']='One synthetic90k-update/10k-delete batch over8M/40M; immutable exact descriptor vector with actual revision map and canonical recorded_at. Full changed-image and added-row digests assume SHA256 collision resistance and preserve multiplicity; immutable qualified baseline custody replaces repeated inherited-row scans in this controlled fixture. Full r281 sweep remains independent qualification; this run still includes final global identity/deletion/endpoints. Not a general unknown-writer or concurrent-fencing proof. No real producer fencing/ACK, complete canonical CTAS constraints, retained-storage inventory, sustained10k/s or100k/s burst, caller/cold/concurrency or billion admission. Latency misses tune owner-selected UC Delta architecture, not architecture veto.';save();print(json.dumps({'state':a['state'],'mutation_s':a['mutation_s'],'processing_s':a['processing_s'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles and partial commits; no blind replay or descriptor advance',error=str(e));save();raise
if __name__=='__main__':main()
