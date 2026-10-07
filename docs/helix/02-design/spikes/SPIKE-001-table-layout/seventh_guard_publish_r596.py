"""Future seventh private publisher with concurrent closing; never replay.
Native input/budget/eligibility receipts are mandatory; no execution on import.
"""
import copy,hashlib,json,time
from pathlib import Path
from normalized_apply_sql_r276 import pin,append,current_merge,adjacency_merge,predecessor_check,mutation_source
from normalized_input_sql_r264 import role_row
from inline_guard_sql_r421 import guard_merge
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from bounded_reads_r145 import BoundedReads
from publisher_custody_r462 import collect
from publisher_commit_closing_r553 import close_after
from publisher_validation_r507 import validate
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent
F='client_dev.ashlar_entropy_20261006_r86'

def main():
 paths={'untouched':'out/native/ashlar_inline_guard_untouched_r438/audited-summary.json','guard':'out/native/ashlar_inline_guard_finish_r425/audited-summary.json','vector':'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json','inputs':'out/native/ashlar_seventh_delta_stage_r593/audited-summary.json','cdf':'out/seventh-cdf-image-oracle-r588.json','tomb':'out/seventh-canonical-tomb-r588.json','budget':'out/seventh-publisher-budget-r594.json','eligibility':'out/native/ashlar_seventh_publisher_preflight_r595/summary.json'}
 source={k:json.loads((B/p).read_text()) for k,p in paths.items()}
 assert source['vector']['state']=='Integrated private sixth100k guarded publication passes full change custody'
 assert source['untouched']['state']=='Guard preserves exact unmatched target and old pin;5updates/1delete pass'
 assert source['guard']['state']=='Inline full20field guards,26atomic refusals, clean CDF and all stopped costs independently audited'
 assert source['inputs']['state']=='Four complete normalized input roles match independent source digests and are Delta-version pinned'
 base=copy.deepcopy(source['vector']['tables']);inputs=source['inputs']['tables'];raw=inputs['source_record'];current=inputs['current_replacement'];bounds=source['budget']['stages']['integrated_publisher']
 assert base==source['budget']['base_vector']
 O=B/'out/native/ashlar_seventh_guard_publish_r596';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=180,cancel_after=bounds['statement_cancel_after_s'])
 a={'state':'Integrated ready-input preflight','processing_start_epoch':time.time(),'sources':paths,'source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'base':base,'tables':copy.deepcopy(base),'inputs':inputs,'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['seventh_guard_publish_r596.py','inline_guard_sql_r421.py','normalized_apply_sql_r276.py','publisher_custody_r462.py','bounded_reads_r145.py','publisher_validation_r507.py','publisher_content_r503.py','publisher_commit_closing_r553.py','publisher_closing_r548.py']},'checks':{},'commit_events':[],'bounds':bounds,'clock':'Ready-input start precedes native preflight, inline-guard mutation, validation, custody, manifest setup/publication/readback and final telemetry. Local generation/transfer/native staging/eligibility are separately charged preparation, not source arrival evidence.'}
 workers=[]
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def sql(label,q):
  assert time.monotonic()-start<bounds['wall_s'];return c.sql(label,q)
 def objects(label,q):
  r=sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,x)) for x in r]
 def metrics():
  for worker in workers:worker.cursor.close();worker.cursor=worker.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records+[r for worker in workers for r in worker.records],O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
  assert all(v<=bounds[k] for k,v in a['costs'].items());assert time.monotonic()-start<bounds['wall_s'];save();return h
 def write(role,label,q):
  metrics();sql(label,q);sid=c.records[-1]['statement_id'];t=a['tables'][role];history=objects('bind-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 20');new=[x for x in history if int(x['version'])>base[role]['version']]
  assert len(new)==1 and new[0]['queryHistoryStatementId']==sid and int(new[0]['version'])==base[role]['version']+1
  t['version']=int(new[0]['version']);a['commit_events'].append({'role':role,'statement_id':sid,'history':new[0]});save()
 def custody(label):
  result={}
  for role,t in a['tables'].items():
   if role=='object_current':continue
   h=objects(label+'-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 100');interval=[x for x in h if int(x['version'])>base[role]['version']];approved=[e for e in a['commit_events'] if e['role']==role]
   assert int(h[0]['version'])==t['version'] and len(interval)==len(approved)==1
   assert interval[0]['queryHistoryStatementId']==approved[0]['statement_id'] and int(interval[0]['version'])==t['version']
   result[role]=interval
  a['checks'][label]=result;save()
 def closing_profiles(label):
  accepted=close_after(workers,expected,base,a['tables'],a['commit_events'],label)
  a.setdefault('closing_metadata',{})[label]=accepted
  a['checks'][label]={row['key'].removeprefix(label+'-'):{'id':row['detail']['id'],'head':row['head'],'selected_pin':row['table'],'physical_profile_checked':True,'schema_checked':True} for row in accepted}
  save()
 save()
 try:
  metadata_start=time.monotonic();expected=[]
  for key,e in source['eligibility']['tables'].items():expected.append({'key':key,'table':e['pin']['table'],'detail':e['detail'],'head':e['head'],'schema':e['schema']})
  for j in range(4):
   worker=BoundedReads(O/('metadata-worker-'+str(j)),socket_timeout=30);workers.append(worker);assert worker.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  setup=time.monotonic()-metadata_start;mt=time.monotonic();accepted=collect(workers,expected)
  known={e['key']:e for e in expected}
  for row in accepted:assert row['head']['queryHistoryStatementId']==known[row['key']]['head']['queryHistoryStatementId']
  a['metadata']={'expected':expected,'accepted':accepted,'setup_s':setup,'collection_s':time.monotonic()-mt}
  for worker in workers:worker.cursor.close();worker.cursor=worker.connection.cursor()
  save()
  assert sql('pinned-node-count','SELECT count(*) FROM '+pin(base['object_current']['table'],6))==[['8000000']]
  for role,t in inputs.items():
   e=source['inputs']['checks'][role];fields=e['fields'];q=f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list(sha2(input_json,256)))),256),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(fields)}))),256) FROM {pin(t['table'],t['version'])}"
   assert sql('input-digest-'+role,q)==[[str(e['rows']),e['original_input_digest'],e['all_known_field_digest']]]
  q=mutation_source(raw['table'],raw['version'],current['table'],current['version'])
  assert sql('mutation-partition',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_delete),count_if(NOT is_delete),count_if(entity_version IS NULL OR entity_version<>1 OR apply_batch_id<>'mixed-bootstrap'),count_if(NOT is_delete AND (after_entity_version IS NULL OR after_entity_version<>2 OR NOT (source_system <=> after_source_system) OR NOT (rel_type_id <=> after_rel_type_id) OR NOT (id <=> after_id))) FROM ({q})")==[['100000','100000','10000','90000','0','0']]
  a['checks']['predecessors']='Complete20field predecessor guard is executed atomically inside current MERGE, qualified by r426 refusal/CDF evidence; prepared100k unique mutation partition passes before writes';metrics();a['preflight_s']=time.monotonic()-start;a['state']='Applying seventh guarded private batch';save();mutation=time.monotonic()
  # Fail if mutable heads changed since expensive predecessor work; no general worker fence claimed.
  for role,t in base.items():
   if role!='object_current':assert int(objects('prewrite-head-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0]['version'])==t['version']
  for role in ['source_record','property_journal','tombstone']:
   t=inputs[role];write(role,'append-'+role,append(a['tables'][role]['table'],role,t['table'],t['version']))
  write('edge_current','merge-current',guard_merge(a['tables']['edge_current']['table'],raw['table'],raw['version'],current['table'],current['version']))
  write('adjacency_forward','merge-forward',adjacency_merge(a['tables']['adjacency_forward']['table'],raw['table'],raw['version'],current['table'],current['version']))
  a['mutation_s']=time.monotonic()-mutation;a['state']='Validating complete mutations and inherited custody';save()
  source['revision_rows']=[['synthetic-scale-mixed','synthetic-mixed/1']]
  a['cohorts']=[]
  def checkpoint(offset,values):
   a['cohorts'].append({'offset':offset,'labels':list(values),'statement_ids':[v['statement_id'] for v in values.values()]});metrics()
  validation_start=time.monotonic();a['accepted_validation']=validate(workers,a['tables'],inputs,source,checkpoint);a['validation_with_telemetry_s']=time.monotonic()-validation_start
  a['checks']['post_commit']={label:v['result'] for label,v in a['accepted_validation'].items()}
  a['checks']['global_integrity']='Complete15 concurrent post-commit checks: exact E/A/raw/journal CDF, canonical tombstone, all role counts, typed edge uniqueness/all70k deletions absent/typed closure/schema revisions';custody('custody-before-publication');closing_profiles('profiles-before-publication');metrics()
  manifest=F+'.seventh_guard_manifest_r596';sql('create-manifest',f'CREATE TABLE {manifest} (publication_id STRING NOT NULL,profile_version STRING NOT NULL,table_versions_json STRING NOT NULL,source_progress_json STRING NOT NULL,schema_revisions_json STRING NOT NULL,validation_report_json STRING NOT NULL,recorded_at TIMESTAMP NOT NULL) USING DELTA');a['manifest']={'table':manifest,'create_statement_id':c.records[-1]['statement_id'],'detail':objects('manifest-detail','DESCRIBE DETAIL '+manifest)[0]}
  vector={t['table']:t['version'] for t in a['tables'].values()};values=['incremental-seventh-guard-r596','ashlar-delta/0.3-synthetic-mixed',json.dumps(vector,sort_keys=True,separators=(',',':')),json.dumps({'inputs':inputs,'members':100000,'batch_id':'mixed-change/7','profile':'synthetic-mixed-change/1','real_source_ack':False},sort_keys=True),json.dumps({'synthetic-scale-mixed':'synthetic-mixed/1'},sort_keys=True),json.dumps(a['checks'],sort_keys=True)]
  cols='publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json';lit=lambda x:"decode(unhex('"+x.encode().hex()+"'),'UTF-8')";metrics();sql('publish',f'INSERT INTO {manifest} ({cols},recorded_at) SELECT '+','.join(lit(x) for x in values)+',current_timestamp()');a['publish_statement_id']=c.records[-1]['statement_id'];a['descriptor_values']=values;assert sql('descriptor-readback',f'SELECT {cols} FROM {manifest}')==[values]
  a['manifest']['history']=objects('manifest-history','DESCRIBE HISTORY '+manifest+' LIMIT 10');mh=a['manifest']['history'];assert [int(x['version']) for x in mh]==[1,0] and [x['queryHistoryStatementId'] for x in mh]==[a['publish_statement_id'],a['manifest']['create_statement_id']]
  custody('custody-after-readback');closing_profiles('profiles-after-readback');a['active_details']={role:objects('final-detail-'+role,'DESCRIBE DETAIL '+t['table'])[0] for role,t in a['tables'].items()};metrics();a['processing_s']=time.monotonic()-start;a['publication_vector']=vector;a['state']='Integrated private seventh100k guarded publication passes full change custody';a['qualification']='Complete20field E/8field A190kCDF images, complete raw/journal new intervals and direct canonical10field tombstones match independent digests. Qualified immutable inherited vector plus closed submitted-commit intervals; SHA256 assumption. Entire ready-input processing clock includes preflight/inline atomic predecessor guard/telemetry. Synthetic input availability is not actual arrival or sustained10k/s/burst; no production worker fence, ACK, caller/cold/concurrency, retention or1B/5B admission.';save()
  (O/'live-statement.json').rename(O/'completed-last-statement.json')
  for worker in workers:(worker.out/'inflight-request.json').rename(worker.out/'completed-last-request.json');worker.close()
  print(json.dumps({k:a[k] for k in ['state','preflight_s','mutation_s','validation_with_telemetry_s','processing_s','costs','metadata'] if k!='metadata'},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same submitted handles and partial commits; no blind replay',error=str(e),elapsed_s=time.monotonic()-start);save();raise
if __name__=='__main__':main()
