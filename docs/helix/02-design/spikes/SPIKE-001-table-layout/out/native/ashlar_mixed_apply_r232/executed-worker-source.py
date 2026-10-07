"""Owned complete-bootstrap mixed publication, exact independent final oracle."""
import json,time,hashlib,collections
from pathlib import Path
from mixed_changes_r228 import Changes,verify
from mixed_change_queries_r230 import extract,mutation_source,sample,fields_sql,INTS,BOOLS,TIMES,row_hash,row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_apply_r232';assert not O.exists();c=Client(O,observation_timeout=200,cancel_after=180);stage=json.loads((B/'out/native/ashlar_mixed_change_stage_r229/summary.json').read_text());base=stage['source_pins'];T=stage['target'];F='client_dev.ashlar_entropy_20261006_r86';a={'state':'running','tables':{},'versions':{},'checks':{},'stage':{'table':T,'id':stage['table_id'],'version':8},'clock':'Processing starts after clones/input preflight, includes all writes/checks/final metrics and descriptor readback; excludes earlier source staging and modeled arrivals'}
changes=Changes();selected={};oracle=collections.defaultdict(list);columns={}
for i in range(changes.count):
 x=changes.change(i);assert verify(x);selected[x['ordinal']]=x
 def add(role,row):
  columns.setdefault(role,list(row));oracle[role].append(row_hash(row,columns[role]))
 add('source_record',x['raw'])
 for event in x['events']:add('property_journal',event)
 if x['tombstone']:add('tombstone',x['tombstone'])
for kind,count in [('node',4096),('edge',20480)]:
 for i in range(count):
  x=selected.get(i) if kind=='edge' else None
  for role,row in changes.w.roles(kind,i):
   if role in ['edge_current','adjacency_forward'] and x:
    if x['after'] is None:continue
    row=x['after'] if role=='edge_current' else {key:x['after'][key] for key in row}
   add(role,row)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=10000000000 and a['costs']['write_remote_bytes']+reserve<=1000000000
 return h
def detail(label,table):
 rows=c.sql(label,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,rows[0]))
def commit(role,label,q):
 c.sql(label,q);sid=c.records[-1]['statement_id'];table=a['tables'][role]['table'];rows=c.sql(label+'-history','DESCRIBE HISTORY '+table+' LIMIT 20');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];found=[dict(zip(names,r)) for r in rows if dict(zip(names,r)).get('queryHistoryStatementId')==sid];assert len(found)==1,(label,sid);a['versions'][role]=int(found[0]['version']);a.setdefault('commits',{})[role]=found[0];save()
def digest(role,query):
 actual=c.sql('digest-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(columns[role])}))),256) FROM ({query})");expected=hashlib.sha256(''.join(sorted(oracle[role])).encode()).hexdigest();assert actual==[[str(len(oracle[role])),expected]],(role,actual);a['checks'][role]={'rows':len(oracle[role]),'all_field_multiset_digest':expected};save()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180');assert detail('stage-detail',T)['id']==stage['table_id']
 assert c.sql('stage-count',f'SELECT count(*),count(DISTINCT change_index) FROM {T} VERSION AS OF 8')==[['2048','2048']]
 for role in ['object_current','edge_current','source_record','property_journal','adjacency_forward']:
  assert detail('source-'+role,base[role]['table'])['id']==base[role]['id']
  if role=='object_current':a['tables'][role]=base[role];a['versions'][role]=0;continue
  table=F+'.mixed_'+role+'_r232';assert c.sql('absent-'+role,f"SHOW TABLES IN {F} LIKE 'mixed_{role}_r232'")==[];c.sql('clone-'+role,f"CREATE TABLE {table} SHALLOW CLONE {base[role]['table']} VERSION AS OF 0");d=detail('detail-'+role,table);a['tables'][role]={'table':table,'id':d['id']};a['versions'][role]=0;save()
 table=F+'.mixed_tombstone_r232';assert c.sql('absent-tombstone',f"SHOW TABLES IN {F} LIKE 'mixed_tombstone_r232'")==[];c.sql('create-tombstone',f'CREATE TABLE {table} USING DELTA AS SELECT * FROM ({extract("tombstone",T)}) WHERE false');a['tables']['tombstone']={'table':table,'id':detail('detail-tombstone',table)['id']};a['versions']['tombstone']=0
 manifest=F+'.mixed_manifest_r232';assert c.sql('absent-manifest',f"SHOW TABLES IN {F} LIKE 'mixed_manifest_r232'")==[];c.sql('create-manifest',f'CREATE TABLE {manifest} (publication_id STRING,profile_version STRING,table_versions_json STRING,source_progress_json STRING,schema_revisions_json STRING,validation_report_json STRING,published_at TIMESTAMP) USING DELTA')
 # Fresh clone predecessor is checked across every selected source field before mutation.
 source=mutation_source(T);edge=a['tables']['edge_current']['table'];assert c.sql('eligibility',f"SELECT count(*) FROM ({source}) s LEFT JOIN {edge} VERSION AS OF 0 b ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id WHERE b.id IS NULL OR b.entity_version<>1 OR b.apply_batch_id<>'mixed-bootstrap'")==[['0']]
 metrics(100000000);start=time.monotonic();a['processing_start_epoch']=time.time();save()
 for role in ['source_record','property_journal','tombstone']:commit(role,'append-'+role,f'INSERT INTO {a["tables"][role]["table"]} ({",".join(sample(role))}) SELECT * FROM ({extract(role,T)})')
 assignments=[]
 for key in sample('current'):
  typ='BIGINT' if key in INTS else 'TIMESTAMP' if key in TIMES else 'STRING';assignments.append(f'b.{key}=cast(s.after.{key} AS {typ})')
 commit('edge_current','apply-current',f"MERGE INTO {edge} b USING ({source}) s ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id AND b.entity_version=1 AND b.apply_batch_id='mixed-bootstrap' WHEN MATCHED AND s.after IS NULL THEN DELETE WHEN MATCHED THEN UPDATE SET "+','.join(assignments))
 adj=a['tables']['adjacency_forward']['table'];commit('adjacency_forward','apply-adjacency',f'MERGE INTO {adj} b USING ({source}) s ON b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id AND b.entity_version=1 WHEN MATCHED AND s.after IS NULL THEN DELETE WHEN MATCHED THEN UPDATE SET b.entity_version=2')
 for role in oracle:
  table=a['tables'][role]['table'];digest(role,f'SELECT * FROM {table} VERSION AS OF {a["versions"][role]}')
 node=base['object_current']['table']+' VERSION AS OF 0';ev=a['versions']['edge_current'];av=a['versions']['adjacency_forward']
 for side in ['source','target']:assert c.sql('endpoint-'+side,f'SELECT count(*) FROM {edge} VERSION AS OF {ev} e LEFT ANTI JOIN {node} n ON e.source_system=n.source_system AND e.{side}_type=n.type_id AND e.{side}_id=n.id')==[['0']]
 assert c.sql('identity',f'SELECT count(*) FROM (SELECT source_system,rel_type_id,id FROM {edge} VERSION AS OF {ev} GROUP BY ALL HAVING count(*)>1)')==[['0']]
 metrics();vector={a['tables'][role]['table']:version for role,version in a['versions'].items()};values=['mixed-r232','ashlar-delta/0.3-synthetic-mixed',json.dumps(vector,sort_keys=True,separators=(',',':')),json.dumps({'stage':T,'version':8,'members':2048,'profile':'synthetic-mixed-change/1','real_source_ack':False},sort_keys=True),'{}',json.dumps(a['checks'],sort_keys=True)]
 def lit(v):return "decode(unhex('"+v.encode().hex()+"'),'UTF-8')"
 c.sql('publish',f'INSERT INTO {manifest} SELECT '+','.join(lit(x) for x in values)+',current_timestamp()');assert c.sql('readback',f'SELECT publication_id,profile_version,table_versions_json,source_progress_json,schema_revisions_json,validation_report_json FROM {manifest}')==[values];a['processing_s']=time.monotonic()-start;a['publication_vector']=vector;a['manifest']=manifest
 metrics();a['active_details']={role:detail('final-detail-'+role,m['table']) for role,m in a['tables'].items()};metrics();a['state']='Complete mixed native publication and independent final preservation verified';a['qualification']='One pre-staged2048-edge batch on4096nodes20480edges:1843updates205deletes. Full all-row field-digest oracle and final endpoint/identity checks; native commit statement IDs bind exact versions. Processing clock excludes original source stage/clone setup, includes final-cost barrier before descriptor and readback; final storage diagnostics follow clock. Not sustained10k/s,100k/s burst, real producer fencing, durable recovery, complete retained physical storage or billion/caller admission.';save();print(json.dumps({'state':a['state'],'processing_s':a['processing_s'],'versions':a['versions'],'costs':a['costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect exact handles, no mutation replay or manifest advance',error=str(e));save();raise
