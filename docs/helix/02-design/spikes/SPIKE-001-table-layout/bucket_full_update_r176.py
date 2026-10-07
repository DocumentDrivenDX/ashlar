"""Matched100k same-length wide updates against full20M LC clone and bucket8."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS,PropertyApply
from bucket_apply_queries import BUCKET_SQL,bucket_apply
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_bucket_update_r176'
assert not (O/'statements.jsonl').exists(),'Inspect saved native write handles and commits; no restart'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';S=F+'.schedule_r139_1';L=F+'.bucket_lc_r176';U=F+'.bucket_stage_r176'
prior=json.loads((B/'out/native/ashlar_bucket_zorder_r172/audited-summary.json').read_text());P=prior['table'];assert prior['version']==8
c=BoundedReads(O);deadline=time.monotonic()+600
state={'owned':{},'stage':U,'source':E,'source_version':23,'state':'Owned matched updates in progress; unpublished'}
def save():(O/'checkpoint.json').write_text(json.dumps(state,indent=2)+'\n')
def sql(label,q,parameters=None):
 assert time.monotonic()<deadline,'Controller admission deadline; inspect existing commits'
 return c.sql(label,q,parameters=parameters)
def detail(label,t):
 rows=sql(label,'DESCRIBE DETAIL '+t);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return dict(zip(names,rows[0]))
def costs():
 c.cursor.close();c.cursor=c.connection.cursor()
 for attempt in range(12):
  h={q['query_id']:q for q in c.history()}
  if all(r['statement_id'] in h and h[r['statement_id']]['is_final'] for r in c.records):break
  if attempt<11:time.sleep(5)
 assert all(h[r['statement_id']]['is_final'] and h[r['statement_id']]['status']=='FINISHED' for r in c.records),'Inspect same native IDs'
 state['costs']={k:sum(h[r['statement_id']]['metrics'].get(k,0) for r in c.records) for k in ('read_bytes','write_remote_bytes','spill_to_disk_bytes')};save()
 assert state['costs']['read_bytes']<=75000000000 and state['costs']['write_remote_bytes']<=4000000000,'Stop further admission: budget crossed'
 return h
try:
 sql('timeout','SET STATEMENT_TIMEOUT=180');assert sql('absence',f"SHOW TABLES IN {F} LIKE 'bucket_*_r176'")==[]
 d=detail('part-before-detail',P);assert d['id']==prior['id']
 assert sql('part-before-version','DESCRIBE HISTORY '+P+' LIMIT 1')[0][0]=='8'
 state['owned']['part']={'table':P,'id':d['id'],'old_version':8,'version':8,'before_detail':d};save()
 sql('lc-create',f'CREATE TABLE {L} SHALLOW CLONE {E} VERSION AS OF 23');d=detail('lc-before-detail',L)
 assert sql('lc-before-version','DESCRIBE HISTORY '+L+' LIMIT 1')[0][0]=='0'
 state['owned']['lc']={'table':L,'id':d['id'],'old_version':0,'version':0,'before_detail':d};save()
 base=f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0"
 overrides={'entity_version':'entity_version+1','props_json':"replace(props_json,concat(char(34),'105',char(34),':',char(34),old_opaque,char(34)),concat(char(34),'105',char(34),':',char(34),new_opaque,char(34)))",'source_epoch':"'bucket-r176'",'apply_batch_id':"'r176-b1'",'published_at':'current_timestamp()','source_delivery_id':"concat('r176-b1:edge:',cast(id AS STRING))",'source_cursor_json':"concat('{',char(34),'xid',char(34),':',char(34),'9007199254742501',char(34),',',char(34),'seq',char(34),':',char(34),cast(id AS STRING),char(34),'}')"}
 sql('stage-create',f"CREATE TABLE {U} USING DELTA AS SELECT "+','.join(overrides.get(col,col)+' AS '+col for col in COLS)+",concat(char(34),old_opaque,char(34)) old_json FROM (SELECT *,get_json_object(props_json,'$.105') old_opaque,concat_ws('',transform(sequence(0,cast(length(get_json_object(props_json,'$.105'))/64 AS INT)-1),block ->sha2(concat('r176:',cast(id AS STRING),':',cast(block AS STRING)),256))) new_opaque FROM ("+base+'))')
 state['stage_detail']=detail('stage-detail',U);state['stage_id']=state['stage_detail']['id'];save()
 assert sql('stage-membership',f'SELECT count(*),count(DISTINCT id) FROM {U} VERSION AS OF 0')==[['100000','100000']]
 assert sql('stage-length',f"SELECT count(*) FROM {U} VERSION AS OF 0 WHERE length(old_json)-2<>length(get_json_object(props_json,'$.105')) OR (length(old_json)-2)%64<>0")==[['0']]
 for name,t in [('lc',L),('part',P)]:
  costs();x=state['owned'][name];q=PropertyApply(t,U,x['old_version'],15,'r176-b1','r139-b1','bucket-r176',9007199254742501,eligibility_placement='on')
  assert sql(name+'-intended',q.intended())==[['0']];costs()
  sql(name+'-apply',q.apply() if name=='lc' else bucket_apply(q));qid=c.records[-1]['statement_id']
  v=int(sql(name+'-post-version','DESCRIBE HISTORY '+t+' LIMIT 1')[0][0]);assert v==x['old_version']+1
  x.update(version=v,apply_query_id=qid);save();print('Committed',name,'version',v,flush=True)
  assert sql(name+'-output',q.output(v))==[['0']]
  assert sql(name+'-identities',f'SELECT count(*),count(DISTINCT id) FROM {t} VERSION AS OF {v}')==[['20000000','20000000']]
  x['detail']=detail(name+'-post-detail',t);assert x['detail']['id']==x['id'];assert json.loads(x['detail']['properties'])['delta.enableRowTracking']=='true';save()
 assert sql('bucket-drift',f'SELECT count(*) FROM {P} VERSION AS OF {state["owned"]["part"]["version"]} WHERE lookup_bucket IS NULL OR lookup_bucket<>({BUCKET_SQL})')==[['0']]
 costs();oracle=sql('oracle',f"SELECT {','.join(COLS)} FROM {U} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30")
 for i,row in enumerate(oracle):
  for name in (('lc','part') if i%2==0 else ('part','lc')):
   x=state['owned'][name];q=f"SELECT {','.join(COLS)} FROM {x['table']} VERSION AS OF {x['version']} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)";params={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]}
   if name=='part':q+=' AND lookup_bucket=CAST(:bucket AS INT)';params['bucket']=int(row[16][:15],16)%64
   assert sql(name+'-read-'+str(i),q,parameters=params)==[row]
 pins=sql('publication',f"SELECT table_versions_json FROM {F}.publication_manifest_r89 WHERE publication_id='r139-b1'");state['publication_vector']=json.loads(pins[0][0]);assert state['publication_vector'][E]==23
 h=costs();state['apply_metrics']={name:{'caller_ms':next(r['wall_ms'] for r in c.records if r['statement_id']==x['apply_query_id']),'metrics':h[x['apply_query_id']]['metrics']} for name,x in state['owned'].items()}
 state['state']='Both full20M100k wide updates exact;60 changed-key reads pass; untouched/final-wide proof pending';state['qualification']='One same-size high-entropy100k property105 update per owned layout; stage from immutable139 input. Explicit full native tuple plus metadata hash/bucket matching. LC clone inherits E23 DVs/hot files; bucket8 was full copy/ZORDER. Not causal partitioning isolation or a publisher/rate/producer/fencing/cold/billion claim. Stage and final owned versions retained unpublished for19.9M custody/full-wide proof; canonical r139 unchanged.';save();print(json.dumps(state['costs']),flush=True)
except Exception as e:state['state']='Stopped; inspect existing write IDs/owned versions before further admission';state['error']=str(e);save();raise
finally:c.close()
