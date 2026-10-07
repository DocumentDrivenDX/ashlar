"""Bounded100k accepted-overlay append/complete correctness comparison, private only."""
import hashlib,json,time
from pathlib import Path
from normalized_apply_sql_r276 import pin
from overlay_normalized_sql_r399 import overlay_input
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 plan_path=B/'out/overlay100k-admission-plan-r399.json';plan=json.loads(plan_path.read_text());sources={k:json.loads((B/path).read_text()) for k,path in plan['sources'].items() if path.endswith('.json')}
 for k,path in plan['sources'].items():assert hashlib.sha256((B/path).read_bytes()).hexdigest()==plan['source_sha256'][k]
 oracle_path=B/'out/overlay100k-oracle-r400.json';oracle=json.loads(oracle_path.read_text());assert oracle['state']=='Independent100k complete overlay markers and90k live carriers qualified'
 inputs=sources['inputs']['tables'];base=sources['base']['role_pins']['edge_current'];reference=sources['publication']['tables']['edge_current'];node=sources['publication']['tables']['object_current'];assert base['id']==reference['id'] and base['version']==5 and reference['version']==6
 O=B/'out/native/ashlar_overlay_build_r401';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=180,cancel_after=120);table=plan['private_table'];bounds=plan['bounds']
 a={'state':'Preflight existing accepted pins','sources':{'plan':str(plan_path.relative_to(B)),'oracle':str(oracle_path.relative_to(B))},'source_sha256':{'plan':hashlib.sha256(plan_path.read_bytes()).hexdigest(),'oracle':hashlib.sha256(oracle_path.read_bytes()).hexdigest()},'base':base,'reference':reference,'node':node,'inputs':inputs,'bounds':bounds,'checks':{},'qualification':'Reuses third batch accepted/qualified by full r391 publication, with prior source/predecessor/history costs explicit. This measures private overlay storage/append and logical equivalence, not incoming producer admission, complete publication freshness, compaction, singleton latency or1B/5B. Deletion carrier inactive fields diagnostic only. Old current/manifest/raw/journal/tombstone tables are not written.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def sql(label,q):assert time.monotonic()-start<bounds['wall_s'];return c.sql(label,q)
 def objs(label,q):
  rs=sql(label,q);return [dict(zip([x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']],r)) for r in rs]
 def metrics():
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=bounds[k] for k,v in a['costs'].items());assert time.monotonic()-start<bounds['wall_s'];save();return h
 save()
 try:
  for role,t in {'edge':reference,'node':node,**{'input-'+r:t for r,t in inputs.items()}}.items():
   d=objs('detail-'+role,'DESCRIBE DETAIL '+t['table'])[0];assert d['id']==t['id'] and d['format']=='delta'
   if role!='node':assert int(objs('head-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0]['version'])==t['version']
  assert sql('pinned-base-count','SELECT count(*) FROM '+pin(base['table'],5))==[['39980000']]
  normalized=overlay_input(inputs);cols=','.join(FIELDS+('is_deleted',));sql('create-empty',f"CREATE TABLE {table} USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.appendOnly'='true','delta.parquet.compression.codec'='zstd') AS SELECT * FROM ({normalized}) WHERE false");create_sid=c.records[-1]['statement_id'];detail=objs('new-overlay-detail','DESCRIBE DETAIL '+table)[0];history=objs('new-overlay-history','DESCRIBE HISTORY '+table+' LIMIT 10');assert len(history)==1 and int(history[0]['version'])==0 and history[0]['queryHistoryStatementId']==create_sid
  a['table']={'table':table,'id':detail['id'],'version':0,'create_statement_id':create_sid};sql('disable-maintenance','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');settings=sql('maintenance-setting','DESCRIBE TABLE EXTENDED '+table);assert [r[1] for r in settings if r[0]=='Predictive Optimization']==['DISABLE'];a['setup_s']=time.monotonic()-start;metrics();append_start=time.monotonic();sql('append100k',f'INSERT INTO {table} ({cols}) '+normalized);a['append_caller_s']=time.monotonic()-append_start;a['append_statement_id']=c.records[-1]['statement_id'];h=objs('bind-append','DESCRIBE HISTORY '+table+' LIMIT 10');assert [int(x['version']) for x in h]==[1,0] and [x['queryHistoryStatementId'] for x in h]==[a['append_statement_id'],create_sid];a['table']['version']=1;a['history']=h;a['state']='Auditing complete accepted overlay';save();metrics();overlay=pin(table,1)
  q=f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_deleted),count_if(NOT is_deleted),count_if(source_system IS NULL OR rel_type_id IS NULL OR id IS NULL OR lookup_hash IS NULL OR is_deleted IS NULL OR entity_version IS NULL OR entity_version<>2 OR apply_batch_id IS NULL OR apply_batch_id<>'mixed-change/3') FROM {overlay}";assert sql('identity-and-partition',q)==[['100000','100000','10000','90000','0']]
  assert sql('full-overlay-digest',f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(oracle['fields'])}))),256) FROM {overlay}")==[['100000',oracle['digest']]];a['checks']['complete_overlay']=oracle;metrics()
  for label,flag in [('live',False),('deleted',True)]:
   e=oracle[label];q=f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(FIELDS)}))),256) FROM {overlay} WHERE is_deleted={str(flag).lower()}";assert sql('digest-'+label,q)==[[str(e['rows']),e['digest']]]
  raw=inputs['source_record'];q=f"SELECT count(*),count_if(r.delivery_id IS NULL OR NOT(o.source_cursor_json <=> r.source_cursor_json)) FROM {overlay} o LEFT JOIN {pin(raw['table'],raw['version'])} r ON o.source_feed=r.source_feed AND o.source_epoch=r.source_epoch AND o.source_delivery_id=r.delivery_id";assert sql('raw-origin-link',q)==[['100000','0']]
  expected_t=json.loads((B/'out/third-canonical-tomb-r391.json').read_text());tfields=expected_t['fields'];project={f:('rel_type_id' if f=='type_id' else "'edge'" if f=='entity_kind' else f) for f in tfields};rel='SELECT '+','.join(v+' AS '+k for k,v in project.items())+' FROM '+overlay+' WHERE is_deleted';assert sql('canonical-deletions',f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(tfields)}))),256) FROM ({rel})")==[['10000',expected_t['digest']]]
  edge=pin(reference['table'],6);on=' AND '.join('e.'+f+'=o.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);e=oracle['live'];q=f"SELECT count(*),count_if({row_hash_sql(['e.'+f for f in FIELDS])}<>{row_hash_sql(['o.'+f for f in FIELDS])}),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(['e.'+f for f in FIELDS])}))),256) FROM {overlay} o LEFT JOIN {edge} e ON {on} WHERE NOT o.is_deleted";assert sql('reference-all90k-live',q)==[['90000','0',e['digest']]];metrics()
  assert sql('reference10k-deleted-absent',f'SELECT count(*) FROM {overlay} o INNER JOIN {edge} e ON {on} WHERE o.is_deleted')==[['0']];metrics()
  bp=pin(base['table'],5);keys=['source_system','rel_type_id','id'];anti=' AND '.join('b.'+f+'=o.'+f for f in ['lookup_hash']+keys)+' AND o.entity_version>=b.entity_version';logical=f"SELECT {','.join('b.'+f for f in keys)} FROM {bp} b LEFT ANTI JOIN {overlay} o ON {anti} UNION ALL SELECT {','.join(keys)} FROM {overlay} WHERE NOT is_deleted";assert sql('logical-current-key-count',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM ({logical})')==[['39970000','39970000']];metrics()
  np=pin(node['table'],node['version']);q=f'SELECT count(*),count_if(s.id IS NULL OR t.id IS NULL) FROM {overlay} o LEFT JOIN {np} s ON o.source_system=s.source_system AND o.source_type=s.type_id AND o.source_id=s.id LEFT JOIN {np} t ON o.source_system=t.source_system AND o.target_type=t.type_id AND o.target_id=t.id WHERE NOT o.is_deleted';assert sql('new-live-typed-endpoints',q)==[['90000','0']];metrics()
  assert objs('final-overlay-history','DESCRIBE HISTORY '+table+' LIMIT 10')==a['history'];a['final_detail']=objs('final-overlay-detail','DESCRIBE DETAIL '+table)[0];assert a['final_detail']['id']==a['table']['id'];a['checks']['equivalence']='Complete100k markers;90k all20fields equal qualified E6;10k deletions absent; unique39.97M logical keys from pinned E5 anti/new union,90k typed closure and raw origin; inherited immutable carrier/history qualification retained from r391/r376, not freshly swept';h=metrics();a['cached_labels']=[r['label'] for r in c.records if h[r['statement_id']]['metrics'].get('result_from_cache')];a['wall_s']=time.monotonic()-start;a['state']='Private100k accepted overlay passes complete changed carriers and logical key equivalence';save();print(json.dumps({k:a[k] for k in ['state','setup_s','append_caller_s','wall_s','costs','cached_labels']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native handles and partial private commits; no blind replay',error=str(e));save();raise
if __name__=='__main__':main()
