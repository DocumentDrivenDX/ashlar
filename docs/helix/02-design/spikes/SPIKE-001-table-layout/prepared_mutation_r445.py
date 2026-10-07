"""Materialize qualified strict parsed mutation once; raw originals remain pinned."""
import json,hashlib,time
from pathlib import Path
from fourth_changes_r427 import FourthChanges
from normalized_apply_sql_r276 import pin,mutation_source
from inline_guard_sql_r421 import guard_merge_relation
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash,row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent
def main():
 source=B/'out/native/ashlar_fourth_guard_publish_r437/audited-summary.json';pub=json.loads(source.read_text());assert pub['state']=='Integrated private fourth100k guarded publication passes full change custody';raw=pub['inputs']['source_record'];current=pub['inputs']['current_replacement'];fields=[*FIELDS,*('after_'+f for f in FIELDS),'is_delete','change_delivery_id'];hashes=[];changes=FourthChanges()
 for i in range(100000):
  x=changes.change(i);row=dict(x['before']);row.update({'after_'+f:None if x['after'] is None else x['after'][f] for f in FIELDS});row.update(is_delete=x['after'] is None,change_delivery_id=x['raw']['delivery_id'])
  if row['after_published_at'] is not None:row['after_published_at']=row['after_published_at'].replace('T',' ').removesuffix('Z')
  hashes.append(row_hash(row,fields))
 oracle=hashlib.sha256(''.join(sorted(hashes)).encode()).hexdigest();O=B/'out/native/ashlar_prepared_mutation_r445';assert not O.exists();O.mkdir();start=time.monotonic();c=Client(O,observation_timeout=120,cancel_after=60);table='client_dev.ashlar_entropy_20261006_r86.prepared_current_mutation_r445';a={'state':'materializing strict parsed42field input cache','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'fields':fields,'oracle_digest':oracle,'table':{'table':table},'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['prepared_mutation_r445.py','fourth_changes_r427.py','normalized_apply_sql_r276.py','inline_guard_sql_r421.py']},'bounds':{'read_bytes':3000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':500000000,'wall_s':180},'qualification':'Derivative42field cache of already accepted immutable fourth input0; before20/after20/delete flag/new delivery ID. Exact original input/raw/history remains pinned; this cache is not a canonical schema or production receipt. No current MERGE is executed.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objs(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 save()
 try:
  assert c.sql('raw-delivery-uniqueness',f'SELECT count(*),count(DISTINCT delivery_id) FROM {pin(raw["table"],0)}')==[['100000','100000']]
  relation=mutation_source(raw['table'],0,current['table'],0);t=time.monotonic();c.sql('create-prepared',f"CREATE TABLE {table} USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.appendOnly'='true','delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd') AS SELECT {','.join(fields)} FROM ({relation})");a['materialize_caller_s']=time.monotonic()-t;a['create_statement_id']=c.records[-1]['statement_id'];hist=objs('create-history','DESCRIBE HISTORY '+table+' LIMIT 10');assert len(hist)==1 and hist[0]['queryHistoryStatementId']==a['create_statement_id'];version=int(hist[0]['version']);a['table']['version']=version
  c.sql('disable-predictive','ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION');a['detail']=objs('detail','DESCRIBE DETAIL '+table)[0];a['table']['id']=a['detail']['id'];c.sql('statistics','ANALYZE TABLE '+table+' COMPUTE STATISTICS FOR COLUMNS lookup_hash,source_system,rel_type_id,id,props_json,after_props_json')
  result=c.sql('complete-prepared-digest',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count(DISTINCT change_delivery_id),count_if(is_delete),count_if(NOT is_delete),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(fields)}))),256) FROM {pin(table,version)}");assert result==[['100000','100000','100000','10000','90000',oracle]];a['complete_digest_result']=result
  assert int(objs('head-after-statistics','DESCRIBE HISTORY '+table+' LIMIT 1')[0]['version'])==version
  source_relation='SELECT * FROM '+pin(table,version);e=pub['base']['edge_current'];on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);comparisons=' AND '.join(f'(b.{f}<=>s.{f})' for f in FIELDS);probe=f"SELECT count(*),count_if(b.id IS NULL OR NOT ({comparisons})),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(['s.'+f for f in fields])}))),256) FROM ({source_relation}) s LEFT JOIN {pin(e['table'],6)} b ON {on}"
  a['wide_read_sql']=probe;plan=c.sql('prepared-wide-plan','EXPLAIN FORMATTED '+probe);(O/'prepared-wide-plan.txt').write_text('\n'.join(str(r[0]) for r in plan)+'\n')
  a['merge_sql']=guard_merge_relation(pub['tables']['edge_current']['table'],source_relation);plan=c.sql('prepared-merge-plan','EXPLAIN FORMATTED '+a['merge_sql']);(O/'prepared-merge-plan.txt').write_text('\n'.join(str(r[0]) for r in plan)+'\n')
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='Complete42field prepared100k cache matches independent source and native plans captured',wall_s=time.monotonic()-start);assert a['wall_s']<180;save();print(json.dumps({k:a[k] for k in ['state','materialize_caller_s','wall_s','costs','table']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles and actual new cache history; no replay',error=str(e));save();raise
if __name__=='__main__':main()
