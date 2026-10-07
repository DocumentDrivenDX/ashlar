"""Private narrow-index/immutable-carrier experiment; not canonical promotion."""
import hashlib,json,re,time
from pathlib import Path
from persistent_sql import Client
from overlay_sql_r395 import FIELDS
from generated_fingerprint_r491 import expression
B=Path(__file__).resolve().parent
KEY=('lookup_hash','source_system','rel_type_id','id')
IFIELDS=KEY+('entity_version','carrier_version','carrier_fingerprint','is_deleted','source_feed','source_epoch','source_delivery_id')
def fp(alias,prefix=''):
 return re.sub(r'\b('+'|'.join(FIELDS)+r')\b',lambda m:alias+'.'+prefix+m.group(0),expression())
def index_merge(index,source):
 valid=f"NOT b.is_deleted AND b.entity_version=s.entity_version AND b.carrier_version=s.entity_version AND b.carrier_fingerprint={fp('s')} AND b.source_feed=s.source_feed AND b.source_epoch=s.source_epoch AND b.source_delivery_id=s.source_delivery_id AND s.entity_version IS NOT NULL AND (s.is_delete OR (s.after_entity_version=s.entity_version+1 AND "+' AND '.join(f's.after_{f}<=>s.{f}' for f in KEY)+'))'
 after={'entity_version':'s.entity_version+1','carrier_version':'CASE WHEN s.is_delete THEN s.entity_version ELSE s.after_entity_version END','carrier_fingerprint':f"CASE WHEN s.is_delete THEN {fp('s')} ELSE {fp('s','after_')} END",'is_deleted':'s.is_delete','source_feed':'s.new_source_feed','source_epoch':'s.new_source_epoch','source_delivery_id':'s.change_delivery_id'}
 on=' AND '.join(f'b.{f}=s.{f}' for f in KEY)
 return f"MERGE INTO {index} b USING ({source}) s ON {on} WHEN MATCHED AND CASE WHEN ({valid}) THEN true ELSE CAST(raise_error('ASHLAR_INDEX_PREDECESSOR_MISMATCH') AS BOOLEAN) END THEN UPDATE SET "+','.join('b.'+f+'='+v for f,v in after.items())+" WHEN NOT MATCHED THEN INSERT (source_system) VALUES (CAST(raise_error('ASHLAR_INDEX_MISSING_PREDECESSOR') AS STRING))"

def main():
 parent=B/'out/native/scaled_fingerprint_r493/summary.json';p=json.loads(parent.read_text());fourth=json.loads((B/'out/native/ashlar_fourth_guard_publish_r437/summary.json').read_text());raw=fourth['inputs']['source_record'];out=B/'out/native/index_carrier_spike_r513';assert not out.exists();start=time.monotonic();c=Client(out,observation_timeout=140,cancel_after=90);f=p['table'].rsplit('.',1)[0];carrier=f+'.immutable_carrier_r513';index=f+'.current_index_r513';prepared=p['source']['table'];bound=p['upper_exclusive_lookup_hash']
 source=f"SELECT s.*,r.source_feed AS new_source_feed,r.source_epoch AS new_source_epoch FROM {prepared} VERSION AS OF 0 s INNER JOIN {raw['table']} VERSION AS OF 0 r ON s.change_delivery_id=r.delivery_id WHERE s.lookup_hash<'{bound}'"
 a={'state':'running private index/carrier layout','carrier':carrier,'index':index,'sources':{'predecessor':{'table':p['table'],'version':2,'id':p['table_id']},'prepared':p['source'],'raw':raw},'source_sha256':hashlib.sha256(parent.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':30000000000,'write_remote_bytes':1000000000,'spill_to_disk_bytes':2000000000,'wall_s':360},'controls':[],'qualification':'Private2.5M-row projection and6227 fourth-input changes. Immutable full20 carriers/generated hash plus11-field index, full tuple/lifecycle/origin preserved. No canonical/default/consumer/publication/fencing or billion admission.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def objects(label,q):
  rows=c.sql(label,q);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];return [dict(zip(cols,r)) for r in rows]
 def head(label,table):return objects(label,'DESCRIBE HISTORY '+table+' LIMIT 1')[0]
 def audit():
  for i in range(30):
   h=c.history()
   if len(h)==len(c.records) and all(q.get('is_final') for q in h):break
   time.sleep(1)
  assert len(h)==len(c.records) and all(q.get('is_final') for q in h)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h) for k in a['bounds'] if k!='wall_s'};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=360;return {q['query_id']:q for q in h}
 save()
 try:
  for label,table,uid in [('base',p['table'],p['table_id']),('prepared',prepared,p['source']['id']),('raw',raw['table'],raw['id'])]:assert objects(label+'-detail','DESCRIBE DETAIL '+table)[0]['id']==uid
  assert c.sql('raw-unique',f"SELECT count(*),count(DISTINCT delivery_id) FROM {raw['table']} VERSION AS OF 0")==[['100000','100000']]
  counts=c.sql('source-counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_delete),count_if(NOT is_delete) FROM ({source})');assert counts==[['6227','6227','618','5609']];a['source_counts']=counts
  a['clone_result']=objects('clone',f"CREATE TABLE {carrier} SHALLOW CLONE {p['table']} VERSION AS OF 2");assert int(a['clone_result'][0]['num_copied_files'])==0
  c.sql('append-only',f"ALTER TABLE {carrier} SET TBLPROPERTIES ('delta.appendOnly'='true')");assert int(head('carrier-baseline',carrier)['version'])==1
  c.sql('disable-carrier','ALTER TABLE '+carrier+' DISABLE PREDICTIVE OPTIMIZATION')
  projection=','.join(KEY)+',entity_version,entity_version AS carrier_version,carrier_fingerprint,false AS is_deleted,source_feed,source_epoch,source_delivery_id'
  c.sql('create-index',f"CREATE TABLE {index} USING DELTA CLUSTER BY (lookup_hash) TBLPROPERTIES ('delta.targetFileSize'='67108864','delta.parquet.compression.codec'='zstd','delta.enableChangeDataFeed'='true') AS SELECT {projection} FROM {carrier} VERSION AS OF 1");assert int(head('index-baseline',index)['version'])==0;c.sql('disable-index','ALTER TABLE '+index+' DISABLE PREDICTIVE OPTIMIZATION');a['index_before']=objects('index-detail','DESCRIBE DETAIL '+index)[0];a['carrier_before']=objects('carrier-detail','DESCRIBE DETAIL '+carrier)[0];audit()
  on=' AND '.join('i.'+k+'=v.'+k for k in KEY)+' AND i.carrier_version=v.entity_version'
  assert c.sql('baseline-pointer-proof',f'SELECT count(*),count_if(v.id IS NULL OR i.carrier_fingerprint<>v.carrier_fingerprint OR i.is_deleted OR i.entity_version<>i.carrier_version OR i.source_feed<>v.source_feed OR i.source_epoch<>v.source_epoch OR i.source_delivery_id<>v.source_delivery_id) FROM {index} VERSION AS OF 0 i LEFT JOIN {carrier} VERSION AS OF 1 v ON {on}')==[['2498646','0']]
  one='SELECT * FROM ('+source+') ORDER BY id LIMIT 1'
  try:c.sql('carrier-mutation-refusal',f"UPDATE {carrier} SET props_json='{{}}' WHERE lookup_hash=(SELECT lookup_hash FROM ({one}))")
  except RuntimeError:
   assert c.records[-1]['response']['status']['state']=='FAILED' and 'APPEND' in json.dumps(c.records[-1]['response']['status']).upper();a['controls'].append('carrier mutation refused')
  else:raise AssertionError('Mutable carrier admitted')
  assert int(head('carrier-refusal-head',carrier)['version'])==1
  bad="SELECT * EXCEPT(props_json),concat(props_json,' ') AS props_json FROM ("+one+')'
  try:c.sql('index-token-refusal',index_merge(index,bad))
  except RuntimeError:
   assert c.records[-1]['response']['status']['state']=='FAILED' and 'ASHLAR_INDEX_' in json.dumps(c.records[-1]['response']['status']);a['controls'].append('changed predecessor token refused')
  else:raise AssertionError('Changed predecessor admitted')
  assert int(head('index-refusal-head',index)['version'])==0;audit()
  t=time.monotonic();c.sql('append-carriers',f"INSERT INTO {carrier} ({','.join(FIELDS)}) SELECT "+','.join('s.after_'+x for x in FIELDS)+f' FROM ({source}) s WHERE NOT s.is_delete');a['append_statement_id']=c.records[-1]['statement_id'];assert head('append-bind',carrier)['queryHistoryStatementId']==a['append_statement_id'];assert int(head('carrier-after-version',carrier)['version'])==2;a['append_caller_s']=time.monotonic()-t
  t=time.monotonic();c.sql('index-cas',index_merge(index,source));a['index_statement_id']=c.records[-1]['statement_id'];after=head('index-bind',index);assert after['queryHistoryStatementId']==a['index_statement_id'] and int(after['version'])==1;a['index_caller_s']=time.monotonic()-t;audit()
  # CDF for the immutable appended carriers matches every source after-field.
  equal=' AND '.join(f'(v.{k}<=>s.after_{k})' for k in FIELDS)
  oncs=' AND '.join(f'v.{k}=s.{k}' for k in KEY)+' AND v.entity_version=s.after_entity_version'
  q=f"SELECT count(*),count_if(v.id IS NULL OR s.id IS NULL OR NOT ({equal}) OR v.carrier_fingerprint<>{fp('s','after_')} OR v._change_type<>'insert') FROM table_changes('{carrier}',2,2) v FULL OUTER JOIN (SELECT * FROM ({source}) WHERE NOT is_delete) s ON {oncs}";a['carrier_cdf']=c.sql('carrier-cdf',q);assert a['carrier_cdf']==[['5609','0']]
  before={k:'s.'+k for k in KEY};before.update(entity_version='s.entity_version',carrier_version='s.entity_version',carrier_fingerprint=fp('s'),is_deleted='false',source_feed='s.source_feed',source_epoch='s.source_epoch',source_delivery_id='s.source_delivery_id')
  after=dict(before);after.update(entity_version='s.entity_version+1',carrier_version='CASE WHEN s.is_delete THEN s.entity_version ELSE s.after_entity_version END',carrier_fingerprint=f"CASE WHEN s.is_delete THEN {fp('s')} ELSE {fp('s','after_')} END",is_deleted='s.is_delete',source_feed='s.new_source_feed',source_epoch='s.new_source_epoch',source_delivery_id='s.change_delivery_id')
  expected=' UNION ALL '.join('SELECT '+','.join(values[k]+' AS '+k for k in IFIELDS)+",'"+kind+"' AS change_type FROM ("+source+') s' for values,kind in [(before,'update_preimage'),(after,'update_postimage')]);onidx=' AND '.join('e.'+k+'=v.'+k for k in KEY)+' AND e.change_type=v._change_type';equalidx=' AND '.join(f'(e.{k}<=>v.{k})' for k in IFIELDS)
  a['index_cdf']=c.sql('index-cdf',f"SELECT count(*),count_if(e.id IS NULL OR v.id IS NULL OR NOT ({equalidx})) FROM ({expected}) e FULL OUTER JOIN table_changes('{index}',1,1) v ON {onidx}");assert a['index_cdf']==[['12454','0']];audit()
  logical=f"SELECT {','.join('v.'+k for k in FIELDS)} FROM {index} VERSION AS OF 1 i INNER JOIN {carrier} VERSION AS OF 2 v ON {on} AND i.carrier_fingerprint=v.carrier_fingerprint WHERE NOT i.is_deleted"
  exact=' AND '.join(f'(l.{k}<=>r.{k})' for k in FIELDS);onview=' AND '.join('l.'+k+'=r.'+k for k in KEY)
  a['full_logical_parity']=c.sql('full-logical-parity',f"SELECT count(*),count_if(l.id IS NULL OR r.id IS NULL OR NOT ({exact})) FROM ({logical}) l FULL OUTER JOIN {p['table']} VERSION AS OF 3 r ON {onview}");assert a['full_logical_parity']==[['2498028','0']]
  a['index_counts']=c.sql('index-counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_deleted) FROM {index} VERSION AS OF 1');assert a['index_counts']==[['2498646','2498646','618']]
  a['carrier_counts']=c.sql('carrier-counts',f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id,entity_version)) FROM {carrier} VERSION AS OF 2');assert a['carrier_counts']==[['2504255','2504255']]
  a['index_after']=objects('index-final-detail','DESCRIBE DETAIL '+index)[0];a['carrier_after']=objects('carrier-final-detail','DESCRIBE DETAIL '+carrier)[0];h=audit();a['index_metrics']=h[a['index_statement_id']]['metrics'];a['append_metrics']=h[a['append_statement_id']]['metrics'];a['state']='Private immutable carrier and guarded narrow index preserve exact complete logical current, CDF and lifecycle/origin';save();(out/'live-statement.json').rename(out/'completed-last-statement.json');print(json.dumps({k:a[k] for k in ['state','index_metrics','append_metrics','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same submitted handles and private versions; no replay',error=str(e));save();raise
if __name__=='__main__':main()
