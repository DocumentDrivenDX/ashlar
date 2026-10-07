"""Pin four complete synthetic 100k-change role inputs in owned Delta tables.
Run only after the r274 SQL workload terminates; no publisher or source ACK.
"""
import hashlib,json,time
from pathlib import Path
from databricks.sdk.service.catalog import VolumeType
from normalized_input_sql_r264 import volume_query
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 growth=json.loads((B/'out/native/ashlar_scale_edges_r274/summary.json').read_text())
 assert growth['state']=='Complete8M-node/40M-edge staging passes every role field and typed endpoint closure','Growth must be terminal and successful'
 source_path=B/'out/native/ashlar_normalized_batch_upload_r272/audited-summary.json';source=json.loads(source_path.read_text())
 assert source['state']=='All100k source-role parts roundtrip byte-exactly in owned UC volume'
 O=B/'out/native/ashlar_normalized_delta_stage_r275';assert not O.exists();O.mkdir()
 started=time.monotonic();c=Client(O,observation_timeout=200,cancel_after=180)
 a={'state':'Preparing four owned normalized Delta inputs','source_sha256':hashlib.sha256(source_path.read_bytes()).hexdigest(),'tables':{},'checks':{},'bounds':{'read_bytes':10000000000,'write_remote_bytes':3000000000,'spill_to_disk_bytes':2000000000,'wall_s':900,'statement_s':180},'qualification':'Synthetic normalized inputs only. Retains exact input JSON and explicit known typed fields, including tombstone entity_version. Mutable volume files are accepted only if native complete digests match independently generated source expectations. Delta commit/UUID pins qualify the input snapshot, not real producer fencing, canonical production DDL, required-field/duplicate-key validation or publication freshness. Input transfer357s is separate preparation cost; no source ACK or graph manifest.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics(reserve=0):
  for n in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if n==19:raise
    time.sleep(2)
  a['costs']={k:sum(x['metrics'].get(k,0) for x in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
  for k,v in a['costs'].items():assert v+reserve<=a['bounds'][k],k
  save();return h
 save()
 try:
  v=c.w.volumes.read(name=source['volume_full_name']);assert v.volume_id==source['volume_id'] and v.volume_type==VolumeType.MANAGED
  for role,f in source['roles'].items():
   assert time.monotonic()-started<900
   for part in f['parts']:assert c.w.files.get_metadata(file_path=part['path']).content_length==part['bytes']
   table='client_dev.ashlar_entropy_20261006_r86.normalized_'+role+'_r275'
   relation=' UNION ALL '.join('SELECT * FROM ('+volume_query(role,p['path'])+')' for p in f['parts'])
   c.sql('create-'+role,'CREATE TABLE '+table+' USING DELTA AS '+relation);sid=c.records[-1]['statement_id']
   history=c.sql('history-'+role,'DESCRIBE HISTORY '+table+' LIMIT 10');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];commits=[dict(zip(names,r)) for r in history if dict(zip(names,r)).get('queryHistoryStatementId')==sid];assert len(commits)==1
   version=int(commits[0]['version']);d=c.sql('detail-'+role,'DESCRIBE DETAIL '+table);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(names,d[0]));assert detail['format']=='delta'
   a['tables'][role]={'table':table,'id':detail['id'],'version':version,'statement_id':sid,'history':commits[0],'detail':detail};save()
   c.sql('disable-maintenance-'+role,'ALTER TABLE '+table+' DISABLE PREDICTIVE OPTIMIZATION')
   settings=c.sql('maintenance-'+role,'DESCRIBE TABLE EXTENDED '+table);assert [r[1] for r in settings if r[0]=='Predictive Optimization']==['DISABLE']
   q=f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list(sha2(input_json,256)))),256),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(f['fields'])}))),256) FROM {table} VERSION AS OF {version}"
   expected=[[str(f['rows']),f['original_input_multiset_digest'],f['all_known_field_digest']]];assert c.sql('verify-'+role,q)==expected
   a['checks'][role]={'rows':f['rows'],'original_input_digest':f['original_input_multiset_digest'],'all_known_field_digest':f['all_known_field_digest'],'fields':f['fields'],'version':version};save();metrics(100000000)
  def pinned(role):
   t=a['tables'][role];return f"{t['table']} VERSION AS OF {t['version']}"
  raw=pinned('source_record');journal=pinned('property_journal');tomb=pinned('tombstone');current=pinned('current_replacement')
  assert c.sql('raw-unique-and-digests',f"SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,delivery_id)),count_if(payload_digest IS NULL OR payload_digest <> sha2(payload_json,256)) FROM {raw}")==[['100000','100000','0']]
  assert c.sql('replacement-identity',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(entity_version IS NULL OR entity_version <> 2) FROM {current}")==[['90000','90000','0']]
  assert c.sql('deletion-identity',f"SELECT count(*),count(DISTINCT struct(source_system,entity_kind,type_id,id)),count_if(entity_version IS NULL OR entity_version <> 2) FROM {tomb}")==[['10000','10000','0']]
  assert c.sql('journal-identity',f"SELECT count(*),count(DISTINCT struct(source_system,entity_kind,type_id,id,entity_version,event_ordinal)),count_if(entity_version IS NULL OR entity_version <> 2) FROM {journal}")==[['216667','216667','0']]
  for role in ['property_journal','tombstone','current_replacement']:
   assert c.sql('raw-link-'+role,f"SELECT count(*),count_if(r.delivery_id IS NULL OR NOT (d.source_cursor_json <=> r.source_cursor_json)) FROM {pinned(role)} d LEFT JOIN {raw} r ON d.source_feed=r.source_feed AND d.source_epoch=r.source_epoch AND d.source_delivery_id=r.delivery_id")==[[str(source['roles'][role]['rows']),'0']]
  assert c.sql('change-partition',f"SELECT count(*),count_if(c.id IS NOT NULL AND t.id IS NULL AND get_json_object(r.payload_json,'$.operation')='update'),count_if(c.id IS NULL AND t.id IS NOT NULL AND get_json_object(r.payload_json,'$.operation')='delete'),count_if((c.id IS NULL AND t.id IS NULL) OR (c.id IS NOT NULL AND t.id IS NOT NULL)) FROM {raw} r LEFT JOIN {current} c ON r.source_feed=c.source_feed AND r.source_epoch=c.source_epoch AND r.delivery_id=c.source_delivery_id LEFT JOIN {tomb} t ON r.source_feed=t.source_feed AND r.source_epoch=t.source_epoch AND r.delivery_id=t.source_delivery_id")==[['100000','90000','10000','0']]
  a['checks']['cohesion']='Unique role identities/event ordinals, exact raw payload SHA, version2, exact90k-update/10k-delete partition, and every derived row linked to unique raw delivery and exact cursor';save()
  h=metrics();assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in c.records if r['label'].startswith('verify-'))
  a['wall_s']=time.monotonic()-started;assert a['wall_s']<900;a['state']='Four complete normalized input roles match independent source digests and are Delta-version pinned';save();print(json.dumps({'state':a['state'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native handles and created tables; never blindly replay',error=str(e));save();raise

if __name__=='__main__':main()
