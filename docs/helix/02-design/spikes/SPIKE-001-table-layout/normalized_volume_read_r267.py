"""Native TEXT/JSON framing of exact small owned UC input files; no table writes."""
import json,time,hashlib
from pathlib import Path
from databricks.sdk.service.catalog import VolumeType
from normalized_input_sql_r264 import volume_query
from mixed_change_queries_r230 import row_hash_sql
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;prior=B/'out/native/ashlar_input_volume_resume_r266/summary.json';source=json.loads(prior.read_text());O=B/'out/native/ashlar_input_volume_read_r267';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=70,cancel_after=60);a={'state':'Running native small file framing conformance','volume_full_name':source['volume_full_name'],'volume_id':source['volume']['volume_id'],'source_summary_sha256':hashlib.sha256(prior.read_bytes()).hexdigest(),'checks':{},'bounds':{'read_bytes':10000000,'write_bytes':0,'spill_bytes':0}}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 assert source['state']=='All tiny role files roundtrip byte-exactly in original owned volume';v=c.w.volumes.read(name=a['volume_full_name']);assert v.volume_id==a['volume_id'] and v.volume_type==VolumeType.MANAGED
 for role,f in source['files'].items():
  assert c.w.files.get_metadata(file_path=f['path']).content_length==f['bytes'];q=volume_query(role,f['path']);expected=[[str(f['rows']),f['input_multiset_digest'],f['known_field_multiset_digest']]]
  actual=c.sql('file-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list(sha2(input_json,256)))),256),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(f['fields'])}))),256) FROM ({q})");assert actual==expected;a['checks'][role]={'rows':f['rows'],'path':f['path'],'source_file_sha256':f['file_sha256'],'original_input_digest':f['input_multiset_digest'],'all_known_field_digest':f['known_field_multiset_digest']};save()
 for n in range(20):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==19:raise
   time.sleep(2)
 assert len(h)==4 and all(not q['metrics'].get('result_from_cache') for q in h.values());a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=10000000 and a['costs']['write_remote_bytes']==0 and a['costs']['spill_to_disk_bytes']==0;a['sql_channels']=list({json.dumps(q['channel_used'],sort_keys=True) for q in h.values()});a['source_sha256']={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['normalized_volume_read_r267.py','normalized_input_sql_r264.py','mixed_change_queries_r230.py']};a['state']='Native TEXT file framing and all exact normalized role fields verified';a['qualification']='Four small controlled UC files: original input-line multisets and every known typed field match independent local expectations. Includes unknown input content and canonical deletion version. Files are mutable UC objects; client overwrite false is not an immutability/fencing guarantee. Future100k stage must verify hashes/counts and pin immutable Delta input versions before publication use. No actual100k upload/stage/apply, production required-field/duplicate-key validation, source ACK or freshness claim. No data-table/volume/compute mutation.';save();print(json.dumps({'state':a['state'],'costs':a['costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same read-only query handles',error=str(e));save();raise
