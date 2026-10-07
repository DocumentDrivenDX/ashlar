"""Small owned UC input volume, exact source byte roundtrip; no SQL workload."""
import json,hashlib,time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.catalog import VolumeType
from mixed_changes_r228 import Changes,verify
from mixed_history_r215 import compact
from mixed_change_queries_r230 import row_hash
from normalized_input_sql_r264 import role_row,volume_query
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_input_volume_r265';assert not O.exists();O.mkdir();catalog='client_dev';schema='ashlar_entropy_20261006_r86';name='ashlar_inputs_r265';full=catalog+'.'+schema+'.'+name;root='/Volumes/'+catalog+'/'+schema+'/'+name;a={'state':'Preparing tiny source files','volume_full_name':full,'files':{},'bound_total_bytes':2000000,'clock':'Workspace API upload/byte roundtrip only; no SQL'};started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
try:
 ch=Changes(8000000,40000000,16)
 for role in ['source_record','property_journal','tombstone','current_replacement']:
  rows=[];inputs=[];fields=list(role_row(role))
  for i in range(16):
   x=ch.change(i)
   if x['tombstone'] is not None:x['tombstone']['entity_version']='2'
   assert verify(x)
   candidates=[x['raw']] if role=='source_record' else x['events'] if role=='property_journal' else [x['tombstone']] if role=='tombstone' and x['tombstone'] is not None else [x['after']] if role=='current_replacement' and x['after'] is not None else []
   for row in candidates:
    assert list(row)==fields;rows.append(row);inputs.append(compact({**row,'unknown_input':{'big':'9223372036854775809','opaque':[None,'雪','\\u0041']}}))
  local=O/(role+'.jsonl');data=('\n'.join(inputs)+'\n').encode();local.write_bytes(data);path=root+'/'+role+'/sample.jsonl';volume_query(role,path)
  a['files'][role]={'local_file':local.name,'path':path,'rows':len(rows),'fields':fields,'bytes':len(data),'file_sha256':hashlib.sha256(data).hexdigest(),'input_multiset_digest':hashlib.sha256(''.join(sorted(hashlib.sha256(x.encode()).hexdigest() for x in inputs)).encode()).hexdigest(),'known_field_multiset_digest':hashlib.sha256(''.join(sorted(row_hash(row,fields) for row in rows)).encode()).hexdigest(),'state':'Prepared'}
 a['source_bytes']=sum(f['bytes'] for f in a['files'].values());assert a['source_bytes']<=2000000;save();w=WorkspaceClient(profile='aidev-cus');assert not any(v.name==name for v in w.volumes.list(catalog_name=catalog,schema_name=schema));a['state']='Creating uniquely named owned managed volume; inspect same name after unknown outcome';save()
 v=w.volumes.create(catalog_name=catalog,schema_name=schema,name=name,volume_type=VolumeType.MANAGED,comment='Ashlar r265 synthetic exact-input transport fixture; no production data');a['volume']=v.as_dict();save();observed=w.volumes.read(name=full);assert observed.volume_id==v.volume_id and observed.volume_type==VolumeType.MANAGED;a['volume_readback']=observed.as_dict();save()
 for role,f in a['files'].items():
  w.files.create_directory(directory_path=root+'/'+role);f['state']='Uploading once with overwrite false; inspect same path after unknown outcome';save()
  with (O/f['local_file']).open('rb') as source:w.files.upload(file_path=f['path'],contents=source,overwrite=False,use_parallel=False)
  meta=w.files.get_metadata(file_path=f['path']);assert meta.content_length==f['bytes'];f['remote_metadata']=meta.as_dict();save();r=w.files.download(file_path=f['path']);assert r.content_length==f['bytes'] and r.contents is not None;remote=r.contents.read(f['bytes']+1);r.contents.close();assert len(remote)==f['bytes'] and hashlib.sha256(remote).hexdigest()==f['file_sha256'];f['state']='Uploaded bytes roundtrip exactly';save()
 a['state']='All tiny role files uploaded and downloaded byte-exactly';a['wall_s']=time.monotonic()-started;a['source_sha256']={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['normalized_volume_upload_r265.py','normalized_input_sql_r264.py','mixed_changes_r228.py','mixed_change_queries_r230.py','mixed_history_r215.py']};a['qualification']='Owned MANAGED UC volume creation and four small synthetic JSONL uploads with overwrite disabled, length/full-file SHA and download byte roundtrip. No SQL reader/file framing, Delta stage/apply/manifest, real producer custody or100k transport/throughput claim. Local source bytes retained; no deletion/VACUUM/compute operation.';save();print(json.dumps({'state':a['state'],'bytes':a['source_bytes'],'wall_s':a['wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect owned volume name and exact file paths; never blindly overwrite/recreate',error=str(e));save();raise
