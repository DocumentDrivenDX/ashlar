"""Bounded100k input transport into existing owned UC volume; no SQL/mutation apply."""
import json,hashlib,time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.files import FilesAPI
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.errors import NotFound
from normalized_input_sql_r264 import volume_query
def main():
 B=Path(__file__).resolve().parent;source_path=B/'out/ashlar_fifth_batch_files_r471/audited-summary.json';source=json.loads(source_path.read_text());admission=json.loads((B/'out/fifth-local-admission-r474.json').read_text());assert admission['accepted_files_sha256']==hashlib.sha256(source_path.read_bytes()).hexdigest();geometry=json.loads((B/'out/fifth-batch-oracle-r470.json').read_text());assert geometry['original_edges']==40000000 and geometry['source_position_interval']==[400000,500000];volume_path=B/'out/native/ashlar_input_volume_resume_r266/summary.json';volume=json.loads(volume_path.read_text());O=B/'out/native/ashlar_fifth_batch_upload_r475';assert not O.exists();O.mkdir();a={'state':'Inspecting existing owned volume before bounded100k upload','source_summary_sha256':hashlib.sha256(source_path.read_bytes()).hexdigest(),'volume_summary_sha256':hashlib.sha256(volume_path.read_bytes()).hexdigest(),'volume_full_name':volume['volume_full_name'],'volume_id':volume['volume']['volume_id'],'roles':{},'bounds':{'source_bytes':1200000000,'part_bytes':33554432,'wall_s':900},'uploaded_bytes':0,'download_verified_bytes':0};started=time.monotonic()
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def file_digest(path):
  h=hashlib.sha256()
  with path.open('rb') as f:
   for chunk in iter(lambda:f.read(1048576),b''):h.update(chunk)
  return h.hexdigest()
 save()
 try:
  assert source['state']=='Complete100k source files match independent role counts/bytes/fields/digests' and source['processed_changes']==100000 and source['source_bytes']<=1200000000;w=WorkspaceClient(profile='aidev-cus');api=FilesAPI(w.api_client);v=w.volumes.read(name=a['volume_full_name']);assert v.volume_id==a['volume_id'] and v.volume_type==VolumeType.MANAGED;a['volume_observed']=v.as_dict();save();root='/Volumes/'+a['volume_full_name'].replace('.','/')
  for role,r in source['roles'].items():
   info=a['roles'].setdefault(role,{'rows':r['rows'],'bytes':r['bytes'],'fields':r['fields'],'all_known_field_digest':r['all_known_field_digest'],'original_input_multiset_digest':r['original_input_multiset_digest'],'parts':[]})
   for p in r['parts']:
    assert time.monotonic()-started<900 and p['bytes']<=33554432;local=Path(source['local_root'])/p['name'];assert local.stat().st_size==p['bytes'] and file_digest(local)==p['file_sha256'];remote=root+'/'+role+'/batch100k-r475-'+p['name'];volume_query(role,remote);q={**p,'path':remote,'state':'Inspecting exact file path'};info['parts'].append(q);save()
    try:meta=w.files.get_metadata(file_path=remote);q['observation']='Already exists; never upload again'
    except NotFound:
     q['observation']='Authoritative NotFound';q['state']='Submitting one upload with overwrite false; inspect same path after unknown outcome';save()
     with local.open('rb') as f:api.upload(file_path=remote,contents=f,overwrite=False)
     a['uploaded_bytes']+=p['bytes'];meta=w.files.get_metadata(file_path=remote)
    assert meta.content_length==p['bytes'];q['remote_metadata']=meta.as_dict();q['state']='Verifying full downloaded bytes through workspace endpoint';save();rmt=api.download(file_path=remote);assert rmt.content_length==p['bytes'] and rmt.contents is not None;h=hashlib.sha256();count=0
    try:
     while True:
      chunk=rmt.contents.read(1048576)
      if not chunk:break
      count+=len(chunk);assert count<=p['bytes'];h.update(chunk)
    finally:rmt.contents.close()
    assert count==p['bytes'] and h.hexdigest()==p['file_sha256'];q['state']='Uploaded/retained input bytes independently match source SHA';a['download_verified_bytes']+=count;assert a['uploaded_bytes']<=1200000000 and a['download_verified_bytes']<=1200000000;a['elapsed_s']=time.monotonic()-started;save()
  assert a['download_verified_bytes']==source['source_bytes'];a['state']='All100k source-role parts roundtrip byte-exactly in owned UC volume';a['wall_s']=time.monotonic()-started;a['source_sha256']={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['fifth_batch_upload_r475.py','normalized_input_sql_r264.py','fifth_batch_files_r471.py']};a['qualification']='Actual1.10GB/35-part upload/download transport, no SQL table stage/apply/manifest or freshness claim. Source hashes checked before upload; exact volume UUID and every remote file length/full SHA verified. Same volume, no recreation or overwrite. API bytes not SQL counters;900s phase admission guard is not hard network deadline or price cap. UC files remain mutable; future input-stage field/count validation and immutable Delta version pinning required. Fifth distinct100k batch preserves original input JSON including explicit tombstone apply_batch_id extension; native staging and publication remain unmeasured. Full39.96M LC six-role reference separately qualified at r437.';save();print(json.dumps({'state':a['state'],'uploaded_bytes':a['uploaded_bytes'],'verified_bytes':a['download_verified_bytes'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect exact owned paths and partial manifest, no blind upload replay',error=str(e));save();raise
if __name__=='__main__':main()
