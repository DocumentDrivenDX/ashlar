"""Resume exact owned volume files; workspace-proxied public download avoids cloud DNS."""
import json,hashlib,time
from pathlib import Path
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.files import FilesAPI
from databricks.sdk.service.catalog import VolumeType
from databricks.sdk.errors import NotFound
B=Path(__file__).resolve().parent;prior=B/'out/native/ashlar_input_volume_r265';O=B/'out/native/ashlar_input_volume_resume_r266';assert not O.exists();O.mkdir();a=json.loads((prior/'summary.json').read_text());a['prior_summary_sha256']=hashlib.sha256((prior/'summary.json').read_bytes()).hexdigest();a['state']='Inspecting exact prior volume and file states; no recreation/overwrite';started=time.monotonic()
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
try:
 assert a['source_bytes']<=2000000;w=WorkspaceClient(profile='aidev-cus');v=w.volumes.read(name=a['volume_full_name']);assert v.volume_id==a['volume']['volume_id'] and v.volume_type==VolumeType.MANAGED;a['volume_resume_readback']=v.as_dict();save();api=FilesAPI(w.api_client)
 for role,f in a['files'].items():
  data=(prior/f['local_file']).read_bytes();assert len(data)==f['bytes'] and hashlib.sha256(data).hexdigest()==f['file_sha256']
  try:meta=w.files.get_metadata(file_path=f['path']);f['resume_observation']='Already exists; do not replay upload'
  except NotFound:
   f['resume_observation']='Authoritative NotFound; upload once with overwrite false';save();w.files.create_directory(directory_path=f['path'].rsplit('/',1)[0])
   with (prior/f['local_file']).open('rb') as source:w.files.upload(file_path=f['path'],contents=source,overwrite=False,use_parallel=False)
   meta=w.files.get_metadata(file_path=f['path'])
  assert meta.content_length==f['bytes'];f['remote_metadata']=meta.as_dict();save();r=api.download(file_path=f['path']);assert r.contents is not None and r.content_length==f['bytes'];remote=r.contents.read(f['bytes']+1);r.contents.close();assert len(remote)==f['bytes'] and hashlib.sha256(remote).hexdigest()==f['file_sha256'];f['state']='Exact source bytes roundtrip through public workspace FilesAPI.download';save()
 a.pop('error',None);a['state']='All tiny role files roundtrip byte-exactly in original owned volume';a['wall_s']=time.monotonic()-started;a['source_sha256']={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['normalized_volume_resume_r266.py','normalized_volume_upload_r265.py','normalized_input_sql_r264.py']};a['qualification']='Same owned UC volume UUID, initial raw file reused without upload; other paths only uploaded after authoritative NotFound, all overwrite false. Public FilesAPI download uses workspace endpoint rather than FilesExt presigned cloud DNS path; full byte lengths and SHA match. No SQL reader/file framing, native Delta stage/apply/manifest,100k throughput, producer custody or compute mutation claim. Original failed upload-controller evidence retained; no recreation/expiry/deletion.';save();print(json.dumps({'state':a['state'],'bytes':a['source_bytes'],'wall_s':a['wall_s']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same owned file paths/UUID without blind overwrite',error=str(e));save();raise
