"""Offline terminal fifth-batch upload custody and byte-accounting audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_fifth_batch_upload_r475';a=json.loads((p/'summary.json').read_text());assert a['state']=='All100k source-role parts roundtrip byte-exactly in owned UC volume';source=B/'out/ashlar_fifth_batch_files_r471/audited-summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_summary_sha256'];s=json.loads(source.read_text());assert a['volume_observed']['volume_id']==a['volume_id'] and a['volume_observed']['volume_type']=='MANAGED'
 total=0;paths=set();parts=0
 for role,r in a['roles'].items():
  expected=s['roles'][role];assert r['rows']==expected['rows'] and r['bytes']==expected['bytes'] and r['fields']==expected['fields'] and r['all_known_field_digest']==expected['all_known_field_digest'] and r['original_input_multiset_digest']==expected['original_input_multiset_digest'];assert len(r['parts'])==len(expected['parts'])
  for part,old in zip(r['parts'],expected['parts']):
   assert part['state']=='Uploaded/retained input bytes independently match source SHA' and all(part[k]==old[k] for k in ['name','rows','bytes','file_sha256']);assert part['remote_metadata']['content-length']==part['bytes'];assert part['path']=='/Volumes/'+a['volume_full_name'].replace('.','/')+'/'+role+'/batch100k-r475-'+part['name'];assert part['path'] not in paths;paths.add(part['path']);total+=part['bytes'];parts+=1
 assert total==a['download_verified_bytes']==s['source_bytes'] and parts==35 and a['uploaded_bytes']<=total<=a['bounds']['source_bytes'];assert a['wall_s']<a['bounds']['wall_s']
 for name,h in a['source_sha256'].items():assert hashlib.sha256((B/name).read_bytes()).hexdigest()==h
 a['audit']={'source_summary_sha256':hashlib.sha256((p/'summary.json').read_bytes()).hexdigest(),'exact_downloaded_bytes':total,'unique_owned_paths':parts,'metadata_note':'SDK content_length attribute is serialized as content-length; audit checks that exact header key and complete terminal per-file byte accounting.','qualification':'Offline receipt verifies terminal per-file roundtrip checks and full accounting; no new transfer. Local/remote files remain mutable; native original-input/field digests and exact version pins are still required.'};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
