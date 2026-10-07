"""Stream exact100k normalized-role JSONL to bounded local parts; no native writes."""
import json,hashlib,time,collections
from pathlib import Path
from sixth_changes_r526 import SixthChanges,verify_sixth
from mixed_history_r215 import compact
from mixed_change_queries_r230 import row_hash
def main():
 B=Path(__file__).resolve().parent;O=B/'out/ashlar_sixth_batch_files_r527';D=Path('/private/tmp/ashlar-sixth-input-r527');assert not O.exists() and not D.exists();O.mkdir();D.mkdir();cal_path=B/'out/sixth-batch-oracle-r526.json';cal=json.loads(cal_path.read_text());a={'state':'Generating bounded local100k source files','local_root':str(D),'oracle_sha256':hashlib.sha256(cal_path.read_bytes()).hexdigest(),'bounds':{'total_bytes':1200000000,'part_bytes':33554432,'row_bytes':1048576},'roles':{},'processed_changes':0};started=time.monotonic();streams={};field_hashes=collections.defaultdict(list);input_hashes=collections.defaultdict(list)
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def close(role):
  if role in streams:
   stream,h,part=streams.pop(role);stream.close();part['file_sha256']=h.hexdigest();part['state']='Closed exact UTF-8 JSONL part'
 def add(role,row):
  info=a['roles'].setdefault(role,{'fields':list(row),'rows':0,'bytes':0,'parts':[]});assert list(row)==info['fields'];line=compact(row);data=(line+'\n').encode();assert len(data)<=1048576
  if role in streams and streams[role][2]['bytes']+len(data)>33554432:close(role)
  if role not in streams:
   name=role+'-part'+str(len(info['parts'])).zfill(4)+'.jsonl';path=D/name;part={'name':name,'bytes':0,'rows':0,'state':'Writing'};info['parts'].append(part);streams[role]=(path.open('xb'),hashlib.sha256(),part)
  stream,h,part=streams[role];stream.write(data);h.update(data);part['bytes']+=len(data);part['rows']+=1;info['bytes']+=len(data);info['rows']+=1;assert sum(r['bytes'] for r in a['roles'].values())<=1200000000;field_hashes[role].append(row_hash(row,info['fields']));input_hashes[role].append(hashlib.sha256(line.encode()).hexdigest())
 try:
  changes=SixthChanges()
  for i in range(100000):
   x=changes.change(i)
   if x['tombstone'] is not None:x['tombstone']['entity_version']='2'
   assert verify_sixth(x);add('source_record',x['raw'])
   for e in x['events']:add('property_journal',e)
   if x['tombstone'] is not None:add('tombstone',x['tombstone'])
   else:add('current_replacement',x['after'])
   a['processed_changes']=i+1
   if (i+1)%10000==0:a['elapsed_s']=time.monotonic()-started;save()
  for role in list(streams):close(role)
  for role,r in a['roles'].items():
   expected=cal['roles'][role];r['all_known_field_digest']=hashlib.sha256(''.join(sorted(field_hashes[role])).encode()).hexdigest();r['original_input_multiset_digest']=hashlib.sha256(''.join(sorted(input_hashes[role])).encode()).hexdigest();assert r['rows']==expected['rows'] and r['bytes']==expected['utf8_jsonl_bytes'] and r['fields']==expected['fields'] and r['all_known_field_digest']==expected['all_field_multiset_digest'];assert sum(p['rows'] for p in r['parts'])==r['rows'] and sum(p['bytes'] for p in r['parts'])==r['bytes']
  a['source_bytes']=sum(r['bytes'] for r in a['roles'].values());a['parts']=sum(len(r['parts']) for r in a['roles'].values());a['wall_s']=time.monotonic()-started;a['source_sha256']={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['sixth_batch_files_r527.py','sixth_changes_r526.py','mixed_changes_r228.py','mixed_change_queries_r230.py','mixed_history_r215.py']};a['state']='Complete100k source files match independent role counts/bytes/fields/digests';a['qualification']='Local exact JSONL parts only, source fields/payload/cursors/token spelling retained; no native upload/stage/apply/manifest or freshness claim. Data remains outside repository; compact manifest and code are reviewable. Sixth distinct batch over39.95M live edges; native target preflight remains required. Tombstone JSON includes an additional synthetic apply_batch_id metadata field beyond canonical CONTRACT-003 columns; full original input_json must retain it, and future native input schema/projection must explicitly qualify this extension before applying. It is not an implicit canonical DDL change. Files can be altered locally: reverify hashes before upload; volume overwrite false is not immutable source custody. Native input must later be validated and pinned at exact Delta versions.';save();print(json.dumps({'state':a['state'],'bytes':a['source_bytes'],'parts':a['parts'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:
  for role in list(streams):close(role)
  a.update(state='Stopped; inspect exact local parts before any native upload',error=str(e));save();raise
if __name__=='__main__':main()
