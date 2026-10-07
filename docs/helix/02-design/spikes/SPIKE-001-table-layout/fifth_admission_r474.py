"""Bind exact40M fixture geometry; reject mismatched predecessor universe."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent
old=B/'out/fifth-batch-oracle-r466.json';bad=json.loads(old.read_text());assert bad['original_edges']==50000000
new=B/'out/fifth-batch-oracle-r470.json';good=json.loads(new.read_text());assert good['original_edges']==40000000 and good['nodes']==8000000 and good['source_position_interval']==[400000,500000] and good['before_live_edges']==39960000 and good['after_live_edges']==39950000 and good['selected_identities']==100000
assert good['counts']['updates']==90000 and good['counts']['deletes']==10000 and len(good['negative_controls'])==5
files=B/'out/ashlar_fifth_batch_files_r471/audited-summary.json';f=json.loads(files.read_text());assert f['oracle_sha256']==hashlib.sha256(new.read_bytes()).hexdigest() and f['processed_changes']==100000 and f['parts']==35 and f['source_bytes']<=1200000000
for n,d in good['source_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==d
cdf=B/'out/fifth-cdf-image-oracle-r473.json';cf=json.loads(cdf.read_text());assert all(r['total_rows']==190000 for r in cf['roles'].values());tomb=B/'out/fifth-canonical-tomb-r473.json';assert json.loads(tomb.read_text())['rows']==10000
root=Path(f['local_root'])
for role,r in f['roles'].items():
 assert r['rows']==good['roles'][role]['rows'] and f['audit']['roles'][role]['all_known_field_digest']==good['roles'][role]['all_field_multiset_digest']
 for part in r['parts']:assert hashlib.sha256((root/part['name']).read_bytes()).hexdigest()==part['file_sha256']
out={'state':'Corrected fifth100k local input geometry and complete byte/field/CDF bindings pass; native transfer/stage/publication unexecuted','accepted_oracle_sha256':hashlib.sha256(new.read_bytes()).hexdigest(),'accepted_files_sha256':hashlib.sha256(files.read_bytes()).hexdigest(),'rejected_attempt':{'oracle_sha256':hashlib.sha256(old.read_bytes()).hexdigest(),'original_edges':50000000,'reason':'Broad text substitution changed40M modulus to50M. Local replay checks self-consistently passed against wrong universe; never uploaded/applied. No preservation/publication support claim.','local_root':'/private/tmp/ashlar-fifth-input-r467','admissible':False},'roles':{k:r['rows'] for k,r in f['roles'].items()},'source_bytes':f['source_bytes'],'parts':f['parts'],'qualification':'Exact corrected local source only; geometry guard now explicit. Later native complete digests, target heads/UUIDs, inline predecessor guard and six-role custody remain required. No source arrival/freshness/real authority proof.'};(B/'out/fifth-local-admission-r474.json').write_text(json.dumps(out,indent=2)+'\n');print(out)
