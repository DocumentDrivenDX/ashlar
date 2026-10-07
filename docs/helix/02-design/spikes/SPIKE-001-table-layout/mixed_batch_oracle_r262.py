"""Local100k-change oracle for planned8M-node/40M-edge native publication."""
import json,hashlib,time,collections,copy
from pathlib import Path
from mixed_changes_r228 import Changes,verify
from mixed_change_queries_r230 import row_hash
from mixed_history_r215 import compact
B=Path(__file__).resolve().parent
started=time.monotonic();changes=Changes(8000000,40000000,100000)
roles={};fields={};hashes=collections.defaultdict(list);counts=collections.Counter();seen=set();buckets=collections.Counter();ordinal_digest=hashlib.sha256()
def add(role,row):
 fields.setdefault(role,list(row));assert list(row)==fields[role]
 r=roles.setdefault(role,{'rows':0,'utf8_jsonl_bytes':0});r['rows']+=1;r['utf8_jsonl_bytes']+=len((compact(row)+'\n').encode());hashes[role].append(row_hash(row,fields[role]))
for i in range(changes.count):
 x=changes.change(i)
 # Canonical0.3 tombstones retain deletion entity_version; old small CTAS sample omitted it.
 if x['tombstone'] is not None:x['tombstone']['entity_version']='2'
 assert verify(x) and x['ordinal'] not in seen;seen.add(x['ordinal']);buckets[x['ordinal']//100000]+=1;ordinal_digest.update((str(x['ordinal'])+'\n').encode())
 add('source_record',x['raw'])
 for e in x['events']:
  add('property_journal',e);counts[e['operation']]+=1;counts['explicit_null_set']+=e['new_present'] and e['new_json']=='null'
 if x['tombstone'] is not None:add('tombstone',x['tombstone']);counts['deletes']+=1
 else:add('current_replacement',x['after']);counts['updates']+=1
 for side in ['source','target']:
  n=changes.w.carrier('node',int(x['before'][side+'_id'])-1);assert (x['before']['source_system'],x['before'][side+'_type'],x['before'][side+'_id'])==(n['source_system'],n['type_id'],n['id'])
assert len(seen)==100000 and counts['deletes']==10000 and counts['updates']==90000
for role in roles:roles[role].update(fields=fields[role],all_field_multiset_digest=hashlib.sha256(''.join(sorted(hashes[role])).encode()).hexdigest())
controls={}
def refusal(name,x):
 try:verify(x)
 except AssertionError:controls[name]='rejected';return
 raise AssertionError('Malformed control accepted: '+name)
x=changes.change(0);x['tombstone']['entity_version']='3';refusal('wrong_deletion_version',x)
x=changes.change(1);x['raw']['payload_digest']='0'*64;refusal('wrong_raw_digest',x)
x=changes.change(1);x['after']['props_json']=x['after']['props_json'].replace('1.2300e+04','12300');env=json.loads(x['raw']['payload_json']);env['after_carrier']=x['after'];x['raw']['payload_json']=compact(env);x['raw']['payload_digest']=hashlib.sha256(x['raw']['payload_json'].encode()).hexdigest();refusal('decimal_lexeme_drift_with_valid_payload_sha',x)
x=changes.change(1);x['after']['retained_json']='{}';env=json.loads(x['raw']['payload_json']);env['after_carrier']=x['after'];x['raw']['payload_json']=compact(env);x['raw']['payload_digest']=hashlib.sha256(x['raw']['payload_json'].encode()).hexdigest();refusal('retained_content_drift_with_valid_payload_sha',x)
a={'format':'ashlar-large-mixed-change-oracle/1','state':'All100k unique changes replay exactly with retained content and typed endpoint closure','base':{'nodes':8000000,'edges':40000000,'workload':'ashlar-scale-mixed/1'},'changes':100000,'negative_controls':controls,'counts':dict(counts),'roles':roles,'ordinal_stream_sha256':ordinal_digest.hexdigest(),'selected_100k_buckets':dict(sorted(buckets.items())),'wall_s':time.monotonic()-started,'source_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['mixed_batch_oracle_r262.py','mixed_changes_r228.py','mixed_change_queries_r230.py','scale_mixed_r219.py','mixed_history_r215.py']},'qualification':'Local deterministic100k mixed update/delete oracle over planned full40M-edge base; does not assert native40M graph exists. Canonical tombstone entity_version included; old extract/sample adapter cannot be reused unchanged. Exact old/new token replay, raw payload SHA/unknown content and both selected typed endpoints checked; compact digests assume SHA256 collision resistance. No native staging, apply, manifest, fencing/ACK, latency, compression, price, sustained10k/s or100k/s burst evidence. JSONL bytes are uncompressed role payload estimates, not actual transport or Delta storage.'}
(B/'out/mixed-batch-oracle-r262.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({k:a[k] for k in ['state','counts','wall_s']},indent=2))
