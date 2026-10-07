"""Distinct second synthetic batch; no native writes or input file upload."""
import json,hashlib,collections,time,copy
from pathlib import Path
from mixed_changes_r228 import Changes,verify
from mixed_history_r215 import compact
from mixed_change_queries_r230 import row_hash
B=Path(__file__).resolve().parent
class SecondChanges:
 def __init__(self):self.original=Changes(8000000,40000000,200000)
 def change(self,index):
  if type(index) is not int or not 0<=index<100000:raise ValueError('Second batch index0..99999 required')
  x=self.original.change(index+100000);batch='mixed-change/2'
  if x['after'] is not None:x['after']['apply_batch_id']=batch
  for event in x['events']:event['apply_batch_id']=batch
  if x['tombstone'] is not None:x['tombstone'].update(apply_batch_id=batch,entity_version='2')
  envelope=json.loads(x['raw']['payload_json']);envelope['after_carrier']=x['after'];payload=compact(envelope);x['raw'].update(payload_json=payload,payload_digest=hashlib.sha256(payload.encode()).hexdigest(),apply_batch_id=batch);return x

def verify_second(x):
 verify(x);assert x['raw']['apply_batch_id']=='mixed-change/2';assert all(e['apply_batch_id']=='mixed-change/2' for e in x['events'])
 if x['after'] is not None:assert x['after']['apply_batch_id']=='mixed-change/2' and x['after']['source_cursor_json']==x['raw']['source_cursor_json'] and x['after']['source_delivery_id']==x['raw']['delivery_id']
 else:assert x['tombstone']['apply_batch_id']=='mixed-change/2' and x['tombstone']['entity_version']=='2'
 position=int(json.loads(x['raw']['source_cursor_json'])['seq']);assert 100000<=position<200000;assert x['ordinal']==position*104729%40000000
 return True

def main():
 out=B/'out/second-batch-oracle-r318.json';assert not out.exists();started=time.monotonic();changes=SecondChanges();roles={};hashes=collections.defaultdict(list);seen=set();counts=collections.Counter();inverse=pow(104729,-1,40000000)
 def add(role,row):
  r=roles.setdefault(role,{'fields':list(row),'rows':0,'utf8_jsonl_bytes':0});assert r['fields']==list(row);r['rows']+=1;r['utf8_jsonl_bytes']+=len((compact(row)+'\n').encode());hashes[role].append(row_hash(row,r['fields']))
 for i in range(100000):
  x=changes.change(i);assert verify_second(x) and x['ordinal'] not in seen and x['ordinal']*inverse%40000000>=100000;seen.add(x['ordinal']);add('source_record',x['raw'])
  for e in x['events']:add('property_journal',e);counts[e['operation']]+=1
  if x['after'] is None:add('tombstone',x['tombstone']);counts['deletes']+=1
  else:add('current_replacement',x['after']);counts['updates']+=1
 assert len(seen)==100000 and counts['updates']==90000 and counts['deletes']==10000
 for role,r in roles.items():r['all_field_multiset_digest']=hashlib.sha256(''.join(sorted(hashes[role])).encode()).hexdigest()
 corrupt=changes.change(1);corrupt['events'][0]['apply_batch_id']='mixed-change/1'
 try:verify_second(corrupt)
 except AssertionError:negative='refused'
 else:raise AssertionError('Wrong batch metadata accepted')
 result={'format':'ashlar-second-distinct-batch/1','state':'All100k second-batch mutations replay exactly and avoid first-batch identities','nodes':8000000,'original_edges':40000000,'before_live_edges':39990000,'after_live_edges':39980000,'batch_id':'mixed-change/2','source_position_interval':[100000,200000],'roles':roles,'counts':dict(counts),'selected_identities':len(seen),'first_batch_disjointness':'Permutation invertible modulo40M; selected indexes100000..199999 cannot intersect first0..99999. Native full predecessor verification still required.','negative_control':{'wrong_event_batch':negative},'source_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['second_changes_r318.py','mixed_changes_r228.py','mixed_change_queries_r230.py','mixed_history_r215.py']},'wall_s':time.monotonic()-started,'qualification':'Local second distinct version1-to2 mutation oracle only; not version2-to3 conflict/replay, actual producer, immutable native inputs, ingest/publication or freshness evidence. Exact props/retained strings, raw payload SHA, property event tokens and typed identity preserved. No native upload/mutation/ACK or billion admission.'};out.write_text(json.dumps(result,indent=2)+'\n');print(result['state'],dict(counts),result['wall_s'])
if __name__=='__main__':main()
