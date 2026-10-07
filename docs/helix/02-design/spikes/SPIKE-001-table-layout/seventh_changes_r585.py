"""Seventh disjoint100k exact synthetic changes; local oracle only."""
import collections,copy,hashlib,json,time
from pathlib import Path
from mixed_changes_r228 import Changes,verify
from mixed_history_r215 import compact
from mixed_change_queries_r230 import row_hash
B=Path(__file__).resolve().parent
class SeventhChanges:
 def __init__(self):self.original=Changes(8000000,40000000,700000)
 def change(self,index):
  if type(index) is not int or not 0<=index<100000:raise ValueError('Seventh batch index0..99999 required')
  x=self.original.change(index+600000);batch='mixed-change/7'
  if x['after'] is not None:x['after']['apply_batch_id']=batch
  for e in x['events']:e['apply_batch_id']=batch
  if x['tombstone'] is not None:x['tombstone'].update(apply_batch_id=batch,entity_version='2')
  envelope=json.loads(x['raw']['payload_json']);envelope['after_carrier']=x['after'];payload=compact(envelope);x['raw'].update(payload_json=payload,payload_digest=hashlib.sha256(payload.encode()).hexdigest(),apply_batch_id=batch);return x

def verify_seventh(x):
 verify(x);raw=x['raw'];position=int(json.loads(raw['source_cursor_json'])['seq']);assert 600000<=position<700000 and x['ordinal']==position*104729%40000000;assert raw['source_cursor_json']==compact({'xid':'9223372036854775809','seq':str(position)}) and raw['apply_batch_id']=='mixed-change/7';assert all(e['apply_batch_id']=='mixed-change/7' for e in x['events']);env=json.loads(raw['payload_json']);assert env.get('unknown_event')=={'keep':[None,True],'ordinal':str(x['ordinal'])}
 if x['after'] is not None:assert x['after']['apply_batch_id']=='mixed-change/7' and x['after']['source_cursor_json']==raw['source_cursor_json'] and x['after']['source_delivery_id']==raw['delivery_id']
 else:assert x['tombstone']['apply_batch_id']=='mixed-change/7' and x['tombstone']['entity_version']=='2'
 return True

def main():
 out=B/'out/seventh-batch-oracle-r585.json';assert not out.exists();started=time.monotonic();changes=SeventhChanges();roles={};hashes=collections.defaultdict(list);seen=set();counts=collections.Counter();inverse=pow(104729,-1,40000000)
 def add(role,row):
  info=roles.setdefault(role,{'fields':list(row),'rows':0,'utf8_jsonl_bytes':0});assert info['fields']==list(row);info['rows']+=1;info['utf8_jsonl_bytes']+=len((compact(row)+'\n').encode());hashes[role].append(row_hash(row,info['fields']))
 for i in range(100000):
  x=changes.change(i);assert verify_seventh(x) and x['ordinal'] not in seen and 600000<=x['ordinal']*inverse%40000000<700000;seen.add(x['ordinal']);add('source_record',x['raw'])
  for e in x['events']:add('property_journal',e);counts[e['operation']]+=1
  if x['after'] is None:add('tombstone',x['tombstone']);counts['deletes']+=1
  else:
   add('current_replacement',x['after']);counts['updates']+=1
   for side in ['source','target']:
    row=x['after'];node=changes.original.w.carrier('node',int(row[side+'_id'])-1);assert (row['source_system'],row[side+'_type'],row[side+'_id'])==(node['source_system'],node['type_id'],node['id'])
 assert counts['updates']==90000 and counts['deletes']==10000 and len(seen)==100000
 for role,r in roles.items():r['all_field_multiset_digest']=hashlib.sha256(''.join(sorted(hashes[role])).encode()).hexdigest()
 controls=[]
 def refuse(label,mutate):
  x=changes.change(1);mutate(x)
  try:verify_seventh(x)
  except AssertionError:controls.append({'control':label,'result':'refused'})
  else:raise AssertionError('Accepted corrupted input: '+label)
 def reencode(x,env):
  payload=compact(env);x['raw'].update(payload_json=payload,payload_digest=hashlib.sha256(payload.encode()).hexdigest())
 refuse('wrong event batch',lambda x:x['events'][0].update(apply_batch_id='mixed-change/2'))
 def scientific(x):next(e for e in x['events'] if e['property_id']=='902')['new_json']='12300'
 refuse('same numeric value different token spelling',scientific)
 def retained(x):x['after']['retained_json']='{}';env=json.loads(x['raw']['payload_json']);env['after_carrier']=x['after'];reencode(x,env)
 refuse('retained content dropped with recomputed raw SHA',retained)
 def unknown(x):env=json.loads(x['raw']['payload_json']);env.pop('unknown_event');reencode(x,env)
 refuse('unknown envelope dropped with recomputed raw SHA',unknown)
 def cursor(x):
  new=compact({'xid':'9223372036854775808','seq':str(300001)});x['raw']['source_cursor_json']=new;x['after']['source_cursor_json']=new
  for e in x['events']:e['source_cursor_json']=new
  env=json.loads(x['raw']['payload_json']);env['after_carrier']=x['after'];env['source_cursor_json']=new;reencode(x,env)
 refuse('large cursor value changed consistently',cursor)
 result={'format':'ashlar-seventh-distinct-batch/1','state':'All100k seventh-batch mutations replay exactly and avoid all six prior batches','nodes':8000000,'original_edges':40000000,'before_live_edges':39940000,'after_live_edges':39930000,'batch_id':'mixed-change/7','source_position_interval':[600000,700000],'roles':roles,'counts':dict(counts),'selected_identities':len(seen),'disjointness':'Invertible104729 permutation modulo40M; indexes600000..699999 cannot intersect prior0..599999. Native full predecessor qualification still required.','negative_controls':controls,'source_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['seventh_changes_r526.py','mixed_changes_r228.py','mixed_history_r215.py','mixed_change_queries_r230.py']},'wall_s':time.monotonic()-started,'qualification':'Local distinct version1-to2 edge mutation oracle only. Exact token replay, envelopes, typed endpoints and source cursor tested; no native stage/publisher/source ACK, real authority, version2-to3 conflicts, freshness or billion admission.'};out.write_text(json.dumps(result,indent=2)+'\n');print(result['state'],dict(counts),result['wall_s'])
if __name__=='__main__':main()
