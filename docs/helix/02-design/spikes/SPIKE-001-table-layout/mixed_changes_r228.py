"""Deterministic exact mixed property changes and edge lifecycle deletion oracle."""
import json,hashlib,math,collections
from pathlib import Path
from scale_mixed_r219 import Workload,SEED
from mixed_history_r215 import tokens,compact
class Changes:
 def __init__(self,nodes=4096,edges=20480,count=2048):
  self.w=Workload(nodes,edges)
  if type(count) is not int or not 0<=count<=edges or math.gcd(104729,edges)!=1:raise ValueError('Unique edge permutation and valid count required')
  self.count=count
 def change(self,index):
  if type(index) is not int or not 0<=index<self.count:raise ValueError('Invalid change range')
  ordinal=index*104729%self.w.edges;before=self.w.carrier('edge',ordinal);delete=index%10==0;after=None
  delivery=f'edge:{ordinal}:2';cursor=compact({'xid':'9223372036854775809','seq':str(index)})
  if not delete:
   after=before.copy();values=tokens(before['props_json']);opaque=json.loads(values['901']);replacement=''.join(hashlib.sha256(f'{SEED}:change:{ordinal}:{j}'.encode()).hexdigest() for j in range((len(opaque)+63)//64))[:len(opaque)];values['901']=compact(replacement)
   if index%3==0:
    removable=[key for key in values if key!='901']
    if removable:values.pop(removable[0])
   # Explicit-null additions exercise presence independent of JSON null.
   values['902']='null' if index%2==0 else '1.2300e+04'
   after.update(props_json='{'+','.join(compact(k)+':'+v for k,v in values.items())+'}',entity_version='2',published_at='2026-10-07T12:00:00Z',apply_batch_id='mixed-change/1',source_cursor_json=cursor,source_delivery_id=delivery)
  envelope={'profile':'synthetic-mixed-change/1','operation':'delete' if delete else 'update','kind':'edge','before_carrier':before,'after_carrier':after,'source_cursor_json':cursor,'unknown_event':{'keep':[None,True],'ordinal':str(ordinal)}}
  payload=compact(envelope);raw={'source_feed':before['source_feed'],'source_epoch':before['source_epoch'],'delivery_id':delivery,'record_kind':'synthetic-full-change','source_cursor_json':cursor,'payload_json':payload,'payload_digest':hashlib.sha256(payload.encode()).hexdigest(),'schema_revision':before['schema_revision'],'received_at':'2026-10-07T12:00:00Z','apply_batch_id':'mixed-change/1'}
  common={'source_system':before['source_system'],'entity_kind':'edge','type_id':before['rel_type_id'],'id':before['id'],'entity_version':'2','schema_revision':before['schema_revision'],'source_feed':before['source_feed'],'source_epoch':before['source_epoch'],'source_position':None,'source_time_text':None,'published_at':'2026-10-07T12:00:00Z','apply_batch_id':'mixed-change/1','source_cursor_json':cursor,'source_delivery_id':delivery}
  events=[];tombstone=None
  if delete:
   # Synthetic lifecycle marker, never interpreted as a null-valued property.
   events.append(dict(common,property_id=None,operation='delete',old_present=False,old_json=None,new_present=False,new_json=None,event_ordinal='0'))
   tombstone={k:common[k] for k in ['source_system','entity_kind','type_id','id','source_feed','source_epoch','source_position','source_cursor_json','source_delivery_id']}
  else:
   old=tokens(before['props_json']);new=tokens(after['props_json'])
   for key in sorted(set(old)|set(new),key=int):
    if key in old and key in new and old[key]==new[key]:continue
    events.append(dict(common,property_id=key,operation='set' if key in new else 'remove',old_present=key in old,old_json=old.get(key),new_present=key in new,new_json=new.get(key),event_ordinal=str(len(events))))
  return {'ordinal':ordinal,'before':before,'after':after,'raw':raw,'events':events,'tombstone':tombstone}
def verify(change):
 before=change['before'];after=change['after'];raw=change['raw'];envelope=json.loads(raw['payload_json']);assert envelope['before_carrier']==before and envelope['after_carrier']==after and hashlib.sha256(raw['payload_json'].encode()).hexdigest()==raw['payload_digest'];state=tokens(before['props_json'])
 for i,e in enumerate(change['events']):
  assert e['event_ordinal']==str(i) and e['source_delivery_id']==raw['delivery_id'] and e['source_cursor_json']==raw['source_cursor_json']
  assert e['source_system']==before['source_system'] and e['type_id']==before['rel_type_id'] and e['id']==before['id'] and e['entity_kind']=='edge' and e['entity_version']=='2'
  if e['property_id'] is None:
   assert after is None and e['operation']=='delete' and change['tombstone'] is not None and not e['old_present'] and not e['new_present'] and e['old_json'] is None and e['new_json'] is None
   assert change['tombstone']=={k:e[k] for k in change['tombstone']}
   continue
  key=e['property_id'];assert (key in state)==e['old_present'] and state.get(key)==e['old_json']
  if e['new_present']:state[key]=e['new_json']
  else:state.pop(key)
 if after is not None:
  assert state==tokens(after['props_json']) and change['tombstone'] is None
  for key in ['source_system','rel_type_id','id','lookup_hash','source_type','source_id','target_type','target_id','retained_json']:assert before[key]==after[key]
 else:assert len(change['events'])==1
 return True
def calibrate():
 c=Changes();seen=set();roles=collections.defaultdict(lambda:{'rows':0,'utf8_jsonl_bytes':0});digests={};counts=collections.Counter();expected_edges={i:c.w.carrier('edge',i) for i in range(c.w.edges)}
 def add(role,row):
  data=(compact(row)+'\n').encode();roles[role]['rows']+=1;roles[role]['utf8_jsonl_bytes']+=len(data);digests.setdefault(role,hashlib.sha256()).update(data)
 for i in range(c.count):
  change=c.change(i);assert verify(change) and change['ordinal'] not in seen;seen.add(change['ordinal']);add('source_record',change['raw'])
  for e in change['events']:add('property_journal',e);counts[e['operation']]+=1;counts['explicit_null_set']+=e['new_present'] and e['new_json']=='null'
  if change['after'] is None:add('tombstone',change['tombstone']);expected_edges.pop(change['ordinal']);counts['edge_deletes']+=1
  else:add('current_replacement',change['after']);expected_edges[change['ordinal']]=change['after'];counts['edge_updates']+=1
 # All resulting endpoint references still resolve to unchanged node allocation.
 for r in expected_edges.values():
  for side in ['source','target']:
   n=c.w.carrier('node',int(r[side+'_id'])-1);assert (r['source_system'],r[side+'_type'],r[side+'_id'])==(n['source_system'],n['type_id'],n['id'])
 for name,r in roles.items():r['sha256_stream']=digests[name].hexdigest()
 return {'state':'All scattered changes replay exactly with complete raw evidence and final endpoint closure','changed_edges':c.count,'unique_selected_ordinals':len(seen),'final_edge_rows':len(expected_edges),'roles':dict(roles),'counts':dict(counts),'scope':'Deterministic edge-only update/delete batch; first raw predecessor, exact token history and delete tombstone for every selected entity. Nodes unchanged. Lifecycle marker property_id NULL/delete is proposed synthetic vocabulary. No cascade, source ACK/fencing, native mutation, publication, compression or throughput admission.'}
if __name__=='__main__':
 b=Path(__file__).resolve().parent;r=calibrate();(b/'out/mixed-changes-r228.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r,indent=2))
