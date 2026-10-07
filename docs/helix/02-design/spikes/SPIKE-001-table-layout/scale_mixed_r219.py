"""Streaming deterministic complete synthetic bootstrap; no native admission."""
import json,hashlib,time,collections
from pathlib import Path
from mixed_history_r215 import tokens,compact
SEED='ashlar-scale-mixed/1'
def source(ordinal):return 'pilot-b' if ordinal%5==0 else 'pilot-a'
def node_key(ordinal):return (source(ordinal),str(1+ordinal%4),str(ordinal+1))
def endpoint(source_system,index,nodes):
 # Direct indexing into source-specific node allocation; no billion-element pool.
 if source_system=='pilot-b':ordinal=5*(index%((nodes+4)//5))
 else:
  count=nodes-(nodes+4)//5;k=index%count;ordinal=(k//4)*5+1+k%4
 assert ordinal<nodes and source(ordinal)==source_system
 return node_key(ordinal)
def bags(kind,ordinal):
 size=[64,256,1024,4096][(ordinal//8)%4]
 opaque=''.join(hashlib.sha256(f'{SEED}:{kind}:{ordinal}:{j}'.encode()).hexdigest() for j in range((size+63)//64))[:size]
 bases=['{}','{"101":null}','{"102":9007199254740993,"103":-9223372036854775808}','{"104":1.2300e+04,"105":-0.0000}','{"106":"2026-10-06T10:11:12.123456-04:00"}',compact({'107':'quote " slash \\ newline\n Unicode π'}),compact({str(120+j):'shared-'+str(j%3) for j in range(16)}),'{"109":{"nested":[null,true,{"unknown":"kept"}]}}']
 bag=bases[ordinal%8];bag=bag[:-1]+(',' if bag!='{}' else '')+compact('901')+':'+compact(opaque)+'}'
 retained=compact({'future':{'kind':kind,'ordinal':str(ordinal),'opaque':opaque[:64],'unknown':[None,True]}})
 return bag,retained
class Workload:
 def __init__(self,nodes,edges):
  if type(nodes) is not int or type(edges) is not int or nodes<5 or edges<0 or nodes+edges>9223372036854775807:raise ValueError('Valid non-overlapping signed-int64 allocation required')
  self.nodes=nodes;self.edges=edges
 def carrier(self,kind,ordinal):
  count=self.nodes if kind=='node' else self.edges
  if kind not in ['node','edge'] or not 0<=ordinal<count:raise ValueError('Invalid row range')
  if kind=='node':
   src,typ,ident=node_key(ordinal);r={'source_system':src,'type_id':typ,'id':ident,'logical_key_json':compact({'synthetic_native_tuple':[typ,ident]}),'root_id':None};field='type_id'
  else:
   src=source(ordinal);a=endpoint(src,0 if ordinal%5==0 else ordinal*104729,self.nodes);z=a if ordinal%97==0 else endpoint(src,ordinal*13007+1,self.nodes)
   r={'source_system':src,'rel_type_id':str(7+ordinal%3),'id':str(self.nodes+ordinal+1),'source_type':a[1],'source_id':a[2],'target_type':z[1],'target_id':z[2],'order_key':None};field='rel_type_id'
  props,retained=bags(kind,ordinal);delivery=f'{kind}:{ordinal}:1';cursor=compact({'xid':'9223372036854775808','seq':str(ordinal)})
  r.update(schema_revision='synthetic-mixed/1',entity_version='1',props_json=props,retained_json=retained,source_feed='synthetic-scale-mixed',source_epoch=SEED,source_position=None,published_at='2026-10-07T00:00:00Z',apply_batch_id='mixed-bootstrap',source_cursor_json=cursor,source_delivery_id=delivery)
  r['lookup_hash']=hashlib.sha256(compact({'source_system':src,field:int(r[field]),'id':int(r['id'])}).encode()).hexdigest();return r
 def roles(self,kind,ordinal):
  r=self.carrier(kind,ordinal);typ=r['type_id' if kind=='node' else 'rel_type_id'];payload=compact({'profile':SEED,'kind':kind,'carrier':r,'unknown_envelope':{'keep':True}})
  yield 'object_current' if kind=='node' else 'edge_current',r
  yield 'source_record',{'source_feed':r['source_feed'],'source_epoch':r['source_epoch'],'delivery_id':r['source_delivery_id'],'record_kind':'synthetic-full-carrier','source_cursor_json':r['source_cursor_json'],'payload_json':payload,'payload_digest':hashlib.sha256(payload.encode()).hexdigest(),'schema_revision':r['schema_revision'],'received_at':r['published_at'],'apply_batch_id':r['apply_batch_id']}
  for i,(key,value) in enumerate(sorted(tokens(r['props_json']).items(),key=lambda x:int(x[0]))):
   yield 'property_journal',{'source_system':r['source_system'],'entity_kind':kind,'type_id':typ,'id':r['id'],'property_id':key,'entity_version':'1','operation':'set','old_present':False,'old_json':None,'new_present':True,'new_json':value,'schema_revision':r['schema_revision'],'source_feed':r['source_feed'],'source_epoch':r['source_epoch'],'source_position':None,'event_ordinal':str(i),'source_time_text':None,'published_at':r['published_at'],'apply_batch_id':r['apply_batch_id'],'source_cursor_json':r['source_cursor_json'],'source_delivery_id':r['source_delivery_id']}
  if kind=='edge':yield 'adjacency_forward',{k:r[k] for k in ['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','entity_version']}
def calibrate(nodes=4096,edges=20480):
 w=Workload(nodes,edges);stats=collections.defaultdict(lambda:{'rows':0,'utf8_jsonl_bytes':0});digests={};keys=set();shapes=collections.Counter();start=time.monotonic()
 for kind,count in [('node',nodes),('edge',edges)]:
  for i in range(count):
   carrier=w.carrier(kind,i);key=(carrier['source_system'],kind,carrier.get('type_id',carrier.get('rel_type_id')),carrier['id']);assert key not in keys;keys.add(key);shapes[kind+':'+str(i%8)]+=1
   if kind=='edge':
    for side in ['source','target']:
     ordinal=int(carrier[side+'_id'])-1;assert node_key(ordinal)==(carrier['source_system'],carrier[side+'_type'],carrier[side+'_id'])
   replay={};raw=None
   for role,row in w.roles(kind,i):
    content=(compact(row)+'\n').encode();stats[role]['rows']+=1;stats[role]['utf8_jsonl_bytes']+=len(content);digests.setdefault(role,hashlib.sha256()).update(content)
    if role=='source_record':raw=row;assert json.loads(row['payload_json'])['carrier']==carrier
    if role=='property_journal':assert not row['old_present'] and row['old_json'] is None and row['new_present'];replay[row['property_id']]=row['new_json']
   assert replay==tokens(carrier['props_json']) and raw['payload_digest']==hashlib.sha256(raw['payload_json'].encode()).hexdigest()
 for role,d in stats.items():d['sha256_stream']=digests[role].hexdigest()
 return {'state':'Every calibration entity has exact raw/property bootstrap and typed endpoint closure','seed':SEED,'nodes':nodes,'edges':edges,'roles':dict(stats),'shapes':dict(shapes),'generation_s':time.monotonic()-start,'scope':'All calibration rows verified, not all future larger rows. Source80/20 residue allocation,20% edges from source-specific hub and1/97 explicit self-loops; overlaps possible. Opaque64/256/1024/4096 chars distinct per entity; eight semantic bag shapes plus property901. Bootstrap only; future change/deletion schedule absent.','limits':'UTF8 JSONL accounting, not Parquet compression/storage/billing. Small-run verification holds identity set in memory; generator itself random-access and O(payload) per entity. No large materialization, native SQL generator differential, Truss authority, full-scale integrity/SLO or production bootstrap admission.'}
if __name__=='__main__':
 b=Path(__file__).resolve().parent;r=calibrate();(b/'out/mixed-scale-calibration-r219.json').write_text(json.dumps(r,indent=2)+'\n');print(json.dumps(r,indent=2))
