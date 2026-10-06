"""Small deterministic synthetic complete-carrier corpus; no producer claim."""
import hashlib,json,collections
from pathlib import Path
from decimal import Decimal
B=Path(__file__).resolve().parent;O=B/'out/varied-corpus';O.mkdir(exist_ok=True)
def compact(x):return json.dumps(x,ensure_ascii=False,separators=(',',':'))
def digest(x):return hashlib.sha256(x.encode()).hexdigest()
ids=[1,-1,9223372036854775807,-9223372036854775808]+list(range(2,14))
nodes=[];edges=[];raw=[];shape_counts=collections.Counter()
def bags(i):
 shape=i%8;shape_counts[str(shape)]+=1
 opaque=''.join(digest(f'ashlar-varied/1:{i}:{j}') for j in range(32))[:[32,128,512,2048][(i//8)%4]]
 variants=['{}','{"101":null}','{"102":9007199254740993,"103":-9223372036854775808}',
 '{"104":1.2300e+04,"105":-0.0000}','{"106":"2026-10-06T10:11:12.123456-04:00"}',
 compact({'107':'quote " slash \\ newline\n Unicode π','108':opaque}),
 compact({str(120+j):'shared-category-'+str(j%3) for j in range(16)}),
 compact({'109':{'nested':[None,True,{'unknown':'kept'}]},'110':opaque})]
 retained=compact({'future':{'owner':'synthetic','ordinal':str(i),'unknown':{'opaque':opaque[:64],'flags':[True,None]}}})
 return variants[shape],retained
counter=0
for source in ['pilot-a','pilot-b']:
 for typ in [1,2]:
  for ident in ids:
   props,retained=bags(counter);counter+=1
   row={'source_system':source,'type_id':str(typ),'id':str(ident),'logical_key_json':compact({'synthetic_native_tuple':[str(typ),str(ident)]}),'schema_revision':'synthetic-r'+str(1+counter%2),'entity_version':'1','props_json':props,'retained_json':retained,'root_id':None}
   nodes.append(row)
 for rel in [7,8]:
  for ident in range(1,49):
   props,retained=bags(counter);counter+=1
   # Explicit parallel pair (edges 1/2), self-loop (3), plus mixed typed endpoints.
   source_type=1 if ident<=3 else 1+ident%2;source_id=ids[0] if ident<=3 else ids[(ident//2)%12]
   target_type=1 if ident==3 else 2 if ident<=2 else 1+(ident//3)%2
   target_id=ids[0] if ident<=3 else ids[(ident//4)%12]
   edges.append({'source_system':source,'rel_type_id':str(rel),'id':str(ident),'source_type':str(source_type),'source_id':str(source_id),'target_type':str(target_type),'target_id':str(target_id),'schema_revision':'synthetic-r'+str(1+counter%2),'entity_version':'1','props_json':props,'retained_json':retained,'order_key':None})
for i,(kind,row) in enumerate([('node',n) for n in nodes]+[('edge',e) for e in edges]):
 delivery='varied/1/'+str(i);cursor=compact({'xid':str(9223372036854775808+i),'seq':'1'})
 row.update({'source_feed':'synthetic-varied','source_epoch':'fixture-e1','source_position':None,'published_at':'2026-10-06T00:00:00Z','apply_batch_id':'varied-fixture/1','source_cursor_json':cursor,'source_delivery_id':delivery})
 type_field='type_id' if kind=='node' else 'rel_type_id'
 row['lookup_hash']=digest(compact({'source_system':row['source_system'],type_field:int(row[type_field]),'id':int(row['id'])}))
 # Includes every canonical fixture field. It is fixture input, not Truss wire.
 payload=compact({'profile':'synthetic-complete-carrier/1','kind':kind,'cursor':json.loads(cursor),'canonical_carrier':row.copy(),'unknown_envelope_field':{'preserve':True}})
 raw.append({'source_feed':row['source_feed'],'source_epoch':row['source_epoch'],'delivery_id':delivery,'record_kind':'synthetic-full-carrier','source_cursor_json':cursor,'payload_json':payload,'payload_digest':digest(payload),'schema_revision':row['schema_revision'],'received_at':'2026-10-06T00:00:00Z','apply_batch_id':'varied-fixture/1'})
nodekeys={(r['source_system'],r['type_id'],r['id']) for r in nodes};assert len(nodekeys)==64
edgekeys={(r['source_system'],r['rel_type_id'],r['id']) for r in edges};assert len(edgekeys)==192
incident=set();pairs=collections.Counter();selfloops=0
for r in edges:
 a=(r['source_system'],r['source_type'],r['source_id']);z=(r['source_system'],r['target_type'],r['target_id']);assert a in nodekeys and z in nodekeys
 incident.update([a,z]);pairs[(r['rel_type_id'],a,z)]+=1;selfloops+=a==z
origins={r['delivery_id']:r for r in raw};assert len(origins)==256
for row in nodes+edges:
 origin=origins[row['source_delivery_id']];assert digest(origin['payload_json'])==origin['payload_digest']
 assert json.loads(origin['payload_json'])['canonical_carrier']==row
 for field in ['props_json','retained_json']:assert isinstance(json.loads(row[field],parse_float=Decimal),dict)
files={}
for name,rows in [('objects',nodes),('edges',edges),('source_records',raw)]:
 data=(json.dumps(rows,ensure_ascii=False,indent=2)+'\n').encode();path=O/(name+'.json');path.write_bytes(data);files[name]={'path':path.name,'rows':len(rows),'bytes':len(data),'sha256':hashlib.sha256(data).hexdigest()}
summary={'state':'local-corpus-validated','format':'synthetic-complete-carrier/1','generation':'Deterministic SHA256 opaque text; no random state. Shape cycles eight categories equally; length cycles 32/128/512/2048 for opaque-bearing categories. IDs and BIGINT transport fields are decimal strings.','shape_counts':dict(shape_counts),'files':files,'graph':{'nodes':len(nodes),'edges':len(edges),'isolated_nodes':len(nodekeys-incident),'self_loop_edges':selfloops,'parallel_endpoint_groups':sum(n>1 for n in pairs.values())},'preservation':['empty versus explicit null bags','integer above 2^53 and signed-int64 endpoints','decimal exponent and negative-zero token text','timezone text, escaping and Unicode','nested unknown retained content','raw envelope contains every canonical field with matching exact bag strings and digest'],'scope':'Local complete synthetic carrier shape only. No real Truss wire/native source reconstruction, property journal events, source transaction completeness, native SQL hash differential, storage compression, scale or graph-engine compatibility evidence. Distribution is declared for stress coverage, not inferred from a real consumer.'}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
