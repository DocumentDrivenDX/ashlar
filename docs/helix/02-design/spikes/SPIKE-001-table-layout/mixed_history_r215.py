"""Synthetic bootstrap/change property evidence; exact lexical tokens, local only."""
import json,hashlib,collections
from pathlib import Path
B=Path(__file__).resolve().parent;O=B/'out/mixed-history-r215'
def compact(x):return json.dumps(x,ensure_ascii=False,separators=(',',':'))
def tokens(text):
 decoder=json.JSONDecoder();pos=0;out={}
 def ws(p):
  while p<len(text) and text[p].isspace():p+=1
  return p
 pos=ws(pos);assert text[pos]=='{';pos=ws(pos+1)
 if text[pos]=='}':assert ws(pos+1)==len(text);return out
 while True:
  key,end=decoder.raw_decode(text,pos);assert isinstance(key,str) and key.isdecimal() and key not in out;assert 0<=int(key)<=9223372036854775807
  pos=ws(end);assert text[pos]==':';start=ws(pos+1);_,end=decoder.raw_decode(text,start);out[key]=text[start:end];pos=ws(end)
  if text[pos]=='}':assert ws(pos+1)==len(text);return out
  assert text[pos]==',';pos=ws(pos+1)
def generate():
 nodes=json.loads((B/'out/varied-corpus/objects.json').read_text());edges=json.loads((B/'out/varied-corpus/edges.json').read_text());original=nodes+edges
 raw=[];events=[];carriers=[];counts=collections.Counter()
 for i,row in enumerate(original):
  kind='node' if i<len(nodes) else 'edge';typ='type_id' if kind=='node' else 'rel_type_id';previous={}
  for version in [1,2]:
   r=row.copy();r['entity_version']=str(version);r['apply_batch_id']='mixed-r215-v'+str(version);r['source_feed']='synthetic-mixed-history';r['source_epoch']='r215';r['source_delivery_id']=f'r215:{kind}:{i}:{version}';r['source_cursor_json']=compact({'xid':str(9223372036854775808+version),'seq':str(i)})
   # Rotate bag shape for changes; structural/retained fields remain exact.
   if version==2:r['props_json']=original[(i+1)%len(original)]['props_json']
   current=tokens(r['props_json']);ordinal=0
   for key in sorted(set(previous)|set(current),key=int):
    if key in previous and key in current and previous[key]==current[key]:continue
    old=key in previous;new=key in current;counts['set' if new else 'remove']+=1;counts['explicit_null']+=new and current[key]=='null'
    e={'source_system':r['source_system'],'entity_kind':kind,'type_id':r[typ],'id':r['id'],'property_id':key,'entity_version':str(version),'operation':'set' if new else 'remove','old_present':old,'old_json':previous.get(key),'new_present':new,'new_json':current.get(key),'schema_revision':r['schema_revision'],'source_feed':r['source_feed'],'source_epoch':r['source_epoch'],'source_position':None,'event_ordinal':str(ordinal),'source_time_text':None,'published_at':r['published_at'],'apply_batch_id':r['apply_batch_id'],'source_cursor_json':r['source_cursor_json'],'source_delivery_id':r['source_delivery_id']};events.append(e);ordinal+=1
   payload=compact({'profile':'synthetic-mixed-history/1','kind':kind,'carrier':r,'unknown_envelope':{'ordinal':str(i),'keep':[None,True]}});digest=hashlib.sha256(payload.encode()).hexdigest()
   raw.append({'source_feed':r['source_feed'],'source_epoch':r['source_epoch'],'delivery_id':r['source_delivery_id'],'record_kind':'synthetic-full-carrier','source_cursor_json':r['source_cursor_json'],'payload_json':payload,'payload_digest':digest,'schema_revision':r['schema_revision'],'received_at':r['published_at'],'apply_batch_id':r['apply_batch_id']});carriers.append(r);previous=current
 origins={r['delivery_id']:r for r in raw};assert len(origins)==512
 replay={};by_origin=collections.defaultdict(list)
 for e in events:by_origin[e['source_delivery_id']].append(e)
 for i,r in enumerate(carriers):
  key=(r['source_system'],'node' if i//2<len(nodes) else 'edge',r.get('type_id',r.get('rel_type_id')),r['id']);state=replay.setdefault(key,{})
  for e in by_origin[r['source_delivery_id']]:
   k=e['property_id'];assert (k in state)==e['old_present'] and state.get(k)==e['old_json']
   if e['new_present']:state[k]=e['new_json']
   else:state.pop(k)
  assert state==tokens(r['props_json']);origin=origins[r['source_delivery_id']];assert json.loads(origin['payload_json'])['carrier']==r and hashlib.sha256(origin['payload_json'].encode()).hexdigest()==origin['payload_digest']
 return {'carriers':carriers,'source_records':raw,'property_journal':events},dict(counts)
if __name__=='__main__':
 assert not O.exists();O.mkdir();data,counts=generate();files={}
 for name,rows in data.items():
  content=''.join(compact(r)+'\n' for r in rows).encode();(O/(name+'.jsonl')).write_bytes(content);files[name]={'rows':len(rows),'utf8_bytes':len(content),'sha256':hashlib.sha256(content).hexdigest()}
 summary={'state':'Local exact lexical property replay and full raw-carrier parity passed','files':files,'event_counts':counts,'source_shape':'Existing256-carrier mixed corpus, bootstrap plus rotated-bag update;64 nodes192edges, signed BIGINT identities, exact exponent/negative-zero/null/Unicode/retained tokens. Empty bags yield no fabricated property events.','limits':'Synthetic proposed set/remove operation vocabulary, not Truss source authority. Raw exact bag strings preserve full order/whitespace; property replay proves exact per-value tokens, not whole-bag lexical reconstruction. No native compression, hash differential, transaction fencing, scale or latency admission.'};(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary,indent=2))
