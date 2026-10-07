"""Pinned read-only identity, structure, raw and property replay qualification."""
import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from mixed_history_r215 import tokens
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_links_r218';assert not O.exists();s=json.loads((B/'out/native/ashlar_mixed_r217/summary.json').read_text());c=BoundedReads(O);a={'state':'running','pins':s['tables'],'checks':{}}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def table(name):return s['tables'][name]['table']+' VERSION AS OF 0'
def check(name,q,expected):
 actual=c.sql(name,q);assert actual==expected,(name,actual);a['checks'][name]=actual;save()
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=30');a['runtime']=c.sql('runtime','SELECT current_version(),current_timezone()')
 for name,m in s['tables'].items():
  result=c.sql('detail-'+name,'DESCRIBE DETAIL '+m['table']);cols=[p['name'] for p in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,result[0]))['id']==m['id']
 for name,typ,count in [('object_current','type_id',64),('edge_current','rel_type_id',192)]:
  check('identity-'+name,f"SELECT count(*),count(DISTINCT named_struct('source_system',source_system,'type', {typ},'id',id)),count_if(lookup_hash <> sha2(to_json(named_struct('source_system',source_system,'{typ}',{typ},'id',id)),256)) FROM {table(name)}",[[str(count),str(count),'0']])
  check('origin-'+name,f"SELECT count(*) FROM {table(name)} n LEFT JOIN {table('source_record')} r ON n.source_feed=r.source_feed AND n.source_epoch=r.source_epoch AND n.source_delivery_id=r.delivery_id WHERE r.delivery_id IS NULL OR NOT(n.source_cursor_json <=> r.source_cursor_json) OR NOT(hex(encode(n.props_json,'UTF-8')) <=> hex(encode(get_json_object(r.payload_json,'$.carrier.props_json'),'UTF-8'))) OR NOT(hex(encode(n.retained_json,'UTF-8')) <=> hex(encode(get_json_object(r.payload_json,'$.carrier.retained_json'),'UTF-8'))) OR cast(n.id AS STRING) <> get_json_object(r.payload_json,'$.carrier.id') OR cast(n.{typ} AS STRING) <> get_json_object(r.payload_json,'$.carrier.{typ}')",[['0']])
 for side in ['source','target']:
  check('endpoint-'+side,f"SELECT count(*) FROM {table('edge_current')} e LEFT ANTI JOIN {table('object_current')} n ON e.source_system=n.source_system AND e.{side}_type=n.type_id AND e.{side}_id=n.id",[['0']])
 check('raw-identity-digest',f"SELECT count(*),count(DISTINCT named_struct('feed',source_feed,'epoch',source_epoch,'delivery',delivery_id)),count_if(payload_digest<>sha2(payload_json,256)) FROM {table('source_record')}",[['512','512','0']])
 check('event-origin',f"SELECT count(*) FROM {table('property_journal')} j LEFT JOIN {table('source_record')} r ON j.source_feed=r.source_feed AND j.source_epoch=r.source_epoch AND j.source_delivery_id=r.delivery_id WHERE r.delivery_id IS NULL OR NOT(j.source_cursor_json <=> r.source_cursor_json) OR cast(j.entity_version AS STRING) <> get_json_object(r.payload_json,'$.carrier.entity_version') OR j.source_system <> get_json_object(r.payload_json,'$.carrier.source_system') OR cast(j.id AS STRING) <> get_json_object(r.payload_json,'$.carrier.id')",[['0']])
 raw=c.sql('raw-replay',f"SELECT delivery_id,payload_json FROM {table('source_record')}");events=c.sql('events-replay',f"SELECT source_delivery_id,event_ordinal,property_id,old_present,old_json,new_present,new_json,operation,entity_kind,type_id,schema_revision FROM {table('property_journal')}")
 from collections import defaultdict
 groups=defaultdict(list)
 for event in events:groups[event[0]].append(event)
 states={};seen=set()
 for delivery,payload in sorted(raw,key=lambda x:(int(json.loads(x[1])['carrier']['entity_version']),x[0])):
  envelope=json.loads(payload);r=envelope['carrier'];kind=envelope['kind'];typ='type_id' if kind=='node' else 'rel_type_id';key=(r['source_system'],kind,r[typ],r['id']);state=states.setdefault(key,{})
  es=sorted(groups[delivery],key=lambda e:int(e[1]));assert [int(e[1]) for e in es]==list(range(len(es)))
  for e in es:
   _,ordinal,prop,old,oldjson,new,newjson,operation,ek,et,revision=e;assert (delivery,ordinal) not in seen;seen.add((delivery,ordinal));assert ek==kind and et==r[typ] and revision==r['schema_revision'];assert (prop in state)==(old=='True') and state.get(prop)==oldjson
   assert operation==('set' if new=='True' else 'remove')
   if new=='True':state[prop]=newjson
   else:state.pop(prop)
  assert state==tokens(r['props_json'])
 assert len(seen)==2496 and len(states)==256;a['checks']['native-values-local-replay']={'origins':len(raw),'events':len(seen),'entities':len(states)}
 c.cursor.close();c.cursor=c.connection.cursor()
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=1000000000 and a['costs']['write_remote_bytes']==0;a['state']='Pinned native identity, endpoints, origin links and complete native-value property replay passed';a['qualification']='All fixture rows, read-only snapshot0. Native joins and hash differential; replay runs locally on every native raw/event value. No producer fencing, transaction completeness, manifest barrier, native replay implementation, constraint enforcement or scale/SLO claim.';save();print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect existing handles',error=str(e));save();raise
finally:c.close()
