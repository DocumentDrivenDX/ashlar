"""Immediate post-edge-publication identity/endpoint/carrier read control."""
import json,hashlib
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_composite_edges_20261005_q1';p=B/'out/native/ashlar_fenced_scheduled_20261005_r13r1'
s=json.loads((p/'summary.json').read_text());assert s['state']=='completed';v=s['batches'][-1]['versions'][F+'.edge_current'];out=B/'out/native/ashlar_scheduled_post_reads_20261005_r14';c=DriverClient(out);c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');expected={}
for phase in ['prime','repeat','repeat2']:
 for rep in range(51):
  domain=rep%10;source='pilot:'+str(domain//2);rel=7+domain%2;typ=1+domain%2;id=101+(rep*196613)%999900;src=1+((id-1)*104729)%200000;slot=(id-1)//200000;target=1+(src+(17 if slot<2 else slot*17)-1)%200000
  h=hashlib.sha256(json.dumps(dict(source_system=source,rel_type_id=rel,id=id),separators=(',',':')).encode()).hexdigest()
  rows=c.sql(f'{phase}-{rep}',f"SELECT source_system,rel_type_id,id,source_type,source_id,target_type,target_id,props_json,retained_json,lookup_hash FROM {F}.edge_current VERSION AS OF {v} WHERE lookup_hash='{h}' AND source_system='{source}' AND rel_type_id={rel} AND id={id}")
  assert len(rows)==1 and rows[0][:7]==[source,str(rel),str(id),str(typ),str(src),str(typ),str(target)] and rows[0][-1]==h
  band=(((id-1)*104729)%1000000)//20000;changed=band in [0,1,3,4,6];tag='q2' if band==0 else 'r13'
  payload=''.join(hashlib.sha256((f'{source}:{rel}:{id}:{tag}:{i}' if changed else f'{id}:edge:{i}').encode()).hexdigest() for i in range(32 if changed else 8))
  assert rows[0][7]==json.dumps({'201':payload},separators=(',',':'))
  if phase=='prime':expected[rep]=rows
  else:assert rows==expected[rep]
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps(dict(state='completed',edge_version=v,scope='same 51-domain-key shape; exact typed endpoints and independently computed old/new property201 carrier; fixed published snapshot; no extra optimize or concurrent write/full-scale admission'),indent=2)+'\n')
