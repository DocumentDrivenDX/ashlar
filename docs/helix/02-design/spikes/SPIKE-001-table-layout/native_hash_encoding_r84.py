"""Read-only representative SQL/Python exact tuple JSON differential."""
import json,hashlib,time
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_hash_encoding_20261006_r84';c=Client(O);start=time.time()
sources=['pilot','quote\"name','back\\slash','line\nbreak','tab\tname','nul\x00name','del\x7fname',"semi';--"]
ids=['0','9007199254740993','9223372036854775807','-9223372036854775808'];inputs=[];expected=[]
for kind in ['node','edge']:
 field='type_id' if kind=='node' else 'rel_type_id'
 for source in sources:
  for ident in ids:
   ordinal=str(len(inputs));row={'ordinal':ordinal,'kind':kind,'source':source,'type':'7','id':ident};inputs.append(row)
   text=json.dumps({'source_system':source,field:7,'id':int(ident)},ensure_ascii=False,separators=(',',':'))
   expected.append([ordinal,text,hashlib.sha256(text.encode()).hexdigest()])
query="""WITH inputs AS (SELECT explode(from_json(:payload,'ARRAY<STRUCT<ordinal:STRING,kind:STRING,source:STRING,type:STRING,id:STRING>>')) r), encoded AS
(SELECT r.ordinal,CASE WHEN r.kind='node' THEN to_json(named_struct('source_system',r.source,'type_id',cast(r.type AS BIGINT),'id',cast(r.id AS BIGINT))) ELSE to_json(named_struct('source_system',r.source,'rel_type_id',cast(r.type AS BIGINT),'id',cast(r.id AS BIGINT))) END text FROM inputs)
SELECT ordinal,text,sha2(text,256) FROM encoded"""
actual=c.sql('representative-hash-encoding',query,parameters=[{'name':'payload','type':'STRING','value':json.dumps(inputs)}])
actual.sort(key=lambda r:int(r[0]));assert actual==expected,'Encoding differential failed; inspect stored native result'
summary={'state':'passed','cases':len(inputs),'source_cases':['plain','quote','backslash','newline','tab','NUL','DEL','SQL-looking text'],'ids':ids,'entity_kinds':['node','edge'],'encoding':'UTF-8 compact JSON, ordered named members, numeric signed-int64 tokens; Python ensure_ascii=False preserves DEL as native JSON does.','checks':['exact JSON text parity','SHA256 parity','BIGINT values above 2^53 and signed extremes preserved'],'elapsed_seconds':time.time()-start,'cost':'One 64-row constant-input native SELECT on existing authorized warehouse; no table scans/writes or new compute; billing dollars unavailable.','scope':'Representative Python/native SQL hash differential on declared ASCII source/int64 profile, not exhaustive encoder qualification, authority, membership, source feed, native singleton performance or scale admission.'}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Representative native hash encoding passed',flush=True)
