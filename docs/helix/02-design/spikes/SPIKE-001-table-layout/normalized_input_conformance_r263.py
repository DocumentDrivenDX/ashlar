"""Read-only native JSON/typed-field conformance; no volume or table mutation."""
import json,time,hashlib,gzip
from pathlib import Path
from mixed_changes_r228 import Changes,verify
from mixed_history_r215 import compact
from mixed_change_queries_r230 import row_hash,row_hash_sql
from normalized_input_sql_r263 import role_row,inline_query,volume_query
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_normalized_input_r263';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=70,cancel_after=60);a={'state':'Running read-only normalized-role conformance','checks':{},'negative_controls':{},'bounds':{'read_bytes':100000000,'write_bytes':0,'spill_bytes':0}};ch=Changes(8000000,40000000,16)
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
try:
 for role in ['source_record','property_journal','tombstone','current_replacement']:
  inputs=[];rows=[]
  for i in range(16):
   x=ch.change(i)
   if x['tombstone'] is not None:x['tombstone']['entity_version']='2'
   assert verify(x)
   candidates=[x['raw']] if role=='source_record' else x['events'] if role=='property_journal' else [x['tombstone']] if role=='tombstone' and x['tombstone'] is not None else [x['after']] if role=='current_replacement' and x['after'] is not None else []
   for row in candidates:
    fields=list(role_row(role));assert list(row)==fields
    # Unknown top-level content remains in original input_json, outside known-field projection.
    decorated={**row,'unknown_input':{'big':'9223372036854775809','opaque':[None,'雪','\\u0041']}}
    inputs.append(compact(decorated));rows.append(row)
  q=inline_query(role,inputs);expected=[[str(len(rows)),hashlib.sha256(''.join(sorted(hashlib.sha256(x.encode()).hexdigest() for x in inputs)).encode()).hexdigest(),hashlib.sha256(''.join(sorted(row_hash(row,list(role_row(role))) for row in rows)).encode()).hexdigest()]]
  actual=c.sql('role-'+role,f"SELECT count(*),sha2(concat_ws('',sort_array(collect_list(sha2(input_json,256)))),256),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(list(role_row(role)))}))),256) FROM ({q})")
  assert actual==expected;a['checks'][role]={'rows':len(rows),'original_input_digest':expected[0][1],'all_known_field_digest':expected[0][2],'fields':list(role_row(role))};save()
 for name,role,text,code in [('malformed_json','source_record','{','MALFORMED_RECORD'),('invalid_bigint','tombstone',compact({**role_row('tombstone'),'entity_version':'not-an-integer'}),'CAST_INVALID_INPUT')]:
  before=len(c.records)
  try:c.sql('negative-'+name,'SELECT * FROM ('+inline_query(role,[text])+')')
  except RuntimeError:
   assert len(c.records)==before+1 and c.records[-1]['response']['status']['state']=='FAILED'
   assert code in json.dumps(c.records[-1]['response']['status']);a['negative_controls'][name]={'state':'Native rejected','statement_id':c.records[-1]['statement_id'],'status':c.records[-1]['response']['status']};save()
  else:raise AssertionError('Invalid normalized input accepted')
 for bad in ['/Volumes/a/b/c/../x.jsonl',"/Volumes/a/b/c/r/x.jsonl';DROP TABLE x",'/tmp/x.jsonl']:
  try:volume_query('source_record',bad)
  except ValueError:pass
  else:raise AssertionError('Unsafe path accepted')
 for attempt in range(20):
  h=c.history()
  if len(h)==len(c.records) and all(q.get('is_final') is True for q in h):break
  if attempt==19:raise RuntimeError('Native metrics not final; preserve exact handles')
  time.sleep(2)
 by={q['query_id']:q for q in h};assert all(by[r['statement_id']]['status']==('FINISHED' if r['response']['status']['state']=='SUCCEEDED' else 'FAILED') for r in c.records)
 a['costs']={k:sum(q.get('metrics',{}).get(k,0) for q in h) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert a['costs']['read_bytes']<=100000000 and a['costs']['write_remote_bytes']==0 and a['costs']['spill_to_disk_bytes']==0
 a['sql_channels']=list({json.dumps(q.get('channel_used'),sort_keys=True) for q in h});a['source_sha256']={n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['normalized_input_sql_r263.py','normalized_input_conformance_r263.py','mixed_changes_r228.py','mixed_change_queries_r230.py']};a['state']='Exact input JSON and all known typed role fields preserved; malformed JSON and bad integer refused';a['qualification']='Small constant-input conformance on native SQL, including opaque unknown top-level content. Does not qualify actual TEXT/volume upload/file framing, arbitrary untrusted JSON/duplicate-key grammar, complete DDL/required-field constraints,100k native staging/apply/publication, producer fencing or freshness. Original input_json retained without from_json reserialization; explicit columns include tombstone entity_version. Read-only SELECTs, no table/volume/compute mutation.';save();print(json.dumps({'state':a['state'],'checks':{k:v['rows'] for k,v in a['checks'].items()},'costs':a['costs']},indent=2))
except Exception as e:a.update(state='Stopped; inspect exact read-only native handles',error=str(e));save();raise
finally:
 p=O/'statements.jsonl'
 if p.exists():
  raw=p.read_bytes()
  with (O/'statements.jsonl.gz').open('wb') as f:
   with gzip.GzipFile(filename='',mode='wb',fileobj=f,mtime=0) as g:g.write(raw)
  assert gzip.decompress((O/'statements.jsonl.gz').read_bytes())==raw;p.unlink()
