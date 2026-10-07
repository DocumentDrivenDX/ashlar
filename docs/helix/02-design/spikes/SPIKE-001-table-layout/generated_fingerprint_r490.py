"""Tiny native enforced full-carrier fingerprint capability test; no canonical writes."""
import datetime,hashlib,json,time
from pathlib import Path
from persistent_sql import Client
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import INTS,TIMES
from fifth_changes_r470 import FifthChanges
B=Path(__file__).resolve().parent
PROFILE='ashlar-carrier-fingerprint/1;'
def expression():
 parts=["'"+PROFILE+"'"]
 for f in FIELDS:
  v=f'cast(unix_micros({f}) AS STRING)' if f in TIMES else f'cast({f} AS STRING)'
  parts.append(f"CASE WHEN {f} IS NULL THEN 'N;' ELSE concat('V',cast(length(encode({v},'UTF-8')) AS STRING),':',hex(encode({v},'UTF-8')),';') END")
 return 'sha2(concat('+','.join(parts)+'),256)'
def oracle(row):
 s=PROFILE
 for f in FIELDS:
  v=row[f]
  if v is None:s+='N;';continue
  if f in TIMES:
   t=datetime.datetime.fromisoformat(v.replace('Z','+00:00'))
   delta=t-datetime.datetime(1970,1,1,tzinfo=datetime.timezone.utc)
   v=str((delta.days*86400+delta.seconds)*1000000+delta.microseconds)
  d=str(v).encode();s+='V'+str(len(d))+':'+d.hex().upper()+';'
 return hashlib.sha256(s.encode()).hexdigest()
def literal(v,f):
 if v is None:return 'NULL'
 if f in INTS:return str(int(v))
 q="decode(unhex('"+str(v).encode().hex()+"'),'UTF-8')"
 return 'cast('+q+' AS TIMESTAMP)' if f in TIMES else q

def main():
 out=B/'out/native/generated_fingerprint_r490';assert not out.exists()
 # Bound: new private few-row table, <=1MB writes/100MB reads/zero spill,
 # <=20 statements and 180s elapsed; cancel each statement after30s.
 c=Client(out,observation_timeout=90,cancel_after=30);start=time.monotonic()
 table='client_dev.ashlar_entropy_20261006_r86.generated_fingerprint_r490'
 report={'table':table,'profile':PROFILE,'fields':list(FIELDS),'source_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'controls':[]}
 try:
  cols=','.join(f+(' BIGINT' if f in INTS else ' TIMESTAMP' if f in TIMES else ' STRING') for f in FIELDS)
  c.sql('create',f"CREATE TABLE {table} ({cols},carrier_fingerprint STRING GENERATED ALWAYS AS ({expression()})) USING DELTA TBLPROPERTIES ('delta.enableChangeDataFeed'='true')")
  row=FifthChanges().change(1)['before'];digest=oracle(row)
  c.sql('seed',f"INSERT INTO {table} ({','.join(FIELDS)}) VALUES ({','.join(literal(row[f],f) for f in FIELDS)})")
  assert c.sql('automatic',f'SELECT carrier_fingerprint FROM {table}')==[[digest]]
  for f,v in [('props_json','{}'),('retained_json','{}'),('source_cursor_json','{}')]:
   if row[f]==v:v=' { } '
   before=digest;row[f]=v;digest=oracle(row)
   assert before!=digest
   c.sql('update_'+f,f'UPDATE {table} SET {f}={literal(v,f)}')
   assert c.sql('read_'+f,f'SELECT carrier_fingerprint FROM {table}')==[[digest]]
   report['controls'].append({'field':f,'automatic_change':True})
  bad=dict(row);bad['retained_json']='{"unknown":true}'
  try:
   c.sql('stale_explicit',f"INSERT INTO {table} ({','.join(FIELDS)},carrier_fingerprint) VALUES ({','.join(literal(bad[f],f) for f in FIELDS)},'{digest}')")
  except RuntimeError:
   rec=c.records[-1];assert rec['response']['status']['state']=='FAILED'
   assert 'GENERATED' in json.dumps(rec['response']['status']).upper()
   report['stale_explicit_refused']=True
  else:raise AssertionError('Stale fingerprint accepted')
  assert c.sql('after_refusal',f'SELECT count(*),min(carrier_fingerprint) FROM {table}')==[['1',digest]]
  report['detail']=c.sql('detail',f'DESCRIBE DETAIL {table}')
  report['state']='capability passed; no performance or canonical admission'
 except Exception as e:
  report['state']='stopped';report['error']=str(e)
 finally:
  for _ in range(30):
   h=c.history()
   if len(h)==len(c.records) and all(q.get('is_final') for q in h):break
   time.sleep(1)
  assert len(h)==len(c.records) and all(q.get('is_final') for q in h)
  report['metrics']={k:sum(q.get('metrics',{}).get(k,0) or 0 for q in h) for k in ['read_bytes','write_bytes','spill_to_disk_bytes']}
  report['wall_s']=time.monotonic()-start
  assert report['wall_s']<=180 and len(c.records)<=20
  assert report['metrics']['read_bytes']<=100000000 and report['metrics']['write_bytes']<=1000000 and report['metrics']['spill_to_disk_bytes']==0
  (out/'live-statement.json').unlink(missing_ok=True)
  (out/'result.json').write_text(json.dumps(report,indent=2)+'\n')
  print(json.dumps({k:report[k] for k in ['state','metrics','wall_s']}))
if __name__=='__main__':main()
