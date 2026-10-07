"""Immutable full change input, exact source and pinned predecessor check."""
import json,time,hashlib,gzip
from pathlib import Path
from mixed_changes_r228 import Changes,verify
from mixed_history_r215 import compact
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_change_stage_r229';assert not O.exists();c=Client(O,observation_timeout=200,cancel_after=180);base=json.loads((B/'out/native/ashlar_mixed_materialize_r226/summary.json').read_text());T='client_dev.ashlar_entropy_20261006_r86.mixed_change_input_r229';changes=Changes();a={'state':'running','target':T,'source_pins':base['tables'],'chunks':[]};rows=[]
for i in range(changes.count):
 change=changes.change(i);assert verify(change);rows.append(compact(change))
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def metrics(reserve=0):
 for n in range(12):
  try:h=collect_history(c.w,c.records,O/'shared-history.json');break
  except HistoryPending:
   if n==11:raise
   time.sleep(2)
 a['costs']={k:sum(v['metrics'].get(k,0) for v in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']+reserve<=10000000000 and a['costs']['write_remote_bytes']+reserve<=1000000000
try:
 c.sql('timeout','SET STATEMENT_TIMEOUT=180')
 assert c.sql('absent',"SHOW TABLES IN client_dev.ashlar_entropy_20261006_r86 LIKE 'mixed_change_input_r229'")==[]
 # Table created empty so every append has an explicit disjoint input range.
 c.sql('create',f'CREATE TABLE {T} (change_index BIGINT,change_json STRING) USING DELTA CLUSTER BY (change_index)')
 for start in range(0,len(rows),256):
  metrics(100000000);end=min(len(rows),start+256)
  values=','.join(f"({i},'{rows[i].encode().hex()}')" for i in range(start,end))
  c.sql('append-'+str(start),f"INSERT INTO {T} SELECT change_index,decode(unhex(payload_hex),'UTF-8') FROM VALUES {values} AS v(change_index,payload_hex)")
  a['chunks'].append({'start':start,'end':end,'statement_id':c.records[-1]['statement_id']});save()
 detail=c.sql('detail','DESCRIBE DETAIL '+T);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(names,detail[0]));a['table_id']=d['id'];a['active_bytes']=int(d['sizeInBytes']);a['files']=int(d['numFiles'])
 history=c.sql('history','DESCRIBE HISTORY '+T+' LIMIT 1');version=int(history[0][0]);assert version==8;a['version']=version
 expected=hashlib.sha256(''.join(sorted(hashlib.sha256(x.encode()).hexdigest() for x in rows)).encode()).hexdigest()
 assert c.sql('exact-input',f"SELECT count(*),count(DISTINCT change_index),sha2(concat_ws('',sort_array(collect_list(sha2(change_json,256)))),256) FROM {T} VERSION AS OF {version}")==[['2048','2048',expected]];a['all_record_digest']=expected
 edge=base['tables']['edge_current'];before=c.sql('predecessor-detail','DESCRIBE DETAIL '+edge['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,before[0]))['id']==edge['id']
 fields=list(changes.w.carrier('edge',0));ints={'rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','source_position'};tests=[]
 for field in fields:
  source=f"get_json_object(s.change_json,'$.before.{field}')";expected=f'CAST({source} AS BIGINT)' if field in ints else f'CAST({source} AS TIMESTAMP)' if field=='published_at' else source
  tests.append(f'NOT(b.{field} <=> {expected})')
 q=f"SELECT count(*) FROM {T} VERSION AS OF {version} s LEFT JOIN {edge['table']} VERSION AS OF 0 b ON b.source_system=get_json_object(s.change_json,'$.before.source_system') AND b.rel_type_id=cast(get_json_object(s.change_json,'$.before.rel_type_id') AS BIGINT) AND b.id=cast(get_json_object(s.change_json,'$.before.id') AS BIGINT) WHERE b.id IS NULL OR "+' OR '.join(tests)
 assert c.sql('full-predecessor',q)==[['0']];a['full_native_predecessor_rows']=2048
 metrics();a['state']='All2048 immutable native change records and all predecessor fields verified';a['qualification']='Input0..2047 committed in eight disjoint append ranges; targetUUID/version8 exact full-record SHA256 multiset+identity count and native full-field predecessor join at bootstrapE0. Local event/tombstone replay verified before admission. No current/source/journal/tombstone publication writes, manifest advance or freshness/capacity claim. Source-only stage retained; no VACUUM.';save();print(json.dumps({k:v for k,v in a.items() if k!='source_pins'},indent=2))
except Exception as e:a.update(state='Stopped; inspect exact persisted handles and committed ranges before any admission',error=str(e));save();raise
finally:
 # Exact submitted queries/results retained losslessly, compressed to avoid giant
 # repository text blobs. Live statement remains the original recovery pointer.
 p=O/'statements.jsonl'
 if p.exists():
  with gzip.GzipFile(filename='',mode='wb',fileobj=(O/'statements.jsonl.gz').open('wb'),mtime=0) as g:g.write(p.read_bytes())
  assert gzip.decompress((O/'statements.jsonl.gz').read_bytes())==p.read_bytes();p.unlink()
