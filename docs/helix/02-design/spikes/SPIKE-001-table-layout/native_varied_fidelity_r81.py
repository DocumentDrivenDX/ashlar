"""Small native 0.3 carrier fidelity slice; no feed/performance admission."""
import json,re,hashlib,time,datetime
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;I=B/'out/varied-corpus';O=B/'out/native/ashlar_varied_fidelity_20261006_r81';c=Client(O);start=time.time()
N='client_dev.ashlar_layout_v03_20261006_r73';ddl=(B/'sql/delta-layout-v03.sql').read_text();info=json.loads((I/'summary.json').read_text());tables={};versions={};data={}
assert info['state']=='local-corpus-validated'
assert c.sql('absent-private-tables',f"SHOW TABLES IN {N} LIKE 'varied_*_r81'")==[]
for logical,file in [('object_current','objects'),('edge_current','edges'),('source_record','source_records')]:
 raw=(I/(file+'.json')).read_bytes();assert hashlib.sha256(raw).hexdigest()==info['files'][file]['sha256'];rows=json.loads(raw);data[logical]=rows
 block=ddl.split('CREATE TABLE '+logical+' (',1)[1].split(';',1)[0];table=N+'.varied_'+logical+'_r81';tables[logical]=table
 columns=re.findall(r'(\w+)\s+(STRING|BIGINT|TIMESTAMP)',block.split(') USING DELTA')[0]);assert all(set(r)=={n for n,t in columns} for r in rows)
 c.sql('create-'+logical,'CREATE TABLE '+table+' ('+block)
 schema='ARRAY<STRUCT<'+','.join(n+':STRING' for n,t in columns)+'>>'
 expressions=','.join('CAST(r.'+n+' AS '+t+')' if t!='STRING' else 'r.'+n for n,t in columns)
 c.sql('load-'+logical,f"INSERT INTO {table} SELECT {expressions} FROM (SELECT explode(from_json(:payload,'{schema}')) r)",parameters=[{'name':'payload','value':raw.decode(),'type':'STRING'}])
 version=int(c.sql('version-'+logical,f'DESCRIBE HISTORY {table} LIMIT 1')[0][0]);versions[logical]=version
 selects=','.join('cast(unix_micros('+n+') AS STRING)' if t=='TIMESTAMP' else 'cast('+n+' AS STRING)' if t=='BIGINT' else n for n,t in columns)
 actual=c.sql('read-'+logical,f'SELECT {selects} FROM {table} VERSION AS OF {version}')
 expected=[]
 for row in rows:
  values=[]
  for n,t in columns:
   value=row[n]
   if t=='TIMESTAMP' and value is not None:value=str(int(datetime.datetime.fromisoformat(value.replace('Z','+00:00')).timestamp()*1_000_000))
   values.append(value)
  expected.append(values)
 assert sorted(actual,key=lambda r:json.dumps(r))==sorted(expected,key=lambda r:json.dumps(r)),logical
E=tables['edge_current'];V=tables['object_current'];R=tables['source_record'];ev=versions['edge_current'];vv=versions['object_current'];rv=versions['source_record']
for logical,typefield in [('object_current','type_id'),('edge_current','rel_type_id')]:
 t=tables[logical];v=versions[logical]
 assert c.sql('keys-hashes-'+logical,f"SELECT count(*),count(DISTINCT struct(source_system,{typefield},id)),count_if(lookup_hash IS DISTINCT FROM sha2(to_json(named_struct('source_system',source_system,'{typefield}',{typefield},'id',id)),256)) FROM {t} VERSION AS OF {v}")==[[str(len(data[logical])),str(len(data[logical])),'0']]
 assert c.sql('raw-reference-'+logical,f'''SELECT count(*) FROM {t} VERSION AS OF {v} t LEFT JOIN {R} VERSION AS OF {rv} r ON t.source_feed=r.source_feed AND t.source_epoch=r.source_epoch AND t.source_delivery_id=r.delivery_id WHERE r.delivery_id IS NULL OR t.source_cursor_json IS DISTINCT FROM r.source_cursor_json OR r.payload_digest IS DISTINCT FROM sha2(r.payload_json,256)''')==[['0']]
for direction in ['source','target']:
 assert c.sql(direction+'-closure',f'''SELECT count(*) FROM {E} VERSION AS OF {ev} e LEFT ANTI JOIN {V} VERSION AS OF {vv} n ON e.source_system=n.source_system AND e.{direction}_type=n.type_id AND e.{direction}_id=n.id''')==[['0']]
summary={'state':'passed','tables':tables,'versions':versions,'rows':{'objects':64,'edges':192,'source_records':256},'corpus_summary_sha256':hashlib.sha256((I/'summary.json').read_bytes()).hexdigest(),'checks':['exhaustive all-column parity at actual pinned versions; timestamps compared by instant','exact raw/property/retained UTF-8 text including escapes Unicode and numeric tokens','independent native typed keys and SQL hash differential','feed/epoch/delivery references with matching cursors and raw digest','both typed endpoint directions resolve'],'elapsed_seconds':time.time()-start,'cost':'Existing authorized warehouse; 512 total small input rows across three private tables, bound STRING parameters; no larger scale/new compute; billing dollars unavailable.','scope':'Synthetic complete-carrier storage only, using three isolated tables with 0.3 field/physical definitions. No property journal events, producer reconstruction/completeness, manifest/recovery/fencing, projection/engine execution, latency/ingest or billion-scale admission.'}
(O/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Varied native fidelity passed',flush=True)
