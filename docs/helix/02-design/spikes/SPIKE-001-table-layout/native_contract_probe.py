"""Native DDL, exact stored-carrier and fixed-version read probes in owned sandbox."""
import argparse,json,re,subprocess,time
from pathlib import Path
BASE=Path(__file__).resolve().parent
p=argparse.ArgumentParser();p.add_argument('--schema',required=True);a=p.parse_args()
if not a.schema.replace('_','').isalnum():raise ValueError('Invalid schema')
F='client_dev.'+a.schema;out=BASE/'out/native'/a.schema;out.mkdir(exist_ok=True,parents=True)
log=out/'contract-probes.jsonl';records=[json.loads(x) for x in log.read_text().splitlines()] if log.exists() else []
def api(method,path,body=None):
 cmd=['databricks','api',method,path,'--profile','aidev-cus']
 if body:
  req=out/'probe-request.json';req.write_text(json.dumps(body));cmd+=['--json','@'+str(req)]
 r=subprocess.run(cmd,text=True,capture_output=True,timeout=45)
 if r.returncode:raise RuntimeError(r.stderr)
 return json.loads(r.stdout)
def sql(label,statement):
 for prior in records:
  if prior['label']==label and prior['sql']==statement and prior['response']['status']['state']=='SUCCEEDED':return prior['response'].get('result',{}).get('data_array',[])
 r=api('post','/api/2.0/sql/statements',{'warehouse_id':'2439e1f2e37ac563','statement':statement,'wait_timeout':'10s','on_wait_timeout':'CONTINUE','disposition':'INLINE','format':'JSON_ARRAY','row_limit':1000});deadline=time.monotonic()+180
 while r['status']['state'] in ('PENDING','RUNNING'):
  if time.monotonic()>deadline:
   api('post','/api/2.0/sql/statements/'+r['statement_id']+'/cancel',{});raise RuntimeError('Probe timed out and was canceled')
  time.sleep(2);r=api('get','/api/2.0/sql/statements/'+r['statement_id'])
 rec={'label':label,'sql':statement,'response':r};records.append(rec)
 with log.open('a') as handle:handle.write(json.dumps(rec)+'\n')
 if r['status']['state']!='SUCCEEDED':raise RuntimeError(json.dumps(r['status']))
 return r.get('result',{}).get('data_array',[])
text='\n'.join(line for line in (BASE/'sql/delta-candidate.sql').read_text().splitlines() if not line.lstrip().startswith('--'))
for i,statement in enumerate(text.split(';')):
 if not statement.strip():continue
 statement=re.sub(r'CREATE TABLE (\w+)',lambda m:'CREATE TABLE '+F+'.'+m[1],statement)
 sql('ddl-'+str(i),statement.strip())
props='{"101":9223372036854775807,"102":12345678901234567890.123456789,"103":null,"104":"2026-10-05T12:34:56.123456789-04:00"}'
retained='{"future":{"native":{"x":[null,true,"unknown"]}}}'
insert=f"""INSERT INTO {F}.object_current
 (source_system,type_id,id,logical_key_json,schema_revision,entity_version,props_json,retained_json,source_feed,source_epoch,source_position,published_at)
 VALUES ('pilot',1,123,'["A1"]','r1',1,'{props}','{retained}','S','e',1,current_timestamp())"""
sql('insert-exact',insert)
rows=sql('recover-exact',f"SELECT props_json,retained_json FROM {F}.object_current WHERE source_system='pilot' AND type_id=1 AND id=123")
assert rows==[[props,retained]],rows
hist=sql('object-history',f'DESCRIBE HISTORY {F}.object_current LIMIT 1');version=int(hist[0][0])
sql('advance-current',f"UPDATE {F}.object_current SET entity_version=2,source_position=2,props_json='{{\"101\":2}}' WHERE source_system='pilot' AND type_id=1 AND id=123")
assert sql('fixed-version',f"SELECT entity_version,props_json,retained_json FROM {F}.object_current VERSION AS OF {version} WHERE source_system='pilot' AND type_id=1 AND id=123")==[['1',props,retained]]
assert sql('latest-version',f'SELECT entity_version FROM {F}.object_current WHERE id=123')==[['2']]
# Explicit null and absent are distinct in preserved lexical map. Native extraction
# collapses both to SQL null, so presence must come from exact parser/flags.
assert '"103":null' in rows[0][0] and '"missing"' not in rows[0][0]
sql('invalid-fixture-table',f'CREATE TABLE {F}.invalid_object_fixture USING DELTA AS SELECT * FROM {F}.object_current')
sql('inject-duplicate',f'INSERT INTO {F}.invalid_object_fixture SELECT * FROM {F}.object_current')
assert sql('detect-duplicate',f'SELECT count(*) FROM (SELECT source_system,type_id,id FROM {F}.invalid_object_fixture GROUP BY source_system,type_id,id HAVING count(*)>1)')==[['1']]
summary={'state':'passed','schema':F,'ddl_tables':7,'exact_stored_json_text':True,'fixed_version_read_after_current_update':True,'duplicates_not_database_enforced_but_detected':True,'scope':'native DDL/string-preservation/version-pinning probes; not full publisher, source adapter or graph-engine conformance','statement_count':len(records)}
(out/'contract-probe-summary.json').write_text(json.dumps(summary,indent=2)+'\n');print(json.dumps(summary),flush=True)
