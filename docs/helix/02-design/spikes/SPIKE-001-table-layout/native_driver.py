"""Persistent official SQL driver, session cache disabled; read-only controls."""
import json,time
from pathlib import Path
from importlib.metadata import version
from databricks import sql
from databricks.sdk.core import Config
from persistent_sql import Client
BASE=Path(__file__).resolve().parent;out=BASE/'out/native/driver-20261005-f1';out.mkdir(parents=True,exist_ok=True)
cfg=Config(profile='aidev-cus')
def provider():return cfg.authenticate
records=[]
with sql.connect(server_hostname='adb-7405607548213398.18.azuredatabricks.net',http_path='/sql/1.0/warehouses/2439e1f2e37ac563',credentials_provider=provider,session_configuration={'use_cached_result':'false'},use_cloud_fetch=False) as conn:
 with conn.cursor() as cur:
  def run(label,statement):
   start=time.perf_counter();cur.execute(statement);rows=cur.fetchall();rec={'label':label,'sql':statement,'statement_id':cur.query_id,'wall_ms':(time.perf_counter()-start)*1000,'rows':[list(x) for x in rows]};records.append(rec)
   with (out/'statements.jsonl').open('a') as h:h.write(json.dumps(rec)+'\n')
   return rows
  run('disable-cache','SET use_cached_result=false')
  state=run('cache-setting','SET use_cached_result');assert str(state[0][1]).lower()=='false',state
  for rep in range(26):
   key=1+(rep*7919)%600000
   for name,statement in [('constant','SELECT 1'),('tiny','SELECT id,props_json FROM client_dev.ashlar_floor_20261005_b1.tiny WHERE id=1'),('liquid16',f'SELECT id,props_json,retained_json FROM client_dev.ashlar_pruning_20261005_c1.liquid_16m WHERE id={key}'),('liquid128',f'SELECT id,props_json,retained_json FROM client_dev.ashlar_pruning_20261005_c1.liquid_128m WHERE id={key}')]:
    rows=run(f'{name}-{rep}',statement);assert len(rows)==1
    assert rows[0][0]==(1 if name in ['constant','tiny'] else key)
   print('round',rep,flush=True)
c=Client(out);c.records=records;c.history()
(out/'environment.json').write_text(json.dumps({'connector':version('databricks-sql-connector'),'sdk':version('databricks-sdk'),'cache_setting':False,'samples_per_shape':25,'state':'reads completed; asynchronous metrics need completeness check'},indent=2)+'\n')
print('Driver controls completed',flush=True)
