"""Same-session literal versus native bound-parameter singleton comparison."""
import json,time
from pathlib import Path
from databricks import sql
from databricks.sdk.core import Config
from persistent_sql import Client
BASE=Path(__file__).resolve().parent;out=BASE/'out/native/parameter-driver-20261005-f3';out.mkdir(parents=True,exist_ok=True);cfg=Config(profile='aidev-cus');records=[]
def provider():return cfg.authenticate
with sql.connect(server_hostname='adb-7405607548213398.18.azuredatabricks.net',http_path='/sql/1.0/warehouses/2439e1f2e37ac563',credentials_provider=provider,session_configuration={'use_cached_result':'false'},use_cloud_fetch=False) as conn:
 with conn.cursor() as cur:
  cur.execute('SET use_cached_result=false');cur.fetchall()
  for rep in range(51):
   key=1+(rep*104729)%600000;expected=None
   for name in ['literal','parameter']:
    statement='SELECT id,props_json,retained_json FROM client_dev.ashlar_pruning_20261005_c1.liquid_16m WHERE id='+('?' if name=='parameter' else str(key))
    start=time.perf_counter();cur.execute(statement,[key] if name=='parameter' else None);rows=[list(x) for x in cur.fetchall()];rec={'label':f'{name}-{rep}','sql':statement,'parameter':[key] if name=='parameter' else None,'statement_id':cur.query_id,'wall_ms':(time.perf_counter()-start)*1000,'rows':rows};records.append(rec)
    with (out/'statements.jsonl').open('a') as h:h.write(json.dumps(rec)+'\n')
    assert len(rows)==1 and rows[0][0]==key
    if expected is None:expected=rows
    else:assert expected==rows
   if rep%10==0:print('round',rep,flush=True)
c=Client(out);c.records=records;c.history();print('Parameter comparison completed',flush=True)
