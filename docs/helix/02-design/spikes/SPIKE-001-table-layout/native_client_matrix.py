"""Separate transport and UUID query-shape effects; all cache hits inspected."""
import json,time
from pathlib import Path
from databricks import sql
from databricks.sdk.core import Config
from persistent_sql import Client
BASE=Path(__file__).resolve().parent;out=BASE/'out/native/client-matrix-20261005-f2';c=Client(out);driver=[];cfg=Config(profile='aidev-cus')
def provider():return cfg.authenticate
with sql.connect(server_hostname='adb-7405607548213398.18.azuredatabricks.net',http_path='/sql/1.0/warehouses/2439e1f2e37ac563',credentials_provider=provider,session_configuration={'use_cached_result':'false'},use_cloud_fetch=False) as conn:
 with conn.cursor() as cur:
  cur.execute('SET use_cached_result=false');cur.fetchall()
  for rep in range(26):
   key=1+(rep*104729)%600000;expected=None
   for transport,nonce in [('rest',False),('rest',True),('driver',False),('driver',True)]:
    name=transport+('-uuid' if nonce else '-plain');statement=f"SELECT id,props_json,retained_json{',uuid()' if nonce else ''} FROM client_dev.ashlar_pruning_20261005_c1.liquid_16m WHERE id={key}"
    if transport=='rest':rows=c.sql(f'{name}-{rep}',statement)
    else:
     start=time.perf_counter();cur.execute(statement);rows=[list(x) for x in cur.fetchall()];r={'label':f'{name}-{rep}','sql':statement,'statement_id':cur.query_id,'wall_ms':(time.perf_counter()-start)*1000,'rows':rows};driver.append(r)
     with (out/'driver-statements.jsonl').open('a') as h:h.write(json.dumps(r)+'\n')
    clean=[str(rows[0][0]),*rows[0][1:3]];assert len(rows)==1 and clean[0]==str(key)
    if expected is None:expected=clean
    else:assert expected==clean
   print('round',rep,flush=True)
c.records.extend(driver);(out/'all-statements.json').write_text(json.dumps(c.records,indent=2)+'\n');c.history();print('Client/query-shape matrix completed; inspect async metrics',flush=True)
