"""Read-only repeated-key control on the existing 10M canonical fixture."""
import json
from pathlib import Path
from driver_sql import DriverClient
out=Path(__file__).resolve().parent/'out/native/ashlar_scale_warm_20261005_i2'
c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
c.sql('environment','SELECT current_version()')
expected={}
for phase in ('prime','repeat','repeat2'):
 for rep in range(51):
  key=1+(rep*104729)%10000000
  rows=c.sql(f'{phase}-{rep}',f"SELECT id,props_json,retained_json,logical_key_json FROM client_dev.ashlar_scale_20261005_i1.object_current WHERE source_system='pilot' AND type_id=1 AND id={key}")
  assert len(rows)==1 and rows[0][0]==str(key) and rows[0][3]=='['+str(key)+']'
  if phase=='prime':expected[key]=rows
  else:assert rows==expected[key]
 print(phase,'complete',flush=True)
c.history();c.close()
(out/'run.json').write_text(json.dumps({'state':'completed','scope':'read-only same-key repeated control; exact returned-carrier equality; inspect remote bytes before claiming warm','nodes':10000000,'edges':0},indent=2)+'\n')
