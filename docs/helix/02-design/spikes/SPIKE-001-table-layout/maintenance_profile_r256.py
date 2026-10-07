"""Read-only inherited maintenance settings for owned growth tables."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_maintenance_profile_r256';assert not O.exists();O.mkdir();prior=json.loads((B/'out/native/ashlar_scale_edges_r252/audited-summary.json').read_text());c=Client(O,observation_timeout=200,cancel_after=180);a={'state':'Reading extended metadata','roles':{}}
for role,t in prior['tables'].items():
 rows=c.sql('extended-'+role,'DESCRIBE TABLE EXTENDED '+t['table']);a['roles'][role]={'table':t['table'],'expected_uuid':t['id'],'extended_rows':rows}
a['state']='Extended metadata read for five owned staging tables';(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({k:[r for r in v['extended_rows'] if any('predict' in str(x).lower() for x in r)] for k,v in a['roles'].items()},indent=2))
