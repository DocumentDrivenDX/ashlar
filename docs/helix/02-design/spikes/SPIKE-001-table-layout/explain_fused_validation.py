"""Read-only optimizer plans for the completed fused publisher checks."""
import json,re
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent
records=[json.loads(x) for x in (B/'out/native/ashlar_fused_validation_20261005_n2/statements.jsonl').read_text().splitlines()]
sql=next(r['sql'] for r in records if r['label']=='atomic-apply')
queries=re.findall(r'IF \((SELECT.*?)\)(?:<>200000)? THEN SIGNAL',sql,re.S)
assert len(queries)==3
out=B/'out/native/ashlar_fused_validation_plans_20261005_n2';c=DriverClient(out)
for i,q in enumerate(queries):
 rows=c.sql('validation-plan-'+str(i),'EXPLAIN FORMATTED '+q)
 (out/('plan-'+str(i)+'.txt')).write_text('\n'.join(str(row[0]) for row in rows)+'\n')
c.history();c.close();print('Three validation optimizer plans retained; no validation queries rerun',flush=True)
