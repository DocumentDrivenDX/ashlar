"""Read-only EXPLAIN of captured write plans; no INSERT execution."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_write_plans_r136'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
previous=B/'out/native/ashlar_isolation_r133'
main=[json.loads(l) for l in (previous/'statements.jsonl').read_text().splitlines()]
journal=[json.loads(l) for l in (previous/'journal-lane/statements.jsonl').read_text().splitlines()]
queries={'raw-capture':next(r['sql'] for r in main if r['label']=='capture-r133-b1'),
 'journal-capture':next(r['sql'] for r in journal if r['label']=='journal-r133-b1'),
 'raw-stored-replay':"INSERT INTO client_dev.ashlar_entropy_20261006_r86.source_record_r89 SELECT * FROM client_dev.ashlar_entropy_20261006_r86.source_record_r89 VERSION AS OF 16 WHERE apply_batch_id='r133-b1'"}
c=DriverClient(O)
for label,query in queries.items():
 rows=c.sql(label,'EXPLAIN FORMATTED '+query)
 (O/(label+'.txt')).write_text('\n'.join(str(v) for row in rows for v in row)+'\n')
c.history();c.close();print('Three write plans captured; no writes executed')
