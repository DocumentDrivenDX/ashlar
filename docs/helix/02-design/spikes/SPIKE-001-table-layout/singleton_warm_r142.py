"""Warm repeat after maintenance and exact rewritten-row verification."""
import json,math
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_singleton_warm_r142'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';A=F+'.edge_optimize_r140';S=F+'.schedule_r139_1'
c=DriverClient(O);assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
rows=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30")
assert len(rows)==30
query=f"SELECT {','.join(COLS)} FROM {A} VERSION AS OF 2 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
for i,row in enumerate(rows):assert c.sql('warm-read-'+str(i),query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]},tag=False)==[row]
c.history();c.close();print('Thirty exact post-maintenance warm-repeat reads passed; final audit pending')
