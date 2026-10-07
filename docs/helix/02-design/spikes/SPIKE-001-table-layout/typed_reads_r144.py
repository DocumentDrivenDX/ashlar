"""Pinned exact singleton comparison: typed integers versus decimal text casts."""
from pathlib import Path
from driver_sql import DriverClient
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_typed_reads_r144'
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';S=F+'.schedule_r139_1'
c=DriverClient(O);assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
rows=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30")
assert len(rows)==30
base=f"SELECT {','.join(COLS)} FROM {E} VERSION AS OF 23 WHERE lookup_hash=:hash AND source_system=:source AND "
queries={'cast':base+'rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)',
 'typed':base+'rel_type_id=:rel AND id=:id'}
for i,row in enumerate(rows):
 for mode in (('cast','typed') if i%2==0 else ('typed','cast')):
  params={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]}
  if mode=='typed':params.update(rel=int(row[1]),id=int(row[2]))
  assert c.sql(f'{mode}-{i}',queries[mode],parameters=params,tag=False)==[row]
c.history();c.close();print('Sixty exact pinned singleton reads passed')
