"""Read-only child for an owned committed clone; parent enforces60s lifetime."""
import sys
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from property_apply_queries import COLS
B=Path(__file__).resolve().parent;mode=sys.argv[1];assert mode in ('control','candidate')
F='client_dev.ashlar_entropy_20261006_r86';A=F+'.edge_merge_'+mode+'_r146';S=F+'.schedule_r139_1'
O=B/'out/native/ashlar_merge_pruning_r146'/ (mode+'-read-lane')
assert not (O/'statements.jsonl').exists(),'Inspect saved handles before repeating'
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=15')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
rows=c.sql('oracle',f"SELECT {','.join(COLS)} FROM {S} VERSION AS OF 0 ORDER BY sha2(cast(id AS STRING),256) LIMIT 30")
assert len(rows)==30
query=f"SELECT {','.join(COLS)} FROM {A} VERSION AS OF 1 WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:rel AS BIGINT) AND id=CAST(:id AS BIGINT)"
for i,row in enumerate(rows):assert c.sql(mode+'-read-'+str(i),query,parameters={'hash':row[16],'source':row[0],'rel':row[1],'id':row[2]})==[row]
c.close();print('Thirty exact '+mode+' clone reads passed')
