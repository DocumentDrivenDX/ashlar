"""Verify historical/current logical schemas for immutable row custody proof."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;P=B/'out/native/ashlar_isolation_r121';O=P/'schema'
assert not (O/'statements.jsonl').exists(),'Inspect prior handles'
s=json.loads((P/'summary.json').read_text());r=s['batches'][0];F='client_dev.ashlar_entropy_20261006_r86'
c=DriverClient(O);schemas={}
for role,table,old in [('canonical',F+'.edge_current',r['old_current_version']),('raw',F+'.source_record_r89',s['initial_raw_version']),('journal',F+'.property_journal_r89',s['initial_journal_version'])]:
 schemas[role]={}
 for phase,v in [('old',old),('new',r['versions'][table])]:
  c.sql(role+'-'+phase,f'SELECT * FROM {table} VERSION AS OF {v} LIMIT 0')
  schemas[role][phase]=[list(d) for d in c.cursor.description]
 assert schemas[role]['old']==schemas[role]['new'],role
(O/'summary.json').write_text(json.dumps({'state':'All3 pinned predecessor/output logical schemas match','schemas':schemas,'qualification':'DBAPI column descriptors at exact snapshots. Immutable file/physical-row custody relies on Delta immutable-file semantics and stable logical schema; not equivalent to independently re-reading all payload bytes.'},indent=2,default=str)+'\n');c.history();c.close();print('Pinned schemas match')
