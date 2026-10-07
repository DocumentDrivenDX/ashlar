"""Post-publication exhaustive unchanged carriers and inherited history proof."""
import ast,json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;P=B/'out/native/ashlar_isolation_r121';O=P/'integrity-custody'
assert not (O/'statements.jsonl').exists(),'Inspect existing handles before repeating'
s=json.loads((P/'summary.json').read_text());assert s['state'].startswith('completed one property publication')
r=s['batches'][0];F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_current';R=F+'.source_record_r89';J=F+'.property_journal_r89';stage=r['source_stage']
tree=ast.parse((B/'publication_adjacency_r121.py').read_text())
cols=next(ast.literal_eval(n.value) for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='cols' for t in n.targets))
c=DriverClient(O);c.sql('timeout','SET STATEMENT_TIMEOUT=180')
assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
joined=f"a.source_system=k.source_system AND a.rel_type_id=k.rel_type_id AND a.id=k.id"
old=r['old_current_version'];new=r['versions'][E]
physical='a.source_system,a.rel_type_id,a.id,a._metadata.file_path,a._metadata.row_index'
previous=f"SELECT {physical} FROM {E} VERSION AS OF {old} a LEFT ANTI JOIN (SELECT source_system,rel_type_id,id FROM {stage} VERSION AS OF 0) k ON {joined}"
current=f"SELECT {physical} FROM {E} VERSION AS OF {new} a LEFT ANTI JOIN (SELECT source_system,rel_type_id,id FROM {stage} VERSION AS OF 0) k ON {joined}"
assert c.sql('untouched-physical-custody',f'SELECT count(*) FROM (({previous} EXCEPT ALL {current}) UNION ALL ({current} EXCEPT ALL {previous}))')==[['0']]
assert c.sql('untouched-count',f'SELECT count(*) FROM ({current})')==[['19900000']]
for role,table,old_v in [('raw',R,s['initial_raw_version']),('journal',J,s['initial_journal_version'])]:
 new_v=r['versions'][table]
 baseline=f'SELECT * FROM {table} VERSION AS OF {old_v}'
 retained=f"SELECT * FROM {table} VERSION AS OF {new_v} WHERE NOT(apply_batch_id <=> 'r121-b1')"
 assert c.sql(role+'-inherited-parity',f'SELECT count(*) FROM (({baseline} EXCEPT ALL {retained}) UNION ALL ({retained} EXCEPT ALL {baseline}))')==[['0']]
(O/'summary.json').write_text(json.dumps({'state':'Exhaustive19.9M untouched logical-key/file-path/physical-row-index custody and inherited complete raw/journal rows passed','versions':r['versions'],'old_current':old,'old_raw':s['initial_raw_version'],'old_journal':s['initial_journal_version'],'qualification':'Post-publication oracle outside recorded freshness clock. Unchanged physical row custody uses Delta immutable-file semantics, not a fresh full-payload byte comparison. Stable logical schema required; failed wide EXCEPT ALL retained separately. Does not claim publisher gated descriptor on these checks. Legacy raw timestamp representation is compared exactly as stored, not retroactively corrected.'},indent=2)+'\n');c.history();c.close();print('Exhaustive untouched and inherited integrity passed')
