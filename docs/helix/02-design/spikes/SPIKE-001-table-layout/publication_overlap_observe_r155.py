"""Single read-only observation of unpublished current writes and old pinned rows."""
import json
from pathlib import Path
from bounded_reads_r145 import BoundedReads
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_queue_r155/observer'
assert not (O/'statements.jsonl').exists(),'Inspect saved observation; no replay'
F='client_dev.ashlar_entropy_20261006_r86';E=F+'.edge_queue_r155';M=F+'.manifest_queue_r155'
c=BoundedReads(O);c.sql('timeout','SET STATEMENT_TIMEOUT=15')
rows=c.sql('unpublished-window',f"SELECT (SELECT count(*) FROM {M} WHERE publication_id='r155-b1') manifest_rows,(SELECT count(*) FROM {E} WHERE entity_version=16 AND apply_batch_id='r155-b1') applied_rows,(SELECT count(*) FROM {E} VERSION AS OF 0 WHERE entity_version=15 AND apply_batch_id='r139-b1') old_pinned_rows")
assert rows==[['0','100000','100000']], 'Observation missed unpublished window; retain result, do not replay'
(O/'summary.json').write_text(json.dumps({'state':'Unpublished100k current rows observed while manifest absent; old pinned100k rows retained','qualification':'Single read-only observation, not complete consumer concurrency or failure recovery proof'},indent=2)+'\n')
c.history();c.close();print('Unpublished physical writes and retained old pinned rows observed')
