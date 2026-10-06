"""Exhaustive exact native field/hidden metadata parity across maintenance."""
import json
from pathlib import Path
from driver_sql import DriverClient
from edge_fused_guard import COLS
B=Path(__file__).resolve().parent;F='client_dev.ashlar_lc64_20261005_r42'
assert json.loads((B/'out/native/ashlar_conditional_maintenance_20261006_r56/scope.json').read_text())['versions']=={'lc64':5}
out=B/'out/native/ashlar_conditional_maintenance_verify_20261006_r57';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
checks=' OR '.join(f'a.{x} IS DISTINCT FROM b.{x}' for x in COLS+['apply_batch_id'])+ ' OR a.rid IS DISTINCT FROM b.rid OR a.rcv IS DISTINCT FROM b.rcv'
q=f"""WITH a AS (SELECT e.*,e._metadata.row_id rid,e._metadata.row_commit_version rcv FROM {F}.edge_current VERSION AS OF 3 e), b AS (SELECT e.*,e._metadata.row_id rid,e._metadata.row_commit_version rcv FROM {F}.edge_current VERSION AS OF 5 e)
SELECT count(*),count_if(a.id IS NULL),count_if(b.id IS NULL),count_if({checks}) FROM a FULL OUTER JOIN b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.id=b.id"""
assert c.sql('full-carrier-parity',q)==[['10019981','0','0','0']]
for v in [3,5]:
 assert c.sql('unique-'+str(v),f'SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)) FROM {F}.edge_current VERSION AS OF {v}')==[['10019981','10019981']]
c.history();c.close()
(out/'summary.json').write_text(json.dumps({'state':'passed','versions':[3,5],'rows':10019981,'scope':'exhaustive exact18-field and hidden row-id/commit-version parity, full outer native identity join, unique typed keys at both snapshots; maintenance correctness, not ingest/scale admission'},indent=2))
print('Exhaustive edge maintenance parity passed',flush=True)
