"""Exact 30-key reads of r103's new publication, separate from old pinned controls."""
import json
from pathlib import Path
from driver_sql import DriverClient

B=Path(__file__).resolve().parent
P=B/'out/native/ashlar_maintained_contention_20261007_r103'
O=P/'new-publication-reads'
assert not (O/'statements.jsonl').exists(), 'Inspect prior statements; do not blindly rerun'
s=json.loads((P/'summary.json').read_text())
assert s['state']=='completed parallel publication and bounded singleton contention; correctness checks passed'
batch=s['batches'][0]
F='client_dev.ashlar_entropy_20261006_r86'
E=F+'.edge_current';M=F+'.publication_manifest_r89'
cols=['source_system','rel_type_id','id','source_type','source_id','target_type','target_id','schema_revision',
      'entity_version','props_json','retained_json','order_key','source_feed','source_epoch','source_position',
      'published_at','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id']
c=DriverClient(O)
c.sql('new-reader-timeout-set','SET STATEMENT_TIMEOUT=180')
assert c.sql('new-reader-timeout-readback','SET STATEMENT_TIMEOUT')[0][-1]=='180'
assert c.sql('new-reader-cache-readback','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
assert c.sql('new-reader-publication',f"SELECT table_versions_json FROM {M} WHERE publication_id='r103-b1'")==[[json.dumps(batch['versions'],sort_keys=True,separators=(',',':'))]]
rows=c.sql('new-reader-expected',f"SELECT {','.join(cols)} FROM {batch['source_stage']} VERSION AS OF 0 ORDER BY id LIMIT 30")
assert len(rows)==30 and len({r[2] for r in rows})==30
def lit(v): return "decode(unhex('"+v.encode().hex()+"'),'UTF-8')"
for i,row in enumerate(rows):
    query=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF {batch['versions'][E]} WHERE lookup_hash={lit(row[16])} AND source_system={lit(row[0])} AND rel_type_id={int(row[1])} AND id={int(row[2])}"
    assert c.sql('fresh-post-'+str(i),query)==[row]
(O/'summary.json').write_text(json.dumps({'state':'passed new published full-carrier reads','rows':30,'publication_id':'r103-b1','version':batch['versions'][E],
    'scope':'Same 30-key large-token hot-set cohort; independently staged expected full20 carriers; no concurrent publisher. Not a graph-wide or cold-data SLA.'},indent=2)+'\n')
c.history();c.close()
