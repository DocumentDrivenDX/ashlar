"""Bounded routine maintenance with affected-row parity and paired pinned reads."""
import json
from pathlib import Path
from driver_sql import DriverClient

B=Path(__file__).resolve().parent
P=B/'out/native/ashlar_parallel_publication_20261007_r101'
O=B/'out/native/ashlar_maintenance_reads_20261007_r102'
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
assert c.sql('new-reader-publication',f"SELECT table_versions_json FROM {M} WHERE publication_id='r101-b1'")==[[json.dumps(batch['versions'],sort_keys=True,separators=(',',':'))]]
rows=c.sql('new-reader-expected',f"SELECT {','.join(cols)} FROM {batch['source_stage']} VERSION AS OF 0 ORDER BY id LIMIT 30")
assert len(rows)==30 and len({r[2] for r in rows})==30
def lit(v): return "decode(unhex('"+v.encode().hex()+"'),'UTF-8')"
def reads(phase,version):
 for i,row in enumerate(rows):
    query=f"SELECT {','.join(cols)} FROM {E} VERSION AS OF {version} WHERE lookup_hash={lit(row[16])} AND source_system={lit(row[0])} AND rel_type_id={int(row[1])} AND id={int(row[2])}"
    assert c.sql(phase+'-'+str(i),query)==[row]
old=int(c.sql('before-version',f'DESCRIBE HISTORY {E} LIMIT 1')[0][0])
assert old>=13
history=c.sql('before-history',f'DESCRIBE HISTORY {E} LIMIT 30')
names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
lineage=[dict(zip(names,v)) for v in history]
assert {int(v['version']) for v in lineage if int(v['version'])>=13}==set(range(13,old+1))
assert all(v['operation']=='OPTIMIZE' for v in lineage if int(v['version'])>13)
reads('before',old)
c.sql('routine-maintenance',f'OPTIMIZE {E}')
new=int(c.sql('after-version',f'DESCRIBE HISTORY {E} LIMIT 1')[0][0])
assert new>=old
assert c.sql('global-identities',f'SELECT count(*),count(DISTINCT id),count_if(entity_version=9) FROM {E} VERSION AS OF {new}')==[['20000000','20000000','100000']]
text={'source_system','schema_revision','props_json','retained_json','order_key','source_feed','source_epoch','lookup_hash','apply_batch_id','source_cursor_json','source_delivery_id'}
checks=[f"NOT(hex(encode(a.{col},'UTF-8')) <=> hex(encode(b.{col},'UTF-8')))" if col in text else f'NOT(a.{col} <=> b.{col})' for col in cols]
assert c.sql('affected-full-parity',f"SELECT count(*) FROM (SELECT * FROM {E} VERSION AS OF {new} WHERE apply_batch_id='r101-b1') a FULL OUTER JOIN {batch['source_stage']} VERSION AS OF 0 b ON a.source_system=b.source_system AND a.rel_type_id=b.rel_type_id AND a.id=b.id WHERE a.id IS NULL OR b.id IS NULL OR "+' OR '.join(checks))==[['0']]
for i,versions in enumerate(((new,13),(13,new))):
 for version in versions: reads(f'pair-{i}-'+('new' if version==new else 'published'),version)
c.sql('maintenance-history',f'DESCRIBE HISTORY {E} LIMIT 30')
c.sql('current-detail',f'DESCRIBE DETAIL {E}')
(O/'summary.json').write_text(json.dumps({'state':'completed maintenance and exact paired reads','old_version':old,'published_version':13,'new_version':new,
 'scope':'One routine OPTIMIZE, global identity counts and all20 affected100k carriers against immutable intended stage. Not a fresh all-field comparison of all20M rows. Same30 large-token keys, no concurrent publisher. Original manifest remains pinned13; no maintained descriptor installed.'},indent=2)+'\n')
c.history();c.close()
