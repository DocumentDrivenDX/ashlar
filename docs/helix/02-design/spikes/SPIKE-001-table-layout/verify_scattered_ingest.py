"""Post-publication integrity and distribution checks; excluded from freshness."""
import json
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent
out=B/'out/native/ashlar_scattered_verify_20261005_n1'
c=DriverClient(out);F='client_dev.ashlar_scattered_20261005_n1'
summary=json.loads((B/'out/native/ashlar_scattered_20261005_n1/stream-summary.json').read_text())
assert summary['state']=='completed'
v=summary['batches'][0]['versions']['object_current']
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180')
rows=c.sql('distribution',f'SELECT count(*),count(DISTINCT id),min(id),max(id),count(DISTINCT floor((id-1)/100000)) FROM {F}.stage_1')
assert rows[0][0:2]==['200000','200000'] and rows[0][4]=='100'
cols=['source_system','type_id','id','logical_key_json','schema_revision','entity_version','props_json','retained_json','root_id','source_feed','source_epoch','source_position','published_at']
diff=' OR '.join(f'o.{x} IS DISTINCT FROM n.{x}' for x in cols)
rows=c.sql('unchanged-all-carriers',f'SELECT count(*),count_if({diff}) FROM {F}.object_current VERSION AS OF {v} o JOIN client_dev.ashlar_scale_20261005_i1.object_current VERSION AS OF 2 n ON o.source_system=n.source_system AND o.type_id=n.type_id AND o.id=n.id WHERE pmod((o.id-1)*104729,10000000)>=200000')
assert rows==[['9800000','0']]
rows=c.sql('whole-table-identities',f'SELECT count(*),count(DISTINCT id),min(id),max(id) FROM {F}.object_current VERSION AS OF {v}')
assert rows==[['10000000','10000000','1','10000000']]
rows=c.sql('journal-event-keys',f'SELECT count(*),count(DISTINCT struct(source_feed,source_epoch,source_position,event_ordinal)) FROM {F}.property_journal')
assert rows==[['200000','200000']]
c.sql('post-detail',f'DESCRIBE DETAIL {F}.object_current')
c.sql('object-mutation-history',f'DESCRIBE HISTORY {F}.object_current')
c.history();c.close()
(out/'scope.json').write_text(json.dumps(dict(state='completed',object_version=v,changed_entities=200000,unchanged_entities=9800000,identity_bins_100k=100,full_unchanged_column_count=13,limitations='single synthetic batch; no concurrency, native source feed, sustained p95 or billion-scale admission'),indent=2)+'\n')
print('Scattered distribution and all 9.8M unchanged full-carrier rows verified',flush=True)
