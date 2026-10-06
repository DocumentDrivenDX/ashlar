"""Bounded incremental reclustering and pinned lookup control; no vacuum."""
import json,time
from pathlib import Path
from driver_sql import DriverClient
B=Path(__file__).resolve().parent;F='client_dev.ashlar_scattered_20261005_n1';out=B/'out/native/ashlar_recluster_20261005_n7';c=DriverClient(out)
c.sql('statement-cap','SET STATEMENT_TIMEOUT=180');before=int(c.sql('before-version',f'DESCRIBE HISTORY {F}.object_current LIMIT 1')[0][0]);c.sql('before-detail',f'DESCRIBE DETAIL {F}.object_current')
begun=time.time();c.sql('incremental-optimize',f'OPTIMIZE {F}.object_current');elapsed=time.time()-begun
v=int(c.sql('after-version',f'DESCRIBE HISTORY {F}.object_current LIMIT 1')[0][0]);c.sql('after-detail',f'DESCRIBE DETAIL {F}.object_current');c.sql('maintenance-history',f'DESCRIBE HISTORY {F}.object_current')
(out/'scope.json').write_text(json.dumps(dict(state='reading',before_version=before,after_version=v,maintenance_wall_s=elapsed,scope='one incremental OPTIMIZE; not a continuous maintenance policy or publication freshness pass'),indent=2)+'\n')
# Expected exact carriers from the already recorded version-19 singleton probe.
old=[json.loads(x) for x in (B/'out/native/ashlar_post_ingest_reads_20261005_n6/statements.jsonl').read_text().splitlines()]
expected={x['label']:x['response']['result']['data_array'] for x in old if x['label'].startswith('prime-')}
for phase in ['prime','repeat','repeat2']:
 for rep in range(51):
  key=1+(rep*196613)%10000000
  rows=c.sql(f'{phase}-{rep}',f"SELECT id,props_json,retained_json,logical_key_json,entity_version,source_position FROM {F}.object_current VERSION AS OF {v} WHERE source_system='pilot' AND type_id=1 AND id={key}")
  assert rows==expected[f'prime-{rep}']
 print(phase,'completed',flush=True)
c.history();c.close();(out/'scope.json').write_text(json.dumps(dict(state='completed',before_version=before,after_version=v,maintenance_wall_s=elapsed,scope='one incremental OPTIMIZE followed by identical key/projection lookup control; exact equality for 51 returned carriers against version 19; no exhaustive full-table maintenance preservation, continuous policy or billion-scale claim'),indent=2)+'\n');print('Reclustering control completed',flush=True)
