"""Read-only latest-version metadata inventory; no larger scale write."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;T='client_dev.ashlar_lc64_20261005_r42.edge_current'
out=B/'out/native/ashlar_scale_baseline_inventory_20261006_r77';c=Client(out)
before=c.sql('history-before',f'DESCRIBE HISTORY {T} LIMIT 1')
rows=c.sql('detail',f'DESCRIBE DETAIL {T}')
response=c.records[-1]['response'];cols=[col['name'] for col in response['manifest']['schema']['columns']]
detail=dict(zip(cols,rows[0]));after=c.sql('history-after',f'DESCRIBE HISTORY {T} LIMIT 1')
assert before[0][0]==after[0][0], 'Latest changed during metadata inventory; no coherent snapshot claim'
version=int(before[0][0]);files=int(detail['numFiles']);size=int(detail['sizeInBytes'])
summary={'state':'passed','table':T,'version':version,'num_files':files,'size_bytes':size,'mean_active_file_bytes':size/files,'detail':detail,'scope':'Read-only stable latest-version metadata inventory on existing bounded edge fixture. No row-count scan, entropy qualification, full 0.3 width, bill or billion-node extrapolation claim.'}
(out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
print(json.dumps({k:summary[k] for k in ['version','num_files','size_bytes','mean_active_file_bytes']}),flush=True)
