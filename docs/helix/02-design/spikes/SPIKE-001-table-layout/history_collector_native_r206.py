"""Validate shared history collector on saved real publication IDs; no SQL submissions."""
import json,time
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_history_collector_r206';assert not O.exists()
source=B/'out/native/ashlar_queue_serial_r197';records=[]
for lane in ['', 'journal-lane','apply-lane']:records.extend(json.loads(x) for x in (source/lane/'statements.jsonl').read_text().splitlines())
c=Client(O);start=time.perf_counter();h=collect_history(c.w,records,O/'shared-query-history.json',require_final=True);wall=(time.perf_counter()-start)*1000
assert len(h)==len(records)
for r in records:assert h[r['statement_id']]['query_text'].endswith(r['sql'])
s={'state':'Shared collector resolves all saved real publication IDs with final native histories','queries':len(h),'pages':json.loads((O/'shared-query-history.json').read_text())['pages'],'get_wall_ms':wall,'qualification':'GET-only read of historical IDs, zero SQL submissions. Measured collection latency is one observation, not live publication improvement. No relaxation of exact SQL/result/native status requirements.'}
(O/'summary.json').write_text(json.dumps(s,indent=2)+'\n');print(json.dumps(s,indent=2))
