"""Refresh EXPLAIN history by saved IDs and record plan visibility limits."""
import json
from pathlib import Path
from persistent_sql import Client
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_write_plans_r136'
c=Client(O);c.records=[json.loads(l) for l in (O/'statements.jsonl').read_text().splitlines()]
h={q['query_id']:q for q in c.history()}
assert len(c.records)==3 and all(r['sql'].startswith('EXPLAIN FORMATTED INSERT INTO ') for r in c.records)
assert all(r['response']['status']['state']=='SUCCEEDED' and h[r['statement_id']]['is_final'] for r in c.records),'Refresh same IDs only'
plans={f.stem:{'append_wrapper':'AppendDataExecV1' in f.read_text(),'reported_unsupported_append_wrapper':'Unsupported node: AppendDataExecV1' in f.read_text(),'exchange_nodes_exposed':f.read_text().count('Exchange')} for f in O.glob('*.txt')}
(O/'summary.json').write_text(json.dumps({'state':'Three read-only EXPLAIN statements successful/final; write internals not exposed','plans':plans,'qualification':'Plans show AppendDataExecV1 command wrappers and a Photon unsupported-wrapper explanation for all three queries. They do not expose internal sort/shuffle/write stages; this is not evidence that every runtime write task falls back, or that serialization accounts for capture latency. No INSERT was executed.'},indent=2)+'\n')
print('Three EXPLAIN histories final; visibility limits recorded')
