from pathlib import Path
import json,collections
import pyarrow as pa,pyarrow.parquet as pq
from commerce_source_transaction import build_transaction
from run_commerce_outbox_publication import original_commerce_oracle
from fixture_oracle import fixture_columns
root=Path('/private/tmp/ashlar-indexed-commerce-publication-20261010-a');report=json.loads((root/'report.json').read_bytes());model=(root/'original-ontology.json').read_bytes();graph=(root/'original-graph.json').read_bytes();bindings=json.loads((root/'development-bindings.json').read_bytes());batch,rebuilt=build_transaction(model,graph,source_system='private-original-commerce-fixture',binding_profile=bindings['profile']);assert bindings==rebuilt
expected=original_commerce_oracle(model,graph,bindings,batch,fixture_columns(Path.cwd()));versions=json.loads(report['native_manifest']['table_versions_json']);results=[]
def bag(rows):return collections.Counter(json.dumps(r,sort_keys=True,separators=(',',':'))for r in rows)
for item in report['table_registry']:
 role=item['table'].split('.')[-1]
 if role not in expected:continue
 path=Path(item['path']);active={};uuid=None
 for log in sorted((path/'_delta_log').glob('*.json')):
  if int(log.stem)>int(versions[item['table']]):continue
  for line in log.read_text().splitlines():
   action=json.loads(line)
   if 'metaData'in action:uuid=action['metaData']['id']
   if 'add'in action:active[action['add']['path']]=True
   if 'remove'in action:active.pop(action['remove']['path'],None)
 rows=[]
 for name in active:
  table=pq.ParquetFile(path/name).read();columns={}
  for field in table.schema:
   column=table[field.name]
   if pa.types.is_timestamp(field.type):
    unit=field.type.unit;assert unit in ('us','ns')
    numbers=column.cast(pa.int64()).to_pylist()
    if unit=='ns':
     assert all(v is None or v%1000==0 for v in numbers);numbers=[None if v is None else v//1000 for v in numbers]
    column=pa.array(numbers,pa.int64())
   columns[field.name]=[None if v is None else str(v) if type(v)is int else v for v in column.to_pylist()]
  rows.extend([{k:v[i]for k,v in columns.items()}for i in range(table.num_rows)])
 assert uuid==item['uuid']and bag(rows)==bag(expected[role]),role
 results.append({'role':role,'uuid':uuid,'version':versions[item['table']],'rows':len(rows),'active_files':sorted(active),'full_cell_bag_equal':True})
p=Path('/private/tmp/ashlar-indexed-commerce-evidence-20261010-b/offline-original-row-oracle.json');p.write_text(json.dumps({'scope':'Read-onlyDeltaJSON+Parquet vsindependentoriginalsourceoracle; timestampmicroseconds/int64exactstringconversion, nofloat/noengine','pyarrow':pa.__version__,'tables':results},indent=2)+'\n');print(results)
