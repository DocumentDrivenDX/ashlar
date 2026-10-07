"""Read-only r98 paired batch predicate over the exact same raw snapshot."""
import json
from pathlib import Path
from driver_sql import DriverClient

B = Path(__file__).resolve().parent
O = B/'out/native/ashlar_raw_batch_filter_20261007_r98'
assert not (O/'statements.jsonl').exists(), 'Inspect previous completed/unknown statements'
baseline = B/'out/native/ashlar_raw_validation_20261007_r97/persistent-uncached'
records = [json.loads(line) for line in (baseline/'statements.jsonl').read_text().splitlines()]
filtered = next(r['sql'] for r in records if r['label']=='parity-0')
assert filtered.count(" WHERE apply_batch_id='r96-b3'") == 1
unfiltered = filtered.replace(" WHERE apply_batch_id='r96-b3'",'')
c = DriverClient(O)
pairs = []
for i, order in enumerate(('unfiltered-first','filtered-first')):
    timing = {}
    for name in (('unfiltered','filtered') if i==0 else ('filtered','unfiltered')):
        result = c.sql(f'{name}-{i}', filtered if name=='filtered' else unfiltered)
        assert result == [['0']]
        timing[name+'_caller_ms'] = c.records[-1]['wall_ms']
        print(name,round(c.records[-1]['wall_ms']),flush=True)
    pairs.append(dict(order=order,**timing))
(O/'summary.json').write_text(json.dumps(dict(state='completed paired raw batch predicate; final metrics pending',pairs=pairs,
    scope='Identical immutable r96-b3 input and raw version7, same payload/cursor/digest/timestamp parity predicate, persistent uncached connection; batch restriction is the only query difference. No new publication.',
    qualification='Filtering requires an admitted complete batch boundary and preserved global origin uniqueness; no proof of those real producer/writer authorities. Separate membership checks remain required.'),indent=2)+'\n')
c.history()
c.close()
