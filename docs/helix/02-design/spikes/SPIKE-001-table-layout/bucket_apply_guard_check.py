"""Compare owned helper to executed native MERGE and reject unsupported placement."""
import json
from pathlib import Path
from property_apply_queries import PropertyApply,COLS
from bucket_apply_queries import bucket_apply
B=Path(__file__).resolve().parent;F='client_dev.ashlar_entropy_20261006_r86'
q=PropertyApply(F+'.bucket_part_r159',F+'.bucket_stage_r159',0,15,'r159-b1','r139-b1','bucket-r159',9007199254741301,eligibility_placement='on')
records=[json.loads(l) for l in (B/'out/native/ashlar_bucket_screen_r159/statements.jsonl').read_text().splitlines()]
r=next(x for x in records if x['label']=='part-apply');assert bucket_apply(q)==r['sql']
assert 'UPDATE SET *' not in r['sql'] and 't.lookup_bucket=s.lookup_bucket' in r['sql']
assert all('t.'+col+'=s.'+col in r['sql'] for col in COLS)
try:bucket_apply(PropertyApply(F+'.bucket_part_r159',F+'.bucket_stage_r159',0,15,'r159-b1','r139-b1','bucket-r159',9007199254741301))
except ValueError:pass
else:raise AssertionError('Unsupported matched-placement accepted')
(B/'out/bucket-apply-guard-check.json').write_text(json.dumps({'state':'Exact executed native MERGE matches owned helper;20 explicit assignments/full tuple+bucket eligibility; unsupported placement refused','qualification':'Synthetic trusted update-only profile, no generic inserts/authority/fencing admission'},indent=2)+'\n')
print('Owned bucket MERGE matches native SQL; unsupported placement refused')
