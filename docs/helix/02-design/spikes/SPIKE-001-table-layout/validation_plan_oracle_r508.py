"""Offline integration corpus: generated complete stage versus frozen native SQL/results."""
import hashlib,json
from pathlib import Path
from publisher_validation_r507 import plan
B=Path(__file__).resolve().parent

def source():
 p=B/'out/native/ashlar_fifth_guard_publish_r481/summary.json';pub=json.loads(p.read_text());s={k:json.loads((B/pub['sources'][k]).read_text()) for k in ['cdf','inputs','tomb','budget']};prior={r['label']:r for r in map(json.loads,(p.parent/'statements.jsonl').read_text().splitlines())};s['revision_rows']=prior['schema-revisions']['response']['result']['data_array'];return pub,s,prior

def main():
 out=B/'out/validation-plan-oracle-r508.json';assert not out.exists();pub,s,prior=source();checks=plan(pub['tables'],pub['inputs'],s)
 expected={k for k in prior if k.startswith('cdf-') or k.startswith('final-count-')}|{'typed-endpoints','canonical-tombstone','unique-edges','deleted-absent','schema-revisions'}
 assert {c.label for c in checks}==expected and len(checks)==15
 for c in checks:
  r=prior[c.label];assert c.sql==r['sql'] and sorted(c.expected)==sorted(tuple(x) for x in r['response']['result']['data_array']),c.label
 result={'state':'Integrated15-check plan preserves exact native SQL and complete results for every post-commit check','checks':[c.label for c in checks],'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['publisher_validation_r507.py','validation_plan_oracle_r508.py']},'qualification':'Offline regression against independently retained native corpus; no new native call/publication/clock. Before-input and closing custody/fencing remain caller responsibilities.'};out.write_text(json.dumps(result,indent=2)+'\n');print(result['state'])
if __name__=='__main__':main()
