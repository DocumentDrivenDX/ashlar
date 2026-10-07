"""Offline native-final guard, exact descriptor and failed-write ledger audit."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_manifest_atomic_guard_r363';a=json.loads((p/'summary.json').read_text());assert a['state']=='Native exact replay and two conflicting mixed-MERGE rollback controls pass';rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'query-history.json').read_text());native={q['query_id']:q for q in h};assert len(rs)==len(native)==len({r['statement_id'] for r in rs});source=B/'out/native/ashlar_maintenance_manifest_r360/audited-summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_sha256'];s=json.loads(source.read_text());expected=sorted([[r['descriptor']['id'],r['canonical_json'],r['sha256']] for r in s['receipts'][:2]])
 for r in rs:
  q=native[r['statement_id']];failed=r['label'] in ['conflict-content','conflict-digest'];assert q['is_final'] and q['status']==('FAILED' if failed else 'FINISHED') and q['query_text']=='/* ashlar '+p.name+' '+r['label']+' */ '+r['sql']
  assert r['response']['status']['state']==('FAILED' if failed else 'SUCCEEDED')
  if failed:assert 'ASHLAR_PUBLICATION_ID_CONFLICT' in r['response']['status']['error']['message'] and 'rollback-sentinel' in r['sql'] and 'raise_error' in r['sql']
  if r['label']=='baseline' or r['label'].endswith('-readback'):assert r['response']['result']['data_array']==expected
 by={r['label']:r for r in rs};head=int(a['replay_history'][0]['version']);assert [int(c['version']) for c in a['before_history']]==[0] and [int(c['version']) for c in a['after_history']]==list(range(head,-1,-1));assert all(c['queryHistoryStatementId'] in [by['clone']['statement_id'],by['exact-replay']['statement_id']] for c in a['after_history']);assert len(a['controls'])==2 and all(c['head_version']==head for c in a['controls'])
 for control in a['controls']:assert control['statement_id']==by[control['label']]['statement_id']
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s']
 a['audit']={'successful_native_final':len(rs)-2,'failed_native_final':2,'complete_rollback_rows':2,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','query-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
