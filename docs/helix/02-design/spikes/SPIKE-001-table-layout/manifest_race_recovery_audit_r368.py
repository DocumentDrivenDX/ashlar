"""Independent saved native recovery custody/result audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_manifest_race_recovery_r367';a=json.loads((p/'summary.json').read_text());rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];native={q['query_id']:q for q in json.loads((p/'query-history.json').read_text())};assert len(rs)==len(native)==len({r['statement_id'] for r in rs});by_label={r['label']:r for r in rs}
 source=B/'out/native/ashlar_manifest_creation_race_r365/audited-summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_sha256'];parent=json.loads(source.read_text());old={r['statement_id']:r for r in json.loads((source.parent/'all-statements.json').read_text())};old_native={q['query_id']:q for q in json.loads((source.parent/'query-history.json').read_text())}
 for r in rs:
  q=native[r['statement_id']];failed=r['label']=='conflicting-recovery';assert q['is_final'] and q['status']==('FAILED' if failed else 'FINISHED') and q['query_text']=='/* ashlar '+p.name+' '+r['label']+' */ '+r['sql'];assert r['response']['status']['state']==('FAILED' if failed else 'SUCCEEDED')
 assert len(a['controls'])==2
 for control,pair in zip(a['controls'],parent['pairs']):
  kind=pair['kind'];sid=control['old_failed_statement_id'];assert old_native[sid]['is_final'] and old_native[sid]['status']=='FAILED';recovery=by_label[kind+'-recovery'];assert recovery['sql']==old[sid]['sql'] and recovery['parameters']==old[sid]['parameters'] and recovery['statement_id']==control['statement_id'];assert control['uuid']==pair['uuid'];assert by_label[kind+'-winner']['response']['result']['data_array']==by_label[kind+'-readback']['response']['result']['data_array']==pair['rows'];assert control['before_history']==pair['after_history']
  before=int(control['before_history'][0]['version']);after=int(control['after_history'][0]['version']);assert [int(c['version']) for c in control['after_history']]==list(range(after,-1,-1))
  if kind=='conflicting':assert before==after and 'ASHLAR_PUBLICATION_ID_CONFLICT' in recovery['response']['status']['error']['message']
  else:assert control['result']=='exact replay succeeds' and all(c['queryHistoryStatementId']==recovery['statement_id'] for c in control['after_history'] if int(c['version'])>before)
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s']
 a['audit']={'successful_native_final':len(rs)-1,'failed_native_final':1,'terminal_failed_parents':2,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','query-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
