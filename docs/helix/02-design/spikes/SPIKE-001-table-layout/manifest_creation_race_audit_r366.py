"""Audit observed creation races without assuming unique-key enforcement."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_manifest_creation_race_r365';a=json.loads((p/'summary.json').read_text());rs=json.loads((p/'all-statements.json').read_text());history=json.loads((p/'query-history.json').read_text());native={q['query_id']:q for q in history};assert len(rs)==len(native)==len({r['statement_id'] for r in rs});lanes={w['statement_id']:w['lane'] for pair in a['pairs'] for w in pair['workers']}
 for r in rs:
  q=native[r['statement_id']];prefix=lanes.get(r['statement_id'],p.name);assert q['query_text']=='/* ashlar '+prefix+' '+r['label']+' */ '+r['sql'] and q['is_final'];assert q['status']==('FINISHED' if r['response']['status']['state']=='SUCCEEDED' else 'FAILED')
 source=B/'out/native/ashlar_maintenance_manifest_inventory_r362/audited-summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_sha256'];parent=json.loads((B/'out/native/ashlar_maintenance_manifest_r360/audited-summary.json').read_text());baseline=sorted([[r['descriptor']['id'],r['canonical_json'],r['sha256']] for r in parent['receipts'][:2]])
 by_label={r['label']:r for r in rs if r['label']!='race-merge'}
 for pair in a['pairs']:
  kind=pair['kind'];assert by_label[kind+'-absent']['response']['result']['data_array']==[['0']];assert [int(c['version']) for c in pair['before_history']]==[0];head=int(pair['after_history'][0]['version']);assert [int(c['version']) for c in pair['after_history']]==list(range(head,-1,-1));owned={by_label[kind+'-clone']['statement_id'],*[w['statement_id'] for w in pair['workers'] if w['state']=='SUCCEEDED']};assert all(c['queryHistoryStatementId'] in owned for c in pair['after_history']);assert [r for r in pair['rows'] if r[0]!='r365-race']==baseline
  race=[r for r in pair['rows'] if r[0]=='r365-race'];assert len(race)==pair['race_row_count'] and len({r[1] for r in race})==pair['distinct_race_descriptors']
  for w in pair['workers']:
   params=w['parameters'];assert hashlib.sha256(params['text'].encode()).hexdigest()==params['digest'];r=next(r for r in rs if r['statement_id']==w['statement_id']);assert r['parameters']==params and r['response']['status']['state']==w['state'] and 'raise_error' in r['sql']
  assert all(r[1:] in [[w['parameters']['text'],w['parameters']['digest']] for w in pair['workers']] for r in race);q0,q1=[native[w['statement_id']] for w in pair['workers']];overlap=max(0,min(q0['execution_end_time_ms'],q1['execution_end_time_ms'])-max(q0['query_start_time_ms'],q1['query_start_time_ms']));assert overlap==pair['native_statement_overlap_ms']
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s']
 a['audit']={'native_final_statements':len(rs),'observed_pairs':2,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','all-statements.json','query-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
