"""Independent native-final input digests, pins, identities and origin audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_third_delta_stage_r384';a=json.loads((p/'summary.json').read_text());assert a['state']=='Four complete normalized input roles match independent source digests and are Delta-version pinned';source=B/'out/native/ashlar_third_batch_upload_r382/audited-summary-as-used-r384.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_sha256'];s=json.loads(source.read_text());rs=list(map(json.loads,(p/'statements.jsonl').read_text().splitlines()));h=json.loads((p/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(rs)==len(native)==len({r['statement_id'] for r in rs})==36;by={r['label']:r for r in rs};rows=lambda label:by[label]['response']['result']['data_array']
 for r in rs:
  q=native[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and q['query_text']==r['sql'] and r['response']['status']['state']=='SUCCEEDED' and not r['response'].get('manifest',{}).get('truncated')
  if r['label'].startswith(('verify-','third-batch-')):assert not q['metrics'].get('result_from_cache')
 for role,t in a['tables'].items():
  expected=s['roles'][role];assert rows('verify-'+role)==[[str(expected['rows']),expected['original_input_multiset_digest'],expected['all_known_field_digest']]];assert t['version']==0 and t['statement_id']==by['create-'+role]['statement_id']==t['history']['queryHistoryStatementId'];assert a['checks'][role]['fields']==expected['fields'];d=by['detail-'+role];cols=[c['name'] for c in d['response']['manifest']['schema']['columns']];assert dict(zip(cols,rows('detail-'+role)[0]))['id']==t['id'];assert rows('third-batch-'+role)==[['0']]
 assert rows('raw-unique-and-digests')==[['100000','100000','0']] and rows('replacement-identity')==[['90000','90000','0']] and rows('deletion-identity')==[['10000','10000','0']] and rows('journal-identity')==[['216667','216667','0']] and rows('change-partition')==[['100000','90000','10000','0']]
 for role in ['property_journal','tombstone','current_replacement']:assert rows('raw-link-'+role)==[[str(s['roles'][role]['rows']),'0']]
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s'];a['audit']={'native_final_statements':36,'source_receipt_as_used':str(source.relative_to(B)),'source_receipt_note':'Archive retains exact bytes loaded by native stage; transfer audit subsequently corrects SDK header key wording to content-length. Input/file custody is unchanged.','source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualified_input_checks_uncached':True,'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
