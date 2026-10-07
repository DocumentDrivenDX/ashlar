"""Offline audit of the completed r292 controlled incremental publication."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main(evidence_path=None, write=True):
 p=evidence_path or B/'out/native/ashlar_incremental_publish_r332';a=json.loads((p/'summary.json').read_text());assert a['state']=='Private second100k range32 publication passes complete change-image custody checks'
 records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};failed=json.loads((p/'failed-tombstone-query.json').read_text())['native'];bad=[r for r in records if r['response']['status']['state']!='SUCCEEDED'];assert len(bad)==1 and bad[0]['statement_id']==failed['query_id'] and bad[0]['label']=='digest-tombstone' and failed['status']=='FAILED' and failed['is_final'];records=[r for r in records if r['response']['status']['state']=='SUCCEEDED'];sid={r['statement_id']:r for r in records}
 assert len(sid)==len(records)==len(native) and h['require_final'] and not h['missing_ids']
 for r in records:
  q=native[r['statement_id']];assert r['response']['status']['state']=='SUCCEEDED' and q['status']=='FINISHED' and q['is_final'];assert q['query_text']==r['sql'];assert not r['response'].get('manifest',{}).get('truncated')
 def result(label):
  r=[r for r in records if r['label']==label];assert len(r)==1,label
  return r[0]['response'].get('result',{}).get('data_array',[])
 costs={k:failed['metrics'].get(k,0)+sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items());assert a['wall_s']<a['bounds']['wall_s']
 oracle=json.loads((B/'out/second-cdf-image-oracle-r327.json').read_text());inputs=json.loads((B/'out/native/ashlar_second_delta_stage_r324/audited-summary.json').read_text())
 for role in ['edge_current','adjacency_forward']:
  rows=result('cdf-'+role);assert len(rows)==3 and rows==a['checks'][role]['groups']
  for kind,n,lo,hi,digest in rows:
   expected=oracle['roles'][role]['images'][kind];assert int(n)==expected['rows'] and digest==expected['digest'] and int(lo)==int(hi)==a['tables'][role]['version']
 for role in ['source_record','property_journal']:
  expected=inputs['checks'][role];v=str(a['tables'][role]['version']);assert result('cdf-'+role)==[['insert',str(expected['rows']),v,v,expected['all_known_field_digest']]]
 expected=json.loads((B/'out/second-canonical-tomb-r332.json').read_text());assert result('digest-tombstone')==[[str(expected['rows']),expected['digest']]]
 for role,n in {'object_current':8000000,'edge_current':39980000,'adjacency_forward':39980000,'source_record':48200000,'property_journal':192433333,'tombstone':20000}.items():
  assert result('final-count-'+role)==[[str(n)]];assert a['active_details'][role]['id']==a['tables'][role]['id']
 assert result('unique-edges')==[['39980000','39980000']] and result('deleted-absent')==[['0']] and result('typed-endpoints')==[['39980000','0']]
 for role in ['edge_current','source_record','property_journal','adjacency_forward','tombstone']:
  r=next(r for r in records if r['label']=='custody-history-'+role);columns=[c['name'] for c in r['response']['manifest']['schema']['columns']];rows=[dict(zip(columns,x)) for x in result(r['label'])];events={e['version']:e for e in a['commit_events'] if e['role']==role}
  assert set(events)==set(range(a['tables'][role]['version']+1))=={int(x['version']) for x in rows}
  for x in rows:assert x['queryHistoryStatementId']==events[int(x['version'])]['statement_id'] and x['queryHistoryStatementId'] in sid
 d=result('descriptor-readback')[0];assert d[0]=='incremental-r332';assert json.loads(d[2])==a['publication_vector']=={t['table']:t['version'] for t in a['tables'].values()};assert json.loads(d[4])==a['schema_revisions']=={'synthetic-scale-mixed':'synthetic-mixed/1'} and json.loads(d[5])==a['checks'];assert json.loads(d[3])['inputs']==inputs['tables'] and json.loads(d[3])['real_source_ack'] is False
 cdf=[r for r in records if r['label'].startswith('cdf-')];assert len(cdf)==4 and all(not native[r['statement_id']]['metrics'].get('result_from_cache') for r in cdf)
 a['audit']={'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'audit_script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'final_statements':len(records),'uncached_cdf_queries':4,'qualification':a['qualification']}
 if write:(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['processing_s'])
 return a
if __name__=='__main__':main()
