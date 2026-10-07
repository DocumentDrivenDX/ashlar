"""Offline receipt audit for second-batch transport and full-field Delta staging."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent
def sha(p):return hashlib.sha256(p.read_bytes()).hexdigest()
def audit_upload():
 o=B/'out/native/ashlar_second_batch_upload_r323';p=o/'summary.json';a=json.loads(p.read_text());source=B/'out/ashlar_second_batch_files_r319/audited-summary.json';s=json.loads(source.read_text())
 assert a['state']=='All100k source-role parts roundtrip byte-exactly in owned UC volume'
 assert a['source_summary_sha256']==sha(source)
 assert a['volume_id']=='a517e6ec-98a5-4e8a-8978-75f71eee2c31'
 paths=[]
 for role,r in s['roles'].items():
  got=a['roles'][role]
  for k in ['rows','bytes','fields','all_known_field_digest','original_input_multiset_digest']:assert got[k]==r[k]
  assert len(got['parts'])==len(r['parts'])
  for x,y in zip(got['parts'],r['parts']):
   for k in y:
    if k!='state':assert x[k]==y[k],k
   assert x['state']=='Uploaded/retained input bytes independently match source SHA'
   assert x['remote_metadata']['content-length']==x['bytes']
   assert '/batch100k-r323-' in x['path'];paths.append(x['path'])
 assert len(paths)==len(set(paths))==35
 assert a['download_verified_bytes']==s['source_bytes']==1100633476
 assert a['wall_s']<a['bounds']['wall_s']
 a['audit']={'summary_sha256':sha(p),'exact_verified_parts':35,'exact_verified_bytes':s['source_bytes'],'unique_paths':35,'qualification':'Per-part upload/download streamed SHA receipts. Mutable files, not producer fencing or publication performance.'}
 (o/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n')
 return a
if __name__=='__main__':
 a=audit_upload();print(json.dumps(a['audit'],indent=2))

def audit_stage():
 o=B/'out/native/ashlar_second_delta_stage_r324';p=o/'summary.json';a=json.loads(p.read_text())
 assert a['state']=='Four complete normalized input roles match independent source digests and are Delta-version pinned'
 source=B/'out/native/ashlar_second_batch_upload_r323/audited-summary.json';s=json.loads(source.read_text());assert a['source_sha256']==sha(source)
 records=[json.loads(x) for x in (o/'statements.jsonl').read_text().splitlines()]
 h=json.loads((o/'shared-history.json').read_text());queries={q['query_id']:q for q in h['queries']}
 assert len(records)==len({r['statement_id'] for r in records})==36
 totals={k:0 for k in a['costs']}
 for r in records:
  q=queries[r['statement_id']];assert q['status'] in ['FINISHED','SUCCEEDED'] and q['is_final']
  assert q['query_text']==r['sql']
  assert r['response']['status']['state']=='SUCCEEDED'
  for k in totals:totals[k]+=q['metrics'].get(k,0)
  if r['label'].startswith(('verify-','second-batch-')):assert not q['metrics'].get('result_from_cache')
 for role,t in a['tables'].items():
  assert t['version']==0 and t['history']['queryHistoryStatementId']==t['statement_id']
  assert a['checks'][role]['fields']==s['roles'][role]['fields']
  assert a['checks'][role]['all_known_field_digest']==s['roles'][role]['all_known_field_digest']
 assert totals==a['costs']
 a['audit']={'final_statements':len(records),'source_sha256':{n:sha(o/n) for n in ['summary.json','statements.jsonl','shared-history.json']},'qualified_input_checks_uncached':True,'explicit_tombstone_input_extension':'apply_batch_id preserved in original input_json and extracted typed input column; canonical tombstone DDL unchanged'}
 (o/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');return a

def audit_predecessor():
 o=B/'out/native/ashlar_second_predecessor_r325';p=o/'summary.json';a=json.loads(p.read_text());assert a['state']=='Full100k predecessor and prepared mutation-source checks pass'
 records=[json.loads(x) for x in (o/'statements.jsonl').read_text().splitlines()];h=json.loads((o/'shared-history.json').read_text());queries={q['query_id']:q for q in h['queries']}
 assert len(records)==len({r['statement_id'] for r in records})==5
 totals={k:0 for k in a['costs']}
 for r in records:
  q=queries[r['statement_id']];assert q['status'] in ['FINISHED','SUCCEEDED'] and q['is_final'];assert q['query_text']==r['sql'];assert r['response']['status']['state']=='SUCCEEDED'
  for k in totals:totals[k]+=q['metrics'].get(k,0)
  if r['label'] in ['all-predecessor-fields','mutation-partition']:assert not q['metrics'].get('result_from_cache')
 assert totals==a['costs'] and a['pins']['edge']['version']==52
 for k,n in [('inputs','out/native/ashlar_second_delta_stage_r324/audited-summary.json'),('base','out/native/ashlar_range32_maintain_r316/audited-summary.json')]:assert a['source_sha256'][k]==sha(B/n)
 a['audit']={'final_statements':5,'qualified_checks_uncached':True,'source_sha256':{n:sha(o/n) for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':'Read-only full100k exact predecessor comparison and prepared mutation partition; no mutation, fencing or publication claim.'}
 (o/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');return a
