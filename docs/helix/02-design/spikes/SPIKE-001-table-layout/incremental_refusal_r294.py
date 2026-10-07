"""Offline corruption controls on actual r292 receipts; never submits SQL."""
import copy,hashlib,json,tempfile
from pathlib import Path
from incremental_receipt_audit_r293 import main as audit
B=Path(__file__).resolve().parent
P=B/'out/native/ashlar_incremental_publish_r292'

def main():
 summary=json.loads((P/'summary.json').read_text());records=[json.loads(x) for x in (P/'statements.jsonl').read_text().splitlines()];history=json.loads((P/'shared-history.json').read_text())
 audit(P,write=False)
 controls=[]
 for case in ['missing-image','duplicate-image','wrong-image-class','wrong-commit-version','changed-exact-content-digest','missing-raw-append','unexpected-journal-class','wrong-tombstone-digest','unknown-commit-statement','missing-custody-version','changed-table-uuid','wrong-descriptor-vector','nonterminal-native-query','cached-cdf']:
  a,r,h=copy.deepcopy(summary),copy.deepcopy(records),copy.deepcopy(history)
  def row(label):return next(x for x in r if x['label']==label)
  images=row('cdf-edge_current')['response']['result']['data_array']
  if case=='missing-image':images.pop()
  elif case=='duplicate-image':images.append(copy.deepcopy(images[0]))
  elif case=='wrong-image-class':images[0][0]='insert'
  elif case=='wrong-commit-version':images[0][2]='1'
  elif case=='changed-exact-content-digest':images[0][4]='0'*64
  elif case=='missing-raw-append':row('cdf-source_record')['response']['result']['data_array'][0][1]='99999'
  elif case=='unexpected-journal-class':row('cdf-property_journal')['response']['result']['data_array'][0][0]='delete'
  elif case=='wrong-tombstone-digest':row('digest-tombstone')['response']['result']['data_array'][0][1]='0'*64
  elif case in ['unknown-commit-statement','missing-custody-version']:
   x=row('custody-history-edge_current');rows=x['response']['result']['data_array']
   if case=='missing-custody-version':rows.pop()
   else:
    names=[c['name'] for c in x['response']['manifest']['schema']['columns']];rows[0][names.index('queryHistoryStatementId')]='unapproved-writer'
  elif case=='changed-table-uuid':a['active_details']['edge_current']['id']='wrong-uuid'
  elif case=='wrong-descriptor-vector':row('descriptor-readback')['response']['result']['data_array'][0][2]='{}'
  else:
   q=next(x for x in h['queries'] if x['query_id']==row('cdf-edge_current')['statement_id'])
   if case=='nonterminal-native-query':q['is_final']=False
   else:q['metrics']['result_from_cache']=True
  with tempfile.TemporaryDirectory(prefix='ashlar-refusal-') as directory:
   p=Path(directory);(p/'summary.json').write_text(json.dumps(a));(p/'shared-history.json').write_text(json.dumps(h));(p/'statements.jsonl').write_text('\n'.join(json.dumps(x) for x in r)+'\n')
   try:audit(p,write=False)
   except AssertionError:controls.append({'case':case,'result':'refused'})
   else:raise AssertionError('Accepted corruption: '+case)
 result={'format':'ashlar-incremental-receipt-refusal/1','state':'Unmodified native certificate accepted;14 corrupted receipt controls refused','source_sha256':{n:hashlib.sha256((P/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'audit_script_sha256':hashlib.sha256((B/'incremental_receipt_audit_r293.py').read_bytes()).hexdigest(),'controls':controls,'qualification':'Offline receipt-certificate verifier controls only. No native fault injection, concurrency fence, expired-CDF runtime, schema-change proof, or rollback/ACK execution. Digest drift control exercises certificate comparison, not a new native carrier hash computation.'}
 (B/'out/incremental-refusal-r294.json').write_text(json.dumps(result,indent=2)+'\n');print(result['state'])
if __name__=='__main__':main()
