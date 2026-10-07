"""Audit terminal native controls and integrated live custody metadata."""
import json,hashlib
from pathlib import Path
from publisher_custody_r462 import verify,CustodyMismatch
B=Path(__file__).resolve().parent;reports=[]
for rev,name,driver in [(463,'ashlar_custody_controls_r463',False),(464,'ashlar_publisher_custody_live_r464',True)]:
 d=B/'out/native'/name;s=json.loads((d/'summary.json').read_text());h={q['query_id']:q for q in json.loads((d/'shared-history.json').read_text())['queries']};paths=list(d.glob('*/statements.jsonl')) if driver else [d/'statements.jsonl'];records=[]
 for p in paths:
  for line in p.read_text().splitlines():
   r=json.loads(line);q=h[r['statement_id']];assert q['query_text']==('/* ashlar '+p.parent.name+' '+r['label']+' */ ' if driver else '')+r['sql'] and q['is_final'] and q['status']=='FINISHED' and r['response']['status']['state']=='SUCCEEDED';records.append(r)
 costs={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in s['costs']};assert costs==s['costs'] and all(v<=s['bounds'][k] for k,v in costs.items());assert costs['spill_to_disk_bytes']==0
 if rev==463:
  assert len(records)==20 and len(s['controls'])==8 and sum(x['result']=='refused' for x in s['controls'])==6
  for n,digest in s['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==digest
 else:
  assert len(records)==34 and len(s['accepted'])==10 and costs['write_remote_bytes']==0;source=B/'out/native/ashlar_metadata_custody_r460/summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==s['source_sha256'];old=json.loads(source.read_text());expected={x['key']:{'key':x['key'],'table':x['table']['table'],'head':x['physical_head'],'schema':x['schema'],'detail':x['detail']} for x in old['phases']['concurrent']['tables']}
  for x in s['accepted']:verify(x,expected[x['key']])
 reports.append({'revision':rev,'final_statements':len(records),'costs':costs,'summary_sha256':hashlib.sha256((d/'summary.json').read_bytes()).hexdigest()})
(B/'out/custody-component-audit-r465.json').write_text(json.dumps({'state':'Native controls/integrated component terminal SQL/results/costs audit passes','reports':reports,'qualification':'Other native UUID is rebound locally as a refusal input; protocol/worker faults locally injected. No atomic writer fence or fresh publication performance proof.'},indent=2)+'\n');print(reports)
