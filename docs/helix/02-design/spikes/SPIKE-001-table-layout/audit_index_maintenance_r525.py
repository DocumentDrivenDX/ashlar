"""Offline complete singleton paired receipts and metric audit."""
import hashlib,json,math
from pathlib import Path
B=Path(__file__).resolve().parent
results=[]
for run in ['r523']:
 d=B/('out/native/index_carrier_reads_'+run);s=json.loads((d/'summary.json').read_text());h={q['query_id']:q for q in json.loads((d/'shared-history.json').read_text())['queries']};records=list(map(json.loads,(d/'statements.jsonl').read_text().splitlines()));r={x['statement_id']:x for x in records}
 assert len(r)==len(records)==len(h)==47
 assert hashlib.sha256((B/('index_carrier_reads_'+run+'.py')).read_bytes()).hexdigest()==s['code_sha256']
 assert hashlib.sha256((B/'out/native/index_carrier_spike_r513/summary.json').read_bytes()).hexdigest()==s['source_sha256']
 assert s['custody_before']==s['custody_after'] and len(s['custody_before'])==3
 cost={k:0 for k in s['costs']}
 for sid,x in r.items():
  q=h[sid];assert q['status']=='FINISHED' and q['is_final'] and x['response']['status']['state']=='SUCCEEDED'
  assert q['query_text']=='/* ashlar '+d.name+' '+x['label']+' */ '+x['sql']
  for k in cost:cost[k]+=q['metrics'].get(k,0) or 0
 assert cost==s['costs'] and all(v<=s['bounds'][k] for k,v in cost.items()) and s['wall_s']<=180
 assert len(s['reads'])==32 and len(s['keys'])==8
 for key in range(8):
  rr=[x for x in s['reads'] if x['key']==key];assert len(rr)==4
  assert all(x['result']==rr[0]['result']==r[x['statement_id']]['response']['result']['data_array'] for x in rr)
  assert len(rr[0]['result'])==(0 if s['keys'][key][4]=='true' else 1)
  assert not any(h[x['statement_id']]['metrics'].get('result_from_cache') for x in rr)
 for name,g in s['groups'].items():
  family,phase=name.split('-');rr=[x for x in s['reads'] if x['family']==family and x['phase']==int(phase)];p95=lambda values:sorted(values)[math.ceil(.95*len(values))-1]
  assert g['queries']==8 and g['caller_p95_ms']==p95([r[x['statement_id']]['wall_ms'] for x in rr]) and g['engine_p95_ms']==p95([h[x['statement_id']]['metrics']['execution_time_ms'] for x in rr])
  assert g['read_bytes']==sum(h[x['statement_id']]['metrics'].get('read_bytes',0) for x in rr)
 results.append({'run':run,'groups':s['groups'],'costs':cost,'wall_s':s['wall_s'],'statements':len(r),'qualification':s['qualification']})
assert json.loads((B/'out/native/index_carrier_reads_r515/summary.json').read_text())['keys']==s['keys']
d=B/'out/native/index_maintenance_recovery_r524';m=json.loads((d/'summary.json').read_text());old=B/'out/native/index_maintenance_r522';p=json.loads((old/'summary.json').read_text());hh={q['query_id']:q for q in json.loads((d/'combined-history.json').read_text())['queries']};rr=[]
for path in [old/'statements.jsonl',d/'statements.jsonl']:rr+=list(map(json.loads,path.read_text().splitlines()))
assert len(rr)==len(hh)==10
for x in rr:
 q=hh[x['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and x['response']['status']['state']=='SUCCEEDED' and q['query_text']==x['sql']
assert {k:sum(q['metrics'].get(k,0) or 0 for q in hh.values()) for k in m['costs']}==m['costs']
assert m['prior_sha256']==hashlib.sha256((old/'summary.json').read_bytes()).hexdigest()
assert m['code_sha256']==hashlib.sha256((B/'index_maintenance_recovery_r524.py').read_bytes()).hexdigest()
assert p['code_sha256']==hashlib.sha256((B/'index_maintenance_r522.py').read_bytes()).hexdigest()
assert s['maintenance_sha256']==hashlib.sha256((d/'summary.json').read_bytes()).hexdigest()
assert [x['version'] for x in m['history']]==['3','2','1']
assert all(x['queryHistoryStatementId']==p['optimize_statement_id'] and x['operation']=='OPTIMIZE' for x in m['history'][:2])
assert m['parity']==[['2498646','0']] and m['counts']==[['2498646','2498646','618']] and m['cdf']==[['0']]
for label,expected in [('all11-parity',m['parity']),('unique-counts',m['counts']),('logical-cdf',m['cdf'])]:assert next(x for x in rr if x['label']==label)['response']['result']['data_array']==expected
assert m['closing_head']==m['history'][0]==p['after_head']
assert m['before']['id']==m['after']['id']==s['custody_before']['index']['detail']['id'] and s['custody_before']['index']['head'][0]=='3'
assert len([x for x in rr if x['sql'].startswith('OPTIMIZE ')])==1
result={'maintenance':{'original_trial':'stopped after successful OPTIMIZE because head3 contradicted one-commit assumption','commits':[2,3],'same_statement_id':p['optimize_statement_id'],'costs':m['costs'],'before_files':m['before']['numFiles'],'after_files':m['after']['numFiles'],'before_bytes':m['before']['sizeInBytes'],'after_bytes':m['after']['sizeInBytes'],'qualification':'No mutation replay; costs include stopped original execution and recovery proofs. Original whole wall is not captured; recovery wall is separately bounded.'},'state':'Maintained index paired cohort preserves all20 fields/deletion absence, exact SQL/final metrics and unchanged three-table custody','trials':results,'qualification':'Small warm serial cohorts, nearest-rank sample p95; no production p95, cold-data, full publication or scale admission.'}
(B/'out/index-maintenance-audit-r525.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps(result,indent=2))
