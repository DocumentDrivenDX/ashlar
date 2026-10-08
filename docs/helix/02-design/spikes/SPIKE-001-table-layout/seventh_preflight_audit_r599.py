"""Offline fresh ten-table eligibility and budget/source binding audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent
p=B/'out/native/ashlar_seventh_publisher_preflight_r595';s=json.loads((p/'summary.json').read_text());r=list(map(json.loads,(p/'statements.jsonl').read_text().splitlines()));h={q['query_id']:q for q in json.loads((p/'shared-history.json').read_text())['queries']};assert len(r)==len(h)==len({x['statement_id'] for x in r})==31 and len(s['tables'])==10
assert s['state']=='Nine exact mutable/input heads and unchanged pinned node6 eligible for integrated seventh guarded publisher'
by={x['label']:x for x in r}
def native_rows(label):
 x=by[label];cols=[v['name'] for v in x['response']['manifest']['schema']['columns']];return [dict(zip(cols,row)) for row in x['response']['result']['data_array']]
assert by['pinned-readonly-node-count']['response']['result']['data_array']==[['8000000']]
for x in r:
 q=h[x['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and q['query_text']==x['sql'] and x['response']['status']['state']=='SUCCEEDED'
for k,path in s['sources'].items():assert hashlib.sha256((B/path).read_bytes()).hexdigest()==s['source_sha256'][k]
for key,v in s['tables'].items():
 assert native_rows('detail-'+key)==[v['detail']] and native_rows('history-'+key)==[v['head']] and native_rows('schema-'+key)==v['schema']
 for phase,sql in [('detail','DESCRIBE DETAIL '),('history','DESCRIBE HISTORY '),('schema','DESCRIBE TABLE ')]:assert by[phase+'-'+key]['sql']==sql+v['pin']['table']+(' LIMIT 1' if phase=='history' else '')
 assert v['detail']['id']==v['pin']['id'] and int(v['head']['version'])==(8 if key=='base-object_current' else v['pin']['version'])
 if key.startswith('input-'):assert v['head']['queryHistoryStatementId']==v['pin']['statement_id'] and v['pin']['version']==0
assert s['tables']['base-object_current']['pin']['version']==6 and s['tables']['base-object_current']['head']['queryHistoryStatementId']=='8b6cf102-d6ed-4eaa-93d3-e904ef8cbcc1'
cost={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in s['costs']};assert cost==s['costs'] and s['wall_s']<=180 and all(v<=s['bounds'][k] for k,v in cost.items())
result={'state':'Ten-table UUID/head/profile preflight receipts and source budget bind; no seventh role writes','statements':31,'costs':cost,'wall_s':s['wall_s'],'sources_sha256':s['source_sha256'],'qualification':'Read-only observation; publisher must repeat checks and enforce complete guarded writes/closing custody. N selected6/physical8 is explicit; new inputs0 actual UUIDs. All seventh input CTAS SQL text and complete contents independently qualify perR598. No production fence/ACK or performance admission.'};(B/'out/seventh-preflight-audit-r599.json').write_text(json.dumps(result,indent=2)+'\n');print(result['state'])
