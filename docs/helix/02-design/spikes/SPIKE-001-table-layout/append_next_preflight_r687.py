"""Fresh owned-head qualification before the next8M-edge native append phase."""
import json,time
from pathlib import Path
from append_edge_blocks_r681 import coverage_query,block_query
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_append_next_preflight_r687';assert not O.exists()
prior=json.loads((B/'out/native/ashlar_append_edge_growth_r665/summary.json').read_text());oracle=json.loads((B/'out/append-edge-oracle-r656.json').read_text())
c=Client(O,observation_timeout=240,cancel_after=60);started=time.monotonic()
result={'state':'Qualifying current private heads','tables':{},'checks':{},'qualification':'Complete current8M full-field/membership qualification at observed heads; maintenance advances explicitly recorded. No writes, next16M extent or publication/operational gate admission. Observations are not a writer fence.'}
c.sql('timeout','SET STATEMENT_TIMEOUT=180');c.sql('cache','SET use_cached_result=false');c.cancel_after=180
for role,old in prior['tables'].items():
 rows=c.sql('identity-'+role,'DESCRIBE DETAIL '+old['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];detail=dict(zip(names,rows[0]));assert detail['id']==old['id']
 rows=c.sql('history-'+role,'DESCRIBE HISTORY '+old['table']+' LIMIT 50');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];history=[dict(zip(names,row)) for row in rows];version=int(history[0]['version']);base=prior['versions'][role]
 assert version>=base
 intervening=[x for x in history if base<int(x['version'])<=version];assert len(intervening)==version-base
 assert all(x['operation']=='OPTIMIZE' for x in intervening)
 rows=c.sql('schema-'+role,f"SELECT * FROM {old['table']} VERSION AS OF {version} LIMIT 0")
 assert not rows and c.records[-1]['response']['manifest']['schema']['columns']==old['schema']
 result['tables'][role]={'table':old['table'],'id':old['id'],'version':version,'head':history[0],'schema':old['schema'],'maintenance_interval':intervening,'prior_pin':base,'detail':detail}
for role,t in result['tables'].items():
 coverage=c.sql('coverage-'+role,coverage_query(t['table'],t['version'],role,48000000));total=32000000 if role=='property_journal' else 8000000;assert coverage==[[str(total),'0']]
 fields=oracle['chunks'][0]['roles'][role]['fields'];blocks=[]
 for low in [40000000,44000000]:
  rows=c.sql(role+'-'+str(low),block_query(t['table'],t['version'],role,fields,low,low+4000000,48000000));first=(low-40000000)//100000
  expected=[[str(i),str(oracle['chunks'][i]['roles'][role]['rows']),oracle['chunks'][i]['roles'][role]['digest']] for i in range(first,first+40)]
  assert rows==expected;blocks.append({'start':low,'end':low+4000000,'groups':rows})
 assert sum(int(row[1]) for block in blocks for row in block['groups'])==total
 result['checks'][role]={'coverage':coverage,'blocks':blocks}
for role,t in result['tables'].items():
 rows=c.sql('closing-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1');names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];head=dict(zip(names,rows[0]));assert head['version']==t['head']['version'] and head['queryHistoryStatementId']==t['head']['queryHistoryStatementId'];t['closing_head']=head
for attempt in range(12):
 try:h=collect_history(c.w,c.records,O/'shared-history.json');break
 except HistoryPending:
  if attempt==11:raise
  time.sleep(2)
costs={k:sum(q['metrics'][k] for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
assert costs['read_bytes']<=30000000000 and costs['write_remote_bytes']==costs['spill_to_disk_bytes']==0
result.update(state='Complete observed-head full8M edge-role qualification',costs=costs,wall_s=time.monotonic()-started)
(O/'summary.json').write_text(json.dumps(result,indent=2)+'\n');(O/'live-statement.json').unlink(missing_ok=True)
print(json.dumps({'state':result['state'],'costs':costs,'wall_s':result['wall_s'],'heads':{k:t['version'] for k,t in result['tables'].items()}},indent=2))
