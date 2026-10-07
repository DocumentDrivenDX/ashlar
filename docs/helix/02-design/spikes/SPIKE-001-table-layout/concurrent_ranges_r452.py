"""Bounded four-range sequential/concurrent immutable comparison."""
import json,time,hashlib,concurrent.futures
from pathlib import Path
from fourth_changes_r427 import FourthChanges
from overlay_sql_r395 import FIELDS
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_concurrent_ranges_r452';assert not O.exists();O.mkdir();ranges=[('10','14'),('50','54'),('90','94'),('d0','d4')];expected=[0]*4;w=FourthChanges()
for i in range(100000):
 h=w.change(i)['before']['lookup_hash']
 for j,(lo,hi) in enumerate(ranges):expected[j]+=lo<=h<hi
start=time.monotonic();a={'state':'running sequential then concurrent range cohort','ranges':ranges,'expected_counts':expected,'source_code_sha256':hashlib.sha256((B/'fourth_changes_r427.py').read_bytes()).hexdigest(),'phases':{},'bounds':{'read_bytes':15000000000,'write_remote_bytes':0,'spill_to_disk_bytes':1000000000,'wall_s':240},'qualification':'Four disjoint1/64ranges only; fixed sequential-first order/cache warming. No actual mutation/full64range or service p95 claim.'}
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
save()
def run(phase,j):
 d=O/(phase+'-'+str(j));c=Client(d,observation_timeout=120,cancel_after=60);lo,hi=ranges[j];F='client_dev.ashlar_entropy_20261006_r86';pred=f"lookup_hash >= '{lo}' AND lookup_hash < '{hi}'";on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);eq=' AND '.join(f'b.{f}<=>s.{f}' for f in FIELDS);sql=f"/* ashlar concurrent_ranges_r452 {phase}-{j} */ SELECT count(*),count_if(b.id IS NULL OR NOT ({eq})) FROM (SELECT * FROM {F}.prepared_current_mutation_r445 VERSION AS OF 0 WHERE {pred}) s LEFT JOIN {F}.lc_second_edge_current_r337 VERSION AS OF 6 b ON {on}";rows=c.sql(phase+'-'+str(j),sql);assert rows==[[str(expected[j]),'0']];return {'range':j,'directory':d.name,'result':rows,'statement_id':c.records[0]['statement_id'],'caller_ms':c.records[0]['wall_ms']},c
try:
 clients=[]
 for phase in ['sequential','concurrent']:
  t=time.monotonic()
  if phase=='sequential':results=[run(phase,j) for j in range(4)]
  else:
   with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:results=list(pool.map(lambda j:run(phase,j),range(4)))
  a['phases'][phase]={'wall_s':time.monotonic()-t,'reads':[x[0] for x in results]};clients.extend(x[1] for x in results);save()
 records=[r for c in clients for r in c.records]
 for i in range(25):
  try:hist=collect_history(clients[0].w,records,O/'shared-history.json');break
  except HistoryPending:
   if i==24:raise
   time.sleep(2)
 for phase in a['phases'].values():
  for r in phase['reads']:r['metrics']=hist[r['statement_id']]['metrics'];assert not r['metrics'].get('result_from_cache')
  phase['costs']={k:sum(r['metrics'].get(k,0) for r in phase['reads']) for k in a['bounds'] if k!='wall_s'}
 a['costs']={k:sum(q['metrics'].get(k,0) for q in hist.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a.update(state='All8 complete20field range queries match independent local counts',wall_s=time.monotonic()-start);assert a['wall_s']<240;save()
 for c in clients:(c.out/'live-statement.json').rename(c.out/'completed-last-statement.json')
 print(json.dumps(a,indent=2))
except Exception as e:a.update(state='Stopped; inspect each durable handle without replay',error=str(e));save();raise
