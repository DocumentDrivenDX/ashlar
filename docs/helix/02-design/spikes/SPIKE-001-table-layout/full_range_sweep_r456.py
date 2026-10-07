"""Full100k/64range read-only sweep; four persistent bounded workers."""
import json,time,hashlib,concurrent.futures
from pathlib import Path
from fourth_changes_r427 import FourthChanges
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash,row_hash_sql
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_full_range_sweep_r456';assert not O.exists();O.mkdir();fields=[*FIELDS,*('after_'+f for f in FIELDS),'is_delete','change_delivery_id'];counts=[0]*64;hashes=[[] for _ in range(64)];w=FourthChanges();prep=time.monotonic()
for i in range(100000):
 x=w.change(i);j=int(x['before']['lookup_hash'][:2],16)//4;row=dict(x['before']);row.update({'after_'+f:None if x['after'] is None else x['after'][f] for f in FIELDS});row.update(is_delete=x['after'] is None,change_delivery_id=x['raw']['delivery_id'])
 if row['after_published_at'] is not None:row['after_published_at']=row['after_published_at'].replace('T',' ').removesuffix('Z')
 counts[j]+=1;hashes[j].append(row_hash(row,fields))
digests=[hashlib.sha256(''.join(sorted(h)).encode()).hexdigest() for h in hashes];start=time.monotonic();a={'state':'running full64range sweep','expected_counts':counts,'expected_digests':digests,'local_oracle_s':start-prep,'reads':[],'bounds':{'read_bytes':110000000000,'write_remote_bytes':0,'spill_to_disk_bytes':2000000000,'wall_s':240},'qualification':'All100k42field source hashes/full20field before checks; immutable prepared0/E6. Four workers,16waves. No MERGE/publication or billion admission.'};clients=[]
def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
def history():
 records=[r for c in clients for r in c.records]
 for i in range(20):
  try:return collect_history(clients[0].w,records,O/'shared-history.json')
  except HistoryPending:
   if i==19:raise
   time.sleep(1)
def run(worker,j):
 c=clients[worker];lo=f'{j*4:02x}';pred=f"lookup_hash >= '{lo}'"+(f" AND lookup_hash < '{(j+1)*4:02x}'" if j<63 else '');F='client_dev.ashlar_entropy_20261006_r86';on=' AND '.join('b.'+f+'=s.'+f for f in ['lookup_hash','source_system','rel_type_id','id']);eq=' AND '.join(f'b.{f}<=>s.{f}' for f in FIELDS);q=f"SELECT count(*),count_if(b.id IS NULL OR NOT ({eq})),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(['s.'+f for f in fields])}))),256) FROM (SELECT * FROM {F}.prepared_current_mutation_r445 VERSION AS OF 0 WHERE {pred}) s LEFT JOIN {F}.lc_second_edge_current_r337 VERSION AS OF 6 b ON {on}";r=c.sql('range-'+str(j),q);assert r==[[str(counts[j]),'0',digests[j]]];return {'range':j,'worker':worker,'result':r,'statement_id':c.records[-1]['statement_id'],'caller_ms':c.records[-1]['wall_ms']}
save()
try:
 for j in range(4):
  c=BoundedReads(O/('worker-'+str(j)),socket_timeout=60);clients.append(c);c.sql('timeout','SET STATEMENT_TIMEOUT=60');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
 a['connection_setup_s']=time.monotonic()-start;sweep=time.monotonic()
 with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
  for wave in range(16):
   if time.monotonic()-start>180:raise RuntimeError('No new wave: remaining time reserve')
   if a.get('costs',{}).get('read_bytes',0)>100000000000:raise RuntimeError('No new wave:10GBread reserve')
   results=list(pool.map(lambda k:run(k,wave*4+k),range(4)));a['reads'].extend(results)
   for c in clients:c.cursor.close();c.cursor=c.connection.cursor()
   h=history();a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items())
   for r in results:r['metrics']=h[r['statement_id']]['metrics'];assert not r['metrics'].get('result_from_cache')
   a['waves_completed']=wave+1;save()
 a['sweep_with_history_s']=time.monotonic()-sweep;assert len(a['reads'])==64 and sum(int(r['result'][0][0]) for r in a['reads'])==100000;a.update(state='Complete64ranges/all100k predecessor and42field source checks pass',wall_s=time.monotonic()-start);assert a['wall_s']<240;save()
 for c in clients:(c.out/'inflight-request.json').rename(c.out/'completed-last-request.json');c.close()
 print(json.dumps({k:v for k,v in a.items() if k not in ['reads','expected_digests']},indent=2))
except Exception as e:a.update(state='Stopped; inspect same durable worker handles; no replay',error=str(e));save();raise
