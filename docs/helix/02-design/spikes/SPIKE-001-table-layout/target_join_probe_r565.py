"""Bounded read-only range pilot; full predecessor checks, no writes."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from normalized_apply_sql_r276 import predecessor,pin
from overlay_sql_r395 import FIELDS
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 O=B/'out/native/target_join_probe_r565';assert not O.exists();start=time.monotonic();c=BoundedReads(O,socket_timeout=30)
 P=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json';pub=json.loads(P.read_text());raw=pub['inputs']['source_record'];edge=pub['base']['edge_current'];source=f"SELECT * FROM ({predecessor(raw['table'],raw['version'])}) WHERE lookup_hash>='00' AND lookup_hash<'04'";target=f"SELECT * FROM {pin(edge['table'],edge['version'])} WHERE lookup_hash>='00' AND lookup_hash<'04'"
 ints={'rel_type_id','id','source_type','source_id','target_type','target_id','entity_version','source_position'}
 cmp=' AND '.join(f'(b.{f} <=> s.{f})' if f in ints or f=='published_at' else f"(hex(encode(b.{f},'UTF-8')) <=> hex(encode(s.{f},'UTF-8')))" for f in FIELDS)
 queries={label:f"SELECT {hint} count(*),count_if(b.id IS NULL OR NOT ({cmp})) FROM ({source}) s LEFT JOIN ({target}) b ON b.lookup_hash=s.lookup_hash AND b.source_system=s.source_system AND b.rel_type_id=s.rel_type_id AND b.id=s.id" for label,hint in [('default',''),('broadcast','/*+ BROADCAST(s) */')]}
 a={'state':'range target comparison running','source_sha256':hashlib.sha256(P.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'range':['00','04'],'edge_pin':edge,'source_pin':raw,'queries':queries,'runs':[],'plans':{},'bounds':{'wall_s':180,'read_bytes':20000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0},'qualification':'Read-only fixed1/64 hash-domain pilot at E8 before sixth mutations. Complete20field exact predecessor equality, no guard omission. Source hints on SELECT are not proof of MERGE behavior or full100k/billion admission. Ordered same-client cohorts share data cache; no causal cold or p95 claim.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;save();assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=180;return h
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  stats=c.sql('source-bound',f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),sum(length(encode(input_json,'UTF-8'))) FROM ({source})")[0];assert 0<int(stats[0])<=2500 and stats[0]==stats[1] and int(stats[2])<=16000000;a['source_bound']=stats;metrics()
  for label,q in queries.items():
   rows=c.sql('plan-'+label,'EXPLAIN FORMATTED '+q);text='\n'.join(str(x[0]) for x in rows)+'\n';(O/('plan-'+label+'.txt')).write_text(text);a['plans'][label]={'sha256':hashlib.sha256(text.encode()).hexdigest(),'statement_id':c.records[-1]['statement_id']};save()
  for i,label in enumerate(['default','broadcast','broadcast','default']):
   rows=c.sql('read-'+str(i)+'-'+label,queries[label]);assert rows==[[stats[0],'0']];a['runs'].append({'family':label,'statement_id':c.records[-1]['statement_id'],'caller_ms':c.records[-1]['wall_ms'],'result':rows});save();metrics()
  h=metrics()
  for x in a['runs']:x['metrics']=h[x['statement_id']]['metrics']
  a['state']='Four complete range predecessor comparisons pass exact full20field equality';save();(O/'inflight-request.json').rename(O/'completed-last-request.json');print(json.dumps({k:a[k] for k in ['state','source_bound','costs','wall_s','runs']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles without replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
