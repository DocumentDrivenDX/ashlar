"""Read-only exact pinned singleton plan inspection; no cohort replay."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from inline_guard_sql_r421 import guard_merge
from normalized_apply_sql_r276 import current_merge
B=Path(__file__).resolve().parent

def main():
 out=B/'out/native/current_merge_plans_r559';assert not out.exists();start=time.monotonic();c=BoundedReads(out,socket_timeout=30);a={'state':'collecting nonexecuting current MERGE plans','code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'sources':{},'plans':{},'bounds':{'wall_s':120,'read_bytes':1000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0}}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  p=B/'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json';pub=json.loads(p.read_text());a['sources']['sixth_publication']=hashlib.sha256(p.read_bytes()).hexdigest();target=pub['tables']['edge_current']['table'];raw=pub['inputs']['source_record'];current=pub['inputs']['current_replacement']
  for label,builder in [('full_guard',guard_merge),('historical_simple',current_merge)]:
   q=builder(target,raw['table'],raw['version'],current['table'],current['version']);rows=c.sql(label,'EXPLAIN FORMATTED '+q);text='\n'.join(str(r[0]) for r in rows)+'\n';(out/(label+'.txt')).write_text(text);a['plans'][label]={'query':q,'statement_id':c.records[-1]['statement_id'],'sha256':hashlib.sha256(text.encode()).hexdigest()};save()
  a['qualification']='EXPLAIN only; no MERGE execution. Source sixth before-images are stale against E9; must never execute these statements. Historical simple MERGE omits full predecessor guards and is not an admissible implementation. Plans describe current catalog state, not the completed E8-to-E9 runtime adaptive plan.'
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=120;a['state']='Two nonexecuting current MERGE plans retained';save();(out/'inflight-request.json').rename(out/'completed-last-request.json');print(json.dumps(a,indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles without replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
