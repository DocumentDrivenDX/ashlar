"""Read-only exact pinned singleton plan inspection; no cohort replay."""
import hashlib,json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 out=B/'out/native/index_carrier_plans_r519';assert not out.exists();start=time.monotonic();c=BoundedReads(out,socket_timeout=30);a={'state':'collecting exact singleton plans','code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'sources':{},'plans':{},'bounds':{'wall_s':120,'read_bytes':1000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0}}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  for run in ['r515','r517']:
   p=B/('out/native/index_carrier_reads_'+run+'/summary.json');s=json.loads(p.read_text());a['sources'][run]=hashlib.sha256(p.read_bytes()).hexdigest();key=next(k for k in s['keys'] if k[4]=='false');params=dict(zip(['hash','source','type','id'],key[:4]));a['key']=key
   for family in (['wide','join'] if run=='r515' else ['join']):
    label=run+'-'+family;q=s['queries'][family];rows=c.sql(label,'EXPLAIN FORMATTED '+q,parameters=params);text='\n'.join(str(r[0]) for r in rows)+'\n';(out/(label+'.txt')).write_text(text);a['plans'][label]={'query':q,'parameters':params,'statement_id':c.records[-1]['statement_id'],'sha256':hashlib.sha256(text.encode()).hexdigest()};save()
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};a['wall_s']=time.monotonic()-start;assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<=120;a['state']='Three exact parameterized pinned singleton plans retained';save();(out/'inflight-request.json').rename(out/'completed-last-request.json');print(json.dumps(a,indent=2))
 except Exception as e:a.update(state='Stopped; inspect same handles without replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
