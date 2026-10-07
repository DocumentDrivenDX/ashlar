"""Complete singleton reads: pinned narrow-index join versus wide current."""
import hashlib,json,math,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/file256_recover_r571/summary.json';s=json.loads(source.read_text());assert s['parity']==[['0']] and s['counts']==[['2498646','2498646']]
 out=B/'out/native/file256_reads_r574';assert not out.exists();start=time.monotonic();c=BoundedReads(out,socket_timeout=30)
 a={'state':'running complete singleton comparison','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':20000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180},'reads':[],'qualification':'Eight identities from spaced hash prefixes, two passes, pinned 2.5M-row private fixture. Result cache disabled; storage cache and fixed warm history remain. Clone0 versus maintained2; full-carrier responses. No controlled cold-data, concurrent, publication or billion admission.'}
 def save():(out/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,out/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(1)
  a['costs']={k:sum(q['metrics'].get(k,0) or 0 for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and time.monotonic()-start<180;save();return h
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  table=s['table'];uuid=s['id'];queries={}
  def custody(prefix):
   rows=c.sql(prefix+'-detail','DESCRIBE DETAIL '+table);columns=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(columns,rows[0]));assert d['id']==uuid
   head=c.sql(prefix+'-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0];assert int(head[0])==2;return {'detail':d,'head':head}
  a['custody_before']=custody('before');a['table']=table;a['id']=uuid
  keys=c.sql('keys',f"SELECT lookup_hash,source_system,CAST(rel_type_id AS STRING),CAST(id AS STRING) FROM (SELECT *,row_number() OVER(PARTITION BY substr(lookup_hash,1,2) ORDER BY lookup_hash) rn FROM {table} VERSION AS OF 0 WHERE substr(lookup_hash,1,2) IN ('00','02','04','06','08','0a','0c','0e')) WHERE rn=1 ORDER BY lookup_hash")
  assert len(keys)==8 and len({k[0][:2] for k in keys})==8;a['keys']=keys
  cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in FIELDS)
  where='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)'
  queries={family:f'SELECT {cols} FROM {table} VERSION AS OF {version} WHERE {where}' for family,version in [('before64',0),('after256',2)]};a['queries']=queries;expected={}
  for phase in range(2):
   for j,key in enumerate(keys):
    params=dict(zip(['hash','source','type','id'],key[:4]))
    order=['before64','after256'] if phase==0 or j%2==0 else ['after256','before64']
    for family in order:
     result=c.sql(f'point-{family}-{phase}-{j}',queries[family],parameters=params)
     if phase==0 and family=='before64':expected[j]=result;assert len(result)==1
     else:assert result==expected[j]
     a['reads'].append({'family':family,'phase':phase,'key':j,'statement_id':c.records[-1]['statement_id'],'result':result});save()
   metrics()
  a['custody_after']=custody('after');assert a['custody_before']==a['custody_after'];h=metrics();records={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def group(rr):return {'queries':len(rr),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in rr]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rr]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rr]),'read_bytes':sum(h[r['statement_id']]['metrics'].get('read_bytes',0) for r in rr),'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rr)}
  assert len(a['reads'])==32 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads']);a['groups']={f+'-'+str(p):group([r for r in a['reads'] if r['family']==f and r['phase']==p]) for f in queries for p in range(2)};a.update(state='All32 complete singleton responses agree across file targets with unchanged custody',wall_s=time.monotonic()-start);save();(out/'inflight-request.json').rename(out/'completed-last-request.json');print(json.dumps({k:a[k] for k in ['state','groups','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect existing handles without replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
