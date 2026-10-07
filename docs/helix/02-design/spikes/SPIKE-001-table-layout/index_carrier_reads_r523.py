"""Complete singleton reads after qualified private index maintenance."""
import hashlib,json,math,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/index_carrier_spike_r513/summary.json';s=json.loads(source.read_text());assert s['full_logical_parity']==[['2498028','0']]
 maintenance=B/'out/native/index_maintenance_recovery_r524/summary.json';m=json.loads(maintenance.read_text());assert m['state']=='Private index maintenance preserves all11 fields, typed uniqueness, deletions and empty logical CDF' and m['parity']==[['2498646','0']] and m['cdf']==[['0']]
 out=B/'out/native/index_carrier_reads_r523';assert not out.exists();start=time.monotonic();c=BoundedReads(out,socket_timeout=30)
 a={'state':'running complete singleton comparison','maintenance_sha256':hashlib.sha256(maintenance.read_bytes()).hexdigest(),'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'code_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'bounds':{'read_bytes':20000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180},'reads':[],'qualification':'Qualified OPTIMIZE index version3; same explicit full-key predicates asR517 on BOTH index and carrier; eight selected changed identities, two passes, pinned 2.5M-row private fixture. Result cache disabled; storage cache and fixed warm history remain. No cold-data, concurrent, publication or billion admission.'}
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
  tables={'index':(s['index'],s['index_after']['id'],3),'carrier':(s['carrier'],s['carrier_after']['id'],2),'wide':(s['sources']['predecessor']['table'],s['sources']['predecessor']['id'],3)}
  def custody(prefix):
   result={}
   for role,(table,uuid,version) in tables.items():
    rows=c.sql(prefix+'-'+role+'-detail','DESCRIBE DETAIL '+table);columns=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];d=dict(zip(columns,rows[0]));assert d['id']==uuid
    head=c.sql(prefix+'-'+role+'-head','DESCRIBE HISTORY '+table+' LIMIT 1')[0];assert int(head[0])==version;result[role]={'detail':d,'head':head}
   return result
  a['custody_before']=custody('before')
  prepared=s['sources']['prepared']['table'];upper='1000'+'0'*60
  keys=c.sql('keys',f"SELECT lookup_hash,source_system,CAST(rel_type_id AS STRING),CAST(id AS STRING),CAST(is_delete AS STRING) FROM (SELECT *,row_number() OVER(PARTITION BY is_delete ORDER BY lookup_hash) AS rn FROM {prepared} VERSION AS OF 0 WHERE lookup_hash<'{upper}') WHERE (is_delete AND rn<=2) OR (NOT is_delete AND rn<=6) ORDER BY lookup_hash")
  assert len(keys)==8 and sum(x[4]=='true' for x in keys)==2;a['keys']=keys
  cols=lambda alias:','.join(f'CAST({alias}.published_at AS STRING) AS published_at' if f=='published_at' else alias+'.'+f for f in FIELDS)
  where=lambda alias:f'{alias}.lookup_hash=:hash AND {alias}.source_system=:source AND {alias}.rel_type_id=CAST(:type AS BIGINT) AND {alias}.id=CAST(:id AS BIGINT)'
  join=' AND '.join('i.'+f+'=v.'+f for f in ['lookup_hash','source_system','rel_type_id','id'])+' AND i.carrier_version=v.entity_version AND i.carrier_fingerprint=v.carrier_fingerprint'
  queries={'wide':f"SELECT {cols('v')} FROM {tables['wide'][0]} VERSION AS OF 3 v WHERE {where('v')}",'join':f"SELECT {cols('v')} FROM {s['index']} VERSION AS OF 3 i JOIN {s['carrier']} VERSION AS OF 2 v ON {join} WHERE NOT i.is_deleted AND {where('i')} AND {where('v')}"};a['queries']=queries;expected={}
  for phase in range(2):
   for j,key in enumerate(keys):
    params=dict(zip(['hash','source','type','id'],key[:4]))
    order=['wide','join'] if phase==0 or j%2==0 else ['join','wide']
    for family in order:
     result=c.sql(f'point-{family}-{phase}-{j}',queries[family],parameters=params)
     if phase==0 and family=='wide':expected[j]=result;assert len(result)==(0 if key[4]=='true' else 1)
     else:assert result==expected[j]
     a['reads'].append({'family':family,'phase':phase,'key':j,'statement_id':c.records[-1]['statement_id'],'result':result});save()
   metrics()
  a['custody_after']=custody('after');assert a['custody_before']==a['custody_after'];h=metrics();records={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def group(rr):return {'queries':len(rr),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in rr]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rr]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rr]),'read_bytes':sum(h[r['statement_id']]['metrics'].get('read_bytes',0) for r in rr),'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rr)}
  assert len(a['reads'])==32 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads']);a['groups']={f+'-'+str(p):group([r for r in a['reads'] if r['family']==f and r['phase']==p]) for f in queries for p in range(2)};a.update(state='All32 complete singleton results agree, closing three-table custody unchanged',wall_s=time.monotonic()-start);save();(out/'inflight-request.json').rename(out/'completed-last-request.json');print(json.dumps({k:a[k] for k in ['state','groups','costs','wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect existing handles without replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
