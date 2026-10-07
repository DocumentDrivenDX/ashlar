"""Small alternating stable-comment versus unique-comment full-carrier lookup cohort."""
import json,time,math,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from scale_mixed_r219 import Workload
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_scoped_pruning_r301/audited-summary.json';prior=json.loads(source.read_text());p=B/'out/native/ashlar_statement_shape_r306';assert not p.exists();p.mkdir();c=BoundedReads(p);a={'state':'Running stable and unique comment statement-shape comparison','source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'table':prior['table'],'reads':[]};start=time.monotonic();w=Workload(8000000,40000000);fields=list(w.carrier('edge',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if k=='published_at' else k for k in fields)
 def save():(p/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for n in range(15):
   try:h=collect_history(c.w,c.records,p/'shared-history.json');break
   except HistoryPending:
    if n==14:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};save();assert a['costs']['read_bytes']<=10000000000 and a['costs']['write_remote_bytes']==0 and a['costs']['spill_to_disk_bytes']<=1000000000;assert time.monotonic()-start<300;return h
 try:
  save();c.sql('timeout','SET STATEMENT_TIMEOUT=15');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  for i,index in enumerate(list(range(16,24,2))*2):
   if i%4==0:
    metrics();assert a['costs']['read_bytes']+3000000000<=10000000000
   ordinal=prior['reads'][index]['ordinal'];assert prior['reads'][index]['kind']=='unchanged';row=w.carrier('edge',ordinal);expected=[[None if row[k] is None else row[k].replace('T',' ').removesuffix('Z') if k=='published_at' else row[k] for k in fields]];t=a['table'];q=f"SELECT {cols} FROM {t['table']} VERSION AS OF {t['version']} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)";assert c.sql('stable-shape',q,parameters={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']})==expected;assert c.sql('unique-'+str(i),q,parameters={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']})==expected;a['reads'].append({'ordinal':ordinal,'statement_id':c.records[-1]['statement_id']});save()
  h=metrics();p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1];a['cohorts']={}
  for kind in ['stable','unique']:
   rs=[r for r in c.records if r['label'].startswith(kind+'-')];assert len(rs)==8 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in rs)
   a['cohorts'][kind]={'queries':8,'caller_p95_ms':p95([r['wall_ms'] for r in rs]),**{key+'_p95':p95([h[r['statement_id']]['metrics'].get(key,0) for r in rs]) for key in ['total_time_ms','execution_time_ms','compilation_time_ms','read_bytes']},'caller_minus_native_total_p95_ms':p95([r['wall_ms']-h[r['statement_id']]['metrics'].get('total_time_ms',0) for r in rs])}
  a.update(state='All8 stable and8 unique comment full-carrier queries pass',wall_s=time.monotonic()-start,qualification='Small ordered alternating uncached cohort on one persistent SQL connection.4stratified unchanged keys repeated twice with8stable-comment and8unique-comment queries; repeated labels remain bound by unique actual query IDs. not full workload service-tail or cold/concurrent trial. Native total and caller clocks differ; signed residual is approximate client/network/fetch/poll overhead, not isolated network latency. Do not add component p95 values or infer billion admission.');save();(p/'inflight-request.json').unlink();print(json.dumps(a['cohorts'],indent=2))
 except Exception as exc:a.update(state='Stopped; inspect same handles',error=str(exc));save();raise
 finally:c.close()
if __name__=='__main__':main()
