"""Small statement-API transport comparison; exact pinned full carriers."""
import json,time,math,hashlib
from pathlib import Path
from persistent_sql import Client
from publication_history import collect_history,HistoryPending
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_result_cache_r354/summary.json';s=json.loads(source.read_text());assert s['state']=='All32 repeated/cache-disabled pinned singleton carriers match exactly';t=s['table'];O=B/'out/native/ashlar_statement_cache_r356';assert not O.exists();O.mkdir();c=Client(O,observation_timeout=40,cancel_after=20);a={'state':'Comparing statement transport on same pinned eight-key cohort','table':t,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'reads':[],'bounds':{'read_bytes':6000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':180}};start=time.monotonic()
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 save()
 try:
  w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();inverse=pow(104729,-1,40000000);fields=list(w.carrier('edge',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in fields);q=f"SELECT {cols} FROM {t['table']} VERSION AS OF {t['version']} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)"
  for phase in range(2):
   for j,r in enumerate(s['reads'][:8]):
    row=w.carrier('edge',r['ordinal']);n=r['ordinal']*inverse%40000000;after=second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in fields]];p={"hash":row['lookup_hash'],"source":row['source_system'],"type":row['rel_type_id'],"id":row['id']};parameters=[{'name':k,'value':v,'type':'STRING'} for k,v in p.items()];sql='/* ashlar '+O.name+' point-'+str(j)+' */ '+q;assert c.sql('point-'+str(j),sql,parameters=parameters)==expected;a['reads'].append({'phase':phase,'ordinal':r['ordinal'],'kind':r['kind'],'statement_id':c.records[-1]['statement_id']});save()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());records={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def cohort(reads):return {'queries':len(reads),'cache_hits':sum(bool(h[r['statement_id']]['metrics'].get('result_from_cache')) for r in reads),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in reads]),'engine_p95_ms':p95([h[r['statement_id']]['metrics'].get('execution_time_ms',0) for r in reads]),'compile_p95_ms':p95([h[r['statement_id']]['metrics'].get('compilation_time_ms',0) for r in reads])} if reads else None
  a['per_phase']={str(p):cohort([r for r in a['reads'] if r['phase']==p]) for p in range(2)};a['by_hit']={str(flag):cohort([r for r in a['reads'] if bool(h[r['statement_id']]['metrics'].get('result_from_cache'))==flag]) for flag in [True,False]};a['wall_s']=time.monotonic()-start;assert a['wall_s']<180;a['state']='All16 statement-API pinned full-field singleton results match exactly';a['qualification']='Same eight-key cohort, immutable table UUID/version and STRING typed named parameters, two sequential passes through shared SDK HTTP client; default per-statement native session/cache behavior observed through actual history. Cache-hit cohorts are descriptive, not cached production hit rate, service tail, controlled cold, physical pruning, causal transport isolation, new publication or billion admission.';save();print(json.dumps({'state':a['state'],'per_phase':a['per_phase'],'by_hit':a['by_hit'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same statement handles',error=str(e));save();raise
if __name__=='__main__':main()
