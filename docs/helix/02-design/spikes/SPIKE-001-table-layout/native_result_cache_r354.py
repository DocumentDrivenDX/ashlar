"""Small result-cache comparison; repeated pinned reads versus uncached controls."""
import json,time,math,hashlib
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent

def main():
 source=B/'out/native/ashlar_lc_prefix_points_r352/audited-summary.json';s=json.loads(source.read_text());t=s['table'];O=B/'out/native/ashlar_result_cache_r354';assert not O.exists();c=BoundedReads(O);a={'state':'Testing native cache on exact pinned carriers','table':t,'source_sha256':hashlib.sha256(source.read_bytes()).hexdigest(),'reads':[],'bounds':{'read_bytes':12000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':300},'cohort_indices':[0,1,2,3,4,5,16,17]};start=time.monotonic()
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['bounds'] if k!='wall_s'};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());assert time.monotonic()-start<300;save();return h
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=20');rows=c.sql('detail','DESCRIBE DETAIL '+t['table']);names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(names,rows[0]))['id']==t['id'];c.sql('cache-on','SET USE_CACHED_RESULT=true');assert c.sql('cache-on-check','SET USE_CACHED_RESULT')[0][-1].lower()=='true'
  w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();inverse=pow(104729,-1,40000000);fields=list(w.carrier('edge',0));cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in fields);query=f"SELECT {cols} FROM {t['table']} VERSION AS OF {t['version']} WHERE lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)"
  for phase in range(4):
   if phase==3:c.sql('cache-off','SET USE_CACHED_RESULT=false');assert c.sql('cache-off-check','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
   for j,index in enumerate(a['cohort_indices']):
    ordinal=s['reads'][index]['ordinal'];row=w.carrier('edge',ordinal);n=ordinal*inverse%40000000;after=second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in fields]];parameters={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};assert c.sql('point-'+str(j),query,parameters=parameters)==expected;a['reads'].append({'phase':phase,'cache_enabled':phase<3,'ordinal':ordinal,'kind':s['reads'][index]['kind'],'statement_id':c.records[-1]['statement_id']});save()
   metrics()
  h=metrics();records={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def cohort(reads):
   return {'queries':len(reads),'cache_hits':sum(bool(h[r['statement_id']]['metrics'].get('result_from_cache')) for r in reads),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in reads]),'engine_p95_ms':p95([h[r['statement_id']]['metrics'].get('execution_time_ms',0) for r in reads]),'compile_p95_ms':p95([h[r['statement_id']]['metrics'].get('compilation_time_ms',0) for r in reads])} if reads else None
  a['per_phase']={str(p):cohort([r for r in a['reads'] if r['phase']==p]) for p in range(4)};a['by_hit']={str(flag):cohort([r for r in a['reads'] if bool(h[r['statement_id']]['metrics'].get('result_from_cache'))==flag]) for flag in [True,False]};assert all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads'] if r['phase']==3);a['wall_s']=time.monotonic()-start;a['state']='All32 repeated/cache-disabled pinned singleton carriers match exactly';a['qualification']='Eight stratified identities4deleted2updated2unchanged, repeated three times with native result cache enabled then once disabled. Stable exact SQL+parameters+run/key tag; complete20fields/absence independently reconstructed. Session cache setting only, unchanged tableUUID/version. Observed cache-hit latency is not cold/physical-read improvement, general cache-hit rate, robust service p95, live-head invalidation proof, production policy/delegation, arbitrary1Bkey working set, sustained ingest or billion admission.';save();print(json.dumps({'state':a['state'],'per_phase':a['per_phase'],'by_hit':a['by_hit'],'costs':a['costs'],'wall_s':a['wall_s']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same read handles',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
