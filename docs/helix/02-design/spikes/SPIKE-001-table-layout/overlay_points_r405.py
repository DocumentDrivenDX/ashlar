"""Matched uncached full20field native reads: E6 versus pinned E5+100k overlay."""
import hashlib,json,math,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
from overlay_carrier_sql_r403 import carrier_lookup
from overlay_sql_r395 import FIELDS
from normalized_apply_sql_r276 import pin
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from third_changes_r378 import ThirdChanges
B=Path(__file__).resolve().parent

def main():
 sources={'validation':'out/native/ashlar_overlay_validate_r404/summary.json','old_cohort':'out/native/ashlar_lc_prefix_points_r352/audited-summary.json','build':'out/native/ashlar_overlay_build_r401/summary.json'};s={k:json.loads((B/path).read_text()) for k,path in sources.items()};assert s['validation']['state']=='Accepted100k overlay matches qualified E6 change images and effective key integrity';v=s['validation'];o=v['table'];base=v['base'];reference=v['reference'];O=B/'out/native/ashlar_overlay_points_r405';assert not O.exists();O.mkdir();start=time.monotonic();c=BoundedReads(O,socket_timeout=30);a={'state':'Matched full-carrier lookup comparison','sources':sources,'source_sha256':{k:hashlib.sha256((B/path).read_bytes()).hexdigest() for k,path in sources.items()},'base':base,'reference':reference,'overlay':o,'reads':[],'bounds':{'read_bytes':40000000000,'write_remote_bytes':0,'spill_to_disk_bytes':0,'wall_s':300},'combined_read_budget':80000000000,'qualification':'48stratified identities: same prior32 plus16third changes (4deleted12updated),2passes per family, interleaved order. Persistent connector/native cache disabled; full20field actual values/absence independently reconstructed. Validation/previous workload warms data; not controlled cold/service-tail/causal/producer publication/compaction/concurrent or billion admission.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items());a['combined_costs']={k:v['combined_build_validation_costs'][k]+a['costs'][k] for k in a['costs']};assert a['combined_costs']['read_bytes']<=80000000000;assert time.monotonic()-start<300;save();return h
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  for role,t in [('reference',reference),('overlay',o)]:
   rs=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rs[0]))['id']==t['id']
  w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();third=ThirdChanges();inverse=pow(104729,-1,40000000);ordinals=[r['ordinal'] for r in s['old_cohort']['reads'][:32]]+[(200000+i)*104729%40000000 for i in [0,10,20,30,1,2,3,4,5,6,7,8,9,11,12,13]];assert len(ordinals)==len(set(ordinals))==48;a['ordinals']=ordinals
  cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in FIELDS);where='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)';queries={'direct':f'SELECT {cols} FROM {pin(reference["table"],reference["version"])} WHERE {where}','overlay':carrier_lookup(base['table'],base['version'],o['table'],o['version'])};a['queries']=queries;save()
  for phase in range(2):
   for j,ordinal in enumerate(ordinals):
    assert time.monotonic()-start<300;row=w.carrier('edge',ordinal);index=ordinal*inverse%40000000;after=third.change(index-200000)['after'] if 200000<=index<300000 else second.change(index-100000)['after'] if 100000<=index<200000 else first.change(index)['after'] if index<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in FIELDS]];params={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};families=['direct','overlay'] if (j+phase)%2==0 else ['overlay','direct']
    for family in families:
     label=f'point-{family}-{phase}-{j}';assert c.sql(label,queries[family],parameters=params)==expected;a['reads'].append({'family':family,'phase':phase,'cohort_index':j,'ordinal':ordinal,'kind':'deleted' if after is None else 'third-updated' if 200000<=index<300000 else 'earlier-updated' if index<200000 else 'unchanged','statement_id':c.records[-1]['statement_id']});save()
    if j%8==7:metrics()
  h=metrics();by={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def group(reads):return {'queries':len(reads),'caller_p95_ms':p95([by[r['statement_id']]['wall_ms'] for r in reads]),'engine_p95_ms':p95([h[r['statement_id']]['metrics'].get('execution_time_ms',0) for r in reads]),'compile_p95_ms':p95([h[r['statement_id']]['metrics'].get('compilation_time_ms',0) for r in reads]),'read_bytes_p95':p95([h[r['statement_id']]['metrics'].get('read_bytes',0) for r in reads]),'read_files_p95':p95([h[r['statement_id']]['metrics'].get('read_files_count',0) for r in reads]),'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in reads)}
  assert len(a['reads'])==192 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads']);a['per_family']={f:group([r for r in a['reads'] if r['family']==f]) for f in queries};a['per_family_phase']={f+'-'+str(p):group([r for r in a['reads'] if r['family']==f and r['phase']==p]) for f in queries for p in range(2)};a['wall_s']=time.monotonic()-start;a['state']='All192 matched full-carrier direct and overlay reads pass';save();print(json.dumps({k:a[k] for k in ['state','per_family','per_family_phase','wall_s','costs','combined_costs']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same driver/server handles; no blind replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
