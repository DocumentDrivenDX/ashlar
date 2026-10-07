"""Small override/fallback native plan/full-carrier comparison after dominance proof."""
import hashlib,json,math,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from mixed_change_queries_r230 import row_hash_sql
from overlay_qualified_sql_r408 import qualified_pair_lookup
from overlay_sql_r395 import FIELDS
from normalized_apply_sql_r276 import pin
from publication_history import collect_history,HistoryPending
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from third_changes_r378 import ThirdChanges
B=Path(__file__).resolve().parent

def main():
 paths={'maintenance':'out/native/ashlar_overlay_shape_r418/summary.json','oracle':'out/overlay100k-oracle-r400.json','audit':'out/overlay100k-evidence-audit-r406.json','points':'out/native/ashlar_overlay_points_r405/summary.json','validation':'out/native/ashlar_overlay_validate_r404/summary.json'};s={k:json.loads((B/p).read_text()) for k,p in paths.items()};assert s['audit']['state']=='Complete100k overlay accepted-carrier/key equivalence and192full-carrier point evidence audited';assert s['validation']['state']=='Accepted100k overlay matches qualified E6 change images and effective key integrity'
 for name,source in [('ashlar_overlay_points_r405','points'),('ashlar_overlay_validate_r404','validation')]:assert hashlib.sha256((B/paths[source]).read_bytes()).hexdigest()==s['audit']['evidence'][name]['source_sha256']['summary.json']
 maintenance=s['maintenance'];assert maintenance['state']=='Private overlay maintenance terminal; complete preservation/read qualification pending';base=s['points']['base'];o=s['points']['overlay'];reference=s['points']['reference'];assert base==s['validation']['base'] and o==s['validation']['table'] and reference==s['validation']['reference'];assert base['version']==5 and o['version']==1 and reference['version']==6
 O=B/'out/native/ashlar_overlay_shape_points_r419';assert not O.exists();O.mkdir();start=time.monotonic();c=BoundedReads(O,socket_timeout=30);a={'state':'Qualifying maintenance and paired max_by old/new overlay reads','sources':paths,'source_sha256':{k:hashlib.sha256((B/p).read_bytes()).hexdigest() for k,p in paths.items()},'code_sha256':{n:hashlib.sha256((B/n).read_bytes()).hexdigest() for n in ['overlay_qualified_sql_r408.py','overlay_qualified_sql_r408.py','overlay_shape_points_r419.py']},'base':base,'overlay':o,'reference':reference,'cohort_indices':[0,4,16,17,32,36,37,47],'reads':[],'maintenance_new_version':maintenance['new_version'],'digests':{},'bounds':{'read_bytes':15000000000,'write_remote_bytes':300000000,'spill_to_disk_bytes':1000000000,'wall_s':300},'qualification':'Private E5+overlay1 complete digest/key/lineage/dominance proof required before override: each unique overlay version2 dominates its qualified E5 predecessor1; deletion suppresses fallback. All overlay versions2, every affected E5 predecessor1, complete tuple uniqueness and preserved fields independently qualified. Exact duplicates can only be identical accepted carriers. This is not arbitrary-input read safety, real publisher admission/fencing or support for unresolved conflicting versions. Small ordered cohort, not service/cold/concurrent/compaction/billion admission.'}
 def save():(O/'summary.json').write_text(json.dumps(a,indent=2)+'\n')
 def metrics():
  c.cursor.close();c.cursor=c.connection.cursor()
  for i in range(20):
   try:h=collect_history(c.w,c.records,O/'shared-history.json');break
   except HistoryPending:
    if i==19:raise
    time.sleep(2)
  a['costs']={k:maintenance['costs'][k]+sum(q['metrics'].get(k,0) for q in h.values()) for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and time.monotonic()-start+maintenance['wall_s']<300;save();return h
 save()
 try:
  c.sql('timeout','SET STATEMENT_TIMEOUT=30');assert c.sql('cache','SET USE_CACHED_RESULT')[0][-1].lower()=='false'
  for role,t in [('reference',reference),('overlay',o)]:
   rows=c.sql('detail-'+role,'DESCRIBE DETAIL '+t['table']);cols=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']];assert dict(zip(cols,rows[0]))['id']==t['id'];assert c.sql('head-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]==str(maintenance['new_version'] if role=='overlay' else t['version'])
  assert c.sql('new-target-property','SHOW TBLPROPERTIES '+o['table']+" ('delta.targetFileSize')")==[['delta.targetFileSize','16777216']]
  for name,version in [('old',1),('new',maintenance['new_version'])]:
   q=f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_deleted),count_if(NOT is_deleted),count_if(entity_version<>2 OR is_deleted IS NULL),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(s['oracle']['fields'])}))),256) FROM {pin(o['table'],version)}"
   result=c.sql('full-digest-'+name,q);assert result==[['100000','100000','10000','90000','0',s['oracle']['digest']]];a['digests'][name]={'version':version,'result':result,'statement_id':c.records[-1]['statement_id']};save()
  cols=','.join('CAST(published_at AS STRING) AS published_at' if f=='published_at' else f for f in FIELDS);where='lookup_hash=:hash AND source_system=:source AND rel_type_id=CAST(:type AS BIGINT) AND id=CAST(:id AS BIGINT)';queries={'direct':f'SELECT {cols} FROM {pin(reference["table"],6)} WHERE {where}','old':qualified_pair_lookup(base['table'],5,o['table'],1),'new':qualified_pair_lookup(base['table'],5,o['table'],maintenance['new_version'])};a['queries']=queries;w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();third=ThirdChanges();inverse=pow(104729,-1,40000000)
  for index in [16,36]:
   row=w.carrier('edge',s['points']['ordinals'][index]);params={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']}
   for family,q in queries.items():
    label=f'plan-{family}-{index}';result=c.sql(label,'EXPLAIN FORMATTED '+q,parameters=params);(O/(label+'.txt')).write_text('\n'.join(str(x[0]) for x in result)+'\n')
  metrics()
  for phase in range(2):
   for j,index in enumerate(a['cohort_indices']):
    ordinal=s['points']['ordinals'][index];row=w.carrier('edge',ordinal);n=ordinal*inverse%40000000;after=third.change(n-200000)['after'] if 200000<=n<300000 else second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in FIELDS]];params={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};families=list(queries);rotate=(j+phase)%3;families=families[rotate:]+families[:rotate]
    for family in families:
     assert c.sql(f'point-{family}-{phase}-{j}',queries[family],parameters=params)==expected;a['reads'].append({'family':family,'phase':phase,'cohort_index':index,'ordinal':ordinal,'statement_id':c.records[-1]['statement_id']});save()
   metrics()
  for role,t in maintenance['heads'].items():assert c.sql('final-head-'+role,'DESCRIBE HISTORY '+t['table']+' LIMIT 1')[0][0]==t['head']
  assert c.sql('final-overlay-head','DESCRIBE HISTORY '+o['table']+' LIMIT 1')[0][0]==str(maintenance['new_version'])
  h=metrics();records={r['statement_id']:r for r in c.records};p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
  def group(rr):return {'queries':len(rr),'caller_p95_ms':p95([records[r['statement_id']]['wall_ms'] for r in rr]),'engine_p95_ms':p95([h[r['statement_id']]['metrics']['execution_time_ms'] for r in rr]),'compile_p95_ms':p95([h[r['statement_id']]['metrics']['compilation_time_ms'] for r in rr]),'remote_queries':sum(h[r['statement_id']]['metrics'].get('read_remote_bytes',0)>0 for r in rr)}
  assert len(a['reads'])==48 and all(not h[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads']);a['per_family']={f:group([r for r in a['reads'] if r['family']==f]) for f in queries};a['per_family_phase']={f+'-'+str(p):group([r for r in a['reads'] if r['family']==f and r['phase']==p]) for f in queries for p in range(2)};a['wall_s']=time.monotonic()-start;a['combined_wall_s']=a['wall_s']+maintenance['wall_s'];a['state']='Complete old/new100k marker preservation and48paired full-carrier reads pass';save();print(json.dumps({k:a[k] for k in ['state','per_family','per_family_phase','wall_s','costs']},indent=2))
 except Exception as e:a.update(state='Stopped; inspect same native driver handles; no blind replay',error=str(e));save();raise
 finally:c.close()
if __name__=='__main__':main()
