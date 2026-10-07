"""Audit stopped append workload, read-only equivalence follow-up and matched points."""
import hashlib,json,math
from pathlib import Path
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from third_changes_r378 import ThirdChanges
from overlay_sql_r395 import FIELDS
B=Path(__file__).resolve().parent

def main():
 combined={};total={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
 for name in ['ashlar_overlay_build_r401','ashlar_overlay_validate_r404','ashlar_overlay_points_r405']:
  p=B/'out/native'/name;a=json.loads((p/'summary.json').read_text());rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());qs={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids']
  if name.endswith('r401'):
   canceled=json.loads((p/'canceled-native-final.json').read_text());assert canceled['status']=='CANCELED' and canceled['is_final'];qs[canceled['query_id']]=canceled
  assert len(qs)==len(rs)==len({r['statement_id'] for r in rs});by={r['label']:r for r in rs};rows=lambda label:by[label]['response'].get('result',{}).get('data_array',[])
  def objs(label):return [dict(zip([x['name'] for x in by[label]['response']['manifest']['schema']['columns']],r)) for r in rows(label)]
  for r in rs:
   q=qs[r['statement_id']];assert q['is_final'];assert q['query_text']==(('/* ashlar '+name+' '+r['label']+' */ ') if r.get('transport')=='sql-driver' else '')+r['sql'];assert q['status']==('CANCELED' if r['response']['status']['state']=='CANCELED' else 'FINISHED')
  costs={k:sum(q['metrics'].get(k,0) for q in qs.values()) for k in total};assert costs==a['costs']
  for k in total:total[k]+=costs[k]
  if name.endswith('r401'):
   build=a;assert a['state'].startswith('Stopped;') and len(rs)==26 and by['reference-all90k-live']['response']['status']['state']=='CANCELED';assert objs('bind-append')==a['history'] and [int(x['version']) for x in a['history']]==[1,0] and a['history'][0]['queryHistoryStatementId']==by['append100k']['statement_id'];assert a['table']['version']==1 and objs('new-overlay-detail')[0]['id']==a['table']['id'];oracle=json.loads((B/a['sources']['oracle']).read_text());assert rows('identity-and-partition')==[['100000','100000','10000','90000','0']] and rows('full-overlay-digest')==[['100000',oracle['digest']]]
   for label in ['live','deleted']:assert rows('digest-'+label)==[[str(oracle[label]['rows']),oracle[label]['digest']]]
   assert rows('raw-origin-link')==[['100000','0']] and rows('canonical-deletions')==[['10000',json.loads((B/'out/third-canonical-tomb-r391.json').read_text())['digest']]]
   plan=json.loads((B/a['sources']['plan']).read_text());pub=json.loads((B/plan['sources']['publication']).read_text());assert a['base']==pub['base']['edge_current'] and a['reference']==pub['tables']['edge_current'];post=next(x for x in pub['checks']['edge_current']['groups'] if x[0]=='update_postimage');assert post==['update_postimage','90000','6','6',oracle['live']['digest']]
  elif name.endswith('r404'):
   validation=a;assert a['state']=='Accepted100k overlay matches qualified E6 change images and effective key integrity';assert hashlib.sha256((B/'out/native/ashlar_overlay_build_r401/summary.json').read_bytes()).hexdigest()==a['source_sha256']['stopped_summary'];assert objs('initial-overlay-history')==objs('final-overlay-history')==build['history'] and objs('initial-overlay-detail')[0]['id']==a['final_detail']['id']==a['table']['id'];assert rows('reference10k-deleted-absent')==[['0']] and rows('logical-current-key-count')==[['39970000','39970000']] and rows('new-live-typed-endpoints')==[['90000','0']]
  else:
   points=a;assert a['state']=='All192 matched full-carrier direct and overlay reads pass' and len(a['reads'])==192
   w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();third=ThirdChanges();inverse=pow(104729,-1,40000000)
   for read in a['reads']:
    rec=next(r for r in rs if r['statement_id']==read['statement_id']);ordinal=read['ordinal'];original=w.carrier('edge',ordinal);index=ordinal*inverse%40000000;after=third.change(index-200000)['after'] if 200000<=index<300000 else second.change(index-100000)['after'] if 100000<=index<200000 else first.change(index)['after'] if index<100000 else original;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in FIELDS]];assert rec['response']['result']['data_array']==expected and rec['parameters']=={'hash':original['lookup_hash'],'source':original['source_system'],'type':original['rel_type_id'],'id':original['id']} and not qs[rec['statement_id']]['metrics'].get('result_from_cache');assert rec['sql']==a['queries'][read['family']]
   assert len(set(a['ordinals']))==48 and a['ordinals'][:32]==[r['ordinal'] for r in json.loads((B/a['sources']['old_cohort']).read_text())['reads'][:32]]
   p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
   for family in ['direct','overlay']:
    for phase in range(2):
     rr=[r for r in a['reads'] if r['family']==family and r['phase']==phase];assert len(rr)==48 and {r['ordinal'] for r in rr}==set(a['ordinals']);group=a['per_family_phase'][family+'-'+str(phase)];assert group['caller_p95_ms']==p95([next(x['wall_ms'] for x in rs if x['statement_id']==r['statement_id']) for r in rr]) and group['engine_p95_ms']==p95([qs[r['statement_id']]['metrics']['execution_time_ms'] for r in rr])
  if not name.endswith('r401'):assert all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s']
  if (p/'live-statement.json').exists():(p/'live-statement.json').rename(p/'completed-last-request.json')
  if (p/'inflight-request.json').exists():
   request=json.loads((p/'inflight-request.json').read_text());assert request['state']=='returned' and request['query_id'] in qs;(p/'inflight-request.json').rename(p/'completed-last-request.json')
  combined[name]={'native_final_statements':len(rs),'costs':costs,'source_sha256':{n.name:hashlib.sha256(n.read_bytes()).hexdigest() for n in p.iterdir() if n.is_file()}}
 assert total==points['combined_costs'] and total['read_bytes']<=80000000000 and total['write_remote_bytes']<=1000000000 and total['spill_to_disk_bytes']==0
 result={'state':'Complete100k overlay accepted-carrier/key equivalence and192full-carrier point evidence audited','combined_costs':total,'evidence':combined,'append_caller_s':build['append_caller_s'],'append_native_metrics':json.loads((B/'out/native/ashlar_overlay_build_r401/shared-history.json').read_text())['queries'],'point_metrics':points['per_family'],'point_metrics_per_phase':points['per_family_phase'],'overlay_physical_detail':validation['final_detail'],'qualification':'One private overlay; failed/canceled comparison cost included. Full changed-carrier equality via independently audited overlay90k and published E6 complete CDF multisets plus closed E5→E6 lineage, effective-key counts/deletions/typed closure. No claim that canceled wide join passed.192uncached full actual carrier/absence reads, ordered warmed cohort not robust service/cold/concurrent/compaction/publication/billion admission.'}
 # Keep compact append metrics rather than duplicate the full native query list.
 result['append_native_metrics']=next(q['metrics'] for q in result['append_native_metrics'] if q['query_id']==build['append_statement_id'])
 (B/'out/overlay100k-evidence-audit-r406.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k not in ['evidence','overlay_physical_detail','append_native_metrics']},indent=2))
if __name__=='__main__':main()
