"""Independent qualified-pair proof, native plans and full-value point audit."""
import hashlib,json,math,re
from pathlib import Path
from overlay_sql_r395 import FIELDS
from mixed_change_queries_r230 import row_hash_sql
from normalized_apply_sql_r276 import pin
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from third_changes_r378 import ThirdChanges
B=Path(__file__).resolve().parent

def main():
 from overlay_qualified_sql_r408 import qualified_pair_lookup
 p=B/'out/native/ashlar_overlay_shape_points_r419';a=json.loads((p/'summary.json').read_text());assert a['state']=='Complete old/new100k marker preservation and48paired full-carrier reads pass'
 for k,path in a['sources'].items():assert hashlib.sha256((B/path).read_bytes()).hexdigest()==a['source_sha256'][k]
 for n,h in a['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==h
 assert a['queries']['old']==qualified_pair_lookup(a['base']['table'],5,a['overlay']['table'],1) and a['queries']['new']==qualified_pair_lookup(a['base']['table'],5,a['overlay']['table'],a['maintenance_new_version'])
 certificate=json.loads((B/a['sources']['audit']).read_text());validation=json.loads((B/a['sources']['validation']).read_text());baseline=json.loads((B/a['sources']['points']).read_text());assert a['base']==validation['base']==baseline['base'] and a['overlay']==validation['table']==baseline['overlay'];assert certificate['state']=='Complete100k overlay accepted-carrier/key equivalence and192full-carrier point evidence audited';assert certificate['evidence']['ashlar_overlay_validate_r404']['source_sha256']['summary.json']==a['source_sha256']['validation']
 rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());qs={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(rs)==len(qs)==len({r['statement_id'] for r in rs})==70;by={r['label']:r for r in rs}
 for r in rs:
  q=qs[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and r['response']['status']['state']=='SUCCEEDED' and q['query_text']=='/* ashlar '+p.name+' '+r['label']+' */ '+r['sql']
 maintenance=json.loads((B/a['sources']['maintenance']).read_text());mp=B/'out/native/ashlar_overlay_shape_r418';mrs=[json.loads(x) for x in (mp/'statements.jsonl').read_text().splitlines()];mh=json.loads((mp/'shared-history.json').read_text());mq={q['query_id']:q for q in mh['queries']};assert len(mrs)==len(mq)==21 and not mh['missing_ids'] and mh['require_final']
 for r in mrs:
  q=mq[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and r['response']['status']['state']=='SUCCEEDED' and q['query_text']==r['sql']
 assert maintenance['costs']=={k:sum(q['metrics'].get(k,0) for q in mq.values()) for k in maintenance['costs']}
 assert maintenance['code_sha256']==hashlib.sha256((B/'overlay_shape_r418.py').read_bytes()).hexdigest()
 plan=json.loads((B/maintenance['plan']).read_text());assert maintenance['plan_sha256']==hashlib.sha256((B/maintenance['plan']).read_bytes()).hexdigest()
 for k,pth in plan['sources'].items():assert hashlib.sha256((B/pth).read_bytes()).hexdigest()==plan['source_sha256'][k]
 assert maintenance['new_version']==a['maintenance_new_version']==5 and maintenance['table']['version']==1 and maintenance['old_detail']['id']==maintenance['new_detail']['id']==plan['table']['id']
 commits=[x for x in maintenance['new_history'] if int(x['version'])>1];assert [int(x['version']) for x in commits]==[5,4,3,2]
 assert all(x['queryHistoryStatementId'] in [maintenance['alter_statement_id'],maintenance['optimize_statement_id']] for x in commits)
 assert by['new-target-property']['response']['result']['data_array']==[['delta.targetFileSize','16777216']]
 oracle=json.loads((B/a['sources']['oracle']).read_text());assert oracle['fields']==list(FIELDS)+['is_deleted']
 for n,hsh in oracle['source_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==hsh
 for name,version in [('old',1),('new',5)]:
  r=by['full-digest-'+name];expected_sql=f"SELECT count(*),count(DISTINCT struct(source_system,rel_type_id,id)),count_if(is_deleted),count_if(NOT is_deleted),count_if(entity_version<>2 OR is_deleted IS NULL),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(oracle['fields'])}))),256) FROM {pin(a['overlay']['table'],version)}";assert r['sql']==expected_sql;expected=[['100000','100000','10000','90000','0',oracle['digest']]];assert r['response']['result']['data_array']==expected and a['digests'][name]['result']==expected and a['digests'][name]['statement_id']==r['statement_id'] and a['digests'][name]['version']==version and not qs[r['statement_id']]['metrics'].get('result_from_cache')
 for role,t in maintenance['heads'].items():assert by['final-head-'+role]['response']['result']['data_array'][0][0]==t['head']
 assert by['final-overlay-head']['response']['result']['data_array'][0][0]=='5'
 assert a['combined_wall_s']==a['wall_s']+maintenance['wall_s']
 gap=max(0,min(r['start_epoch'] for r in rs)-max(r['start_epoch']+r['wall_ms']/1000 for r in mrs));a['conservative_wall_upper_s']=a['combined_wall_s']+gap;assert a['conservative_wall_upper_s']<300
 live_path=mp/('live-statement.json' if (mp/'live-statement.json').exists() else 'completed-last-statement.json');live=json.loads(live_path.read_text());assert live['statement_id'] in mq
 if live_path.name=='live-statement.json':live_path.rename(mp/'completed-last-statement.json')
 plans={}
 for family in ['direct','old','new']:
  for index in [16,36]:
   label=f'plan-{family}-{index}';text=(p/(label+'.txt')).read_text();assert text=='\n'.join(str(x[0]) for x in by[label]['response']['result']['data_array'])+'\n';assert by[label]['sql']=='EXPLAIN FORMATTED '+a['queries'][family]
   plans[label]={'sha256':hashlib.sha256(text.encode()).hexdigest(),'bytes':len(text.encode()),'photon_fully_supported':'The query is fully supported by Photon.' in text,'shuffle_sink_operators':len(re.findall(r'^\(\d+\) PhotonShuffleExchangeSink$',text,re.M)),'aggregate_operators':len(re.findall(r'^\(\d+\) Photon(?:Grouping)?Agg$',text,re.M))}
 assert plans['plan-old-36']['shuffle_sink_operators']==1 and plans['plan-new-36']['shuffle_sink_operators']==1 and all(x['photon_fully_supported'] for x in plans.values())
 w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();third=ThirdChanges();inverse=pow(104729,-1,40000000);records={r['statement_id']:r for r in rs};assert len(a['reads'])==48
 for read in a['reads']:
  r=records[read['statement_id']];row=w.carrier('edge',read['ordinal']);n=read['ordinal']*inverse%40000000;after=third.change(n-200000)['after'] if 200000<=n<300000 else second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in FIELDS]];assert r['response']['result']['data_array']==expected and r['parameters']=={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']} and r['sql']==a['queries'][read['family']] and not qs[r['statement_id']]['metrics'].get('result_from_cache')
 p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
 for family in ['direct','old','new']:
  rr=[r for r in a['reads'] if r['family']==family];assert len(rr)==16 and a['per_family'][family]['caller_p95_ms']==p95([records[r['statement_id']]['wall_ms'] for r in rr]) and a['per_family'][family]['engine_p95_ms']==p95([qs[r['statement_id']]['metrics']['execution_time_ms'] for r in rr])
  for phase in range(2):assert len([r for r in rr if r['phase']==phase])==8 and {r['cohort_index'] for r in rr if r['phase']==phase}==set(a['cohort_indices'])
 costs={k:maintenance['costs'][k]+sum(q['metrics'].get(k,0) for q in qs.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['combined_wall_s']<300
 request_path=p/('inflight-request.json' if (p/'inflight-request.json').exists() else 'completed-last-request.json');request=json.loads(request_path.read_text());assert request['state']=='returned' and request['query_id'] in qs
 if request_path.name=='inflight-request.json':request_path.rename(p/'completed-last-request.json')
 a['audit']={'qualified_helper_audit_sha256':hashlib.sha256((B/'overlay_qualified_sql_r408.py').read_bytes()).hexdigest(),'maintenance_final_statements':21,'native_final_statements':70,'uncached_full_carrier_reads':48,'native_plan_summary':plans,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'plans':plans,'per_family':a['per_family'],'costs':costs},indent=2))
if __name__=='__main__':main()
