"""Independent qualified-pair proof, native plans and full-value point audit."""
import hashlib,json,math,re
from pathlib import Path
from overlay_sql_r395 import FIELDS
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
from third_changes_r378 import ThirdChanges
B=Path(__file__).resolve().parent

def main():
 from overlay_override_sql_r411 import override_lookup
 p=B/'out/native/ashlar_large_native_read_r416';a=json.loads((p/'summary.json').read_text());assert a['state']=='Full47.97M-carrier scan and288matched full-carrier reads pass'
 for k,path in a['sources'].items():assert hashlib.sha256((B/path).read_bytes()).hexdigest()==a['source_sha256'][k]
 for n,h in a['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==h
 assert a['queries']['override']==override_lookup(a['base']['table'],5,a['overlay']['table'],1)
 certificate=json.loads((B/a['sources']['audit']).read_text());validation=json.loads((B/a['sources']['validation']).read_text());baseline=json.loads((B/a['sources']['points']).read_text());assert a['base']==validation['base']==baseline['base'] and a['overlay']==validation['table']==baseline['overlay'];assert certificate['state']=='Complete100k overlay accepted-carrier/key equivalence and192full-carrier point evidence audited';assert certificate['evidence']['ashlar_overlay_validate_r404']['source_sha256']['summary.json']==a['source_sha256']['validation']
 rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());qs={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(rs)==len(qs)==len({r['statement_id'] for r in rs})==302;by={r['label']:r for r in rs}
 for r in rs:
  q=qs[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'] and r['response']['status']['state']=='SUCCEEDED' and q['query_text']=='/* ashlar '+p.name+' '+r['label']+' */ '+r['sql']
 assert len(a['large_scans'])==2
 for scan in a['large_scans']:
  r=records_scan=by['full-scan-'+scan['kind']];values=r['response']['result']['data_array'][0]
  assert scan['statement_id']==r['statement_id'] and int(values[0])==scan['rows'] and values[1]==scan['fingerprint'] and int(values[2])==scan['invalid']==0
  assert ('FROM '+scan['table']+' VERSION AS OF '+str(scan['version'])) in r['sql'] and not qs[r['statement_id']]['metrics'].get('result_from_cache')
 assert sum(x['rows'] for x in a['large_scans'])==47970000
 plans={}
 for family in ['direct','override','qualified']:
  for index in [16,36]:
   label=f'plan-{family}-{index}';text=(p/(label+'.txt')).read_text();assert text=='\n'.join(str(x[0]) for x in by[label]['response']['result']['data_array'])+'\n';assert by[label]['sql']=='EXPLAIN FORMATTED '+a['queries'][family]
   plans[label]={'sha256':hashlib.sha256(text.encode()).hexdigest(),'bytes':len(text.encode()),'photon_fully_supported':'The query is fully supported by Photon.' in text,'shuffle_sink_operators':len(re.findall(r'^\(\d+\) PhotonShuffleExchangeSink$',text,re.M)),'aggregate_operators':len(re.findall(r'^\(\d+\) Photon(?:Grouping)?Agg$',text,re.M))}
 assert plans['plan-override-36']['shuffle_sink_operators']==0 and plans['plan-qualified-36']['shuffle_sink_operators']==1 and all(x['photon_fully_supported'] for x in plans.values())
 w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();third=ThirdChanges();inverse=pow(104729,-1,40000000);records={r['statement_id']:r for r in rs};assert len(a['reads'])==288
 for read in a['reads']:
  r=records[read['statement_id']];row=w.carrier('edge',read['ordinal']);n=read['ordinal']*inverse%40000000;after=third.change(n-200000)['after'] if 200000<=n<300000 else second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in FIELDS]];assert r['response']['result']['data_array']==expected and r['parameters']=={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']} and r['sql']==a['queries'][read['family']] and not qs[r['statement_id']]['metrics'].get('result_from_cache')
 p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
 for family in ['direct','override','qualified']:
  rr=[r for r in a['reads'] if r['family']==family];assert len(rr)==96 and a['per_family'][family]['caller_p95_ms']==p95([records[r['statement_id']]['wall_ms'] for r in rr]) and a['per_family'][family]['engine_p95_ms']==p95([qs[r['statement_id']]['metrics']['execution_time_ms'] for r in rr])
  for phase in range(2):assert len([r for r in rr if r['phase']==phase])==48 and {r['cohort_index'] for r in rr if r['phase']==phase}==set(a['cohort_indices'])
 costs={k:sum(q['metrics'].get(k,0) for q in qs.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<900
 request=json.loads((p/'inflight-request.json').read_text());assert request['state']=='returned' and request['query_id'] in qs;(p/'inflight-request.json').rename(p/'completed-last-request.json')
 a['audit']={'override_helper_audit_sha256':hashlib.sha256((B/'overlay_override_sql_r411.py').read_bytes()).hexdigest(),'native_final_statements':302,'uncached_full_carrier_reads':288,'native_plan_summary':plans,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({'plans':plans,'per_family':a['per_family'],'costs':costs},indent=2))
if __name__=='__main__':main()
