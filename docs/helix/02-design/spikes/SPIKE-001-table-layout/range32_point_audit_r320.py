"""Offline exact point/cohort and same-SID native history audit after all32partition ZORDER stages."""
import json,hashlib,math
from pathlib import Path
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_range32_pruning_r310';a=json.loads((p/'summary.json').read_text());assert a['state']=='All64 exact post-change points and complete39.99M live-file ranges verified';records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(native)==len(records)==68
 for r in records:
  q=native[r['statement_id']];assert r['response']['status']['state']=='SUCCEEDED' and q['status']=='FINISHED' and q['is_final'];assert q['query_text']=='/* ashlar '+p.name+' '+r['label']+' */ '+r['sql'];assert not r['response'].get('manifest',{}).get('truncated')
 points=[r for r in records if r['label'].startswith('point-')];assert len(points)==len(a['reads'])==64;w=Workload(8000000,40000000);changes=Changes(8000000,40000000,100000);inverse=pow(104729,-1,40000000);scope=[]
 for i,(r,read) in enumerate(zip(points,a['reads'])):
  assert r['label']=='point-'+str(i) and r['statement_id']==read['statement_id'];ordinal=read['ordinal'];row=w.carrier('edge',ordinal);index=ordinal*inverse%40000000;after=changes.change(index)['after'] if index<100000 else row;expected=[] if after is None else [[None if after[k] is None else after[k].replace('T',' ').removesuffix('Z') if k=='published_at' else after[k] for k in row]];assert r['response']['result']['data_array']==expected;assert r['parameters']=={'partition':int(row['lookup_hash'][:2],16)//8,'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};assert not native[r['statement_id']]['metrics'].get('result_from_cache');scope.append(row['lookup_hash']<'1'+'0'*63)
 assert a['reads'][:32]==[{**x,'pass':0,'statement_id':a['reads'][i]['statement_id']} for i,x in enumerate(a['reads'][32:])]
 assert len(a['files'])==int(json.loads((B/'out/native/ashlar_range32_maintain_r316/audited-summary.json').read_text())['final_detail']['numFiles']);assert sum(int(x[2]) for x in a['files'])==39990000
 p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
 baseline=json.loads((B/'out/native/ashlar_scoped_pruning_r301/audited-summary.json').read_text());assert [x['ordinal'] for x in baseline['reads']]==[x['ordinal'] for x in a['reads']]
 a['audit']={'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'audit_script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'native_final_queries':68,'exact_full_field_points':64,'baseline_point_metrics':baseline['point_metrics'],'hash_prefix_cohorts':{name:{'queries':sum(v==flag for v in scope),'caller_p95_ms':p95([r['wall_ms'] for r,v in zip(points,scope) if v==flag]),'engine_p95_ms':p95([native[r['statement_id']]['metrics'].get('execution_time_ms',0) for r,v in zip(points,scope) if v==flag]),'read_bytes_p95':p95([native[r['statement_id']]['metrics'].get('read_bytes',0) for r,v in zip(points,scope) if v==flag])} for name,flag in [('inside',True),('outside',False)]},'qualification':'Exact same64 stratified point cohort, full carrier/absence assertions and native final telemetry. Small hash-prefix subcohorts are descriptive, not tail guarantees or causal matched concurrent trials. Metadata/full digest scans warm data; caller/engine targets and billion admission remain separate.'};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['point_metrics']);print(a['audit']['hash_prefix_cohorts'])
if __name__=='__main__':main()
