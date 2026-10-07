"""Offline native-final cache/transport correctness audit; no new workload."""
import json,hashlib,math
from pathlib import Path
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
B=Path(__file__).resolve().parent

def audit(name,transport):
 p=B/'out/native'/name;a=json.loads((p/'summary.json').read_text());records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};assert len(records)==len(native)==len({r['statement_id'] for r in records}) and h['require_final'] and not h['missing_ids']
 for r in records:
  q=native[r['statement_id']];assert q['status']=='FINISHED' and q['is_final'];assert q['query_text']==('/* ashlar '+p.name+' '+r['label']+' */ '+r['sql'] if transport=='driver' else r['sql']);assert r['response']['status']['state']=='SUCCEEDED' and not r['response']['manifest'].get('truncated')
 points=[r for r in records if r['label'].startswith('point-')];expected_count=32 if transport=='driver' else 16;assert len(points)==len(a['reads'])==expected_count
 w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();inverse=pow(104729,-1,40000000)
 for r,read in zip(points,a['reads']):
  row=w.carrier('edge',read['ordinal']);n=read['ordinal']*inverse%40000000;after=second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in row]];assert r['response'].get('result',{}).get('data_array',[])==expected and r['statement_id']==read['statement_id'];params={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};assert r['parameters']==(params if transport=='driver' else [{'name':k,'value':v,'type':'STRING'} for k,v in params.items()]);assert f"VERSION AS OF {a['table']['version']}" in r['sql'] and 'lookup_hash=:hash AND source_system=:source' in r['sql']
 assert len({r['ordinal'] for r in a['reads']})==8
 for phase in range(4 if transport=='driver' else 2):assert len([r for r in a['reads'] if r['phase']==phase])==8
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s']
 if transport=='driver':assert all(not native[r['statement_id']]['metrics'].get('result_from_cache') for r in a['reads'] if r['phase']==3)
 p95=lambda xs:sorted(xs)[math.ceil(.95*len(xs))-1]
 by_id={r['statement_id']:r for r in records}
 def cohort(reads):return {'queries':len(reads),'cache_hits':sum(bool(native[r['statement_id']]['metrics'].get('result_from_cache')) for r in reads),'caller_p95_ms':p95([by_id[r['statement_id']]['wall_ms'] for r in reads]),'engine_p95_ms':p95([native[r['statement_id']]['metrics'].get('execution_time_ms',0) for r in reads]),'compile_p95_ms':p95([native[r['statement_id']]['metrics'].get('compilation_time_ms',0) for r in reads])} if reads else None
 assert a['per_phase']=={str(p):cohort([r for r in a['reads'] if r['phase']==p]) for p in range(4 if transport=='driver' else 2)}
 assert a['by_hit']=={str(flag):cohort([r for r in a['reads'] if bool(native[r['statement_id']]['metrics'].get('result_from_cache'))==flag]) for flag in [True,False]}
 for phase in range(4 if transport=='driver' else 2):assert [r['ordinal'] for r in a['reads'] if r['phase']==phase]==[r['ordinal'] for r in a['reads'][:8]]
 a['audit']={'final_statements':len(records),'exact_full_field_points':expected_count,'cache_hits':sum(bool(native[r['statement_id']]['metrics'].get('result_from_cache')) for r in a['reads']),'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');return a
if __name__=='__main__':
 print(audit('ashlar_result_cache_r354','driver')['audit'])
 print(audit('ashlar_statement_cache_r356','statement')['audit'])
