"""Offline audit of native-final serialized manifest reference evidence."""
import json,hashlib
from pathlib import Path
from scale_mixed_r219 import Workload
from mixed_changes_r228 import Changes
from second_changes_r318 import SecondChanges
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_maintenance_manifest_r360';a=json.loads((p/'summary.json').read_text());rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(native)==len(rs)==len({r['statement_id'] for r in rs})
 for r in rs:
  q=native[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and q['query_text']=='/* ashlar '+p.name+' '+r['label']+' */ '+r['sql'] and r['response']['status']['state']=='SUCCEEDED' and not q['metrics'].get('result_from_cache')
 by={r['label']:r for r in rs};rows=lambda label:by[label]['response'].get('result',{}).get('data_array',[])
 parent,child,duplicate=a['receipts'];assert child['canonical_json']==duplicate['canonical_json'] and child['sha256']==duplicate['sha256'];d0,d1=parent['descriptor'],child['descriptor'];assert d0['progress']==d1['progress'] and d0['revisions']==d1['revisions'] and d0['vector']['object']==d1['vector']['object'] and d1['parent']==d0['id'];assert d0['vector']['edge']['version']==3 and d1['vector']['edge']['version']==5
 for receipt in a['receipts']:
  text=json.dumps(receipt['descriptor'],sort_keys=True,separators=(',',':'));assert text==receipt['canonical_json'] and hashlib.sha256(text.encode()).hexdigest()==receipt['sha256'];assert rows(receipt['label']+'-readback')==[[text,receipt['sha256']]]
 assert rows('manifest-count')==[['2','2']] and a['conflict_refusal']=='conflicting publication ID';assert not any(r['label'].startswith('conflict-') and r['label']!='conflict-preflight' for r in rs);assert rows('conflict-preflight')==[[child['canonical_json'],child['sha256']]]
 w=Workload(8000000,40000000);first=Changes(8000000,40000000,100000);second=SecondChanges();inverse=pow(104729,-1,40000000);assert len(a['reads'])==16
 for read in a['reads']:
  row=w.carrier('edge',read['ordinal']);n=read['ordinal']*inverse%40000000;after=second.change(n-100000)['after'] if 100000<=n<200000 else first.change(n)['after'] if n<100000 else row;expected=[] if after is None else [[None if after[f] is None else after[f].replace('T',' ').removesuffix('Z') if f=='published_at' else after[f] for f in row]];r=by[read['label']];assert rows(read['label'])==expected and r['statement_id']==read['statement_id'] and r['parameters']=={'hash':row['lookup_hash'],'source':row['source_system'],'type':row['rel_type_id'],'id':row['id']};assert 'VERSION AS OF '+str(read['version'])+' WHERE lookup_hash=:hash AND source_system=:source' in r['sql']
 assert {x['ordinal'] for x in a['reads'] if x['version']==3}=={x['ordinal'] for x in a['reads'] if x['version']==5} and len({x['ordinal'] for x in a['reads']})==8
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s']
 source=B/'out/native/ashlar_lc_prefix_maintain_r350/audited-summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_sha256']==d1['preservation_receipt_sha256'];receipt=json.loads(source.read_text());assert d1['maintenance_statement_id']==receipt['optimize_statement_id']
 a['audit']={'native_final_statements':len(rs),'exact_points':16,'serialized_manifest_rows':2,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
