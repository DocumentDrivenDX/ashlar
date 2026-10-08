"""Bind sixth source geometry and future concurrent checks; no native execution."""
import ast,hashlib,json
from pathlib import Path
from publisher_validation_r507 import plan
B=Path(__file__).resolve().parent

def main():
 paths={'oracle':'out/seventh-batch-oracle-r589.json','files':'out/ashlar_seventh_batch_files_r586/audited-summary.json','cdf':'out/seventh-cdf-image-oracle-r588.json','tomb':'out/seventh-canonical-tomb-r588.json','parent':'out/native/ashlar_sixth_guard_publish_r537/audited-summary.json','parent_budget':'out/sixth-publisher-budget-r535.json'};s={k:json.loads((B/p).read_text()) for k,p in paths.items()};o=s['oracle'];f=s['files'];p=s['parent'];assert p['state']=='Integrated private sixth100k guarded publication passes full change custody'
 assert o['original_edges']==40000000 and o['nodes']==8000000 and o['source_position_interval']==[600000,700000] and o['before_live_edges']==39940000 and o['after_live_edges']==39930000 and o['selected_identities']==100000
 assert o['counts']=={'delete':10000,'deletes':10000,'remove':26667,'set':180000,'updates':90000} and len(o['negative_controls'])==5
 assert o['roles']['property_journal']['rows']==216667 and f['parts']==35 and f['processed_changes']==100000 and f['source_bytes']<=1200000000 and f['oracle_sha256']==hashlib.sha256((B/paths['oracle']).read_bytes()).hexdigest()
 for n,v in o['source_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
 for n,v in f['source_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==v
 for role,r in f['roles'].items():
  assert f['audit']['roles'][role]['all_known_field_digest']==o['roles'][role]['all_field_multiset_digest'] and r['rows']==o['roles'][role]['rows']
  for part in r['parts']:assert hashlib.sha256((Path(f['local_root'])/part['name']).read_bytes()).hexdigest()==part['file_sha256']
 assert len(s['cdf']['roles']['edge_current']['fields'])==20 and len(s['cdf']['roles']['adjacency_forward']['fields'])==8 and all(x['total_rows']==190000 for x in s['cdf']['roles'].values()) and len(s['tomb']['fields'])==10
 counts=dict(s['parent_budget']['expected_final_rows']);counts.update(edge_current=39930000,adjacency_forward=39930000,source_record=48700000,property_journal=193516667,tombstone=70000)
 proposed={role:{**t,'version':t['version']+(role!='object_current')} for role,t in p['tables'].items()};inputs={role:{'table':'client_dev.ashlar_entropy_20261006_r86.normalized_'+role+'_r593','version':0} for role in f['roles']};checks={role:{'rows':r['rows'],'fields':r['fields'],'all_known_field_digest':r['all_known_field_digest']} for role,r in f['roles'].items()};source={'cdf':s['cdf'],'tomb':s['tomb'],'inputs':{'checks':checks},'budget':{'expected_final_rows':counts},'revision_rows':[['synthetic-scale-mixed','synthetic-mixed/1']]};planned=plan(proposed,inputs,source);assert len(planned)==15 and len({x.label for x in planned})==15
 publisher=B/'seventh_guard_publish_r596.py';tree=ast.parse(publisher.read_text());calls=[n for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)];assert sum(n.func.id=='validate' for n in calls)==1 and sum(n.func.id=='guard_merge' for n in calls)==1
 assert 'ashlar_sixth_guard_publish_r537/audited-summary.json' in publisher.read_text() and 'publisher_validation_r507.py' in publisher.read_text()
 result={'state':'Seventh100k local byte/field/source geometry and proposed15 concurrent validation checks bind; native stages unexecuted','sources':paths,'source_sha256':{k:hashlib.sha256((B/path).read_bytes()).hexdigest() for k,path in paths.items()},'accepted_files_sha256':hashlib.sha256((B/paths['files']).read_bytes()).hexdigest(),'accepted_oracle_sha256':hashlib.sha256((B/paths['oracle']).read_bytes()).hexdigest(),'source_bytes':f['source_bytes'],'parts':f['parts'],'expected_final_rows':counts,'proposed_publication_vector':proposed,'planned_checks':[{'label':x.label,'sql':x.sql,'expected':x.expected} for x in planned],'publisher_sha256':hashlib.sha256(publisher.read_bytes()).hexdigest(),'qualification':'Offline future query/expectation binding and syntax/entrypoint wiring only; not executed SQL, native preflight, new input UUIDs, actual role commit, manifest or freshness evidence. Parent table versions are historical qualified pins; actual mutable heads must be refreshed. Generation/transfer/staging costs remain separate; no cloning or real source ACK.'};(B/'out/seventh-local-admission-r591.json').write_text(json.dumps(result,indent=2)+'\n');print(result['state'])
if __name__=='__main__':main()
