"""Independent full edge-multiset bridge and exact six-role reference audit."""
import hashlib,json
from pathlib import Path
from mixed_change_queries_r230 import row_hash_sql
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_lc_full_vector_r376';a=json.loads((p/'summary.json').read_text());assert a['state']=='LC5 complete carrier equivalence and six-role reference vector pass';rs=list(map(json.loads,(p/'statements.jsonl').read_text().splitlines()));history=json.loads((p/'shared-history.json').read_text());native={q['query_id']:q for q in history['queries']};assert history['require_final'] and not history['missing_ids'] and len(rs)==len(native)==len({r['statement_id'] for r in rs});by={r['label']:r for r in rs}
 for r in rs:assert native[r['statement_id']]['is_final'] and native[r['statement_id']]['status']=='FINISHED' and native[r['statement_id']]['query_text']==r['sql'] and r['response']['status']['state']=='SUCCEEDED' and not r['response'].get('manifest',{}).get('truncated')
 src={}
 for key,path in a['sources'].items():assert hashlib.sha256((B/path).read_bytes()).hexdigest()==a['source_sha256'][key];src[key]=json.loads((B/path).read_text())
 lc=src['lc'];s=src['publication'];assert a['lc_edge']=={'table':lc['table'],'id':lc['uuid'],'version':5} and a['comparison_edge']==s['tables']['edge_current'];assert a['fields']==src['oracle']['roles']['edge_current']['fields'] and len(a['fields'])==20
 for name,h in lc['audit']['source_sha256'].items():assert hashlib.sha256((B/a['sources']['lc']).parent.joinpath(name).read_bytes()).hexdigest()==h
 groups=[]
 for first in range(0,400,100):
  r=by['range-groups-'+str(first)];lo=8000001+first*100000;hi=lo+10000000;E=a['comparison_edge'];q=f"SELECT CAST(floor((id-8000001)/100000) AS BIGINT) group_id,count(*),sha2(concat_ws('',sort_array(collect_list({row_hash_sql(a['fields'])}))),256) FROM {E['table']} VERSION AS OF {E['version']} WHERE id>={lo} AND id<{hi} GROUP BY group_id ORDER BY group_id";assert r['sql']=='/* ashlar '+p.name+' '+r['label']+' */ '+q and not native[r['statement_id']]['metrics'].get('result_from_cache');groups.extend(r['response']['result']['data_array'])
 assert groups==a['groups']==lc['checks']['after'] and [int(r[0]) for r in groups]==list(range(400)) and sum(int(r[1]) for r in groups)==39980000
 for role,t in a['role_pins'].items():
  r=by['detail-'+role];cols=[c['name'] for c in r['response']['manifest']['schema']['columns']];assert dict(zip(cols,r['response']['result']['data_array'][0]))['id']==t['id'];assert by['available-'+role]['response']['result']['data_array']==[['1']] and f"VERSION AS OF {t['version']} LIMIT 1" in by['available-'+role]['sql']
 assert all(t==s['tables'][role] for role,t in a['role_pins'].items() if role!='edge_current') and a['role_pins']['edge_current']==a['lc_edge'];old=a['original_descriptor'];values=a['descriptor_values'];assert json.loads(old[2])==s['publication_vector'] and json.loads(values[2])=={t['table']:t['version'] for t in a['role_pins'].values()} and values[3:5]==old[3:5];report=json.loads(values[5]);assert report['original_validation_report_json']==old[5] and report['full20field_equivalence_groups']==groups and report['production_writer_fence'] is False;assert by['descriptor-readback']['response']['result']['data_array']==[values]
 assert [int(c['version']) for c in a['manifest_history']]==[1,0] and {c['queryHistoryStatementId'] for c in a['manifest_history']}=={by['create-manifest']['statement_id'],by['reference-publication']['statement_id']}
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s'];a['audit']={'native_final_statements':len(rs),'complete_edge_carriers':39980000,'fields':20,'uncached_groups':400,'exact_role_pins':6,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
