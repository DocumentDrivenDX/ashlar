"""Independent terminal/custody/result audit of the integrated private publisher."""
import hashlib,json
from pathlib import Path
from inline_guard_sql_r421 import guard_merge
from publisher_validation_r507 import plan
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_seventh_guard_publish_r596';a=json.loads((p/'summary.json').read_text());assert a['state']=='Integrated private seventh100k guarded publication passes full change custody'
 source={k:json.loads((B/path).read_text()) for k,path in a['sources'].items()}
 for k,path in a['sources'].items():assert hashlib.sha256((B/path).read_bytes()).hexdigest()==a['source_sha256'][k]
 for n,hsh in a['code_sha256'].items():assert hashlib.sha256((B/n).read_bytes()).hexdigest()==hsh
 assert source['guard']['state']=='Inline full20field guards,26atomic refusals, clean CDF and all stopped costs independently audited' and source['guard']['controls']==26
 assert source['untouched']['state']=='Guard preserves exact unmatched target and old pin;5updates/1delete pass'
 main_rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];worker_rs=[dict(json.loads(x),_worker=q.parent.name) for q in p.glob('metadata-worker-*/statements.jsonl') for x in q.read_text().splitlines()];rs=main_rs+worker_rs;h=json.loads((p/'shared-history.json').read_text());qs={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(rs)==len(qs)==len({r['statement_id'] for r in rs});by={r['label']:r for r in main_rs};assert len(by)==len(main_rs)
 def rows(label):return by[label]['response'].get('result',{}).get('data_array',[])
 def objects(label):return [dict(zip([x['name'] for x in by[label]['response']['manifest']['schema']['columns']],r)) for r in rows(label)]
 for r in rs:
  q=qs[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and q['query_text']==(('/* ashlar '+r['_worker']+' '+r['label']+' */ ') if '_worker' in r else '')+r['sql'] and r['response']['status']['state']=='SUCCEEDED' and not r['response'].get('manifest',{}).get('truncated')
 assert len(worker_rs)==109 and len(a['metadata']['accepted'])==10
 accepted={x['key']:x for x in a['metadata']['accepted']}
 for prefix,ts in [('base',a['base']),('input',a['inputs'])]:
  for role,t in ts.items():
   key=prefix+'-'+role;x=accepted[key];e=source['eligibility']['tables'][key];assert x['table']==t['table'] and x['detail']['id']==t['id'] and x['detail']['format']=='delta' and x['schema']==e['schema'];assert int(x['head']['version'])==int(e['head']['version']) and x['head']['queryHistoryStatementId']==e['head']['queryHistoryStatementId']
   for field in ['properties','partitionColumns','clusteringColumns','tableFeatures']:assert json.loads(x['detail'][field])==json.loads(e['detail'][field])
   for field in ['minReaderVersion','minWriterVersion']:assert str(x['detail'][field])==str(e['detail'][field])
   for kind,part in [('detail','detail'),('head','head'),('schema','schema')]:
    matches=[r for r in worker_rs if r['label']==kind+'-'+key];assert len(matches)==1;record=matches[0];cols=[c['name'] for c in record['response']['manifest']['schema']['columns']];values=[dict(zip(cols,v)) for v in record['response']['result']['data_array']];assert values==(x['schema'] if part=='schema' else [x[part]])
 assert rows('pinned-node-count')==[['8000000']]
 for role,e in source['inputs']['checks'].items():
  if role=='cohesion':continue
  assert rows('input-digest-'+role)==[[str(e['rows']),e['original_input_digest'],e['all_known_field_digest']]]
 assert 'all-predecessor-fields' not in by and rows('mutation-partition')==[['100000','100000','10000','90000','0','0']]
 raw=a['inputs']['source_record'];current=a['inputs']['current_replacement'];assert by['merge-current']['sql']==guard_merge(a['base']['edge_current']['table'],raw['table'],raw['version'],current['table'],current['version'])
 events={e['role']:e for e in a['commit_events']};assert len(events)==5
 for role,t in a['tables'].items():
  if role=='object_current':assert t==a['base'][role];continue
  assert t['id']==a['base'][role]['id'] and t['version']==a['base'][role]['version']+1
  label='merge-current' if role=='edge_current' else 'merge-forward' if role=='adjacency_forward' else 'append-'+role
  assert events[role]['statement_id']==by[label]['statement_id']==events[role]['history']['queryHistoryStatementId']
  assert rows('prewrite-head-'+role)[0][0]==str(a['base'][role]['version'])
  for label in ['custody-before-publication','custody-after-readback']:
   hist=objects(label+'-'+role);interval=[x for x in hist if int(x['version'])>a['base'][role]['version']];assert interval==[events[role]['history']] and int(hist[0]['version'])==t['version']
 source['revision_rows']=[['synthetic-scale-mixed','synthetic-mixed/1']]
 expected={c.label:c for c in plan(a['tables'],a['inputs'],source)};assert len(expected)==15 and set(a['accepted_validation'])==set(expected)
 for label,e in expected.items():
  matches=[r for r in worker_rs if r['label']==label];assert len(matches)==1;r=matches[0];v=a['accepted_validation'][label]
  assert r['sql']==e.sql and r['statement_id']==v['statement_id'] and tuple(tuple(x) for x in r['response']['result']['data_array'])==e.expected and v['result']==r['response']['result']['data_array']
  assert not qs[r['statement_id']]['metrics'].get('result_from_cache')
 assert [c['offset'] for c in a['cohorts']]==[0,4,8,12]
 assert set(a['checks']['post_commit'])==set(expected)
 for label in ['profiles-before-publication','profiles-after-readback']:
  cohort=a['closing_metadata'][label];assert len(cohort)==10 and len(a['checks'][label])==10
  closing={x['key'].removeprefix(label+'-'):x for x in cohort};assert set(closing)==set(source['eligibility']['tables'])
  for key,entry in source['eligibility']['tables'].items():
   x=closing[key];assert x['table']==entry['pin']['table'] and x['schema']==entry['schema']
   d=x['detail'];h=x['head'];check=a['checks'][label][key]
   assert d['id']==entry['detail']['id']==check['id'] and h==check['head'] and check['selected_pin']==x['table'] and check['physical_profile_checked'] is True and check['schema_checked'] is True
   for f in ['properties','partitionColumns','clusteringColumns']:assert json.loads(d[f])==json.loads(entry['detail'][f])
   for f in ['minReaderVersion','minWriterVersion']:assert str(d[f])==str(entry['detail'][f])
   assert set(json.loads(d['tableFeatures']))==set(json.loads(entry['detail']['tableFeatures']))
   if key.startswith('base-') and key!='base-object_current':
    role=key.removeprefix('base-');assert h['queryHistoryStatementId']==events[role]['statement_id'] and int(h['version'])==a['tables'][role]['version']
   else:
    assert h==accepted[key]['head']
    assert int(h['version'])==int(entry['head']['version']) and h['queryHistoryStatementId']==entry['head']['queryHistoryStatementId']
   for kind in ['detail','head','schema']:
    matches=[r for r in worker_rs if r['label']==kind+'-'+label+'-'+key];assert len(matches)==1
    rec=matches[0];cols=[c['name'] for c in rec['response']['manifest']['schema']['columns']];native=[dict(zip(cols,v)) for v in rec['response']['result']['data_array']];assert native==(x[kind] if kind=='schema' else [x[kind]])
 assert rows('descriptor-readback')==[a['descriptor_values']];assert json.loads(a['descriptor_values'][2])==a['publication_vector']=={t['table']:t['version'] for t in a['tables'].values()};assert json.loads(a['descriptor_values'][3])['real_source_ack'] is False
 mh=objects('manifest-history');assert [int(x['version']) for x in mh]==[1,0] and [x['queryHistoryStatementId'] for x in mh]==[by['publish']['statement_id'],by['create-manifest']['statement_id']]
 costs={k:sum(q['metrics'].get(k,0) for q in qs.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items());assert a['processing_s']<a['bounds']['wall_s'];assert min(r['start_epoch'] for r in rs)>=a['processing_start_epoch'] and max(r['start_epoch']+r['wall_ms']/1000 for r in rs)<=a['processing_start_epoch']+a['processing_s']+1
 cached=[r['label'] for r in rs if qs[r['statement_id']]['metrics'].get('result_from_cache')]
 assert not any(x in cached for x in ['mutation-partition'])
 a['audit']={'history_transport_qualification':'Closing unchanged heads equal initial SQL-connector heads exactly; SDK heads bind independently by exact version and queryHistoryStatementId. Changed heads bind exact submitted commit version/SID; every complete closing head independently equals its native SQL response. Full maps are retained; SDK/connector timestamp/boolean/nested-map serialization differs, so byte equality across those transports is not asserted. Strict R600 failure is preserved; no query or mutation replay.','audit_code_sha256':hashlib.sha256((B/'seventh_guard_publish_audit_r601.py').read_bytes()).hexdigest(),'native_final_statements':len(rs),'cached_labels':cached,'costs':costs,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification'],'validation_with_telemetry_s':a['validation_with_telemetry_s'],'freshness_gate':'A single synthetic ready-input clock is not p95/source-arrival throughput admission; add source accumulation and all prior prep separately.'}
 # Caller retires live markers only after terminal complete telemetry.
 (p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps(a['audit'],indent=2))
if __name__=='__main__':main()
