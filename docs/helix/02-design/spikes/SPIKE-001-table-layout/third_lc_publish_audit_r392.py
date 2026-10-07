"""Independent terminal/custody/result audit of the integrated private publisher."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_third_lc_publish_r391';a=json.loads((p/'summary.json').read_text());assert a['state']=='Integrated private third100k LC publication passes full change custody'
 source={k:json.loads((B/path).read_text()) for k,path in a['sources'].items()}
 for k,path in a['sources'].items():assert hashlib.sha256((B/path).read_bytes()).hexdigest()==a['source_sha256'][k]
 rs=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());qs={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(rs)==len(qs)==len({r['statement_id'] for r in rs});by={r['label']:r for r in rs};assert len(by)==len(rs)
 def rows(label):return by[label]['response'].get('result',{}).get('data_array',[])
 def objects(label):return [dict(zip([x['name'] for x in by[label]['response']['manifest']['schema']['columns']],r)) for r in rows(label)]
 for r in rs:
  q=qs[r['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and q['query_text']==r['sql'] and r['response']['status']['state']=='SUCCEEDED' and not r['response'].get('manifest',{}).get('truncated')
 for prefix,ts in [('base',a['base']),('input',a['inputs'])]:
  for role,t in ts.items():
   key=prefix+'-'+role;assert objects('detail-'+key)[0]['id']==t['id'];e=source['eligibility']['tables'][key];assert objects('head-'+key)==[e['head']] and objects('schema-'+key)==e['schema']
 assert rows('pinned-node-count')==[['8000000']]
 for role,e in source['inputs']['checks'].items():
  if role=='cohesion':continue
  assert rows('input-digest-'+role)==[[str(e['rows']),e['original_input_digest'],e['all_known_field_digest']]]
 assert rows('all-predecessor-fields')==[['100000','0']] and rows('mutation-partition')==[['100000','100000','10000','90000','0','0']]
 events={e['role']:e for e in a['commit_events']};assert len(events)==5
 for role,t in a['tables'].items():
  if role=='object_current':assert t==a['base'][role];continue
  assert t['id']==a['base'][role]['id'] and t['version']==a['base'][role]['version']+1
  label='merge-current' if role=='edge_current' else 'merge-forward' if role=='adjacency_forward' else 'append-'+role
  assert events[role]['statement_id']==by[label]['statement_id']==events[role]['history']['queryHistoryStatementId']
  assert rows('prewrite-head-'+role)[0][0]==str(a['base'][role]['version'])
  for label in ['custody-before-publication','custody-after-readback']:
   hist=objects(label+'-'+role);interval=[x for x in hist if int(x['version'])>a['base'][role]['version']];assert interval==[events[role]['history']] and int(hist[0]['version'])==t['version']
 for role in ['edge_current','adjacency_forward']:
  e=source['cdf']['roles'][role];v=a['tables'][role]['version'];want=[[kind,str(x['rows']),str(v),str(v),x['digest']] for kind,x in sorted(e['images'].items())];assert rows('cdf-'+role)==want
 for role in ['source_record','property_journal']:
  e=source['inputs']['checks'][role];v=a['tables'][role]['version'];assert rows('cdf-'+role)==[['insert',str(e['rows']),str(v),str(v),e['all_known_field_digest']]]
 assert rows('canonical-tombstone')==[['10000',source['tomb']['digest']]]
 for role,n in source['budget']['expected_final_rows'].items():assert rows('final-count-'+role)==[[str(n)]]
 assert rows('unique-edges')==[['39970000','39970000']] and rows('deleted-absent')==[['0']] and rows('typed-endpoints')==[['39970000','0']] and rows('schema-revisions')==[['synthetic-scale-mixed','synthetic-mixed/1']]
 assert rows('descriptor-readback')==[a['descriptor_values']];assert json.loads(a['descriptor_values'][2])==a['publication_vector']=={t['table']:t['version'] for t in a['tables'].values()};assert json.loads(a['descriptor_values'][3])['real_source_ack'] is False
 mh=objects('manifest-history');assert [int(x['version']) for x in mh]==[1,0] and [x['queryHistoryStatementId'] for x in mh]==[by['publish']['statement_id'],by['create-manifest']['statement_id']]
 costs={k:sum(q['metrics'].get(k,0) for q in qs.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items());assert a['processing_s']<a['bounds']['wall_s'];assert rs[0]['start_epoch']>=a['processing_start_epoch'] and rs[-1]['start_epoch']+rs[-1]['wall_ms']/1000<=a['processing_start_epoch']+a['processing_s']+1
 cached=[r['label'] for r in rs if qs[r['statement_id']]['metrics'].get('result_from_cache')]
 assert not any(x in cached for x in ['all-predecessor-fields','mutation-partition','unique-edges','deleted-absent','typed-endpoints','canonical-tombstone']+[ 'cdf-'+role for role in events])
 a['audit']={'native_final_statements':len(rs),'cached_labels':cached,'costs':costs,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'qualification':a['qualification'],'freshness_gate':'A single synthetic ready-input clock is not p95/source-arrival throughput admission; add source accumulation and all prior prep separately.'}
 if (p/'live-statement.json').exists():(p/'live-statement.json').rename(p/'completed-last-request.json')
 (p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(json.dumps(a['audit'],indent=2))
if __name__=='__main__':main()
