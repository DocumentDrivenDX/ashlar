"""Offline full native mixed-publication receipt audit; preserves original evidence."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_mixed_publish_r281';target=p/'audited-summary.json';assert not target.exists();a=json.loads((p/'summary.json').read_text());assert a['state']=='Complete private100k mixed publication passes full intermediate preservation audit'
 records=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];h=json.loads((p/'shared-history.json').read_text());history={x['query_id']:x for x in h['queries']};assert h['require_final'] and not h['missing_ids'] and len(history)==len(records)
 bylabel={r['label']:r for r in records};bysid={r['statement_id']:r for r in records};assert len(bysid)==len(records)
 for r in records:
  q=history[r['statement_id']];assert r['response']['status']['state']=='SUCCEEDED' and q['status']=='FINISHED' and q['is_final'] ;assert not r['response'].get('manifest',{}).get('truncated')
 native_query_text_truncations=[]
 for r in records:
  text=history[r['statement_id']]['query_text']
  if text!=r['sql']:
   assert r['label']=='publish' and text.endswith('...') and text[:-3]==r['sql'][:len(text)-3]
   native_query_text_truncations.append({'label':r['label'],'statement_id':r['statement_id'],'native_chars':len(text),'submitted_chars':len(r['sql']),'submitted_sha256':hashlib.sha256(r['sql'].encode()).hexdigest(),'qualification':'Native history text truncates; client full SQL plus same SID terminal status and exact descriptor readback qualify contents.'})
 costs={k:sum(x['metrics'].get(k,0) for x in history.values()) for k in a['costs']};assert costs==a['costs'];assert all(v<=a['bounds'][k] for k,v in costs.items());assert a['wall_s']<a['bounds']['wall_s']
 base=json.loads((B/'out/native/ashlar_scale_edges_r274/audited-summary.json').read_text());inputs=json.loads((B/'out/native/ashlar_normalized_delta_stage_r275/audited-summary.json').read_text());changed=json.loads((B/'out/mixed-changed-oracle-r277.json').read_text());assert a['schema_revisions']=={'synthetic-scale-mixed':'synthetic-mixed/1'}
 def result(label):
  matched=[r for r in records if r['label']==label];assert len(matched)==1,label
  return matched[0]['response'].get('result',{}).get('data_array',[])
 for role in ['edge_current','adjacency_forward']:
  reference=sum((result('reference-'+role+'-'+str(i)) for i in [0,100,200,300]),[]);actual=sum((result('unchanged-'+role+'-'+str(i)) for i in [0,100,200,300]),[])
  assert len(reference)==400 and sum(int(r[1]) for r in reference)==39900000 and reference==actual and actual==a['checks']['unchanged-'+role]['groups']
  assert result('digest-'+role)==[['90000',changed['roles'][role]['digest']]]
 for role in ['source_record','property_journal']:
  actual=sum((result('bootstrap-'+role+'-'+str(i)) for i in [0,100,200,300,400]),[]);assert actual==base['checks']['complete-'+role]['groups'] and actual==a['checks']['bootstrap-'+role]['groups']
 for role in ['source_record','property_journal','tombstone']:
  expected=inputs['checks'][role];assert result('digest-added-'+role)==[[str(expected['rows']),expected['all_known_field_digest']]]
 totals={'object_current':8000000,'edge_current':39990000,'adjacency_forward':39990000,'source_record':48100000,'property_journal':192216667,'tombstone':10000}
 for role,n in totals.items():assert result('final-count-'+role)==[[str(n)]] and a['active_details'][role]['id']==a['tables'][role]['id']
 assert result('unique-edges')==[['39990000','39990000']] and result('deleted-absent')==[['0']] and result('typed-endpoints')==[['39990000','0']]
 mutations=[x for x in a['commit_events'] if bysid[x['statement_id']]['label'].startswith(('append-','merge-'))];assert len(mutations)==5
 for e in a['commit_events']:
  assert e['history']['queryHistoryStatementId']==e['statement_id'] and int(e['history']['version'])==e['version'];assert bysid[e['statement_id']]['response']['status']['state']=='SUCCEEDED'
 for e in mutations:
  m=json.loads(e['history']['operationMetrics']);role=e['role']
  if role in ['edge_current','adjacency_forward']:assert m['numTargetRowsUpdated']=='90000' and m['numTargetRowsDeleted']=='10000' and m['numTargetRowsInserted']=='0'
  else:assert int(m['numOutputRows'])==inputs['checks'][role]['rows']
  assert e['version']==a['tables'][role]['version']
 descriptor=result('descriptor-readback');assert len(descriptor)==1;d=descriptor[0];assert d[0]=='mixed-r281' and d[1]=='ashlar-delta/0.3-synthetic-mixed';assert json.loads(d[2])==a['publication_vector']=={t['table']:t['version'] for t in a['tables'].values()};assert json.loads(d[4])==a['schema_revisions'] and json.loads(d[5])==a['checks'];progress=json.loads(d[3]);assert progress['inputs']==inputs['tables'] and progress['members']==100000 and progress['real_source_ack'] is False
 assert a['publish_statement_id']==bylabel['publish']['statement_id']
 validation=[r for r in records if r['label'].startswith(('unchanged-','bootstrap-','digest-','final-count-')) or r['label'] in ['unique-edges','deleted-absent','typed-endpoints','descriptor-readback','schema-revisions']]
 assert all(not history[r['statement_id']]['metrics'].get('result_from_cache') for r in validation)
 a['audit']={'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'audit_script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'final_statements':len(records),'native_query_text_truncations':native_query_text_truncations,'validation_queries_uncached':len(validation),'mutation_receipts':mutations,'active_bytes':sum(int(x['sizeInBytes']) for x in a['active_details'].values()),'active_files':sum(int(x['numFiles']) for x in a['active_details'].values()),'mutation_telemetry':{r['label']:{'caller_ms':r['wall_ms'],'metrics':history[r['statement_id']]['metrics']} for r in records if r['label'].startswith(('append-','merge-'))},'qualification':'Full native input/baseline/change preservation and exact descriptor audited under SHA256 collision-resistance assumption; scope counts partition all final rows. Active shallow-clone metadata is not physical retained inventory. Mutation-only time is not freshness. No real source authority/fencing/ACK, concurrency/cold/caller or billion admission.'};target.write_text(json.dumps(a,indent=2)+'\n');print(json.dumps({k:a[k] for k in ['state','mutation_s','processing_s','costs','wall_s']},indent=2))
if __name__=='__main__':main()
