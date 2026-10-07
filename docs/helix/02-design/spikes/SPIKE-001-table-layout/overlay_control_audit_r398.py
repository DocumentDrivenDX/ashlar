"""Independent audit of native winner controls and repaired conflict fixtures."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_overlay_controls_r396';s=json.loads((p/'summary.json').read_text());r=B/'out/native/ashlar_overlay_conflicts_r397';a=json.loads((r/'summary.json').read_text());assert s['state']=='Stopped; same native handles only; no blind replay' and a['state']=='Both true accepted-carrier conflicts native-fail with unchanged pinned tables'
 original=dict(s);original.pop('costs');original.pop('terminal_observation');raw=(json.dumps(original,indent=2)+'\n').encode();assert hashlib.sha256(raw).hexdigest()==a['source_sha256']['summary.json'];(p/'summary-as-used-r397.json').write_bytes(raw);assert hashlib.sha256((p/'statements.jsonl').read_bytes()).hexdigest()==a['source_sha256']['statements.jsonl']
 combined={};total={k:0 for k in ['read_bytes','write_remote_bytes','spill_to_disk_bytes']}
 for folder,n in [(p,16),(r,8)]:
  records=[json.loads(x) for x in (folder/'statements.jsonl').read_text().splitlines()];h=json.loads((folder/'shared-history.json').read_text());native={q['query_id']:q for q in h['queries']};assert h['require_final'] and not h['missing_ids'];failed=json.loads((folder/'failed-native-final.json').read_text()) if (folder/'failed-native-final.json').exists() else {};assert not set(native)&set(failed);native.update(failed);assert len(native)==len(records)==n
  for rec in records:
   q=native[rec['statement_id']];assert q['is_final'] and q['query_text']==rec['sql'] and q['status']==('FAILED' if rec['response']['status']['state']=='FAILED' else 'FINISHED')
   if rec['response']['status']['state']=='FAILED':assert folder==r and rec['label'] in ['conflicting-live','conflicting-deleted'] and 'ASHLAR_OVERLAY_VERSION_CONFLICT' in q['error_message']
  costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in total};assert costs==json.loads((folder/'summary.json').read_text())['costs']
  for k in total:total[k]+=costs[k]
  by={x['label']:x for x in records};rows=lambda label:by[label]['response'].get('result',{}).get('data_array',[])
  def objs(label):return [dict(zip([x['name'] for x in by[label]['response']['manifest']['schema']['columns']],row)) for row in rows(label)]
  if folder==p:
   assert set(s['checks'])=={'updated','deleted','unchanged','hash-collision-other-type','missing-key'}
   for label,e in s['checks'].items():assert rows(label)==e['expected']
   assert rows('conflicting-live')==s['checks']['updated']['expected']
   for role,t in s['tables'].items():assert objs('detail-'+role)[0]['id']==t['id'] and objs('history-'+role)==t['history'] and t['version']==0 and t['history'][0]['queryHistoryStatementId']==t['create_statement_id']
  else:
   assert a['tables']==s['tables'] and len(failed)==2
   for role,t in a['tables'].items():assert objs('initial-history-'+role)==objs('final-history-'+role)==t['history'] and objs('initial-detail-'+role)[0]['id']==t['id']
  if (folder/'live-statement.json').exists():(folder/'live-statement.json').rename(folder/'completed-last-request.json')
  combined[folder.name]={'statements':n,'costs':costs,'source_sha256':{path.name:hashlib.sha256(path.read_bytes()).hexdigest() for path in folder.iterdir() if path.is_file() and path.name!='audited-summary.json'}}
 result={'state':'Native overlay winner and true conflict control audit passes','native_successful_statements':22,'native_failed_expected_statements':2,'costs':total,'evidence':combined,'qualification':'Four-row base/four-row overlay. Correct live/delete/replay/stale/unchanged/missing/full-typed-key behavior and read-only same-version rejection at unchanged native pins. Prior misnamed field generated an exact replay, not a failed guard. No publisher conflict admission, source semantics, general signed-key support, latency, compaction, full100k overlay or billion-scale evidence.'}
 (B/'out/overlay-controls-audit-r398.json').write_text(json.dumps(result,indent=2)+'\n');print(json.dumps({k:v for k,v in result.items() if k!='evidence'},indent=2))
if __name__=='__main__':main()
