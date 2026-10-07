"""Independent saved-evidence generation/rollback/handover audit."""
import hashlib,json
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_authority_generation_r371';a=json.loads((p/'summary.json').read_text());assert a['state']=='Two invalid generations roll back; one concurrent handover wins';rs=json.loads((p/'all-statements.json').read_text());native={q['query_id']:q for q in json.loads((p/'query-history.json').read_text())};assert len(rs)==len(native)==len({r['statement_id'] for r in rs});lanes={w['statement_id']:w['lane'] for w in a['contenders']};by={r['label']:r for r in rs if r['label']!='takeover'}
 for r in rs:
  q=native[r['statement_id']];failed=r['response']['status']['state']=='FAILED';assert q['is_final'] and q['status']==('FAILED' if failed else 'FINISHED') and q['query_text']=='/* ashlar '+lanes.get(r['statement_id'],p.name)+' '+r['label']+' */ '+r['sql']
 source=B/'out/native/ashlar_full_vector_authority_r369/audited-summary.json';assert hashlib.sha256(source.read_bytes()).hexdigest()==a['source_sha256'];parent=json.loads(source.read_text());assert a['baseline']==parent['final_rows'];assert [int(c['version']) for c in a['before_history']]==[0]
 for control in a['controls']:
  r=by[control['label']];assert r['statement_id']==control['statement_id'] and r['parameters']['next_token']==('2' if control['label']=='regressing' else '5');assert r['response']['status']['state']=='FAILED' and 'ASHLAR_AUTHORITY_GUARD' in r['response']['status']['error']['message'];assert 's.token=t.token+1' in r['sql'] and 's.token>t.token' in r['sql'];assert by[control['label']+'-readback']['response']['result']['data_array']==a['baseline']
 winner=next(w for w in a['contenders'] if w['state']=='SUCCEEDED');loser=next(w for w in a['contenders'] if w['state']=='FAILED');assert loser['error'] and a['final_rows'][0][:4]==['authority','authority',winner['parameters']['owner'],'4'] and a['final_rows'][1:]==a['baseline'][1:]
 for w in a['contenders']:
  r=next(r for r in rs if r['statement_id']==w['statement_id']);assert r['parameters']==w['parameters'] and r['parameters']['expected_token']=='3' and r['parameters']['next_token']=='4' and r['response']['status']['state']==w['state'] and 's.token=t.token+1' in r['sql']
 assert [int(c['version']) for c in a['final_history']]==[1,0];assert {c['queryHistoryStatementId'] for c in a['final_history']}=={by['clone']['statement_id'],winner['statement_id']};q0,q1=[native[w['statement_id']] for w in a['contenders']];assert a['native_statement_overlap_ms']==max(0,min(q0['execution_end_time_ms'],q1['execution_end_time_ms'])-max(q0['query_start_time_ms'],q1['query_start_time_ms']))
 costs={k:sum(q['metrics'].get(k,0) for q in native.values()) for k in a['costs']};assert costs==a['costs'] and all(v<=a['bounds'][k] for k,v in costs.items()) and a['wall_s']<a['bounds']['wall_s'];failed=sum(q['status']=='FAILED' for q in native.values());assert failed==3
 a['audit']={'successful_native_final':len(rs)-failed,'failed_native_final':failed,'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','all-statements.json','query-history.json']},'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['audit'])
if __name__=='__main__':main()
