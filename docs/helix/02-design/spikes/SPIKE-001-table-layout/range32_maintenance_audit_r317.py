"""Offline all32partition maintenance receipts, full carriers and global integrity."""
import json,hashlib
from pathlib import Path
B=Path(__file__).resolve().parent

def main():
 p=B/'out/native/ashlar_range32_maintain_r316';a=json.loads((p/'summary.json').read_text());assert a['state']=='Full39.99M range32 current candidate preserves all20carrier fields';source_path=B/'out/native/ashlar_range32_grow_r314/audited-summary.json';assert hashlib.sha256(source_path.read_bytes()).hexdigest()==a['source_sha256'];s=json.loads(source_path.read_text());r=[json.loads(x) for x in (p/'statements.jsonl').read_text().splitlines()];hh=json.loads((p/'shared-history.json').read_text());h={q['query_id']:q for q in hh['queries']};assert hh['require_final'] and not hh['missing_ids'] and len(h)==len(r)==len({x['statement_id'] for x in r})
 for x in r:
  q=h[x['statement_id']];assert q['is_final'] and q['status']=='FINISHED' and q['query_text']==x['sql'];assert x['response']['status']['state']=='SUCCEEDED' and not x['response'].get('manifest',{}).get('truncated')
 def result(label):
  xs=[x for x in r if x['label']==label];assert len(xs)==1;return xs[0]['response'].get('result',{}).get('data_array',[])
 assert len(a['stages'])==32 and [x['partition'] for x in a['stages']]==list(range(32))
 for stage in a['stages']:
  sid=stage['statement_id'];record=[x for x in r if x['statement_id']==sid];assert len(record)==1 and record[0]['label']=='optimize-'+str(stage['partition']);events=[e for e in a['commits'] if e['queryHistoryStatementId']==sid];assert events and {int(e['version']) for e in events}==set(stage['versions']) and all(e['operation']=='OPTIMIZE' for e in events)
 actual=sum((result('digest-'+str(i)) for i in range(0,400,100)),[]);assert actual==a['groups']==s['groups'] and sum(int(x[1]) for x in actual)==39990000;assert result('coverage')==a['coverage']==[['39990000','39990000','0']] and result('deleted-absent')==[['0']] and result('typed-endpoints')==[['39990000','0']];assert a['uuid']==s['uuid']==a['final_detail']['id'];assert {int(e['version']) for e in a['commits']}==set(range(a['version']+1))
 assert a['costs']=={k:sum(q['metrics'].get(k,0) for q in h.values()) for k in a['costs']};assert all(v<=a['bounds'][k] for k,v in a['costs'].items()) and a['wall_s']<1800 and a['combined_phase_wall_s']<2400;assert a['aggregate_attempt_costs']['read_bytes']<=600000000000 and a['aggregate_attempt_costs']['write_remote_bytes']<=120000000000
 digest=[x for x in r if x['label'].startswith('digest-')];assert len(digest)==4 and all(not h[x['statement_id']]['metrics'].get('result_from_cache') for x in digest)
 a['audit']={'source_sha256':{n:hashlib.sha256((p/n).read_bytes()).hexdigest() for n in ['summary.json','statements.jsonl','shared-history.json']},'audit_script_sha256':hashlib.sha256(Path(__file__).read_bytes()).hexdigest(),'final_statements':len(r),'qualified_zorder_partitions':32,'complete_carrier_groups':400,'qualified_rows':39990000,'qualification':a['qualification']};(p/'audited-summary.json').write_text(json.dumps(a,indent=2)+'\n');print(a['state'],a['costs'])
if __name__=='__main__':main()
