import json,time
from pathlib import Path
from bounded_reads_r145 import BoundedReads
from publication_history import collect_history,HistoryPending
B=Path(__file__).resolve().parent;O=B/'out/native/ashlar_mixed_r216';records=[json.loads(x) for x in (O/'statements.jsonl').read_text().splitlines()];failed=records[-1];assert failed['response']['status']['state']=='FAILED' and failed['statement_id']
c=BoundedReads(O/'audit')
try:
 q=c.w.api_client.do('GET','/api/2.0/sql/history/queries',query={'filter_by.query_start_time_range.start_time_ms':int(failed['start_epoch']*1000)-1000,'max_results':1000})
 found=[x for x in q['res'] if x['query_id']==failed['statement_id']];assert len(found)==1 and found[0]['status']=='FAILED'
 assert c.sql('absence',"SHOW TABLES IN client_dev.ashlar_entropy_20261006_r86 LIKE 'mixed_object_current_r216'")==[]
 (O/'failed-native-history.json').write_text(json.dumps(found,indent=2)+'\n');(O/'failure-audit.json').write_text(json.dumps({'native_state':'FAILED','query_id':failed['statement_id'],'target_absent':True,'no_write_replay':True},indent=2)+'\n');print('Native FAILED and target absent verified')
finally:c.close()
