"""One read-only SQL custody check; explicit journal, no replacement submissions."""
import argparse
import json
from pathlib import Path
from databricks.sdk import WorkspaceClient
from durable_sql import DurableSQL

parser=argparse.ArgumentParser()
parser.add_argument('--journal',required=True)
parser.add_argument('--output',required=True)
args=parser.parse_args()
w=WorkspaceClient(profile='aidev-cus')
authority=w.current_user.me().id
out=Path(args.output);out.mkdir(parents=True,exist_ok=True)
def connect():return DurableSQL(args.journal,w.api_client,'2439e1f2e37ac563',authority)
c=connect()
try:response=c.query('native-custody-read-1','SELECT :value AS original',{'value':'retained-original'})
finally:c.close()
c=connect()
try:assert c.query('native-custody-read-1','SELECT :value AS original',{'value':'retained-original'})==response
finally:c.close()
assert response['result']['data_array']==[['retained-original']]
(out/'terminal-response.json').write_text(json.dumps(response,indent=2)+'\n')
(out/'summary.json').write_text(json.dumps({'state':'passed','statement_id':response['statement_id'],'checks':['one bound SELECT on existing warehouse','fresh SQLite journal connection replays original terminal response'],'qualification':'Read-only native transport check; no graph effects, publication admission, fencing or long-term journal retention proof.'},indent=2)+'\n')
print('Original native response and fresh-connection replay passed')
