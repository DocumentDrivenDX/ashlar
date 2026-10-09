"""Nine-event recoverable native fixture; no completed read publication."""
import argparse,base64,fcntl,hashlib,json,re,sys,time
from contextlib import contextmanager
from pathlib import Path
from databricks.sdk import WorkspaceClient
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.apply import empty_state
from ashlar.source import jsonl_batches
from ashlar.staging import batch_row
from durable_sql import DurableSQL,SQLPending
from durable_effects import DurableEffects
from whole_graph_sql import graph_sql_plan
from persistent_sql import Client
def _endpoint(value):
    if not value.strip():raise argparse.ArgumentTypeError('Explicit nonempty dedicated endpoint required')
    return value


parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('--profile',type=_endpoint,required=True)
parser.add_argument('--warehouse',type=_endpoint,required=True)
args=parser.parse_args()
N='client_dev.ashlar_recoverable_graph_20261008';OUT=B/'out/native/recoverable_graph_20261008'
if (OUT/'summary.json').exists():raise SystemExit('Completed fixture retained; inspect evidence instead of rerunning setup/parity. Retain original journal for recovery.')
JOURNAL='/private/tmp/ashlar-recoverable-graph-20261008.sqlite'
w=WorkspaceClient(profile=args.profile);authority=w.current_user.me().id
class CountAPI:
    def __init__(self):self.posts=0
    def do(self,method,path,**kwargs):
        if method=='POST':self.posts+=1
        return w.api_client.do(method,path,**kwargs)
api=CountAPI();transport=DurableSQL(JOURNAL,api,args.warehouse,authority)
def query(operation,sql,params):
    deadline=time.monotonic()+180
    while True:
        try:return transport.query(operation,sql,params)
        except SQLPending:
            if time.monotonic()>deadline:raise
            time.sleep(.2)
query('setup-schema','CREATE SCHEMA '+N,{})
baseline=(ROOT/'sql/ashlar-delta-v03/01-baseline.sql').read_text()
tables={k:N+'.'+k for k in ['object_current','edge_current','tombstone','whole_source_history']}
for k in ['object_current','edge_current','tombstone']:
    ddl=re.search(r'CREATE TABLE '+k+r' \(.*?;',baseline,re.S).group(0)
    query('setup-'+k,ddl.replace('CREATE TABLE '+k,'CREATE TABLE '+tables[k],1),{})
query('setup-history','CREATE TABLE '+tables['whole_source_history']+' (feed STRING NOT NULL,epoch STRING NOT NULL,delivery_id STRING NOT NULL,digest STRING NOT NULL,change_json STRING NOT NULL,raw_base64 STRING NOT NULL) USING DELTA',{})
c=Client(OUT,profile=args.profile,warehouse_id=args.warehouse)
uuids={}
for table in tables.values():
    rows=c.sql('register-target','DESCRIBE DETAIL '+table)
    names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
    uuids[table]=dict(zip(names,rows[0]))['id']
class Policy:
    def __init__(self,expected):self.expected=expected
    @contextmanager
    def writer(self,operation,context):
        if context!='private-fixture':raise PermissionError('Wrong source authority')
        with open('/private/tmp/ashlar-recoverable-graph.lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX)
            try:yield
            finally:fcntl.flock(lock,fcntl.LOCK_UN)
    def admit(self,plan,context):
        if plan['steps']!=self.expected:raise PermissionError('Changed independently generated plan')
        for table,uuid in uuids.items():
            rows=c.sql('admit-target','DESCRIBE DETAIL '+table)
            names=[x['name'] for x in c.records[-1]['response']['manifest']['schema']['columns']]
            assert dict(zip(names,rows[0]))['id']==uuid
batches=list(jsonl_batches((ROOT/'examples/end-to-end/graph-source.jsonl').read_bytes().splitlines(keepends=True),feed='whole-entity-fixture',epoch='epoch-1'))
state=empty_state();results=[]
def schema(change):
    if change.state.schema_revision!='fixture-schema-1' or change.state.key.source!='whole-fixture':raise PermissionError('Unaccepted fixture source/schema')
for ordinal,batch in enumerate(batches):
    after,steps=graph_sql_plan(state,batch,tables,materialized_at='2026-10-08T17:00:00+00:00',schema_policy=schema)
    custody=batch_row(batch)
    request={'stream':'recoverable-whole-fixture','batch_id':batch.batch_id,'predecessor':'fixture-'+str(ordinal),'schema_revisions_json':'{"whole-fixture":"fixture-schema-1"}','source_batch_json':custody['batch_json'],'source_batch_digest':custody['batch_digest']}
    digest=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    runner=DurableEffects(transport,Policy(steps));deadline=time.monotonic()+180
    while True:
        try:result=runner.run(batch.batch_id,digest,steps,context='private-fixture');break
        except SQLPending:
            if time.monotonic()>deadline:raise
            time.sleep(.2)
    results.append(result)
    before=api.posts;transport.close();transport=DurableSQL(JOURNAL,api,args.warehouse,authority)
    assert DurableEffects(transport,Policy(steps)).run(batch.batch_id,digest,steps,context='private-fixture')==result
    assert api.posts==before
    objects=c.sql('object-parity','SELECT cast(type_id AS STRING),cast(id AS STRING),cast(entity_version AS STRING),props_json,retained_json FROM '+tables['object_current']+' ORDER BY type_id,id')
    edges=c.sql('edge-parity','SELECT cast(rel_type_id AS STRING),cast(id AS STRING),cast(source_type AS STRING),cast(source_id AS STRING),cast(target_type AS STRING),cast(target_id AS STRING) FROM '+tables['edge_current']+' ORDER BY rel_type_id,id')
    expected=[['1','1','1','{"23":"first"}','{}'],['1','2','1','{"23":"second"}','{}'],['1','3','1','{"23":"isolated"}','{}']] if ordinal==0 else [['1','1','2','{"23":"updated","24":null}','{"future":18446744073709551615}'],['1','3','1','{"23":"isolated"}','{}']]
    assert objects==expected
    assert edges==([['2','1','1','1','1','2'],['2','2','1','1','1','2']] if ordinal==0 else [])
    state=after
history=c.sql('original-history','SELECT delivery_id,raw_base64 FROM '+tables['whole_source_history']+' ORDER BY delivery_id')
assert history==sorted([[r.delivery_id,base64.b64encode(r.raw).decode()] for b in batches for r in b.records])
assert c.sql('delete-parity','SELECT entity_kind,cast(type_id AS STRING),cast(id AS STRING),cast(entity_version AS STRING) FROM '+tables['tombstone']+' ORDER BY entity_kind,id')==[['edge','2','1','2'],['edge','2','2','2'],['object','1','2','2']]
(OUT/'effect-results.json').write_text(json.dumps(results,indent=2)+'\n')
summary={'state':'passed','schema':N,'table_uuids':uuids,'source_events':9,'source_batches':2,'checks':['admitted exact SQL plans retained before effects','original native handle/terminal receipts for ordered writes','fresh connection replay emits no new statements','independent create/update/delete/parallel/isolated inventories','exact original source bytes and three tombstones'],'qualification':'Private admin whole-entity fixture; local cooperating-process lock, no remote/native fencing or immutable grants. Replay qualified for retained same-host journal and exact plans; interrupted native mutation not injected. No accepted Truss schema/feed, complete all-column effect validation, retained pin proof, publication or acknowledgement. No scale/resize.'}
(OUT/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');transport.close();print(json.dumps(summary,indent=2))
