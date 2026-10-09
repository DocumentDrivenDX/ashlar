"""Stage bounded complete JSONL source batches in an admitted UC runtime.

Development owner lane with same-host serialization; not production remote source
fencing. Retain the journal. Staging never publishes or acknowledges the source.
"""
import argparse,fcntl,hashlib,json,re,sys,time
from contextlib import contextmanager
from dataclasses import asdict
from pathlib import Path
from databricks.sdk import WorkspaceClient
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.authority import validate_writer_inventory
from ashlar.source import jsonl_batches
from ashlar.staging import DeltaBatchStage,batch_row
from durable_sql import DurableSQL,SQLPending
from databricks_transport import DatabricksTransport,OperationExecutor
from persistent_sql import Client
FIELDS=['source_profile','feed','epoch','batch_id','cursor_before','cursor_after','records_digest','batch_json','batch_digest']

def _endpoint(value):
    if not value.strip():raise argparse.ArgumentTypeError('Explicit nonempty dedicated endpoint required')
    return value


def main():
    parser=argparse.ArgumentParser(description=__doc__)
    for key in ['installation','input','feed','epoch','journal','output']:parser.add_argument('--'+key,required=True)
    parser.add_argument('--cursor-before',default='0');parser.add_argument('--profile',type=_endpoint,required=True);parser.add_argument('--warehouse',type=_endpoint,required=True)
    args=parser.parse_args();installation=json.loads(Path(args.installation).read_text());namespace=installation['namespace']
    if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',namespace):parser.error('Safe installed namespace required')
    w=WorkspaceClient(profile=args.profile);user=w.current_user.me();actor=user.user_name
    if actor!=installation['authenticated_owner']:raise PermissionError('Installed owner differs from authenticated source writer')
    out=Path(args.output);c=Client(out,profile=args.profile,warehouse_id=args.warehouse);c.w=w
    journal=DurableSQL(args.journal,w.api_client,args.warehouse,user.id);transport=DatabricksTransport(c,journal)
    table=namespace+'.source_batch_stage'
    def authorities():
        catalog,schema=namespace.split('.')
        for kind,target,owner in [('CATALOG',catalog,w.catalogs.get(name=catalog).owner),('SCHEMA',namespace,w.schemas.get(full_name=namespace).owner)]:
            validate_writer_inventory(owner,transport.query('SHOW GRANTS ON '+kind+' '+target,{}).rows,trusted_writers=[actor])
    def original(operation,sql,params):
        deadline=time.monotonic()+180
        while True:
            try:return transport.mutation(operation,sql,params)
            except SQLPending:
                if time.monotonic()>deadline:raise
                time.sleep(.2)
    try:
        with open(args.journal+'.source-writer-lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX);authorities()
            original(table+':install','CREATE TABLE '+table+' ('+','.join(k+' STRING NOT NULL' for k in FIELDS)+') USING DELTA',{})
            detail=transport.query('DESCRIBE DETAIL '+table,{}).rows
            if len(detail)!=1:raise ValueError('Ambiguous stage identity')
            uuid=detail[0]['id']
            with journal.db:
                journal.db.execute('CREATE TABLE IF NOT EXISTS source_target (name TEXT PRIMARY KEY,uuid TEXT NOT NULL)')
                journal.db.execute('INSERT OR IGNORE INTO source_target VALUES (?,?)',(table,uuid))
                if journal.db.execute('SELECT uuid FROM source_target WHERE name=?',(table,)).fetchone()[0]!=uuid:raise ValueError('Stage replaced')
            schema=transport.query('SELECT * FROM '+table+' LIMIT 0',{})
            if schema.columns!=tuple((k,'STRING') for k in FIELDS):raise ValueError('Stage schema differs')
            class Policy:
                @contextmanager
                def writer(self,target,target_uuid,context):
                    if (target,target_uuid)!=(table,uuid) or context is not lock:raise PermissionError('Wrong development lane')
                    for phase in [0,1]:
                        authorities()
                        validate_writer_inventory(w.tables.get(full_name=table).owner,transport.query('SHOW GRANTS ON TABLE '+table,{}).rows,trusted_writers=[actor])
                        if phase==0:yield
            receipts=[]
            with open(args.input,'rb') as source:
                for batch in jsonl_batches(source,feed=args.feed,epoch=args.epoch,cursor_before=args.cursor_before):
                    # Stable original batch identity, independent of changed bytes:
                    # changed same-identity request conflicts in retained journal.
                    identity=[table,uuid,batch.feed,batch.epoch,batch.batch_id]
                    operation='stage:'+hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()
                    executor=OperationExecutor(transport,operation);stage=DeltaBatchStage(executor,Policy(),table,uuid)
                    deadline=time.monotonic()+180
                    while True:
                        try:receipt=stage.stage(batch,context=lock);break
                        except SQLPending:
                            if time.monotonic()>deadline:raise
                            time.sleep(.2)
                    receipts.append(asdict(receipt))
            summary={'state':'staged','table':table,'table_uuid':uuid,'batches':receipts,'qualification':'Original complete source custody, no normalized schema/graph admission or publication/acknowledgement. Authenticated owner/inherited grants and same-host writer lock verified; remote writer lifecycle/source fencing remains unqualified.'}
            (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n');print('Staged '+str(len(receipts))+' complete source batches; no acknowledgement')
            journal.db.row_factory=__import__('sqlite3').Row
            (out/'journal-export.json').write_text(json.dumps([dict(r) for r in journal.db.execute('SELECT * FROM submission ORDER BY operation')],indent=2)+'\n')
    finally:journal.close()
if __name__=='__main__':main()
