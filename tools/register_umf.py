"""Run pinned UMF validation and retain its raw document/receipt in UC Delta.

Intake is not target catalog acceptance, executable binding or Truss head advance.
"""
import argparse,fcntl,hashlib,json,re,subprocess,sys,time
from contextlib import contextmanager
from pathlib import Path
from databricks.sdk import WorkspaceClient
ROOT=Path(__file__).resolve().parents[1];B=ROOT/'docs/helix/02-design/spikes/SPIKE-001-table-layout'
sys.path.insert(0,str(ROOT/'src'));sys.path.insert(0,str(B))
from ashlar.schema import SchemaIntake
from ashlar.schema_registry import DeltaSchemaRegistry
from ashlar.authority import validate_writer_inventory
from durable_sql import DurableSQL,SQLPending
from databricks_transport import DatabricksTransport,OperationExecutor
from persistent_sql import Client

def main():
    p=argparse.ArgumentParser(description=__doc__)
    for key in ['installation','umf-source','validator-revision','document','revision','journal','output']:p.add_argument('--'+key,required=True)
    p.add_argument('--bun',default='bun');p.add_argument('--profile',default='aidev-cus');p.add_argument('--warehouse',default='2439e1f2e37ac563')
    a=p.parse_args();installation=json.loads(Path(a.installation).read_text());namespace=installation['namespace']
    if not re.fullmatch('[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*',namespace):p.error('Safe installed namespace required')
    # Actual pinned clean UMF code, not caller-authored validation flags.
    result=subprocess.run([a.bun,str(ROOT/'tools/inspect_umf.ts'),a.umf_source,a.validator_revision,a.document],capture_output=True,timeout=60)
    if result.returncode:raise RuntimeError('UMF inspection refused: '+result.stderr.decode('utf-8','replace'))
    artifact=result.stdout;intake=SchemaIntake.read(artifact,a.revision,trusted_validator_revision=a.validator_revision)
    out=Path(a.output);out.mkdir(parents=True,exist_ok=True);(out/'intake-artifact.json').write_bytes(artifact)
    w=WorkspaceClient(profile=a.profile);user=w.current_user.me();actor=user.user_name
    if actor!=installation['authenticated_owner']:raise PermissionError('Installed owner differs from authenticated registry writer')
    c=Client(out,profile=a.profile,warehouse_id=a.warehouse);c.w=w
    journal=DurableSQL(a.journal,w.api_client,a.warehouse,user.id);t=DatabricksTransport(c,journal);table=namespace+'.schema_intake'
    def admit():
        catalog,schema=namespace.split('.')
        for kind,name,owner in [('CATALOG',catalog,w.catalogs.get(name=catalog).owner),('SCHEMA',namespace,w.schemas.get(full_name=namespace).owner)]:
            validate_writer_inventory(owner,t.query('SHOW GRANTS ON '+kind+' '+name,{}).rows,trusted_writers=[actor])
    def mutation(op,sql,params):
        end=time.monotonic()+180
        while True:
            try:return t.mutation(op,sql,params)
            except SQLPending:
                if time.monotonic()>end:raise
                time.sleep(.2)
    try:
        with open(a.journal+'.registry-writer-lock','a') as lock:
            fcntl.flock(lock,fcntl.LOCK_EX);admit()
            fields=list(intake.row());definition=','.join(k+' '+('BOOLEAN' if k=='complete_interpretation' else 'STRING')+' NOT NULL' for k in fields)
            mutation(table+':install','CREATE TABLE '+table+' ('+definition+') USING DELTA',{})
            detail=t.query('DESCRIBE DETAIL '+table,{}).rows
            if len(detail)!=1:raise ValueError('Ambiguous registry identity')
            uuid=detail[0]['id']
            with journal.db:
                journal.db.execute('CREATE TABLE IF NOT EXISTS schema_target (name TEXT PRIMARY KEY,uuid TEXT NOT NULL)')
                journal.db.execute('INSERT OR IGNORE INTO schema_target VALUES (?,?)',(table,uuid))
                if journal.db.execute('SELECT uuid FROM schema_target WHERE name=?',(table,)).fetchone()[0]!=uuid:raise ValueError('Registry replaced')
            actual=t.query('SELECT * FROM '+table+' LIMIT 0',{})
            if actual.columns!=tuple((k,'BOOLEAN' if k=='complete_interpretation' else 'STRING') for k in fields):raise ValueError('Registry schema differs')
            class Policy:
                @contextmanager
                def writer(self,target,target_uuid,context):
                    if (target,target_uuid)!=(table,uuid) or context is not lock:raise PermissionError('Wrong registry writer lane')
                    for phase in [0,1]:
                        admit();validate_writer_inventory(w.tables.get(full_name=table).owner,t.query('SHOW GRANTS ON TABLE '+table,{}).rows,trusted_writers=[actor])
                        if phase==0:yield
            identity=[table,uuid,intake.document_id,a.revision]
            op='schema-intake:'+hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest()
            registry=DeltaSchemaRegistry(OperationExecutor(t,op),Policy(),table,uuid);end=time.monotonic()+180
            while True:
                try:stored=registry.register(artifact,a.revision,trusted_validator_revision=a.validator_revision,context=lock);break
                except SQLPending:
                    if time.monotonic()>end:raise
                    time.sleep(.2)
            assert stored==intake
            summary={'state':'retained','table':table,'table_uuid':uuid,'document_id':intake.document_id,'revision':a.revision,'source_sha256':intake.source_sha256,'artifact_sha256':intake.artifact_sha256,'validator_revision':a.validator_revision,'complete_interpretation':intake.complete_interpretation,'qualification':'Actual pinned UMF structural validation and exact raw native intake/readback only. Target semantic/schema/catalog acceptance, ID allocation, Truss head/runtime and feed are separate. Owner/inherited grants and same-host lock; remote writer lifecycle remains unqualified.'}
            (out/'summary.json').write_text(json.dumps(summary,indent=2)+'\n')
            journal.db.row_factory=__import__('sqlite3').Row
            (out/'journal-export.json').write_text(json.dumps([dict(r) for r in journal.db.execute('SELECT * FROM submission ORDER BY operation')],indent=2)+'\n')
            print('Retained UMF '+intake.document_id+' revision '+a.revision+'; complete interpretation='+str(intake.complete_interpretation))
    finally:journal.close()
if __name__=='__main__':main()
