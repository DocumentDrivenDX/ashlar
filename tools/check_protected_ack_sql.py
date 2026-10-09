"""Bounded native SQL-service checks on fresh private PostgreSQL namespaces.

This checks protected source ACK storage only. Manifests are explicit test
carriers; no Delta publication, resolver authority or Truss integration claim.
"""
import argparse,hashlib,json,subprocess,uuid
from pathlib import Path
from ashlar.source import jsonl_batches
from ashlar.outbox import OutboxTransaction
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.staging import batch_row
from protected_outbox_ack import AckScope,render_ddl,receipt_bytes
ROOT=Path(__file__).resolve().parents[1]
CONTAINER='ashlar-e2e-truss-pg17'
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':')).encode()
def literal(raw):return "convert_from(decode('"+raw.hex()+"','hex'),'UTF8')"
def run(output):
    output=Path(output);output.mkdir(parents=True,exist_ok=False)
    label=subprocess.check_output(['docker','inspect',CONTAINER,'--format','{{index .Config.Labels "ashlar.purpose"}}']).decode().strip()
    if label!='end-to-end-development':raise ValueError('Private development container required')
    observations=[]
    def pg(sql,fail=False):
        r=subprocess.run(['docker','exec','-i',CONTAINER,'psql','-qAt','-U','postgres','-d','truss_e2e','-v','ON_ERROR_STOP=1'],input=sql,text=True,capture_output=True)
        observations.append({'sql':sql,'exit':r.returncode,'stdout':r.stdout,'stderr':r.stderr})
        (output/'observations.json').write_text(json.dumps(observations,indent=2)+'\n')
        if (r.returncode!=0)!=fail:raise ValueError('Unexpected native SQL outcome: '+r.stderr)
        return r.stdout.strip()
    suffix=uuid.uuid4().hex[:12];source='ashlar_ack_source_'+suffix;service='ashlar_ack_service_'+suffix;role='ashlar_ack_operator_'+suffix
    writer=source+'_writer';reader=source+'_reader'
    source_ddl=(ROOT/'sql/ashlar-outbox/01-postgresql.sql').read_text().replace('ashlar_outbox_writer',writer).replace('ashlar_outbox_reader',reader).replace('ashlar_outbox',source)
    pg(source_ddl);ddl=render_ddl(service,role);pg(ddl)
    scope=AckScope(service,str(uuid.uuid4()),str(uuid.uuid4()),'isolated-sql-check','sql-source','epoch-one')
    pg(f"SELECT {service}.register_source('{scope.installation_id}','{source}'); SELECT {service}.register_consumer('{scope.scope_id}','{scope.installation_id}','{scope.consumer}','{scope.feed}','{scope.epoch}');")
    raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes();batch=next(jsonl_batches(raw.splitlines(keepends=True),feed=scope.feed,epoch=scope.epoch))
    self_digest=hashlib.sha256(raw).hexdigest()
    assert pg(f'SET SESSION AUTHORIZATION {writer}; SELECT {source}.append('+literal(batch.batch_id.encode())+','+literal(raw)+');')=='1'
    assert pg(f'SET SESSION AUTHORIZATION {role}; SELECT session_user || chr(58) || current_user;')==role+':'+role
    tx=OutboxTransaction('ashlar-postgresql-outbox/0.1',scope.feed,scope.epoch,'0','1',self_digest,batch);row=batch_row(batch)
    request={'stream':'isolated-sql-service','batch_id':batch.batch_id,'predecessor':'explicit-test-origin','schema_revisions_json':'{"test":"1"}','source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':outbox_checkpoint(tx)}
    request['request_digest']=hashlib.sha256(encoded(request)).hexdigest();request_raw=encoded(request)
    manifest={'publication_id':'explicit-test-carrier','profile_version':'test-only','table_versions_json':'{"test.table":0}','schema_revisions_json':request['schema_revisions_json'],'source_progress_json':json.dumps({scope.feed:json.loads(outbox_checkpoint(tx))}),'validation_report_json':json.dumps({'complete':True,'request_digest':request['request_digest']}),'recorded_at':'1'}
    manifest_raw=encoded(manifest);_,_,_,receipt=receipt_bytes(scope,request_raw,manifest_raw)
    def ack(req=request_raw,man=manifest_raw,proof=receipt):
        return f"SELECT encode({service}.ack('{scope.scope_id}',0,1,"+literal(batch.batch_id.encode())+",decode('"+self_digest+"','hex'),decode('"+req.hex()+"','hex'),decode('"+man.hex()+"','hex'),decode('"+proof.hex()+"','hex')),'hex');"
    pg(f'SET SESSION AUTHORIZATION {reader}; '+ack(),True)
    ack_signature=f'{service}.ack(uuid,bigint,bigint,text,bytea,bytea,bytea,bytea)'
    pg(f'GRANT USAGE ON SCHEMA {service} TO {reader}; GRANT EXECUTE ON FUNCTION {ack_signature} TO {reader};')
    pg(f'SET SESSION AUTHORIZATION {reader}; '+ack(),True)
    assert 'ACK_CALLER_UNAUTHORIZED' in observations[-1]['stderr']
    pg(f'REVOKE EXECUTE ON FUNCTION {ack_signature} FROM {reader}; REVOKE USAGE ON SCHEMA {service} FROM {reader};')
    pg(f'SET SESSION AUTHORIZATION {role}; UPDATE {service}.consumer_scope SET position=99;',True)
    pg(f'SET SESSION AUTHORIZATION {role}; '+ack(req=encoded({k:v for k,v in request.items() if k!='source_checkpoint_json'})),True)
    assert pg(f'BEGIN; SET SESSION AUTHORIZATION {role}; '+ack()+' ROLLBACK;')==receipt.hex()
    assert pg(f"SET SESSION AUTHORIZATION {role}; SELECT position FROM {service}.observe('{scope.scope_id}',1);")=='0'
    assert pg(f'SET SESSION AUTHORIZATION {role}; '+ack())==receipt.hex()
    assert pg(f'SET SESSION AUTHORIZATION {role}; '+ack())==receipt.hex()
    pg(f'SET SESSION AUTHORIZATION {role}; '+ack(proof=receipt+b' '),True)
    assert pg(f'SELECT count(*) FROM {service}.receipt;')=='1'
    pg(f'REVOKE EXECUTE ON FUNCTION {source}.append(text,text) FROM {writer};')
    pg(f'SET SESSION AUTHORIZATION {role}; '+ack(),True)
    assert pg(f'SELECT position FROM {service}.consumer_scope;')=='1'
    report={'format':'ashlar-protected-ack-sql-check/0.1','source':source,'service':service,'scope':scope.__dict__,'checks':['ordinary reader cannot ACK','execute permission alone cannot grant caller membership','ACK role cannot direct DML','missing checkpoint refuses','rollback leaves head zero','commit and exact replay preserve one receipt','changed receipt bytes refuse','source ACL drift refuses'],'qualification':__doc__,'ddl_sha256':hashlib.sha256(ddl.encode()).hexdigest()}
    (output/'observations.json').write_text(json.dumps(observations,indent=2)+'\n');(output/'report.json').write_text(json.dumps(report,indent=2)+'\n');return report
if __name__=='__main__':
    p=argparse.ArgumentParser(description=__doc__);p.add_argument('--output',required=True,type=Path);a=p.parse_args();print(json.dumps(run(a.output)))
