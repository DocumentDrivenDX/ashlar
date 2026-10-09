import copy,hashlib,json,unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from pathlib import Path
from ashlar.manifest import manifest_pin_vector
from ashlar.native import SQLResult
from ashlar.outbox import OutboxTransaction
from ashlar.source import jsonl_batches
from ashlar.source_checkpoint import outbox_checkpoint
from ashlar.staging import batch_row
from test_publication import FakeBackend,TABLE,OTHER
from protected_outbox_ack import AckScope,AckError,AckOutcomeUncertain,ProtectedOutboxAck,receipt_bytes,render_ddl

ROOT=Path(__file__).resolve().parents[1]
def encoded(v):return (json.dumps(v,ensure_ascii=False,sort_keys=True,separators=(',',':'))+'\n').encode()

def original():
    raw=(ROOT/'examples/end-to-end/source.jsonl').read_bytes()
    batch=next(iter(jsonl_batches(raw.splitlines(keepends=True),feed='feed',epoch='epoch')))
    transaction=OutboxTransaction('ashlar-postgresql-outbox/0.1','feed','epoch','0','1',hashlib.sha256(raw).hexdigest(),batch)
    row=batch_row(batch)
    request={'stream':'global-stream','batch_id':batch.batch_id,'predecessor':'other-feed-publication','schema_revisions_json':'{"source":"r1"}','source_batch_json':row['batch_json'],'source_batch_digest':row['batch_digest'],'source_checkpoint_json':outbox_checkpoint(transaction)}
    request['request_digest']=hashlib.sha256(json.dumps(request,sort_keys=True,separators=(',',':')).encode()).hexdigest()
    manifest={'publication_id':'p1','profile_version':'ashlar-delta/0.3','recorded_at':'1','table_versions_json':json.dumps({TABLE:6,OTHER:2}),
              'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':json.dumps({'feed':json.loads(request['source_checkpoint_json']),'other-feed':{'position':'17'}}),
              'validation_report_json':json.dumps({'complete':True,'request_digest':request['request_digest']})}
    return request,manifest

class Harness:
    def __init__(self):
        self.scope=AckScope('ashlar_ack_test','00000000-0000-0000-0000-000000000001','00000000-0000-0000-0000-000000000002','consumer','feed','epoch')
        self.request,self.manifest=original();self.request_bytes=encoded(self.request);self.manifest_bytes=encoded(self.manifest)
        self.vector=manifest_pin_vector(self.manifest,{TABLE:'trusted-uuid',OTHER:'trusted-uuid'},authority='owner')
        self.backend=FakeBackend();self.backend.rows=[self.manifest]
        self.context=object();self.held=False;self.connections=[];self.stored=None;self.unknown_commit=False;self.policy_false=False;self.close_during_policy=False;self.policy_calls=0
        outer=self
        class Pins:
            @contextmanager
            def hold(self,vector,*,context):
                assert vector==outer.vector and context is outer.context
                outer.held=True
                try:yield
                finally:outer.held=False
        class Policy:
            def admit_scope(self,scope,session,context):
                assert outer.held and scope==outer.scope and context is outer.context
                if outer.policy_false:return False
            def admit_publication(self,scope,request,resolved,session,context):
                assert outer.held and request==outer.request and resolved.descriptor.source_progress['other-feed']['position']=='17'
                # Global predecessor is allowed to belong to another feed.
                assert request['predecessor']=='other-feed-publication'
                outer.policy_calls+=1
                if outer.close_during_policy and outer.policy_calls==2:session.connection.info.transaction_status=0
        self.port=ProtectedOutboxAck(self.factory,Pins(),self.backend,Policy(),self.scope,supported_profiles=['ashlar-delta/0.3'],supported_revisions={'source':['r1']})
    def factory(self,context):
        assert self.held and context is self.context
        outer=self
        class Connection:
            autocommit=False
            def __init__(self):self.info=SimpleNamespace(transaction_status=0);self.pending=None;self.closed=False;self.writes=0
            def query(self,sql,parameters):
                self.info.transaction_status=2
                if '.scope_binding' in sql:
                    return SQLResult([{'binding':{'scope_id':outer.scope.scope_id,'installation_id':outer.scope.installation_id,'consumer':outer.scope.consumer,'feed':'feed','epoch':'epoch','source_schema':'ashlar_ack_source_test','source_signature_sha256':'a'*64}}])
                if '.ack(' in sql:
                    self.writes+=1;self.pending=(parameters['request'],parameters['manifest'],parameters['receipt']);return SQLResult([{'receipt_hex':parameters['receipt']}])
                if '.observe(' in sql:
                    stored=outer.stored
                    return SQLResult([{'position':'1' if stored else '0','request_hex':stored[0] if stored else None,'manifest_hex':stored[1] if stored else None,'receipt_hex':stored[2] if stored else None}])
                raise AssertionError(sql)
            def commit(self):
                assert outer.held
                if self.pending:outer.stored=self.pending
                self.info.transaction_status=0
                if outer.unknown_commit:raise OSError('Lost original COMMIT response')
            def rollback(self):self.pending=None;self.info.transaction_status=0
            def close(self):self.closed=True
        connection=Connection();self.connections.append(connection);return connection

class FakeSession:
    def __init__(self,connection):self.connection=connection;self.active=True
    def query(self,sql,parameters):return self.connection.query(sql,parameters)

class Tests(unittest.TestCase):
    def call(self,h,method='acknowledge'):
        with patch('protected_outbox_ack.Session',FakeSession):
            return getattr(h.port,method)(h.request_bytes,h.manifest_bytes,h.vector,context=h.context)
    def test_exact_original_receipt_and_held_pins_through_commit(self):
        h=Harness();receipt=self.call(h)
        self.assertEqual(receipt,receipt_bytes(h.scope,h.request_bytes,h.manifest_bytes)[3]);self.assertEqual(h.connections[0].writes,1)
        self.assertFalse(h.held);self.assertTrue(h.connections[0].closed);self.assertEqual(h.policy_calls,2)
    def test_lost_commit_reconciles_exact_original_on_fresh_connection_without_write(self):
        h=Harness();h.unknown_commit=True
        with self.assertRaises(AckOutcomeUncertain) as raised:self.call(h)
        self.assertEqual(raised.exception.original_request,h.request_bytes);self.assertTrue(raised.exception.commit_attempted)
        h.unknown_commit=False;h.policy_calls=0
        result=self.call(h,'reconcile');self.assertEqual(result,receipt_bytes(h.scope,h.request_bytes,h.manifest_bytes)[3])
        self.assertEqual(len(h.connections),2);self.assertEqual(h.connections[-1].writes,0)
    def test_absence_and_changed_original_receipt_refuse(self):
        h=Harness();self.assertIsNone(self.call(h,'reconcile'))
        self.call(h);h.stored=(h.stored[0],encoded({**h.manifest,'recorded_at':'2'}).hex(),h.stored[2])
        with self.assertRaises(AckError):self.call(h,'reconcile')
    def test_current_authority_and_native_publication_refuse_before_ack(self):
        h=Harness();h.policy_false=True
        with self.assertRaises(AckError):self.call(h)
        self.assertEqual(h.connections[0].writes,0)
        h=Harness();h.backend.rows=[{**h.manifest,'recorded_at':'changed'}]
        with self.assertRaises(AckError):self.call(h)
        self.assertEqual(h.connections[0].writes,0)
    def test_scope_and_safe_rendering_no_hidden_native_endpoints(self):
        h=Harness();wrong=AckScope('ashlar_ack_test',h.scope.scope_id,h.scope.installation_id,'consumer','wrong','epoch')
        with self.assertRaises(AckError):receipt_bytes(wrong,h.request_bytes,h.manifest_bytes)
        ddl=render_ddl('ashlar_ack_new','ashlar_ack_operator')
        self.assertNotIn('__ACK_',ddl);self.assertIn('FOR SHARE OF s',ddl)
        with self.assertRaises(AckError):render_ddl('public','role;DROP')
    def test_suppressed_inner_failure_cannot_report_ack_success(self):
        h=Harness();h.policy_false=True
        class Suppress:
            @contextmanager
            def hold(self,*args,**kwargs):
                h.held=True
                try:yield
                except AckError:pass
                finally:h.held=False
        h.port.pins=Suppress()
        with self.assertRaisesRegex(AckError,'suppressed'):self.call(h)
        self.assertEqual(h.connections[0].writes,0)

    def test_unexpected_transaction_closure_after_write_is_outcome_unknown(self):
        h=Harness();h.close_during_policy=True
        with self.assertRaises(AckOutcomeUncertain) as raised:self.call(h)
        self.assertFalse(raised.exception.commit_attempted);self.assertFalse(raised.exception.commit_observed)
        self.assertEqual(raised.exception.original_manifest,h.manifest_bytes)
    def test_scope_native_binding_replaced_is_not_an_authorized_noop_policy(self):
        h=Harness();original_factory=h.port.factory
        def changed(context):
            connection=original_factory(context);query=connection.query
            def substitute(sql,parameters):
                result=query(sql,parameters)
                if '.scope_binding' in sql:result.rows[0]['binding']['installation_id']='00000000-0000-0000-0000-000000000099'
                return result
            connection.query=substitute;return connection
        h.port.factory=changed
        with self.assertRaisesRegex(AckError,'installation differs'):self.call(h)
        self.assertEqual(h.connections[0].writes,0)

    def test_close_failure_after_observed_commit_retains_uncertain_original_custody(self):
        h=Harness();factory=h.port.factory
        def bad_close(context):
            connection=factory(context)
            def close():raise OSError('Lost connection during final close')
            connection.close=close;return connection
        h.port.factory=bad_close
        with self.assertRaises(AckOutcomeUncertain) as raised:self.call(h)
        self.assertTrue(raised.exception.commit_attempted);self.assertTrue(raised.exception.commit_observed)
        self.assertEqual(raised.exception.original_request,h.request_bytes)
        self.assertIsInstance(raised.exception.__cause__,OSError)
        self.assertIsNotNone(h.stored)
    def test_lost_status_during_unknown_commit_cleanup_does_not_mask_original(self):
        h=Harness();factory=h.port.factory
        class LostInfo:
            @property
            def transaction_status(self):raise OSError('Connection status unavailable')
        def lost_status(context):
            connection=factory(context);commit=connection.commit
            def unknown():
                commit();connection.info=LostInfo();raise OSError('Original COMMIT response lost')
            def rollback():raise OSError('Lost connection cannot roll back')
            connection.commit=unknown;connection.rollback=rollback;return connection
        h.port.factory=lost_status
        with self.assertRaises(AckOutcomeUncertain) as raised:self.call(h)
        self.assertEqual(raised.exception.original_manifest,h.manifest_bytes)
        self.assertIn('Original COMMIT response lost',str(raised.exception.__cause__.__cause__))
        self.assertIsNotNone(h.stored)

    def test_closing_policy_commit_then_error_is_uncertain_without_false_rollback(self):
        h=Harness();admit=h.port.policy.admit_publication
        def closing(scope,request,resolved,session,context):
            admit(scope,request,resolved,session,context)
            if h.policy_calls==2:
                session.connection.commit()
                raise ValueError('Closing authority failed after external commit')
        h.port.policy.admit_publication=closing
        with self.assertRaises(AckOutcomeUncertain) as raised:self.call(h)
        self.assertFalse(raised.exception.commit_attempted);self.assertIsNotNone(h.stored)
        self.assertIn('Closing authority failed',str(raised.exception.__cause__))
    def test_suppressed_commit_exception_is_still_uncertain(self):
        h=Harness();h.unknown_commit=True
        class Suppress:
            @contextmanager
            def hold(self,*args,**kwargs):
                h.held=True
                try:yield
                except OSError:pass
                finally:h.held=False
        h.port.pins=Suppress()
        with self.assertRaises(AckOutcomeUncertain) as raised:self.call(h)
        self.assertTrue(raised.exception.commit_attempted);self.assertFalse(raised.exception.commit_observed)
        self.assertIsNotNone(h.stored)
    def test_postgresql_identifier_truncation_cannot_alias_service_scope(self):
        h=Harness()
        with self.assertRaises(AckError):render_ddl('ashlar_ack_'+'a'*64,'ashlar_ack_short')
        with self.assertRaises(AckError):render_ddl('ashlar_ack_short','ashlar_ack_'+'a'*64)
        with self.assertRaises(AckError):AckScope('ashlar_ack_'+'a'*64,h.scope.scope_id,h.scope.installation_id,'consumer','feed','epoch')

    def test_known_sql_abort_rolled_back_does_not_become_unknown_commit(self):
        h=Harness();factory=h.port.factory
        def aborted(context):
            connection=factory(context);query=connection.query
            def refusal(sql,parameters):
                if '.ack(' in sql:
                    connection.info.transaction_status=3
                    raise AckError('Known native SQL refusal')
                return query(sql,parameters)
            connection.query=refusal;return connection
        h.port.factory=aborted
        with self.assertRaises(AckError) as raised:self.call(h)
        self.assertNotIsInstance(raised.exception,AckOutcomeUncertain);self.assertIn('Known native SQL refusal',str(raised.exception))
        self.assertIsNone(h.stored)
