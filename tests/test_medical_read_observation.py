"""Actual medical plan and typed read owner; native/ordinary lower frames inert."""
import json,hashlib,unittest
from dataclasses import replace
from types import SimpleNamespace,MappingProxyType
from pathlib import Path
from unittest.mock import patch
import test_medical_publication_plan as plans
import test_original_medical_local_sources as sources
from ashlar_host.medical_read_observation import MedicalReadObservation
from ashlar_host.original_medical_local_read_sources import original_medical_local_read_sources
from ashlar_host.read_only_transport import ReadOnlyDeltaTransport
from ashlar_host.delta_custody import DeltaTarget,encoded
from ashlar_host.evolution_verification_types import VerificationBounds
from ashlar_host.local_physical_carriers import original_local_physical_carriers
from ashlar_host.existing_outbox_authority import ExistingOutboxAuthority

class Tests(unittest.TestCase):
    def setUp(self):
        self.fixture=plans.Tests();self.fixture.setUp();self.addCleanup(self.fixture.doCleanups);self.plan=self.fixture.plan();self.view=self.plan.snapshot();self.calls=[]
        self.bounds=VerificationBounds(262144,10000,1048576,16777216,1048576,1048576)
        self.transport=object.__new__(ReadOnlyDeltaTransport);self.transport.observation_bounds=self.bounds
        root=self.fixture.fixture.root
        for role in self.view['tables']:(root/role).mkdir()
        self.transport.targets=MappingProxyType({t:DeltaTarget(t,root/r,'uuid-'+r)for r,t in self.view['tables'].items()})
        self.transport.original_history=lambda target:[{'version':2},{'version':1},{'version':0}]
        schemas=original_local_physical_carriers()['carriers']
        def snapshot(target,version,*,remaining_bytes):
            self.calls.append((target.table,remaining_bytes));role=target.table.split('.')[-1];rows=sorted(self.view['expected'].get(role,[]),key=encoded)
            return {'rows':rows,'row_sha256':hashlib.sha256(encoded(rows).encode()).hexdigest()},json.loads(schemas[role]['schemaJson'])
        self.transport.bounded_snapshot=snapshot
        self.factory=original_medical_local_read_sources(sources.Tests().selection())
        self.policy=ExistingOutboxAuthority((self.factory.registration,),SimpleNamespace(admit_source_writer=lambda *args:None))
        self.owner=MedicalReadObservation(self.plan,self.transport,self.bounds,self.factory,self.policy,lambda:None,lambda t:{'location':'file:'+str(t.path)})
        self.row={'table_versions_json':encoded({self.view['tables'][r]:2 for r in self.view['expected']})}
    def test_all_six_complete_schema_and_four_original_bags_cumulative_closing(self):
        observed=self.owner.capture(self.row,object());self.assertEqual(len(observed),6);self.assertEqual(len(self.calls),12)
        self.assertEqual({r:observed[r]['rows']for r in self.view['expected']},{'object_current':51,'edge_current':62,'tombstone':0,'whole_source_history':113})
        for half in [self.calls[:6],self.calls[6:]]:self.assertTrue(all(a[1]>b[1]for a,b in zip(half,half[1:])))
    def test_schema_drift_refuses_and_attempts_every_closing_role(self):
        original=self.transport.bounded_snapshot
        def drift(target,version,**kw):
            snapshot,schema=original(target,version,**kw)
            if target.table.endswith('attempts'):schema['fields'][0]['metadata']={'drift':True}
            return snapshot,schema
        self.transport.bounded_snapshot=drift
        with self.assertRaises(PermissionError):self.owner.capture(self.row,object())
        self.assertEqual(set(t for t,b in self.calls[-6:]),set(self.transport.targets))
    def test_cancel_primary_preserved_all_six_closings(self):
        primary=KeyboardInterrupt('primary');original=self.transport.bounded_snapshot;count=[0]
        def fail(target,version,**kw):
            count[0]+=1
            if count[0]==1:raise primary
            if count[0]==2:raise OSError('secondary')
            return original(target,version,**kw)
        self.transport.bounded_snapshot=fail
        with self.assertRaises(KeyboardInterrupt)as caught:self.owner.capture(self.row,object())
        self.assertIs(caught.exception,primary);self.assertEqual(count[0],7)
    def test_resolver_full_vector_shares_budget_and_unknown_or_repeated_refuses(self):
        for role in self.view['expected']:self.owner.snapshot(self.view['tables'][role],2)
        self.assertTrue(all(a[1]>b[1]for a,b in zip(self.calls,self.calls[1:])))
        table=self.view['tables']['object_current'];self.owner.snapshot(table,2)
        with self.assertRaises(PermissionError):self.owner.snapshot(table,2)
        with self.assertRaises(PermissionError):self.owner.snapshot(self.view['tables']['attempts'],2)
    def test_public_driver_binding_requires_registered_read_gate_and_exact_plan(self):
        import sqlite3
        from ashlar_host.driver import NativeDriver
        from ashlar_host.original_medical_local_read import MedicalReadPolicy
        from ashlar.native import SQLResult
        root=self.fixture.fixture.root;journal=root/'read.sqlite';db=sqlite3.connect(journal)
        db.execute('CREATE TABLE local_installation(id INTEGER,original TEXT)');registry={'targets':{t:x.uuid for t,x in self.transport.targets.items()}}
        db.execute('INSERT INTO local_installation VALUES(1,?)',(encoded(registry),));db.commit();self.addCleanup(db.close)
        self.transport.db=db;self.transport.journal_path=journal;self.transport.reservation=encoded({'registry':registry})
        for target in self.transport.targets.values():(target.path/'_delta_log').mkdir()
        self.transport.query=lambda sql,args:SQLResult([{'id':self.transport.targets[sql.split(' ',2)[2].replace('`','')].uuid,'location':'file:'+str(self.transport.targets[sql.split(' ',2)[2].replace('`','')].path)}])
        self.transport._detail=lambda t:{'id':t.uuid,'location':'file:'+str(t.path)}
        (root/'.pipeline-writer.lock').touch()
        d=object.__new__(NativeDriver);d.transport=self.transport;d.context=object();d.held=False;d.pin_held=False;d.source_sessions=None
        d.source_admission=self.plan.admission;d.original_admission=encoded(d.source_admission.metadata());d.tables=dict(self.view['tables']);d.columns=self.view['columns']
        manifest=dict(self.row,publication_id=self.view['publication_id'],source_progress_json=encoded(self.view['progress']))
        active={k:self.view[v]for k,v in [('request','request'),('expected','expected'),('publication_id','publication_id'),('progress','progress'),('previous_progress','previous_progress'),('previous_expected','previous_expected')]};active['manifest']=manifest
        d.policy=MedicalReadPolicy(d.context,tuple(self.transport.targets.values()),active,lambda:None)
        r=self.factory.registration;d.scope_ports={r.source.scope.feed:{'scope':r.source.scope,'source':r.source.source_schema,'signature':r.source.source_signature_sha256,'reader':r.source_role,'role':r.ack_role}}
        with self.assertRaises(PermissionError):d.bind_original_medical_read(self.plan,self.bounds)
        d.bind_query_sources(self.factory);d.bind_original_medical_read(self.plan,self.bounds)
        with self.assertRaises(PermissionError):d.bind_original_medical_read(self.plan,self.bounds)
        with d.query_read_interval(d.context):
            d.pin_held=True
            d.inspect_snapshot(self.view['tables']['object_current'],2)
            with self.assertRaises(PermissionError):d.inspect_snapshot(self.view['tables']['edge_current'],1)
            d.policy.active['request']=dict(self.view['request'],request_digest='b'*64)
            with self.assertRaises(PermissionError):d._require_original_medical_read_binding()
            d.policy.active['request']=self.view['request'];d.pin_held=False
    def test_ack_exact_original_bytes_and_fresh_session_admission_cleanup(self):
        from ashlar_host.ack import receipt_bytes
        from ashlar_host.postgres import Session
        events=[];request=encoded(self.view['request']).encode();manifest=encoded({'publication_id':self.view['publication_id']}).encode()
        _,_,checkpoint,receipt=receipt_bytes(self.factory.registration.source.scope,request,manifest)
        row={'position':checkpoint['position'],'request_hex':request.hex(),'manifest_hex':manifest.hex(),'receipt_hex':receipt.hex()}
        connection=SimpleNamespace(autocommit=False,rollback=lambda:events.append('rollback'),close=lambda:events.append('close'))
        object.__setattr__(self.factory.registration,'metadata_factory',lambda context:connection)
        def admit(scope,session,context):
            self.assertIs(type(session),Session);self.assertIs(session.connection,connection);events.append('admit')
        object.__setattr__(self.owner.source_policy,'admit_scope',admit)
        self.owner._selected=(self.owner.plan,self.owner.transport,self.owner.bounds,self.owner.factory,self.owner.source_policy,self.owner.renew,self.owner.native_detail)
        def query(session,sql,args):
            self.assertTrue(sql.startswith('SELECT * FROM'));self.assertNotIn('.ack(',sql);events.append('select');return SimpleNamespace(rows=[row])
        with patch.object(Session,'query',query):self.assertEqual(self.owner.ack(request,manifest,object()),row)
        self.assertEqual(events,['admit','select','admit','admit','select','rollback','close'])
        events.clear()
        with patch.object(Session,'query',query),self.assertRaises(PermissionError):self.owner.ack(request+b'\n',manifest,object())
        self.assertEqual(events[-2:],['rollback','close'])
    def test_ack_fresh_admission_failure_refuses_select_and_closes(self):
        from ashlar_host.postgres import Session
        events=[];primary=KeyboardInterrupt('ordinary opening');connection=SimpleNamespace(autocommit=False,rollback=lambda:events.append('rollback'),close=lambda:events.append('close'))
        object.__setattr__(self.factory.registration,'metadata_factory',lambda context:connection)
        def admit(*args):events.append('admit');raise primary
        object.__setattr__(self.owner.source_policy,'admit_scope',admit);self.owner._selected=(self.owner.plan,self.owner.transport,self.owner.bounds,self.owner.factory,self.owner.source_policy,self.owner.renew,self.owner.native_detail)
        with patch.object(Session,'query')as query,self.assertRaises(KeyboardInterrupt)as caught:
            self.owner.ack(encoded(self.view['request']).encode(),encoded({'publication_id':self.view['publication_id']}).encode(),object())
        self.assertIs(caught.exception,primary);query.assert_not_called();self.assertEqual(events[-2:],['rollback','close'])
    def test_nominal_and_replaced_owners_refuse(self):
        with self.assertRaises(ValueError):MedicalReadObservation(self.plan,object(),self.bounds,self.factory,SimpleNamespace(),lambda:None,lambda t:{})
        self.owner.factory=object()
        with self.assertRaises(PermissionError):self.owner.capture(self.row,object())

if __name__=='__main__':unittest.main()
