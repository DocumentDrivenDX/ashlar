"""Real core sessions/SQLite, original finite semantics and inert native owners."""
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,json,unittest
import test_evolution_run as harness
from ashlar.attempt_store import DeltaAttemptStore,AttemptStoreError,PHASES
from ashlar.phase_read import PhaseReadRequest
from ashlar_host.driver import AttemptExecutor,CarrierPolicy,encoded
from ashlar_host.delta_custody import LocalDeltaTransport,LocalDeltaEffects
from ashlar_host.phase_absence import PhaseAbsenceOwner
from ashlar_host.immutable_snapshot_memo import immutable_snapshot_memo
from ashlar_host.evolution_run import EvolutionRunLedgerView
from ashlar_host.evolution_composition import OriginalRunCorrespondence
from ashlar_host.ack_sessions import RegisteredOutboxAcks


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):harness.Tests.setUpClass()
    def setUp(self):
        self.host=harness.Tests();self.host.setUp();self.addCleanup(self.host.doCleanups)
        self.driver=self.host.driver;self.driver.held=True;self.context=self.driver.context
        real=object.__new__(LocalDeltaTransport);real.__dict__.update(vars(self.driver.transport));self.driver.transport=real;self.transport=real
        self.original=self.driver.capture_evolution_run(self.host.request,context=self.context)
        self.correspondence=OriginalRunCorrespondence(self.original,self.driver.source_admission)
        self.facts=[self.correspondence.at(i)for i in range(8)];self.table=self.driver.tables['attempts'];self.target=real.targets[self.table]
        real.db.execute('CREATE TABLE phase_rows(stream TEXT,batch_id TEXT,phase TEXT,request_digest TEXT,payload_json TEXT,payload_digest TEXT)')
        self.queries=[];self.identities=[];self.writer_calls=[];self.candidates=[];outer=self
        original_detail=real._detail
        real._detail=lambda target:(self.identities.append(target.table),original_detail(target))[1]
        def query(sql,parameters):
            outer.queries.append(sql)
            if sql.startswith('DESCRIBE DETAIL'):
                real._detail(outer.target);return SimpleNamespace(rows=[{'id':outer.target.uuid}])
            adapted=sql.replace('`'+'`.`'.join(outer.table.split('.'))+'`','phase_rows')
            cursor=real.db.execute(adapted,parameters);return SimpleNamespace(rows=[dict(zip([d[0]for d in cursor.description],r))for r in cursor.fetchall()])
        real.query=query
        class Frame:
            schema=SimpleNamespace(fields=[SimpleNamespace(name=n,dataType=SimpleNamespace(simpleString=lambda:'string'))for n in ('stream','batch_id')])
            def __init__(self,rows):self.rows=rows
        def spark_sql(sql,args):
            outer.candidates.append(sql)
            assert ' VERSION AS OF 0 'in sql
            prefix='delta.`'+str(outer.target.path)+'` VERSION AS OF 0'
            cursor=real.db.execute(sql.replace(prefix,'phase_rows'),args)
            return Frame([dict(zip(('stream','batch_id'),r))for r in cursor.fetchall()])
        real.spark=SimpleNamespace(sql=spark_sql);real._bounded_native_rows=lambda frame,maximum_rows:frame.rows
        self.inventory=EvolutionRunLedgerView(tuple((i,'unprepared',None,None,None)for i in range(8)))
    def store(self,request,owner):
        outer=self
        class Policy:
            @contextmanager
            def writer(self,table,uuid,context):
                if context is not outer.context:raise PermissionError('Current context')
                outer.writer_calls.append('enter')
                try:yield
                finally:outer.writer_calls.append('exit')
        return DeltaAttemptStore(AttemptExecutor(self.driver,phase_absence=owner),Policy(),self.table,self.target.uuid,original_request=request)
    def read(self,index,owner):
        request=self.facts[index]['request']
        with self.store(request,owner).session(self.context)as session:return session.read(request['stream'],request['batch_id'])
    def owner(self):return PhaseAbsenceOwner(self.driver,self.original,self.inventory,self.correspondence,self.context)
    def test_component_five_core_sessions_one_zero_query_without_gate_changes(self):
        for mode in ('raw','memo'):
            self.queries.clear();self.candidates.clear();self.writer_calls.clear()
            with immutable_snapshot_memo(self.transport):
                owner=self.owner()if mode=='memo'else None
                try:
                    for i in range(3,8):self.assertEqual(self.read(i,owner),())
                finally:
                    if owner:owner.close()
            self.assertEqual(sum(q.startswith('SELECT phase,')for q in self.queries),5 if mode=='raw'else 0)
            self.assertEqual(len(self.candidates),0 if mode=='raw'else 1)
            self.assertEqual(sum(q.startswith('DESCRIBE DETAIL')for q in self.queries),10)
            self.assertEqual(self.writer_calls,['enter','exit']*5)
    def test_any_foreign_scope_row_falls_back_and_malformed_foreign_refuses(self):
        from ashlar.source import jsonl_batches
        from ashlar.outbox import OutboxTransaction
        from ashlar_host.driver import request_for
        request=self.facts[3]['request'];batch=self.driver.source_admission.admissions[1].prepared.batches[1]
        raw=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit
        foreign_batch,=jsonl_batches(raw.splitlines(keepends=True),feed='foreign',epoch='foreign-epoch')
        tx=OutboxTransaction('ashlar-postgresql-outbox/0.1','foreign','foreign-epoch','1','2',hashlib.sha256(raw).hexdigest(),foreign_batch)
        foreign=request_for(request['stream'],tx,'other-predecessor',{'foreign':'revision'})
        self.assertEqual(foreign['batch_id'],request['batch_id'])
        payload=encoded({'request':foreign,'result_json':None,'descriptor_json':None})
        self.transport.db.execute('INSERT INTO phase_rows VALUES(?,?,?,?,?,?)',(request['stream'],request['batch_id'],'prepared',foreign['request_digest'],payload,hashlib.sha256(payload.encode()).hexdigest()))
        with immutable_snapshot_memo(self.transport):
            owner=self.owner()
            try:
                self.assertEqual(self.read(3,owner),()) # Valid foreign group is ignored by current scope.
                self.assertTrue(owner.disabled)
                self.transport.db.execute("UPDATE phase_rows SET payload_digest='bad'")
                with self.assertRaises(AttemptStoreError):self.read(3,owner)
                self.assertEqual(self.read(4,owner),())
            finally:owner.close()
        self.assertEqual(len(self.candidates),1);self.assertEqual(sum(q.startswith('SELECT phase,')for q in self.queries),3)
    def test_full_eight_slot_driver_delegation_closes_owner_before_history_exit(self):
        calls=[];actual=PhaseAbsenceOwner.close
        def closing(owner):calls.append(owner);actual(owner)
        with patch.object(PhaseAbsenceOwner,'close',closing):
            self.driver.admit_evolution_run_ledger(self.original,self.inventory,context=self.context,
                attempt_admission=SimpleNamespace(admit=lambda *args:None))
        self.assertEqual(len(self.candidates),1)
        self.assertEqual(sum(q.startswith('SELECT phase,')for q in self.queries),0)
        self.assertEqual(sum(q.startswith('DESCRIBE DETAIL')for q in self.queries),16)
        self.assertEqual(len(calls),1);self.assertFalse(calls[0].active);self.assertEqual(calls[0].requests,{})
        self.assertIsNone(self.transport._immutable_snapshot_memo)
        # Retained/completed keys never use the component, irrespective of payload size.
        slots=list(self.inventory.slots)
        for i,state in ((0,'completed'),(1,'completed'),(2,'retained')):
            slots[i]=(i,state,b'inert retained plan','inert',b'inert descriptor'if state=='completed'else None)
        self.inventory=EvolutionRunLedgerView(tuple(slots));self.candidates.clear();self.queries.clear()
        with immutable_snapshot_memo(self.transport):
            owner=self.owner()
            try:
                for i in range(3):self.read(i,owner)
                for i in range(3,8):self.read(i,owner)
            finally:owner.close()
        self.assertEqual(len(self.candidates),1);self.assertEqual(sum(q.startswith('SELECT phase,')for q in self.queries),3)

    def test_core_session_nominality_context_port_and_carrier_refuse(self):
        request=self.facts[3]['request']
        with self.assertRaises(AttemptStoreError):PhaseReadRequest(object(),self.context,self.table,self.target.uuid,request['stream'],request['batch_id'],encoded(request))
        store=self.store(request,None);store.executor.observe_phase_absence=None
        with store.session(self.context)as session:
            with self.assertRaises(AttemptStoreError):session.read(request['stream'],request['batch_id'])
        store=self.store(request,None);store.executor.observe_phase_absence=lambda *a,**k:True
        with store.session(self.context)as session:
            with self.assertRaises(AttemptStoreError):session.read(request['stream'],request['batch_id'])
        captured=[]
        store=self.store(request,None)
        def capture(read,*,context):
            captured.append(read)
            with self.assertRaises(AttemptStoreError):read.renew(object())
            return None
        store.executor.observe_phase_absence=capture
        with store.session(self.context)as session:session.read(request['stream'],request['batch_id'])
        with self.assertRaises(AttemptStoreError):captured[0].renew(self.context)
    def test_changed_inventory_request_identity_and_closing_history_withhold(self):
        for control in ('inventory','request','uuid','history','cancel'):
            with self.subTest(control=control):
                original_slots=self.inventory.slots;history=self.transport.original_history
                with self.assertRaises((PermissionError,ValueError,KeyboardInterrupt)):
                    with immutable_snapshot_memo(self.transport):
                        owner=self.owner()
                        try:
                            self.read(3,owner)
                            if control=='inventory':object.__setattr__(self.inventory,'slots',self.inventory.slots[:-1]);self.read(4,owner)
                            elif control=='request':owner.requests[next(iter(owner.requests))]='changed';self.read(4,owner)
                            elif control=='uuid':self.target.uuid='changed';self.read(4,owner)
                            elif control=='history':self.transport.original_history=lambda target:[{'version':99}]
                            else:
                                self.transport.original_history=lambda target:[{'version':99}];raise KeyboardInterrupt()
                        finally:owner.close()
                object.__setattr__(self.inventory,'slots',original_slots);self.transport.original_history=history;self.target.uuid='uuid-attempts'
        self.assertIsNone(self.transport._immutable_snapshot_memo)
    def test_capacity_closed_owner_and_noneligible_keys_use_original_reads(self):
        with immutable_snapshot_memo(self.transport)as memo:
            owner=self.owner()
            with patch.object(memo,'_fits',return_value=False):self.assertEqual(self.read(3,owner),())
            owner.close();self.assertEqual(self.read(4,owner),())
        self.assertEqual(len(self.candidates),0);self.assertEqual(sum(q.startswith('SELECT phase,')for q in self.queries),2)
        self.queries.clear()
        original_at=self.correspondence.at
        def duplicate_at(correspondence,ordinal):
            facts=original_at(ordinal)
            if ordinal==4:
                facts=dict(facts);facts['request']=self.facts[3]['request']
            return facts
        with patch.object(OriginalRunCorrespondence,'at',duplicate_at):
            with immutable_snapshot_memo(self.transport):
                owner=self.owner()
                try:
                    self.assertTrue(owner.disabled);self.assertEqual(owner.requests,{})
                    self.assertEqual(self.read(3,owner),());self.assertEqual(self.read(4,owner),())
                finally:owner.close()
        self.assertEqual(len(self.candidates),0);self.assertEqual(sum(q.startswith('SELECT phase,')for q in self.queries),2)
