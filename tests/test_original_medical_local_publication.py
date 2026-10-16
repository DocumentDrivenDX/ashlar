"""Composition controls; actual shared phases, inert native/ordinary lower ports."""
from contextlib import contextmanager
import json
import os
from pathlib import Path
from types import SimpleNamespace
import tempfile
import hashlib
import sqlite3
from dataclasses import replace
import unittest
from unittest.mock import patch
from ashlar.publisher import PublicationError
from ashlar_host.evolution_verification_types import VerificationBounds
import ashlar_host.original_medical_local_publication as owner
import test_stored_publisher as shared


class Tests(unittest.TestCase):
    def setUp(self):
        self.bounds=VerificationBounds(262144,10000,1048576,16777216,1048576,1048576)

    def test_settled_shared_phase_replay_never_applies_or_appends(self):
        fixture=shared.StoredPublisherTests()
        with fixture.fixture() as (phases,driver,policy,transport,api):
            context=object();backend=fixture.backend(phases,driver,transport,policy)
            descriptor=fixture.publish(backend,context)
            rows=list(phases.rows);posts=list(api.calls);calls=list(driver.calls)
            settled=owner._SettledBackend(backend.attempts,driver,backend.manifest_factory)
            self.assertEqual(fixture.publish(settled,context),descriptor)
            self.assertEqual(phases.rows,rows);self.assertEqual(api.calls,posts)
            self.assertEqual(driver.calls[len(calls):],['ack'])

    def test_settled_missing_and_prepared_refuse_before_recovery(self):
        fixture=shared.StoredPublisherTests()
        for partial in (False,True):
            with fixture.fixture() as (phases,driver,policy,transport,api):
                backend=fixture.backend(phases,driver,transport,policy);context=object()
                if partial:
                    driver.fail='apply'
                    with self.assertRaises(RuntimeError):fixture.publish(backend,context)
                rows=list(phases.rows);calls=list(driver.calls)
                settled=owner._SettledBackend(backend.attempts,driver,backend.manifest_factory)
                with self.assertRaises(PublicationError):fixture.publish(settled,context)
                self.assertEqual(phases.rows,rows);self.assertEqual(driver.calls,calls)

    def test_actual_adapter_observes_before_ack_and_pinned_manifest_refuses(self):
        events=[];request={'request_digest':'original'};descriptor=SimpleNamespace(raw={'publication_id':'original'})
        plan=SimpleNamespace(require_request=lambda value:events.append('request'))
        driver=SimpleNamespace(policy=SimpleNamespace(active={}),observe_original_medical_publication=lambda *a,**k:events.append('observe'),acknowledge=lambda *a:events.append('ack'))
        adapter=owner._MedicalPublisherDriver(driver,plan,self.bounds,owner._bounded(dict(descriptor.raw),262144))
        adapter.acknowledge(request,descriptor,object());self.assertEqual(events,['request','observe','ack'])
        events.clear();descriptor.raw['publication_id']='replacement'
        with self.assertRaises(PermissionError):adapter.acknowledge(request,descriptor,object())
        self.assertEqual(events,['request'])

    def test_retained_adapter_activates_only_exact_original_manifest(self):
        from ashlar.stored_publisher import validate_applied_artifact
        fixture=shared.StoredPublisherTests()
        with fixture.fixture()as(phases,source_driver,policy,transport,api):
            context=object();backend=fixture.backend(phases,source_driver,transport,policy)
            descriptor=fixture.publish(backend,context)
            request=json.loads(phases.rows[-1]['payload_json'])['request'];artifact=source_driver.original
            events=[];source_port=object()
            def require(supplied):
                if supplied is not context:raise PermissionError('Original context required')
            def original(value):
                if value!=request:raise PermissionError('Original request required')
            def validate(value,text,current,supplied):
                require(supplied);self.assertIs(driver.source_port,source_port)
                self.assertEqual(driver.policy.active['manifest'],dict(descriptor.raw))
                self.assertEqual((value,text,current),(request,artifact,descriptor));events.append('full-validation')
            driver=SimpleNamespace(require=require,validate=validate,policy=SimpleNamespace(active={}),source_port=source_port)
            adapter=owner._MedicalPublisherDriver(driver,SimpleNamespace(require_request=original),self.bounds)
            adapter.validate(request,artifact,descriptor,context)
            self.assertEqual(events,['full-validation']);self.assertEqual(driver.policy.active['manifest'],dict(descriptor.raw))
            for kind in ('request','artifact','descriptor','active','context'):
                driver.policy.active.clear();supplied_request=dict(request);supplied_artifact=artifact;supplied_descriptor=descriptor;supplied_context=context
                if kind=='request':supplied_request['request_digest']='f'*64
                elif kind=='artifact':
                    foreign=json.loads(artifact);foreign['manifest']['publication_id']='foreign';supplied_artifact=json.dumps(foreign)
                elif kind=='descriptor':supplied_descriptor=replace(descriptor,versions={'c.s.object_current':True})
                elif kind=='active':driver.policy.active['manifest']={'publication_id':'foreign'}
                else:supplied_context=object()
                with self.assertRaises((PermissionError,ValueError)):adapter.validate(supplied_request,supplied_artifact,supplied_descriptor,supplied_context)
                self.assertEqual(events,['full-validation'])
                self.assertEqual(driver.policy.active,{'manifest':{'publication_id':'foreign'}}if kind=='active'else{})

    def test_adapter_observer_cancellation_preserved_zero_ack(self):
        primary=KeyboardInterrupt('original');calls=[]
        def observe(*a,**k):calls.append('observe');raise primary
        driver=SimpleNamespace(policy=SimpleNamespace(active={}),observe_original_medical_publication=observe,acknowledge=lambda *a:calls.append('ack'))
        adapter=owner._MedicalPublisherDriver(driver,SimpleNamespace(require_request=lambda request:None),self.bounds)
        with self.assertRaises(KeyboardInterrupt) as caught:adapter.acknowledge({},SimpleNamespace(raw={}),object())
        self.assertIs(caught.exception,primary);self.assertEqual(calls,['observe'])

    def test_adapter_writer_closing_does_not_mask_body_cancellation(self):
        primary=GeneratorExit('original');events=[]
        @contextmanager
        def writer(*a):
            try:yield
            finally:events.append('closed');raise OSError('secondary')
        adapter=owner._MedicalPublisherDriver(SimpleNamespace(writer=writer),None,self.bounds)
        with self.assertRaises(GeneratorExit) as caught:
            with adapter.writer('original',object()):raise primary
        self.assertIs(caught.exception,primary);self.assertEqual(events,['closed'])

    def test_fresh_capture_is_cumulative_and_all_six_closing_attempted(self):
        targets=tuple(SimpleNamespace(table='local.original_medical.'+role) for role in owner.ROLES)
        schemas=owner.original_local_physical_carriers()['carriers'];calls=[];closing=False
        def snapshot(target,version,remaining_bytes):
            calls.append((target.table,remaining_bytes))
            return {'rows':[]},json.loads(schemas[target.table.split('.')[-1]]['schemaJson'])
        transport=SimpleNamespace(db=SimpleNamespace(execute=lambda *a:SimpleNamespace(fetchone=lambda:None)),original_history=lambda target:[{'version':1},{'version':0}],bounded_snapshot=snapshot)
        owner._fresh(SimpleNamespace(transport=transport),SimpleNamespace(targets=targets),self.bounds)
        self.assertEqual(len(calls),12)
        self.assertTrue(all(calls[n][1]>calls[n+1][1] for n in range(5)))
        primary=KeyboardInterrupt('opening');calls.clear();seen=0
        def fail(target,version,remaining_bytes):
            nonlocal seen
            seen+=1;calls.append(target.table)
            if seen==1:raise primary
            if seen==2:raise OSError('first closing')
            return {'rows':[]},json.loads(schemas[target.table.split('.')[-1]]['schemaJson'])
        transport.bounded_snapshot=fail
        with self.assertRaises(KeyboardInterrupt) as caught:owner._fresh(SimpleNamespace(transport=transport),SimpleNamespace(targets=targets),self.bounds)
        self.assertIs(caught.exception,primary);self.assertEqual(calls[1:],[target.table for target in targets])

    def test_output_current_custody_zero_partial_write_and_close(self):
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve();root.chmod(0o700)
            config=SimpleNamespace(output=root/'output',bounds=SimpleNamespace(input_bytes=262144),__post_init__=lambda:None)
            # The public config check is separately exercised before this owner.
            with patch.object(owner,'_plain',return_value={'fixed':'selection'}):
                output=owner._Output(config);output.acquire()
                output.write('original.json',b'{}\n',262144)
                (config.output/'original.json').write_bytes(b'changed')
                with self.assertRaises((ValueError,PermissionError)):output.renew()
                output.close();self.assertIsNone(output.fd)
                second=owner._Output(SimpleNamespace(output=root/'second',bounds=config.bounds,__post_init__=lambda:None));second.acquire()
                try:
                    with patch.object(owner.os,'write',return_value=0),self.assertRaises(OSError):second.write('partial.json',b'{}\n',262144)
                    self.assertEqual(second.files,[])
                finally:second.close()

    def test_bounded_output_encoding_refuses_before_write(self):
        with self.assertRaises(ValueError):owner._bounded({'payload':'x'*100},16)
        self.assertEqual(owner._bounded({'a':None},32),b'{"a":null}\n')

    def test_public_entries_refuse_wrong_nominal_config_before_resources(self):
        with patch.object(owner,'OriginalMedicalRuntimeCustody') as runtime:
            for operation in (owner.publish_original_medical_local,owner.replay_original_medical_local):
                with self.assertRaises(ValueError):operation(object())
            runtime.open.assert_not_called()

    def installation(self):
        import test_original_medical_runtime as runtime
        import test_original_medical_installation_handoff as handoff
        from ashlar.medical_source import UMF_PIN
        r=runtime.Tests();r.setUp();self.addCleanup(r.doCleanups)
        h=handoff.Tests();h.setUp();self.addCleanup(h.doCleanups)
        h.config.output.parent.chmod(0o700)
        raw,config=h.selected()
        config=replace(config,runtime=r.runtime,inputs=r.inputs,producer=r.producer,bounds=r.bounds)
        config.installation.path.parent.mkdir(parents=True,exist_ok=True);config.installation.path.write_bytes(raw)
        selection=owner.validate_original_medical_installation_handoff(raw,config)
        registry=owner.encoded({'installation':selection.installation_id,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in sorted(selection.targets,key=lambda t:t.table)],'operation_capacity':dict(selection.capacity)})
        reservation=owner.encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(registry),'original_journal_path':str(selection.journal)})+'\n'
        for t in selection.targets:(t.path/'.ashlar-local-installation-reservation.json').write_text(reservation)
        db=sqlite3.connect(selection.journal)
        try:
            for sql in ('CREATE TABLE local_installation(id INTEGER PRIMARY KEY CHECK(id=1), original TEXT NOT NULL)','CREATE TABLE local_operation(operation TEXT PRIMARY KEY,intent TEXT NOT NULL,intent_sha TEXT NOT NULL,before_version INTEGER NOT NULL,state TEXT NOT NULL,receipt TEXT)','CREATE TABLE local_plan(operation TEXT PRIMARY KEY,original TEXT NOT NULL,digest TEXT NOT NULL)','CREATE TABLE local_publication_artifact (request_digest TEXT PRIMARY KEY,request TEXT NOT NULL,artifact TEXT NOT NULL)','CREATE TABLE local_source_plan (request_digest TEXT PRIMARY KEY,original TEXT NOT NULL)'):db.execute(sql)
            db.execute('INSERT INTO local_installation VALUES(1,?)',(registry,));db.commit()
        finally:db.close()
        baseline=json.loads(r.inputs.baseline.path.read_bytes())
        originals=tuple(pin.path.read_bytes()for pin in (r.inputs.model,r.inputs.graph,r.inputs.baseline))
        def capture(argv,**kwargs):
            if argv[0]==str(config.producer.git):return ((UMF_PIN+'\n').encode()if argv[-1]=='HEAD'else b''),b''
            result={'profile':'ashlar-original-medical-public-revalidation/0.1','umfRevision':UMF_PIN,'modelSha256':hashlib.sha256(originals[0]).hexdigest(),'graphSha256':hashlib.sha256(originals[1]).hexdigest(),'baselineSha256':hashlib.sha256(originals[2]).hexdigest(),'receipt':baseline['receipt'],'booleanReceipts':baseline['booleanReceipts']}
            Path(argv[3]).write_bytes(json.dumps(result,separators=(',',':')).encode());return b'',b''
        return config,selection,capture

    def test_actual_runtime_lease_and_admission_native_open_cancel_no_source_or_receipt(self):
        config,selection,capture=self.installation()
        primary=KeyboardInterrupt('native acquisition');events=[]
        @contextmanager
        def native(custody):
            events.append('native-opening');raise primary;yield
        with patch('ashlar_host.medical_producer.capture',side_effect=capture),patch.object(owner,'original_medical_native_session',side_effect=native),patch.object(owner,'original_medical_local_source_ports') as source:
            with self.assertRaises(KeyboardInterrupt) as caught:owner.publish_original_medical_local(config)
        self.assertIs(caught.exception,primary);source.assert_not_called();self.assertEqual(events,['native-opening'])
        self.assertFalse((config.output/'receipt.json').exists())
        # The actual POSIX lease is released despite session acquisition failure.
        from ashlar_host.medical_installation import OriginalMedicalInstallationLease
        with OriginalMedicalInstallationLease.open(selection,object()):pass

    def test_driver_constructor_cancellation_closes_all_six_with_cumulative_budget(self):
        config,selection,capture=self.installation();events=[];remaining=[]
        primary=KeyboardInterrupt('driver construction');original=owner.LocalDeltaTransport
        @contextmanager
        def native(custody):
            try:yield object()
            finally:events.append('native-close')
        def construct(spark,journal,installation,targets,policy,**kwargs):
            transport=original.__new__(original);transport.db=sqlite3.connect(journal)
            transport.journal_path=journal;transport.targets={target.table:target for target in targets}
            transport.reservation=owner.encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(transport.db.execute('SELECT original FROM local_installation').fetchone()[0]),'original_journal_path':str(journal)})+'\n'
            @contextmanager
            def interval(bounds):yield
            transport.bounded_observations=interval
            transport.original_history=lambda target:[{'version':1}]
            def snapshot(target,version,*,remaining_bytes):
                events.append(target.table);remaining.append(remaining_bytes)
                return {'rows':[]},{'fields':[]}
            transport.bounded_snapshot=snapshot
            transport.close=lambda:(events.append('transport-close'),transport.db.close())
            return transport
        class Source:
            def __init__(self,*args):pass
            def sources(self):return object()
            def bind(self,*args):raise AssertionError('No ACK binding after constructor failure')
            def append_original(self,*args,**kwargs):raise AssertionError('No append after constructor failure')
        with patch('ashlar_host.medical_producer.capture',side_effect=capture),patch.object(owner,'original_medical_native_session',side_effect=native),patch.object(owner,'LocalDeltaTransport',side_effect=construct),patch.object(owner,'OriginalMedicalSourceOwner',Source),patch.object(owner,'original_medical_local_source_ports',return_value=object()),patch.object(owner.NativeDriver,'with_registered_evolution',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt) as caught:owner.publish_original_medical_local(config)
        self.assertIs(caught.exception,primary)
        self.assertEqual(events[:-2],[target.table for target in selection.targets])
        size=len(owner.encoded({'rows':[]}).encode())+len(owner.encoded({'fields':[]}).encode())
        self.assertEqual(remaining,[config.bounds.total_bytes-index*size for index in range(6)])
        self.assertEqual(events[-2:],['transport-close','native-close'])
        self.assertFalse((config.output/'receipt.json').exists())

    def test_selected_lower_journal_budget_refuses_before_native_or_source(self):
        config,selection,capture=self.installation()
        config=replace(config,bounds=replace(config.bounds,journal_bytes=1))
        with patch('ashlar_host.medical_producer.capture',side_effect=capture),patch.object(owner,'original_medical_native_session') as native,patch.object(owner,'original_medical_local_source_ports') as source:
            with self.assertRaises(PermissionError):owner.publish_original_medical_local(config)
        native.assert_not_called();source.assert_not_called()
        self.assertTrue(selection.journal.exists())
        self.assertFalse((config.output/'receipt.json').exists())

    def test_public_composition_actual_inputs_plan_and_lease_with_inert_native_delegation(self):
        from contextlib import ExitStack
        from ashlar.outbox import OutboxTransaction
        from ashlar.stored_publisher import validate_applied_artifact
        config,selection,capture=self.installation();events=[];holder={}
        @contextmanager
        def native(custody):
            events.append('native-open')
            try:yield object()
            finally:events.append('native-close')
        def transport(spark,journal,installation,targets,policy,**kwargs):
            result=original_transport.__new__(original_transport)
            result.db=sqlite3.connect(journal);result.journal_path=journal;result.targets={t.table:t for t in targets}
            result.reservation=(owner.encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(result.db.execute('SELECT original FROM local_installation').fetchone()[0]),'original_journal_path':str(journal)})+'\n')
            result.observation_bounds=None
            @contextmanager
            def bounded(bounds):
                result.observation_bounds=bounds
                try:yield
                finally:result.observation_bounds=None
            result.bounded_observations=bounded
            schemas=owner.original_local_physical_carriers()['carriers']
            result.bounded_snapshot=lambda target,version,**k:({'rows':[]},json.loads(schemas[target.table.split('.')[-1]]['schemaJson']))
            result.original_history=lambda target:[{'version':1},{'version':0}]
            result.close=lambda:(events.append('transport-close'),result.db.close())
            holder['transport']=result;return result
        class Source:
            def __init__(self,ports,lease):self.lease=lease
            def sources(self):return object()
            def bind(self,driver):return object()
            def append_original(self,batch,*,context):
                events.append('append');raw=batch.begin+b''.join(r.raw for r in batch.records)+batch.commit
                return OutboxTransaction('ashlar-postgresql-outbox/0.1',owner.FEED,'original-medical-v1','0','1',hashlib.sha256(raw).hexdigest(),batch)
            def read_original(self,batch,*,context):events.append('reader-closing')
            def renew_scope(self,context):events.append('ack-closing')
            def renew_original(self,batch,*,context):
                self.read_original(batch,context=context);self.renew_scope(context)
        def driver(transport,policy,context,tables,changes,columns,**kwargs):
            @contextmanager
            def writer(stream,supplied):
                self.assertIs(supplied,context);events.append('writer-enter')
                try:yield
                finally:events.append('writer-exit')
            d=SimpleNamespace(transport=transport,policy=policy,writer=writer)
            def bind(plan,*,bounds):
                self.assertIs(type(plan),owner.MedicalPublicationPlan);self.assertIs(plan.admission,kwargs['source_admission']);holder['plan']=plan;holder['bounds']=bounds;events.append('bind')
            def observe(plan,descriptor,**k):
                events.append('observe');view=plan.snapshot();return {'descriptor':dict(descriptor.raw),'bags':view['expected'],'schemas':view['schemas']}
            d.bind_original_medical_publication=bind;d.observe_original_medical_publication=observe;d.acknowledge=lambda *a:events.append('ack')
            return d
        def publish(backend,stream,transaction,**kwargs):
            events.append('shared-publisher');view=holder['plan'].snapshot();request=view['request']
            raw={'publication_id':view['publication_id'],'profile_version':'ashlar-delta/0.3','table_versions_json':owner.encoded({view['tables'][r]:2 for r in owner.ROLES[:4]}),'schema_revisions_json':request['schema_revisions_json'],'source_progress_json':owner.encoded(view['progress']),'validation_report_json':owner.encoded({'complete':True,'request_digest':request['request_digest']}),'recorded_at':view['recorded_at']}
            _,descriptor=validate_applied_artifact(owner.encoded({'effects':{},'manifest':raw}),request)
            with backend.driver.writer(stream,kwargs['context']):backend.driver.acknowledge(request,descriptor,kwargs['context'])
            return descriptor
        original_transport=owner.LocalDeltaTransport
        with ExitStack() as stack:
            stack.enter_context(patch('ashlar_host.medical_producer.capture',side_effect=capture))
            stack.enter_context(patch.object(owner,'original_medical_native_session',side_effect=native))
            # Keep exact nominal LocalDeltaTransport for actual lease verification.
            stack.enter_context(patch.object(original_transport,'__init__',side_effect=lambda *a,**k:None))
            def construct(*a,**k):return transport(*a,**k)
            stack.enter_context(patch.object(owner,'LocalDeltaTransport',side_effect=construct))
            stack.enter_context(patch.object(owner.NativeDriver,'with_registered_evolution',side_effect=driver))
            stack.enter_context(patch.object(owner,'OriginalMedicalSourceOwner',Source))
            stack.enter_context(patch.object(owner,'original_medical_local_source_ports',return_value=object()))
            stack.enter_context(patch.object(owner,'DeltaAttemptStore',return_value=object()))
            stack.enter_context(patch.object(owner,'publish_outbox_transaction',side_effect=publish))
            answer=owner.publish_original_medical_local(config)
        self.assertTrue(answer['closing']);self.assertEqual(events.count('append'),1);self.assertEqual(events.count('ack'),1)
        self.assertLess(events.index('bind'),events.index('shared-publisher'));self.assertLess(events.index('observe'),events.index('ack'))
        self.assertLess(events.index('transport-close'),events.index('native-close'))
        observed=(config.output/'observation.json').read_bytes();self.assertGreater(len(observed),1048576)
        self.assertEqual(answer['observation']['sha256'],hashlib.sha256(observed).hexdigest());self.assertLess((config.output/'receipt.json').stat().st_size,1048576)


if __name__=='__main__':unittest.main()
