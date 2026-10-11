"""Fresh initializer composition through actual custody; native/producer ports inert."""
from contextlib import contextmanager
from dataclasses import replace
import json,os,sys,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import test_original_medical_runtime as runtime_fixture
import test_original_medical_local_sources as source_fixture
from ashlar_host.original_medical_local_configuration import OriginalMedicalInitializationConfig
from ashlar_host.local_physical_carriers import original_local_physical_carriers
from ashlar_host.original_medical_local_setup import initialize_original_medical_local,ROLES
from ashlar_host.medical_process_supervision import OriginalMedicalNativePhase

class Row(dict):
    def asDict(self):return dict(self)
class Frame:
    def __init__(self,rows=(),schema=None):self.rows=list(rows);self.schema=SimpleNamespace(json=lambda:json.dumps(schema))
    def first(self):return self.rows[0]
    def select(self,*args):return self
    def limit(self,n):return Frame(self.rows[:n],json.loads(self.schema.json()))
    def collect(self):return self.rows
    def count(self):return len(self.rows)
class Spark:
    def __init__(self):self.tables={};self.fail_role=None;self.creation_cancel=KeyboardInterrupt('creation');self.closing=[];self.read=SimpleNamespace(format=lambda kind:SimpleNamespace(load=lambda path:Frame(schema=self.tables[path]['schema'])))
    def createDataFrame(self,rows,schema):
        def save(path):
            Path(path).mkdir();self.tables[path]={'schema':schema,'id':'00000000-0000-4000-8000-'+str(len(self.tables)+1).zfill(12),'properties':{},'history':[Row(version=0,userMetadata=None)]}
            if Path(path).name==self.fail_role:raise self.creation_cancel
        return SimpleNamespace(write=SimpleNamespace(format=lambda kind:SimpleNamespace(save=save)))
    def sql(self,statement):
        path=statement.split('`')[1];t=self.tables[path]
        if 'DETAIL'in statement:
            self.closing.append(Path(path).name)
            return Frame([Row(id=t['id'],location=Path(path).as_uri(),partitionColumns=[],properties=t['properties'],minReaderVersion=1,minWriterVersion=2)])
        return Frame(t['history'])
class Tests(unittest.TestCase):
    def test_selected_journal_budget_refuses_after_creation_and_closes_native(self):
        self.config=replace(self.config,bounds=replace(self.config.bounds,journal_bytes=1))
        with self.ports(),self.assertRaisesRegex(PermissionError,'journal main-file budget exceeded'):
            initialize_original_medical_local(self.config)
        self.assertIn('session-close',self.events)
        self.assertIn('transport-close',self.events)
        self.assertTrue((self.config.root/'operations.sqlite').exists())

    def test_supervision_refuses_foreign_owner_before_initialization(self):
        with self.assertRaises(ValueError):
            initialize_original_medical_local(self.config, supervision=object())
        self.assertFalse(self.config.root.exists());self.assertFalse(self.config.output.exists())
        self.assertFalse(self.events)

    def test_exact_supervision_reaches_actual_custody_native_boundary(self):
        read,write=os.pipe()
        self.addCleanup(os.close,read);self.addCleanup(os.close,write)
        os.set_blocking(write,False);phase=OriginalMedicalNativePhase(write)
        @contextmanager
        def selected(custody,*,phase):
            phase.begin(custody)
            with self.session(custody)as spark:yield spark
            phase.end(custody)
        with self.ports(),patch('ashlar_host.original_medical_local_setup.original_medical_native_session',side_effect=selected):
            initialize_original_medical_local(self.config,supervision=phase)
        self.assertEqual(os.read(read,32),b'native.begin\nnative.end\n')
        self.assertIn('session-close',self.events)

    def setUp(self):
        fixture=runtime_fixture.Tests();fixture.setUp();self.addCleanup(fixture.doCleanups);self.fixture=fixture
        schemas=fixture.make('generated.json',json.dumps(original_local_physical_carriers()).encode())
        self.config=OriginalMedicalInitializationConfig('ashlar-original-medical-local-initialization/0.1',fixture.root/'output',fixture.inputs,fixture.producer,fixture.runtime,source_fixture.Tests().selection(),fixture.bounds,fixture.root/'native','22222222-2222-4222-8222-222222222222',{'profile':'ashlar-private-local-operation-capacity/0.1','max_intent_bytes':8388608},schemas)
        self.spark=Spark();self.events=[];self.transport=None;self.cancel=None
    @contextmanager
    def session(self,custody):
        self.events.append('session-open')
        try:yield self.spark
        finally:self.events.append('session-close')
    def dataset(self,*args):
        self.events.append('producer');Path(args[-1]).write_bytes(b'inert producer retained bytes')
        return SimpleNamespace(renew=lambda:self.events.append('producer-renew'))
    def initialize(self,spark,journal,installation,targets,policy,*,context,capacity):
        intent={'profile':'ashlar-local-delta-installation/0.1','installation':installation,'targets':[{'table':t.table,'path':str(t.path),'uuid':t.uuid}for t in sorted(targets,key=lambda t:t.table)],'operation_capacity':dict(capacity)}
        with policy.writer('initialize:'+installation,context):self.assertIsNone(policy.admit(intent,context))
        Path(journal).write_bytes(b'inert original journal')
        transport=SimpleNamespace(installation_id=installation,registry_sha='a'*64,journal_sha='b'*64,close=lambda:self.events.append('transport-close'))
        for target in targets:
            t=spark.tables[str(target.path)];t['properties']={'ashlar.local.installation.id':installation,'ashlar.local.registry.sha256':transport.registry_sha,'ashlar.local.journal.sha256':transport.journal_sha}
            metadata=json.dumps({'profile':'ashlar-local-delta-installation-commit/0.1','installation_id':installation,'registry_sha256':transport.registry_sha,'journal_sha256':transport.journal_sha,'uuid':target.uuid},sort_keys=True,separators=(',',':'))
            t['history'].insert(0,Row(version=1,operation='SET TBLPROPERTIES',userMetadata=metadata))
        transport.original_history=lambda target:spark.tables[str(target.path)]['history'];self.transport=transport;self.events.append('initialize');return transport
    def ports(self):
        from contextlib import ExitStack
        stack=ExitStack();stack.enter_context(patch('ashlar_host.original_medical_local_setup.HeldMedicalDataset',side_effect=self.dataset));stack.enter_context(patch('ashlar_host.original_medical_local_setup.original_medical_native_session',side_effect=self.session));stack.enter_context(patch('ashlar_host.original_medical_local_setup.LocalDeltaTransport.initialize',side_effect=self.initialize));stack.enter_context(patch('ashlar_host.original_medical_local_setup.LocalDeltaTransport.require_existing_journal',side_effect=lambda *a,**k:self.events.append('journal-current')));stack.enter_context(patch.dict(sys.modules,{'pyspark.sql.types':SimpleNamespace(StructType=SimpleNamespace(fromJson=lambda schema:schema))}));return stack
    def test_actual_fresh_custody_six_creation_and_registration_handoff(self):
        with self.ports():result=initialize_original_medical_local(self.config)
        self.assertEqual(tuple(result['native']),ROLES);self.assertEqual([t['table']for t in result['targets']],['local.original_medical.'+r for r in ROLES]);self.assertLess(self.events.index('producer'),self.events.index('session-open'))
        for role in ROLES:self.assertEqual(result['native'][role]['creation']['version'],0);self.assertEqual(result['native'][role]['registration']['version'],1)
        self.assertIn('journal-current',self.events);self.assertIn('transport-close',self.events);self.assertIn('session-close',self.events)
    def test_existing_root_and_wrong_generated_schema_zero_native(self):
        self.config.root.mkdir()
        with self.ports(),self.assertRaises(ValueError):initialize_original_medical_local(self.config)
        self.assertFalse(self.events)
        self.config.root.rmdir();self.config.schemas.path.write_bytes(b'{}')
        with self.ports(),self.assertRaises((ValueError,PermissionError)):initialize_original_medical_local(self.config)
        self.assertFalse(self.events)
    def test_partial_creation_cancel_closes_all_attempted_targets_no_handoff(self):
        self.spark.fail_role='tombstone'
        with self.ports(),self.assertRaises(KeyboardInterrupt)as caught:initialize_original_medical_local(self.config)
        self.assertIs(caught.exception,self.spark.creation_cancel);self.assertIn('session-close',self.events);self.assertNotIn('initialize',self.events)
        self.assertGreaterEqual(self.spark.closing.count('object_current'),2);self.assertGreaterEqual(self.spark.closing.count('edge_current'),2);self.assertIn('tombstone',self.spark.closing)
        fd=os.open(self.config.root/'.evolution-operator.lock',os.O_RDWR)
        try:
            import fcntl
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(fd)
    def test_bad_registration_one_role_closing_does_not_skip_other_roles(self):
        initial=self.initialize
        def changed(*a,**k):
            transport=initial(*a,**k);self.spark.tables[str(self.config.root/ROLES[0])]['properties']={};return transport
        self.initialize=changed
        with self.ports(),self.assertRaises(ValueError):initialize_original_medical_local(self.config)
        for role in ROLES:self.assertGreaterEqual(self.spark.closing.count(role),2)
        self.assertIn('transport-close',self.events);self.assertIn('session-close',self.events)
    def test_cleanup_failure_withholds_result(self):
        initial=self.initialize
        def changed(*a,**k):
            t=initial(*a,**k);t.close=lambda:(_ for _ in ()).throw(OSError('close'));return t
        self.initialize=changed
        with self.ports(),self.assertRaises(OSError):initialize_original_medical_local(self.config)
        self.assertIn('session-close',self.events)

    def test_output_acquisition_cancellation_closes_runtime_and_retains_partial(self):
        primary=GeneratorExit('output acquisition');actual=os.open
        def opened(path,*args,**kwargs):
            if Path(path)==self.config.output:raise primary
            return actual(path,*args,**kwargs)
        with self.ports(),patch('ashlar_host.original_medical_local_setup.os.open',side_effect=opened),self.assertRaises(GeneratorExit)as caught:initialize_original_medical_local(self.config)
        self.assertIs(caught.exception,primary);self.assertTrue(self.config.output.is_dir());self.assertFalse(self.config.root.exists());self.assertFalse(self.events)
    def test_session_open_cancel_renews_bootstrap_and_releases_lock(self):
        primary=KeyboardInterrupt('session acquisition')
        @contextmanager
        def refused(custody):
            raise primary
            yield
        self.session=refused
        with self.ports(),self.assertRaises(KeyboardInterrupt)as caught:initialize_original_medical_local(self.config)
        self.assertIs(caught.exception,primary);self.assertIn('producer-renew',self.events)
        fd=os.open(self.config.root/'.evolution-operator.lock',os.O_RDWR)
        try:
            import fcntl
            fcntl.flock(fd,fcntl.LOCK_EX|fcntl.LOCK_NB)
        finally:os.close(fd)
    def test_body_cancel_with_multiple_closing_failures_preserves_exact_primary(self):
        primary=GeneratorExit('registration');initial=self.initialize
        def refused(*args,**kwargs):
            initial(*args,**kwargs);raise primary
        self.initialize=refused
        actual=__import__('ashlar_host.original_medical_local_setup',fromlist=['observe_empty_medical_target']).observe_empty_medical_target
        seen=[]
        def observed(spark,target,schema,registration=None):
            seen.append(target.table)
            if self.transport is not None:raise OSError('closing native')
            return actual(spark,target,schema,registration)
        with self.ports(),patch('ashlar_host.original_medical_local_setup.observe_empty_medical_target',side_effect=observed),self.assertRaises(GeneratorExit)as caught:initialize_original_medical_local(self.config)
        self.assertIs(caught.exception,primary);self.assertEqual(len(seen),12);self.assertIn('session-close',self.events)
    def test_selected_receipt_limit_withholds_handoff_without_truncation(self):
        self.config=replace(self.config,bounds=replace(self.config.bounds,receipt_bytes=1024))
        with self.ports(),self.assertRaises(ValueError):initialize_original_medical_local(self.config)
        self.assertIn('transport-close',self.events);self.assertIn('session-close',self.events)

    def test_acquired_target_disappearance_refuses_and_other_closings_attempted(self):
        import shutil
        initial=self.initialize
        def removed(*args,**kwargs):
            t=initial(*args,**kwargs);shutil.rmtree(self.config.root/ROLES[0]);return t
        self.initialize=removed
        with self.ports(),self.assertRaises(PermissionError):initialize_original_medical_local(self.config)
        for role in ROLES[1:]:self.assertGreaterEqual(self.spark.closing.count(role),2)
        self.assertIn('transport-close',self.events);self.assertIn('session-close',self.events)
    def test_prepolicy_producer_cancel_preserved_and_created_output_closed(self):
        primary=KeyboardInterrupt('producer');actual_close=os.close;closed=[]
        def refused(*args):raise primary
        def close(fd):closed.append(fd);return actual_close(fd)
        self.dataset=refused
        with self.ports(),patch('ashlar_host.original_medical_local_setup.os.close',side_effect=close),self.assertRaises(KeyboardInterrupt)as caught:initialize_original_medical_local(self.config)
        self.assertIs(caught.exception,primary);self.assertTrue(closed);self.assertFalse(self.config.root.exists());self.assertNotIn('session-open',self.events)
