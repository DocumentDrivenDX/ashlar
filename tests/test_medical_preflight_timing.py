"""Fixed diagnostic prefix/cleanup and exact existing recovery route; no native."""
from contextlib import contextmanager,ExitStack
from pathlib import Path
from dataclasses import replace
import io,json,tempfile,unittest
from unittest.mock import patch
from ashlar_host.medical_preflight_timing import MedicalPreflightTiming
from ashlar_host.lifecycle import finish
import ashlar_host.original_medical_local_publication as owner
import ashlar_host.original_medical_local_cli as dispatch
import test_original_medical_local_configuration as selections
import test_medical_recovery_preflight as preflight

class Tests(unittest.TestCase):
 def test_actual_private_prefix_safe_closed_shapes_and_no_raw_failure(self):
  with tempfile.TemporaryDirectory()as temporary:
   root=Path(temporary);events=[]
   def write(name,raw,limit):
    self.assertLessEqual(len(raw),limit)
    with(root/name).open('xb')as file:file.write(raw)
   timing=MedicalPreflightTiming(write);primary=KeyboardInterrupt('private-payload')
   def action():
    events.append('action');self.assertEqual(json.loads((root/'preflight-stage-000.json').read_bytes())['phase'],'begin');raise primary
   with self.assertRaises(KeyboardInterrupt)as caught:timing.call('native-target',action,role='object_current')
   self.assertIs(caught.exception,primary);self.assertEqual(events,['action']);records=[json.loads(p.read_bytes())for p in sorted(root.iterdir())];self.assertEqual([v['phase']for v in records],['begin','end']);self.assertEqual(records[1]['outcome'],'refused');self.assertNotIn('private-payload',''.join(p.read_text()for p in root.iterdir()))
   for phase,kwargs in [('begin',{'duration_ns':0}),('begin',{'outcome':'completed'}),('end',{}),('end',{'duration_ns':0,'outcome':'unknown'})]:
    with self.assertRaises(ValueError):timing.emit('binding',phase,**kwargs)
 def test_emission_capacity_and_write_failure_never_skip_required_closing(self):
  for failure in ('capacity','write'):
   events=[];timing=MedicalPreflightTiming(lambda *args:(_ for _ in()).throw(OSError('private')))
   if failure=='capacity':timing.records=128
   with self.assertRaises((OSError,ValueError)):timing.call('source-closing',lambda:events.append('source-close'))
   self.assertEqual(events,['source-close'])
 def test_business_cancellation_and_first_cleanup_cancellation_precedence(self):
  primary=KeyboardInterrupt('original');events=[]
  timing=MedicalPreflightTiming(lambda *args:(_ for _ in()).throw(OSError('private')))
  with self.assertRaises(KeyboardInterrupt)as caught:timing.call('binding',lambda:(_ for _ in()).throw(primary))
  self.assertIs(caught.exception,primary);self.assertTrue(primary.cleanup_failed)
  for cancellation in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
   calls=[]
   def write(*args):
    calls.append('write')
    if len(calls)==2:raise cancellation
   timing=MedicalPreflightTiming(write)
   with self.assertRaises(type(cancellation))as caught:timing.call('binding',lambda:(_ for _ in()).throw(OSError('ordinary')))
   self.assertIs(caught.exception,cancellation);self.assertEqual(calls,['write','write'])
 def test_first_begin_cancellation_survives_later_action_cancellation(self):
  first=KeyboardInterrupt();later=SystemExit();count=0;actions=[]
  def write(*args):
   nonlocal count
   count+=1
   if count==1:raise first
  def action():actions.append('required');raise later
  with self.assertRaises(KeyboardInterrupt)as caught:MedicalPreflightTiming(write).call('source-closing',action)
  self.assertIs(caught.exception,first);self.assertEqual(actions,['required']);self.assertEqual(count,2)
 def test_context_end_record_failure_still_closes_acquired_owner_and_preserves_body(self):
  events=[];count=0
  def write(*args):
   nonlocal count
   count+=1
   if count==2:raise OSError('end-write')
  @contextmanager
  def manager():
   events.append('open')
   try:yield 'owner'
   finally:events.append('close')
  with self.assertRaises(OSError):
   with MedicalPreflightTiming(write).context('runtime-custody','runtime-closing',manager()):self.fail('No successful acquisition result')
  self.assertEqual(events,['open','close'])
 def test_configuration_ceiling_public_route_and_lazy_cli_dispatch(self):
  helper=selections.OriginalMedicalConfigurationTests();helper.setUp();self.addCleanup(helper.doCleanups)
  raw=helper.configuration('publication');raw['profile']='ashlar-original-medical-local-recovery/0.1';raw['request']=helper.pin('retained/request.json');raw['bounds']['native_seconds']=60
  config=dispatch._configuration(json.dumps(raw).encode(),'recovery-preflight')
  with patch.object(owner,'_operate',return_value={'preflight_completed':True})as operate:owner.preflight_original_medical_recovery_local(config)
  operate.assert_called_once_with(config,False,None,recovery=True,preflight_only=True)
  for native,whole in ((0,540),(301,540),(True,540),(60,60),(60,59),(60,True)):
   changed=json.loads(json.dumps(raw));changed['bounds'].update(native_seconds=native,whole_seconds=whole)
   with self.assertRaises(ValueError):dispatch._configuration(json.dumps(changed).encode(),'recovery-preflight')
  old=json.loads(json.dumps(raw));old['bounds'].update(native_seconds=300,whole_seconds=540)
  self.assertEqual(dispatch._configuration(json.dumps(old).encode(),'recovery').bounds.native_seconds,300)
  raw['bounds']['native_seconds']=61
  with patch.object(dispatch.OriginalMedicalRuntimeCustody,'open')as runtime,self.assertRaises(ValueError):dispatch._configuration(json.dumps(raw).encode(),'recovery-preflight')
  runtime.assert_not_called()
  from ashlar.cli import main
  from contextlib import redirect_stdout
  with patch('sys.argv',['ashlar','preflight-original-medical','--configuration','/config']),patch.object(dispatch,'run_original_medical_recovery_preflight_command',return_value={})as command,redirect_stdout(io.StringIO()):main()
  command.assert_called_once_with(Path('/config'))
 def test_actual_outer_preflight_stops_before_publisher_and_closes_on_success_cancel(self):
  import hashlib,sqlite3
  from types import SimpleNamespace
  import test_original_medical_local_publication as composition
  from ashlar_host.original_medical_local_configuration import OriginalMedicalRecoveryConfig
  for cancel in (False,True):
   helper=composition.Tests();helper.setUp();self.addCleanup(helper.doCleanups);publication,selection,capture=helper.installation();events=[];primary=KeyboardInterrupt()
   raw=b'{"request_digest":"original"}\n';path=publication.output.parent/'retained-request.json';path.write_bytes(raw)
   config=OriginalMedicalRecoveryConfig(**{**{f.name:getattr(publication,f.name)for f in owner.fields(publication)},'profile':'ashlar-original-medical-local-recovery/0.1','request':publication.installation.__class__(path,hashlib.sha256(raw).hexdigest(),len(raw)),'bounds':replace(publication.bounds,native_seconds=60)})
   @contextmanager
   def native(custody):
    events.append('native-open')
    try:yield object()
    finally:events.append('native-close')
   actual_transport=owner.LocalDeltaTransport
   def transport(spark,journal,installation,targets,policy,**kwargs):
    result=actual_transport.__new__(actual_transport);result.db=sqlite3.connect(journal);result.journal_path=journal;result.targets={t.table:t for t in targets};result.reservation=owner.encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(result.db.execute('SELECT original FROM local_installation').fetchone()[0]),'original_journal_path':str(journal)})+'\n'
    @contextmanager
    def bounded(bounds):yield
    result.bounded_observations=bounded;result.original_history=lambda target:[{'version':1}];schemas=owner.original_local_physical_carriers()['carriers'];result.bounded_snapshot=lambda target,*args,**kwargs:({'rows':[]},json.loads(schemas[target.table.split('.')[-1]]['schemaJson']));result.close=lambda:(events.append('transport-close'),result.db.close());return result
   class Plan:
    def __init__(self,admission,transaction,tables):admission.metadata()
    def renew(self):pass
    def snapshot(self):return {'request':{'request_digest':'original'},'selected_steps':[],'expected':{},'publication_id':'not-published','previous_progress':{},'progress':{},'previous_expected':{},'generated_steps':[],'zero_match_elisions':[],'recorded_at':'0'}
   class Source:
    def __init__(self,*args):pass
    def sources(self):return object()
    def bind(self,*args):return object()
    def read_original(self,*args,**kwargs):events.append('read-original');return object()
    def append_original(self,*args,**kwargs):raise AssertionError('No append')
    def renew_original(self,*args,**kwargs):events.append('source-close')
   def driver(transport,policy,context,*args,**kwargs):
    @contextmanager
    def writer(*args):yield
    def preflight(plan,**kwargs):
     events.append('preflight');kwargs['stages'].call('binding',lambda:None)
     if cancel:raise primary
    return SimpleNamespace(writer=writer,bind_original_medical_publication=lambda *a,**k:None,preflight_original_medical_recovery=preflight)
   with ExitStack()as stack:
    stack.enter_context(patch('ashlar_host.medical_producer.capture',side_effect=capture));stack.enter_context(patch.object(owner,'original_medical_native_session',side_effect=native));stack.enter_context(patch.object(owner,'LocalDeltaTransport',side_effect=transport));stack.enter_context(patch.object(owner,'MedicalPublicationPlan',Plan));stack.enter_context(patch.object(owner,'OriginalMedicalSourceOwner',Source));stack.enter_context(patch.object(owner,'original_medical_local_source_ports',return_value=object()));stack.enter_context(patch.object(owner.NativeDriver,'with_registered_evolution',side_effect=driver))
    publisher=stack.enter_context(patch.object(owner,'publish_outbox_transaction',side_effect=AssertionError('No publisher/ACK')));attempts=stack.enter_context(patch.object(owner,'DeltaAttemptStore',side_effect=AssertionError('No attempt submission')))
    if cancel:
     with self.assertRaises(KeyboardInterrupt)as caught:owner.preflight_original_medical_recovery_local(config)
     self.assertIs(caught.exception,primary);self.assertFalse((config.output/'receipt.json').exists())
    else:
     answer=owner.preflight_original_medical_recovery_local(config);self.assertTrue(answer['preflight_completed']);self.assertNotIn('publication',answer);self.assertTrue(answer['closing']);self.assertGreater(answer['stage_records'],0)
    publisher.assert_not_called();attempts.assert_not_called()
   self.assertLess(events.index('transport-close'),events.index('native-close'));self.assertIn('source-close',events)
   self.assertTrue(list(config.output.glob('preflight-stage-*.json')))
   with owner.OriginalMedicalInstallationLease.open(selection,object()):pass

 def test_real_nominal_preflight_records_all_six_and_never_mutates_journal(self):
  helper=preflight.Tests();self.addCleanup(helper.doCleanups);rows=[]
  with ExitStack()as stack:
   _,driver,plan,bounds,_,_,captures=helper.fixture(stack);before=list(driver.transport.db.iterdump());timing=MedicalPreflightTiming(lambda name,raw,limit:rows.append(json.loads(raw)))
   with driver.writer('original',driver.context),driver.transport.bounded_observations(bounds):driver.preflight_original_medical_recovery(plan,context=driver.context,bounds=bounds,stages=timing)
   self.assertEqual(before,list(driver.transport.db.iterdump()));self.assertEqual({r.get('role')for r in rows if r['stage']=='native-target'},set(owner.ROLES));self.assertTrue(any(r['stage']=='prior-capture'for r in rows));self.assertEqual(rows[-1]['stage'],'binding');self.assertEqual(rows[-1]['phase'],'end');self.assertLessEqual(len(rows),128)
