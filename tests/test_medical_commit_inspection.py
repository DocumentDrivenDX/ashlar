"""Read-only outcome/process composition controls; inert native/OS boundaries."""
from contextlib import contextmanager,ExitStack
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import hashlib,json,os,sqlite3,tempfile,unittest
from ashlar_host import medical_process_inspection as processes
from ashlar_host import medical_commit_inspection as inspection
from ashlar_host import medical_commit_inspection_configuration as configuration

class ProcessTests(unittest.TestCase):
 def rows(self,value):return {pid:(*row,'Mon Oct 19 10:00:00 2026',501)if len(row)==3 else row for pid,row in value.items()}
 def observer(self):
  value=processes.MedicalProcessInspection.__new__(processes.MedicalProcessInspection)
  value.pid=100;value.parent=50;value.group=10;value.config=SimpleNamespace(process_observer=SimpleNamespace(timeout_seconds=5,owner_uid=501,visibility_profile='ashlar-private-local-same-uid-process-visibility/0.1'));value.java_identity=(1,1);value.python_identities={(2,2)}
  value.configuration_roots=();value.allowed_java=();value.started=False;value.observations=[];value.renew=lambda:None
  return value
 def test_observed_absence_and_exact_inspector_lineage_excludes_distinct_datagrip_binary(self):
  observer=self.observer();rows={100:(50,10,'python -m ashlar_host.original_medical_local_cli --operation commit-inspection'),425:(1,20,'/apps/datagrip/jbr/bin/java')}
  def physical(pid):return ('/selected/python',(2,2))if pid==100 else('/apps/datagrip/jbr/bin/java',(9,9))if pid==425 else('/selected/jdk/bin/java',(1,1))
  with patch.object(observer,'inventory',side_effect=lambda deadline:self.rows(rows)),patch.object(processes,'physical_executable',side_effect=physical):
   result=observer.observe(before=True);self.assertEqual(result['observed_external_original_executors'],0);self.assertEqual(result['physically_distinct_java'][0]['pid'],425)
   rows[300]=(100,10,'/selected/jdk/bin/java pyspark-shell')
   observer.observe(bind=True);self.assertEqual(observer.allowed_java[0][0],300);observer.observe()
 def test_original_live_java_or_any_medical_worker_refuses_even_unlisted_configuration(self):
  for args,physical in [('/selected/jdk/bin/java SparkSubmit',('/selected/jdk/bin/java',(1,1))),('python -m ashlar_host.original_medical_local_cli --operation recovery --configuration /unlisted.json',('/foreign/python',(8,8)))]:
   observer=self.observer();rows={100:(50,10,'inspector'),400:(1,20,args)}
   with patch.object(observer,'inventory',side_effect=lambda deadline:self.rows(rows)),patch.object(processes,'physical_executable',side_effect=lambda pid:('/inspector/python',(3,3))if pid==100 else physical),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_native_ps_signed_nobody_uid_and_owned_reaped_ps_row(self):
  observer=self.observer();observer.config.process_observer.maximum_processes=4096
  raw=(b'100 50 10 501 Mon Oct 19 10:00:00 2026 inspector\n'
       b'200 1 200 -2 Mon Oct 19 10:00:00 2026 unrelated\n'
       b'900 100 10 501 Mon Oct 19 10:00:00 2026 ps\n')
  with patch.object(processes,'process_rows',return_value=(raw,900)):
   rows=observer.inventory(5)
  self.assertEqual(rows[200][4],4294967294);self.assertNotIn(900,rows)
 def test_complete_observation_deadline_covers_physical_enumeration(self):
  observer=self.observer()
  with patch.object(observer,'inventory',return_value=self.rows({100:(50,10,'inspector')})),patch.object(processes.time,'monotonic',side_effect=[0,6]),patch.object(processes,'physical_executable')as physical,self.assertRaises(PermissionError):observer.observe(before=True)
  physical.assert_not_called()
 def test_masked_selected_jdk_title_still_refuses_by_physical_image(self):
  observer=self.observer();rows={100:(50,10,'inspector'),400:(1,20,'innocuous-title')}
  with patch.object(observer,'inventory',side_effect=lambda deadline:self.rows(rows)),patch.object(processes,'physical_executable',side_effect=lambda pid:('/inspector/python',(3,3))if pid==100 else('/selected/jdk/bin/java',(1,1))),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_ancestor_inspection_text_cannot_exempt_an_original_worker(self):
  observer=self.observer();rows={100:(50,10,'inspector'),50:(40,10,'supervisor'),40:(1,10,'python -m ashlar_host.original_medical_local_cli --operation commit-inspection')}
  with patch.object(observer,'inventory',side_effect=lambda deadline:self.rows(rows)),patch.object(processes,'physical_executable',return_value=('/unrelated/python',(3,3))),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_original_outer_operator_script_path_refuses_future_worker_parent(self):
  observer=self.observer();observer.configuration_roots=('/private/tmp/original-g',)
  rows={100:(50,10,'inspector'),400:(1,20,'python /private/tmp/original-g/run-recovery.py')}
  with patch.object(observer,'inventory',side_effect=lambda deadline:self.rows(rows)),patch.object(processes,'physical_executable',side_effect=lambda pid:('/inspector/python',(3,3))if pid==100 else('/different/python',(9,9))),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_unknown_raced_and_cancelled_physical_candidates_never_prove_absence(self):
  for outcome in ('unknown','raced','cancel'):
   observer=self.observer();rows={100:(50,10,'inspector'),400:(1,20,'/foreign/bin/java')}
   effect=[('/selected/python',(3,3)),('/selected/python',(3,3)),PermissionError('unknown')if outcome=='unknown'else KeyboardInterrupt('original')]if outcome!='raced'else[('/selected/python',(3,3)),('/selected/python',(3,3)),('/foreign/bin/java',(9,9)),('/changed/java',(8,8))]
   with patch.object(observer,'inventory',side_effect=lambda deadline:self.rows(rows)),patch.object(processes,'physical_executable',side_effect=effect),self.assertRaises(KeyboardInterrupt if outcome=='cancel'else PermissionError):observer.observe(before=True)
   self.assertEqual(observer.observations,[])
 def test_disappearing_candidate_and_new_java_during_observation_refuse(self):
  observer=self.observer();opening={100:(50,10,'inspector'),400:(1,20,'/foreign/bin/java')}
  with patch.object(observer,'inventory',side_effect=[self.rows(opening),self.rows({100:opening[100]})]),patch.object(processes,'physical_executable',return_value=('/foreign/bin/java',(9,9))),self.assertRaises(PermissionError):observer.observe(before=True)

class ConfigurationTests(unittest.TestCase):
 def test_closed_wrapper_and_lazy_installed_dispatch(self):
  import test_original_medical_local_configuration as fixture
  helper=fixture.OriginalMedicalConfigurationTests();helper.setUp();self.addCleanup(helper.doCleanups)
  recovery=helper.configuration('publication');recovery['profile']='ashlar-original-medical-local-recovery/0.1';recovery['request']=helper.pin('request.json')
  value={'profile':configuration.PROFILE,'recovery':recovery,'original_configurations':[helper.pin('old.json')],
   'process_observer':{'ps':{'path':'/bin/ps','bytes':Path('/bin/ps').stat().st_size,'sha256':'a'*64},'java':recovery['runtime']['java'],'maximum_processes':4096,'maximum_output_bytes':1048576,'timeout_seconds':5,'owner_uid':os.getuid(),'visibility_profile':'ashlar-private-local-same-uid-process-visibility/0.1'}}
  # Model Darwin's canonical /bin/ps without inheriting Linux usr-merge symlinks.
  resolve=Path.resolve
  with patch.object(Path,'resolve',lambda path,*a,**kw:path if path==Path('/bin/ps')else resolve(path,*a,**kw)):
   config=configuration.parse_original_medical_commit_inspection(json.dumps(value).encode());self.assertEqual(config.recovery.journal,helper.root/'native/operations.sqlite')
   for key,changed in [('maximum_processes',4097),('maximum_output_bytes',1048577),('timeout_seconds',True)]:
    altered=json.loads(json.dumps(value));altered['process_observer'][key]=changed
    with self.assertRaises(ValueError):configuration.parse_original_medical_commit_inspection(json.dumps(altered).encode())
   altered={**value,'grant':True}
   with self.assertRaises(ValueError):configuration.parse_original_medical_commit_inspection(json.dumps(altered).encode())
  from ashlar.cli import main
  with patch('sys.argv',['ashlar','inspect-original-medical-commit','--configuration','/selection.json']),patch('ashlar_host.original_medical_local_cli.run_original_medical_commit_inspection_command',return_value={})as run,patch('builtins.print'):main()
  run.assert_called_once_with(Path('/selection.json'))

class OutcomeTests(unittest.TestCase):
 def test_observed_absent_and_existing_commit_use_exact_retained_proposal_and_all_closing(self):
  for existing in (False,True):self.run_case(existing,False)
 def test_cancellation_still_delegates_source_ack_phase_and_native_closing(self):self.run_case(False,True)
 def test_strong_existing_commit_requires_actual_original_receipt_inspection(self):self.run_case(True,False,settled=True)
 def test_arbitrary_manifest_metadata_cannot_claim_existing_original_commit(self):self.run_case(True,False,foreign_metadata=True)
 def run_case(self,existing,cancel,*,settled=False,foreign_metadata=False):
  import test_stored_publisher as fixture
  from ashlar.native import SQLResult
  from ashlar_host.local_physical_carriers import original_local_physical_carriers
  helper=fixture.StoredPublisherTests();events=[];context=object()
  with helper.fixture()as(phases,source_driver,policy,remote,api):
   backend=helper.backend(phases,source_driver,remote,policy);source_driver.fail='validate'
   with self.assertRaises(PermissionError):helper.publish(backend,context)
   with backend.writer('stream',context):
    last=backend.observe('stream',helper.batch().batch_id);backend.start_commit('stream',helper.batch().batch_id,last.request_digest)
   payload=json.loads(phases.rows[-1]['payload_json']);request=payload['request'];row=json.loads(source_driver.original)['manifest']
   database=sqlite3.connect(':memory:');self.addCleanup(database.close)
   database.execute('CREATE TABLE local_publication_artifact(request_digest TEXT,request TEXT,artifact TEXT)')
   database.execute('CREATE TABLE local_operation(operation TEXT,state TEXT,intent_sha TEXT,intent TEXT)')
   database.execute('INSERT INTO local_publication_artifact VALUES(?,?,?)',(request['request_digest'],inspection.encoded(request),source_driver.original))
   targets={'c.s.attempt':SimpleNamespace(table='c.s.attempt',uuid='attempt-uuid'),'c.s.manifest':SimpleNamespace(table='c.s.manifest',uuid='manifest-uuid')}
   history=[{'version':1,'userMetadata':'registration','operation':'SET TBLPROPERTIES'},{'version':0,'userMetadata':None}]
   anchor=list(history)
   if existing:history=[{'version':2,'userMetadata':json.dumps({'profile':'ashlar-local-delta-commit/0.1','installation_id':'original','request_digest':request['request_digest'],'operation':'manifest:'+request['request_digest']+':'+'a'*64,'operation_sha256':'b'*64})}]+history
   if foreign_metadata:
    changed=json.loads(history[0]['userMetadata']);changed['request_digest']='f'*64;history[0]['userMetadata']=json.dumps(changed)
   receipt_calls=[]
   if settled:
    metadata=json.loads(history[0]['userMetadata']);original_intent={'statement':'MERGE original retained statement','parameters':{'row':inspection.encoded(row)}}
    database.execute('INSERT INTO local_operation VALUES(?,?,?,?)',(metadata['operation'],'committed',metadata['operation_sha256'],json.dumps(original_intent)))
   schemas=json.loads(original_local_physical_carriers()['carriers']['manifest']['schemaJson'])
   def native(target):events.append('manifest-history');return history
   transport=SimpleNamespace(db=database,targets=targets,original_history=native,
    bounded_snapshot=lambda *args,**kwargs:({'rows':[row]if existing else[]},schemas))
   transport.operation_capacity=None
   def receipt(operation,statement,parameters,*,intent_digest,context):
    receipt_calls.append((operation,statement,parameters,intent_digest))
    self.assertEqual(statement,original_intent['statement']);self.assertEqual(parameters,original_intent['parameters'])
    return {'operation_sha256':'b'*64,'uuid':'manifest-uuid','version':2,'snapshot':{'rows':[row]}}
   transport.inspect_committed=receipt
   def validate(*args):
    events.append('validate')
    if cancel:raise KeyboardInterrupt('original inspection cancellation')
   driver=SimpleNamespace(transport=transport,tables={'attempts':'c.s.attempt','manifest':'c.s.manifest'},policy=SimpleNamespace(active={}),require=lambda context:None,validate=validate,source_admit=lambda request:events.append('source-closing'))
   plan=SimpleNamespace(snapshot=lambda:{'request':request},require_request=lambda request:None)
   class Connection:
    def query(self,*args):events.append('ack-observe');return SQLResult([{'position':'0','request_hex':None,'manifest_hex':None,'receipt_hex':None}])
    def rollback(self):events.append('ack-rollback')
    def close(self):events.append('ack-close')
   registration=SimpleNamespace(source=SimpleNamespace(scope=SimpleNamespace(service_schema='ack',scope_id='scope')),metadata_factory=lambda context:Connection())
   source=SimpleNamespace(ports=SimpleNamespace(registration=registration),authority=SimpleNamespace(admit_scope=lambda *args:events.append('ack-scope')))
   @contextmanager
   def session(context):
    class Reader:
     def read(self,*args):events.append('phase-read');return tuple(SimpleNamespace(**{key:value for key,value in record.items()if key in ('phase','request_digest','payload_json')})for record in phases.rows)
    yield Reader()
   store=SimpleNamespace(session=session)
   installation={'installation_id':'original','native':{'manifest':{'registration':{'uuid':'manifest-uuid','history':anchor}}}}
   with patch.object(inspection,'DeltaAttemptStore',return_value=store),patch.object(inspection,'Session',side_effect=lambda connection:connection):
    kwargs={'context':context,'bounds':SimpleNamespace(input_bytes=4194304,total_bytes=16777216),'installation':installation}
    if foreign_metadata:
     with self.assertRaises(PermissionError):inspection.inspect_medical_commit(driver,plan,source,**kwargs)
    elif cancel:
     with self.assertRaises(KeyboardInterrupt):inspection.inspect_medical_commit(driver,plan,source,**kwargs)
    else:
     result,proposal=inspection.inspect_medical_commit(driver,plan,source,**kwargs)
     self.assertEqual(result['classification'],'exact-existing-commit'if settled else'observed-exact-manifest-row-no-original-intent'if existing else'observed-absent-at-anchor')
     self.assertEqual(len(receipt_calls),2 if settled else 0)
     self.assertEqual(json.loads(proposal),row);self.assertEqual(result['proposal_sha256'],hashlib.sha256(proposal).hexdigest())
     self.assertEqual(api.calls,[])
   self.assertIn('source-closing',events);self.assertIn('ack-close',events);self.assertIn('phase-read',events);self.assertIn('manifest-history',events)

class CompositionTests(unittest.TestCase):
 def test_actual_outer_inspection_never_constructs_publisher_and_closes_source_native(self):
  import test_original_medical_local_publication as fixture
  import ashlar_host.original_medical_local_publication as owner
  from ashlar_host.original_medical_local_configuration import OriginalMedicalRecoveryConfig
  for outcome in ('success','cancel','closing-refusal'):
   helper=fixture.Tests();helper.setUp();self.addCleanup(helper.doCleanups);publication,selection,capture=helper.installation();events=[]
   raw=owner._bounded({'request_digest':'original','predecessor':None,'schema_revisions_json':'{}'},262143)
   request=publication.output.parent/'retained-request.json';request.write_bytes(raw)
   config=OriginalMedicalRecoveryConfig(**{**{f.name:getattr(publication,f.name)for f in owner.fields(publication)},'profile':'ashlar-original-medical-local-recovery/0.1','request':publication.installation.__class__(request,hashlib.sha256(raw).hexdigest(),len(raw))})
   primary=KeyboardInterrupt('inert inspection cancellation')
   class Observer:
    observations=[]
    def before(self,lease,context):lease.renew(context);events.append('process-before')
    def bind(self):events.append('process-bind')
    def closing(self):events.append('process-closing')
    def renew(self):events.append('process-selection-close')
   @contextmanager
   def native(custody):
    events.append('native-open')
    try:yield object()
    finally:events.append('native-close')
   original_transport=owner.LocalDeltaTransport
   def transport(spark,journal,installation,targets,policy,**kwargs):
    result=original_transport.__new__(original_transport);result.db=sqlite3.connect(journal);result.journal_path=journal;result.targets={target.table:target for target in targets}
    result.reservation=owner.encoded({'profile':'ashlar-private-native-installation-reservation/0.1','registry':json.loads(result.db.execute('SELECT original FROM local_installation').fetchone()[0]),'original_journal_path':str(journal)})+'\n'
    @contextmanager
    def bounded(bounds):yield
    result.bounded_observations=bounded;result.original_history=lambda target:[{'version':1}]
    schemas=owner.original_local_physical_carriers()['carriers'];result.bounded_snapshot=lambda target,*args,**kwargs:({'rows':[]},json.loads(schemas[target.table.split('.')[-1]]['schemaJson']))
    result.close=lambda:(events.append('transport-close'),result.db.close());return result
   class Plan:
    def __init__(self,*args):pass
    def renew(self):pass
    def snapshot(self):return {'request':json.loads(raw),'selected_steps':[],'expected':{},'publication_id':'not-published','previous_progress':{},'progress':{},'previous_expected':{},'generated_steps':[],'zero_match_elisions':[],'recorded_at':'0'}
   class Source:
    def __init__(self,*args):pass
    def sources(self):return object()
    def bind(self,*args):return object()
    def read_original(self,*args,**kwargs):events.append('read-original');return object()
    def append_original(self,*args,**kwargs):raise AssertionError('No source append')
    def renew_original(self,*args,**kwargs):
     events.append('source-close')
     if outcome=='closing-refusal':raise PermissionError('inert source closing refusal')
   def driver(*args,**kwargs):
    @contextmanager
    def writer(*args):yield
    return SimpleNamespace(writer=writer,bind_original_medical_publication=lambda *a,**k:None,preflight_original_medical_recovery=lambda *a,**k:events.append('preflight'))
   def inspect(*args,**kwargs):
    events.append('inspect')
    if outcome=='cancel':raise primary
    return {'classification':'observed-absent-at-anchor','proposal_sha256':'a'*64},b'{}\n'
   with ExitStack()as stack:
    for target,name,value in [(owner,'original_medical_native_session',native),(owner,'LocalDeltaTransport',transport),(owner,'MedicalPublicationPlan',Plan),(owner,'OriginalMedicalSourceOwner',Source),(owner,'original_medical_local_source_ports',lambda *args:object()),(owner.NativeDriver,'with_registered_evolution',driver),(processes,'MedicalProcessInspection',Observer),(inspection,'inspect_medical_commit',inspect)]:stack.enter_context(patch.object(target,name,value))
    stack.enter_context(patch('ashlar_host.medical_producer.capture',side_effect=capture))
    publisher=stack.enter_context(patch.object(owner,'_MedicalPreparedBackend',side_effect=AssertionError('No publisher construction')))
    submit=stack.enter_context(patch.object(owner,'publish_outbox_transaction',side_effect=AssertionError('No publication/ACK')))
    if outcome=='success':
     result=owner.operate_original_medical_commit_inspection(config,Observer());self.assertEqual(result['classification'],'observed-absent-at-anchor');self.assertTrue((config.output/'proposal.json').exists())
    else:
     with self.assertRaises(KeyboardInterrupt if outcome=='cancel'else PermissionError):owner.operate_original_medical_commit_inspection(config,Observer())
     self.assertFalse((config.output/'receipt.json').exists());self.assertFalse((config.output/'proposal.json').exists())
    publisher.assert_not_called();submit.assert_not_called()
   self.assertLess(events.index('process-before'),events.index('native-open'));self.assertLess(events.index('preflight'),events.index('inspect'))
   self.assertIn('source-close',events);self.assertIn('process-closing',events);self.assertLess(events.index('transport-close'),events.index('native-close'))
