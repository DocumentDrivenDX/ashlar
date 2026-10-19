# SPDX-License-Identifier: MIT OR Apache-2.0
"""Optional pinned native mapping observer controls; no Spark or source operations."""
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import errno,hashlib,json,os,tempfile,time,unittest
from ashlar_host import medical_mapped_vnodes as mapped
from ashlar_host import medical_process_inspection as processes
from ashlar_host.medical_commit_inspection_configuration import MAPPED_VISIBILITY,STRICT_VISIBILITY,MedicalPinnedFile,MedicalProcessSelection
import test_medical_commit_inspection as fixture

class Tests(unittest.TestCase):
 def setUp(self):
  self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);self.root=Path(self.temp.name).resolve()
  self.helper=self.root/'helper';self.helper.write_bytes(b'exact helper bytes');self.helper.chmod(0o700)
  self.pin=MedicalPinnedFile(self.helper,hashlib.sha256(self.helper.read_bytes()).hexdigest(),self.helper.stat().st_size)
  self.selection=SimpleNamespace(mapped_vnode_helper=self.pin,owner_uid=501,maximum_output_bytes=4096)
  self.value={'profile':mapped.PROFILE,'pid':400,'owner_uid':501,'start_seconds':1792404000,'start_microseconds':12,'regions':97,'file_backed_regions':27,'executable_file_regions':7,'no_vnode_regions':70,'selected_vnode_matches':0,'terminal_errno':22,'stable_bsd_identity':True}
 def observe(self,value=None):
  with patch.object(mapped,'execute_mapped_helper',return_value=json.dumps(self.value if value is None else value).encode())as execute:
   result=mapped.observe_mapped_vnodes(self.selection,400,((1,2),(3,4)),deadline=time.monotonic()+5)
  self.assertEqual(execute.call_args.args[0][:4],[str(self.helper),'400','501','4096'])
  self.assertEqual(execute.call_args.args[0][-4:],['1','2','3','4']);return result
 def test_closed_exact_native_nonmatching_response_and_original_file_identity(self):
  self.assertEqual(self.observe(),self.value)
  identity=mapped.pinned_executable_identity(self.pin)
  self.assertEqual(identity,(self.helper.stat().st_dev,self.helper.stat().st_ino))
  self.helper.write_bytes(b'changed')
  with self.assertRaises(Exception):mapped.pinned_executable_identity(self.pin)
 def test_matching_selected_java_or_python_and_malformed_partial_identity_refuse(self):
  for changes in ({'selected_vnode_matches':1},{'stable_bsd_identity':False},{'pid':401},{'owner_uid':502},{'terminal_errno':3},{'regions':4097},{'file_backed_regions':0},{'no_vnode_regions':69},{'executable_file_regions':28},{'start_microseconds':1000000},{'regions':True},{'extra':1}):
   with self.subTest(changes=changes),self.assertRaises(PermissionError):self.observe({**self.value,**changes})
 def test_helper_cancellation_renews_original_pin_and_preserves_primary(self):
  primary=KeyboardInterrupt('original')
  with patch.object(mapped,'execute_mapped_helper',side_effect=primary),patch.object(mapped,'read_selected',side_effect=[b'original',b'changed']),self.assertRaises(KeyboardInterrupt)as caught:
   mapped.observe_mapped_vnodes(self.selection,400,((1,2),),deadline=time.monotonic()+5)
  self.assertIs(caught.exception,primary)
 def observer(self):
  observer=fixture.ProcessTests().observer();observer.config.process_observer.visibility_profile=MAPPED_VISIBILITY
  observer.mapped_executables=[(None,(1,1)),(None,(2,2))]
  return observer
 def test_enoent_fallback_is_bound_to_same_ps_row_and_repeated_kernel_microsecond_identity(self):
  observer=self.observer();rows={100:(50,10,'inspector'),400:(1,20,'masked title')}
  def physical(pid):
   if pid==400:raise processes.ProcessImageUnavailable(errno.ENOENT,'missing primary path')
   return '/inspector/python',(3,3)
  value={**self.value,'start_seconds':1792404000}
  expected=time.strftime('%a %b %d %H:%M:%S %Y',time.gmtime(value['start_seconds']))
  def inventory(deadline):
   result=fixture.ProcessTests().rows(rows);result[400]=(1,20,'masked title',expected,501);return result
  with patch.object(observer,'inventory',side_effect=inventory),patch.object(processes,'physical_executable',side_effect=physical),patch.object(processes,'observe_mapped_vnodes',return_value=value)as observe:
   result=observer.observe(before=True)
  self.assertEqual(observe.call_count,2);self.assertEqual(result['mapped_vnode_fallbacks'][0]['opening'],value)
  self.assertIn('Anonymous mappings remain unknown',result['mapped_vnode_assumptions'])
  retained=json.loads(json.dumps([result]));self.assertIsNone(processes.validate_medical_process_observations(retained))
  retained[0]['mapped_vnode_fallbacks'][0]['closing']['start_microseconds']+=1
  with self.assertRaises(ValueError):processes.validate_medical_process_observations(retained)
 def test_closed_profile_specific_helper_selection(self):
  import test_original_medical_local_configuration as selections
  from ashlar_host import medical_commit_inspection_configuration as configuration
  helper=selections.OriginalMedicalConfigurationTests();helper.setUp();self.addCleanup(helper.doCleanups)
  recovery=helper.configuration('publication');recovery['profile']='ashlar-original-medical-local-recovery/0.1';recovery['request']=helper.pin('request.json')
  process={'ps':{'path':'/bin/ps','bytes':Path('/bin/ps').stat().st_size,'sha256':'a'*64},'java':recovery['runtime']['java'],
   'maximum_processes':4096,'maximum_output_bytes':1048576,'timeout_seconds':5,'owner_uid':os.getuid(),'visibility_profile':MAPPED_VISIBILITY,
   'mapped_vnode_helper':{'path':str(self.pin.path),'bytes':self.pin.bytes,'sha256':self.pin.sha256}}
  value={'profile':configuration.PROFILE,'recovery':recovery,'original_configurations':[helper.pin('old.json')],'process_observer':process}
  # Model Darwin's canonical /bin/ps; preserve every other path's real resolution.
  resolve=Path.resolve
  with patch.object(Path,'resolve',lambda path,*a,**kw:path if path==Path('/bin/ps')else resolve(path,*a,**kw)):
   self.assertEqual(configuration.parse_original_medical_commit_inspection(json.dumps(value).encode()).process_observer.mapped_vnode_helper,self.pin)
   process['visibility_profile']=STRICT_VISIBILITY
   with self.assertRaises(ValueError):configuration.parse_original_medical_commit_inspection(json.dumps(value).encode())
   process['visibility_profile']=MAPPED_VISIBILITY;del process['mapped_vnode_helper']
   with self.assertRaises(ValueError):configuration.parse_original_medical_commit_inspection(json.dumps(value).encode())
 def test_original_executable_vnode_replacement_is_not_renewed_by_equal_bytes(self):
  identity=mapped.pinned_executable_identity(self.pin);replacement=self.root/'replacement';replacement.write_bytes(self.helper.read_bytes());replacement.replace(self.helper)
  with self.assertRaises(PermissionError):processes.MedicalProcessInspection.same_executable(self.pin,identity)
 def test_permission_error_strict_profile_or_primary_race_never_downgrades(self):
  for mode in ('permission','strict','second-primary'):
   observer=self.observer();rows={100:(50,10,'inspector')};count=0
   if mode=='strict':observer.config.process_observer.visibility_profile=STRICT_VISIBILITY
   def physical(pid):
    nonlocal count
    count+=1
    if mode=='second-primary'and count==1:return '/inspector/python',(3,3)
    raise processes.ProcessImageUnavailable(errno.EPERM if mode=='permission'else errno.ENOENT,'unavailable')
   with patch.object(observer,'inventory',side_effect=lambda deadline:fixture.ProcessTests().rows(rows)),patch.object(processes,'physical_executable',side_effect=physical),patch.object(processes,'observe_mapped_vnodes')as helper,self.assertRaises(PermissionError):observer.observe(before=True)
   helper.assert_not_called()
 def test_esrch_requires_independent_kernel_absence_at_both_ends_and_missing_closing_pid(self):
  for outcome in ('terminated','still-live','permission','pid-reused','no-image-esrch'):
   observer=self.observer();observer.config.process_observer.visibility_profile=STRICT_VISIBILITY
   opening=fixture.ProcessTests().rows({100:(50,10,'inspector'),400:(1,20,'original worker')})
   closing={100:opening[100]}
   if outcome=='pid-reused':closing[400]=(1,20,'new process','Mon Oct 19 10:00:01 2026',501)
   def physical(pid):
    if pid==400:
     if outcome=='no-image-esrch':raise PermissionError('Unknown image')
     raise processes.ProcessImageUnavailable(errno.ESRCH,'exited')
    return '/inspector/python',(3,3)
   effect=None if outcome=='still-live'else PermissionError(errno.EPERM,'denied')if outcome=='permission'else ProcessLookupError(errno.ESRCH,'absent')
   with patch.object(observer,'inventory',side_effect=[opening,closing]),patch.object(processes,'physical_executable',side_effect=physical),patch.object(processes.os,'kill',side_effect=effect)as kill:
    if outcome=='terminated':
     result=observer.observe(before=True);self.assertEqual(kill.call_count,2);self.assertEqual(result['terminal_local_processes'][0]['closing_kernel_errno'],errno.ESRCH)
    else:
     with self.assertRaises(PermissionError):observer.observe(before=True)
     if outcome=='no-image-esrch':kill.assert_not_called()
 def test_new_unrelated_process_is_fully_observed_then_independently_closed(self):
  observer=self.observer();opening=fixture.ProcessTests().rows({100:(50,10,'inspector')})
  closing={**opening,**fixture.ProcessTests().rows({400:(1,20,'masked unrelated image')})};resolved=[]
  def physical(pid):resolved.append(pid);return '/unrelated/image',(3,3)
  with patch.object(observer,'inventory',side_effect=[opening,closing,closing]),patch.object(processes,'physical_executable',side_effect=physical):
   result=observer.observe(before=True)
  self.assertEqual(resolved.count(400),3);self.assertEqual(result['observed_external_original_executors'],0)
 def test_new_original_jdk_process_refuses_and_constant_churn_hits_original_deadline(self):
  import itertools
  observer=self.observer();opening=fixture.ProcessTests().rows({100:(50,10,'inspector')});closing={**opening,**fixture.ProcessTests().rows({400:(1,20,'masked new original JVM')})}
  with patch.object(observer,'inventory',side_effect=[opening,closing]),patch.object(processes,'physical_executable',side_effect=lambda pid:('/selected/java',(1,1))if pid==400 else('/unrelated/image',(3,3))),self.assertRaises(PermissionError):observer.observe(before=True)
  observer=self.observer();rows={100:(50,10,'inspector')};count=400
  def inventory(deadline):
   nonlocal count
   count+=1;rows[count]=(1,20,'unrelated newborn');return fixture.ProcessTests().rows(rows)
  with patch.object(observer,'inventory',side_effect=inventory),patch.object(processes,'physical_executable',return_value=('/unrelated/image',(3,3))),patch.object(processes.time,'monotonic',side_effect=itertools.count()),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_missing_ps_row_and_kernel_esrch_do_not_hide_still_resolvable_image(self):
  observer=self.observer();opening=fixture.ProcessTests().rows({100:(50,10,'inspector'),400:(1,20,'unrelated')});closing={100:opening[100]}
  with patch.object(observer,'inventory',side_effect=[opening,closing]),patch.object(processes,'physical_executable',return_value=('/still/resolvable',(3,3))),patch.object(processes.os,'kill',side_effect=ProcessLookupError(errno.ESRCH,'kernel absent'))as kill,self.assertRaises(PermissionError):observer.observe(before=True)
  kill.assert_not_called()
 def test_cumulative_process_identity_limit_honors_selected_lower_bound(self):
  observer=self.observer();observer.config.process_observer.maximum_processes=1
  opening=fixture.ProcessTests().rows({100:(50,10,'inspector')});closing={**opening,**fixture.ProcessTests().rows({400:(1,20,'unrelated new')})}
  with patch.object(observer,'inventory',side_effect=[opening,closing]),patch.object(processes,'physical_executable',return_value=('/unrelated/image',(3,3))),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_same_primary_image_argument_update_reclassifies_and_closes_without_new_pid_slot(self):
  observer=self.observer();observer.config.process_observer.maximum_processes=1
  opening=fixture.ProcessTests().rows({100:(50,10,'benign first title')});closing=fixture.ProcessTests().rows({100:(50,10,'benign updated title')})
  with patch.object(observer,'inventory',side_effect=[opening,closing,closing])as inventories,patch.object(processes,'physical_executable',return_value=('/unrelated/image',(3,3)))as physical:
   result=observer.observe(before=True)
  self.assertEqual(result['observed_external_original_executors'],0);self.assertEqual(inventories.call_count,3);self.assertEqual(physical.call_count,5)
 def test_argument_update_medical_or_original_root_or_changed_image_refuses(self):
  for outcome in ('medical','root','image'):
   observer=self.observer();observer.configuration_roots=('/private/tmp/original-g',)
   opening=fixture.ProcessTests().rows({100:(50,10,'benign initial title'),400:(1,20,'unrelated')})
   args='python -m ashlar_host.original_medical_local_cli --operation recovery'if outcome=='medical'else'python /private/tmp/original-g/operator.py'if outcome=='root'else'benign updated title'
   closing=fixture.ProcessTests().rows({100:(50,10,'benign initial title'),400:(1,20,args)});calls=0
   def physical(pid):
    nonlocal calls
    if pid==400:calls+=1
    return ('/changed/image',(4,4))if outcome=='image'and pid==400 and calls>=3 else('/unrelated/image',(3,3))
   with patch.object(observer,'inventory',side_effect=[opening,closing]),patch.object(processes,'physical_executable',side_effect=physical),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_same_mapped_identity_arguments_update_retains_original_opening(self):
  observer=self.observer();opening=fixture.ProcessTests().rows({100:(50,10,'inspector'),400:(1,20,'benign title')});closing=fixture.ProcessTests().rows({100:(50,10,'inspector'),400:(1,20,'benign updated title')})
  value={**self.value,'start_seconds':1792404000}
  def physical(pid):
   if pid==400:raise processes.ProcessImageUnavailable(errno.ENOENT,'original missing primary path')
   return '/unrelated/image',(3,3)
  with patch.object(observer,'inventory',side_effect=[opening,closing,closing])as inventories,patch.object(processes,'physical_executable',side_effect=physical),patch.object(observer,'mapped',return_value=value)as observations:
   result=observer.observe(before=True)
  self.assertEqual(observations.call_count,3);self.assertEqual(inventories.call_count,3);self.assertEqual(result['mapped_vnode_fallbacks'][0],{'opening':value,'closing':value})
 def test_continuous_argument_changes_remain_under_original_deadline(self):
  import itertools
  observer=self.observer();observer.config.process_observer.maximum_processes=1;number=0
  def inventory(deadline):
   nonlocal number
   number+=1;return fixture.ProcessTests().rows({100:(50,10,'benign title '+str(number))})
  with patch.object(observer,'inventory',side_effect=inventory),patch.object(processes,'physical_executable',return_value=('/unrelated/image',(3,3))),patch.object(processes.time,'monotonic',side_effect=itertools.count()),self.assertRaises(PermissionError):observer.observe(before=True)
 def test_ps_start_mismatch_and_kernel_process_reuse_refuse(self):
  observer=self.observer();row=(1,20,'masked title','Mon Oct 19 10:00:00 2026',501)
  with patch.object(processes,'observe_mapped_vnodes',return_value={**self.value,'start_seconds':0}),self.assertRaises(PermissionError):observer.mapped(400,row,time.monotonic()+5)
  opening={**self.value,'start_seconds':1792404000};closing={**opening,'start_microseconds':13}
  rows={100:(50,10,'inspector'),400:(1,20,'masked title')}
  def physical(pid):
   if pid==400:raise processes.ProcessImageUnavailable(errno.ENOENT,'missing primary')
   return '/inspector/python',(3,3)
  with patch.object(observer,'inventory',side_effect=lambda deadline:fixture.ProcessTests().rows(rows)),patch.object(processes,'physical_executable',side_effect=physical),patch.object(observer,'mapped',side_effect=[opening,closing]),self.assertRaises(PermissionError):observer.observe(before=True)
