# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual shared singleton execution with inert native/public-source ports."""
import base64,hashlib,json,unittest
from contextlib import contextmanager
from types import SimpleNamespace
from ashlar.native import SQLResult
from ashlar.publication import Snapshot
from ashlar_host.driver import NativeDriver
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.truss_feed_sessions import TrussFeedSessions
from ashlar_host.truss_singleton import read_original_truss_singleton
from test_publication import FakeBackend

class Tests(unittest.TestCase):
 def fixture(self,kind='object',present=True):
  driver=object.__new__(NativeDriver);context=object();driver.context=context
  driver.source_admission=object.__new__(TrussGraphAdmission)
  driver.source_admission.batch=SimpleNamespace(cursor_before=base64.b64encode(b'{"installationId":"original-source"}').decode())
  driver.source_sessions=object.__new__(TrussFeedSessions);object.__setattr__(driver.source_sessions,'feed','feed')
  driver.ack_sessions=driver.source_sessions
  roles=('object_current','edge_current','tombstone','whole_source_history');driver.tables={role:'local.trussgraph.'+role for role in roles}
  backend=FakeBackend();manifest=dict(backend.rows[0],table_versions_json=json.dumps({table:6 for table in driver.tables.values()}),recorded_at='1791486158556147',validation_report_json='{"complete":true}')
  driver.policy=SimpleNamespace(active={'manifest':manifest,'expected':{role:[]for role in roles}})
  typed='type_id'if kind=='object'else'rel_type_id';identity={'source_system':'original-source',typed:1,'id':2}
  row={'source_system':'original-source',typed:'1','id':'2','entity_version':'1','lookup_hash':hashlib.sha256(json.dumps(identity,separators=(',',':')).encode()).hexdigest(),
   'published_at':'1791486158556147','props_json':'{"1":"雪"}','retained_json':' {} ','logical_key_json':'["key"]'}
  if kind=='edge':row.update(source_type='1',source_id='3',target_type='1',target_id='4')
  role='object_current'if kind=='object'else'edge_current'
  if present:driver.policy.active['expected'][role]=[dict(row)]
  driver.admission_facts=lambda:{'profile':'unit-original-truss'};state={'writer':False,'pins':False};calls=[]
  def require(supplied):
   if supplied is not context or not state['writer']:raise PermissionError('original context')
  driver.require=require
  @contextmanager
  def writer(feed,supplied):
   if supplied is not context:raise PermissionError('original context')
   state['writer']=True
   try:yield
   finally:state['writer']=False
  @contextmanager
  def hold(vector,*,context):
   require(context);state['pins']=True
   try:yield
   finally:state['pins']=False
  driver.writer=writer;driver.hold=hold
  driver.authorize=lambda *args:require(args[0])
  driver.descriptors=lambda publication_id:[manifest]
  driver.validate_descriptor=lambda descriptor,supplied:require(supplied)
  driver.inspect_snapshot=lambda table,version:Snapshot(table,'trusted-uuid',version)
  def query(sql,parameters):
   self.assertTrue(state['pins']);calls.append(sql)
   return SQLResult([{'id':'trusted-uuid'}]if sql.startswith('DESCRIBE')else([dict(row)]if present else[]))
  driver.transport=SimpleNamespace(targets={table:SimpleNamespace(uuid='trusted-uuid')for table in driver.tables.values()},query=query)
  return driver,row,state,calls
 def test_object_and_edge_preserve_complete_original_carriers(self):
  for kind in ('object','edge'):
   driver,row,state,calls=self.fixture(kind)
   answer=read_original_truss_singleton(driver,kind=kind,type_id=1,entity_id=2)
   self.assertEqual(dict(answer),row);self.assertFalse(state['writer']);self.assertFalse(state['pins'])
   self.assertTrue(any('VERSION AS OF 6' in sql and 'LIMIT 2' in sql for sql in calls))
 def test_absence_requires_independent_absence(self):
  driver,_,state,_=self.fixture(present=False)
  self.assertIsNone(read_original_truss_singleton(driver,kind='object',type_id=1,entity_id=2))
  driver,row,state,_=self.fixture();original=driver.transport.query
  driver.transport.query=lambda sql,p:original(sql,p)if sql.startswith('DESCRIBE')else SQLResult([])
  with self.assertRaises(PermissionError):read_original_truss_singleton(driver,kind='object',type_id=1,entity_id=2)
  self.assertFalse(state['pins'])
 def test_changed_full_row_refuses_without_result(self):
  driver,row,state,_=self.fixture();row['props_json']='{"1":"foreign"}'
  with self.assertRaises(PermissionError):read_original_truss_singleton(driver,kind='object',type_id=1,entity_id=2)
  self.assertFalse(state['writer']);self.assertFalse(state['pins'])
 def test_command_validates_before_owner_then_restores_before_read(self):
  from unittest.mock import patch
  from ashlar_host.truss_singleton import run_read_truss_singleton_command
  driver,row,_,_=self.fixture();events=[]
  def owner(configuration,output,operation):
   events.append('owner');return operation(driver,{'request_digest':'original'})
  def restore(selected,digest):
   self.assertIs(selected,driver);self.assertEqual(digest,'original');events.append('restore')
  def read(selected,**identity):
   self.assertIs(selected,driver);events.append('read');return row
  with patch('ashlar_host.truss_resume_cli.run_selected_truss_command',side_effect=owner),patch('ashlar_host.truss_recovery.restore_applied_truss_publication',side_effect=restore),patch('ashlar_host.truss_singleton.read_original_truss_singleton',side_effect=read):
   with self.assertRaises(ValueError):run_read_truss_singleton_command('config','output',kind='object',type_id=True,entity_id=2)
   self.assertEqual(events,[])
   result=run_read_truss_singleton_command('config','output',kind='object',type_id=1,entity_id=2)
  self.assertEqual(events,['owner','restore','read']);self.assertEqual(result['row'],row);self.assertEqual(result['publication_id'],'p1')
