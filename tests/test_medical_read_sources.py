# SPDX-License-Identifier: MIT OR Apache-2.0
"""Medical admission with shared actual reader owners; lower SQL/native ports inert."""
import hashlib
import unittest
from unittest.mock import patch
from ashlar.medical_source import build_transaction
from ashlar.outbox import OutboxTransaction,PostgresOutbox
from ashlar_host.driver import NativeDriver,request_for
from ashlar_host.delta_custody import encoded
from ashlar_host.ack import AckScope
from ashlar_host.source_sessions import OutboxSourceRegistration
from ashlar_host.existing_outbox_authority import OutboxAuthorityRegistration
from ashlar_host.medical_read_sources import MedicalReadSourceFactory
from ashlar_host.postgres import Session
import test_original_medical_admission as medical_controls
import test_commerce_graphframes_installed as graph_controls

class Tests(unittest.TestCase):
 def fixture(self):
  m=medical_controls.MedicalAdmissionTests();m.setUpClass();m.setUp();self.addCleanup(m.doCleanups)
  m.batch,m.bindings=build_transaction(m.model,m.graph,m.baseline,source_system='private-original-medical-fixture')
  capture=patch('ashlar_host.medical_producer.capture',side_effect=m.capture);capture.start();self.addCleanup(capture.stop)
  admission=m.admission()
  with patch.object(NativeDriver,'bind_query_sources'):
   f=graph_controls.GraphCurrentBindingTests.fixture(self)
  d=f.driver;scope=AckScope('ashlar_ack_existing','11111111-1111-1111-1111-111111111111','22222222-2222-2222-2222-222222222222','ordinary',m.batch.feed,m.batch.epoch)
  old=f.observer.source
  source=OutboxSourceRegistration(scope,old.source_schema,old.source_signature_sha256,old.connection_factory)
  oldregistration=OutboxAuthorityRegistration(source,'ordinary_reader','ordinary_ack','truss_e2e',lambda context:Connection(f.events,'ordinary_ack'))
  factory=MedicalReadSourceFactory(oldregistration)
  raw=m.batch.begin+b''.join(r.raw for r in m.batch.records)+m.batch.commit
  f.transaction=OutboxTransaction('ashlar-postgresql-outbox/0.1',m.batch.feed,m.batch.epoch,'0','1',hashlib.sha256(raw).hexdigest(),m.batch)
  d.policy.active['request']=request_for('original',f.transaction,'origin',{m.batch.feed:'original-schema'})
  d.scope_ports={m.batch.feed:{'scope':scope,'source':source.source_schema,'signature':source.source_signature_sha256,'reader':'ordinary_reader','role':'ordinary_ack'}}
  d.source_admission=admission;d.original_admission=encoded(admission.metadata());del d.admission_facts
  f.observer.scope=scope;f.observer.source=source
  d.bind_query_sources(factory);f.factory=factory;f.admission=admission
  return f
 def authority(self,f):
  a=patch.object(Session,'query',lambda session,sql,args:f.authority_type.observe(f.observer,sql,args,session.connection.role))
  b=patch.object(PostgresOutbox,'read',return_value=[f.transaction])
  a.start();b.start();self.addCleanup(a.stop);self.addCleanup(b.stop)
 def test_actual_medical_admission_and_full_shared_current_readers(self):
  f=self.fixture();self.authority(f);d=f.driver
  with d.query_read_interval(d.context):
   d.pin_held=True
   with self.assertRaises(PermissionError):d._apply(d.policy.active['request'],d.context,False)
   with self.assertRaises(PermissionError):d.acknowledge(d.policy.active['request'],None,d.context)
   d.renew_original_graph_read_source(f.vector,context=d.context)
   self.assertEqual(set(f.details),set(d.transport.targets));self.assertEqual(len(f.details),12)
   with self.assertRaises(PermissionError):d.source_sessions.policy.writer.admit_source_writer(f.factory.registration.source,d.context)
   self.assertEqual(len(f.snapshots),0) # A source-only graph port is not complete medical read admission.
   d.pin_held=False
  self.assertTrue(f.events);self.assertEqual(sum(x.startswith('close:')for x in f.events),sum(x.startswith('rollback:')for x in f.events))
  with self.assertRaises(PermissionError):d.renew_original_graph_read_source(f.vector,context=d.context)
 def test_foreign_nominal_admission_and_wrong_medical_scope_refuse(self):
  f=self.fixture()
  with self.assertRaises(PermissionError):f.factory.require_admission(object())
  from dataclasses import replace
  source=f.factory.registration.source
  with self.assertRaises(ValueError):MedicalReadSourceFactory(OutboxAuthorityRegistration(replace(source,scope=replace(source.scope,feed='commerce')),'ordinary_reader','ordinary_ack','truss_e2e',f.factory.registration.metadata_factory))
 def test_replaced_admission_and_reader_callback_refuse(self):
  for change in ('admission','reader'):
   f=self.fixture();self.authority(f);d=f.driver
   with d.query_read_interval(d.context):
    d.pin_held=True
    if change=='admission':d.source_admission=object()
    else:object.__setattr__(f.factory.registration.source,'connection_factory',lambda context:Connection(f.events,'ordinary_reader'))
    with self.assertRaises(PermissionError):d.renew_original_graph_read_source(f.vector,context=d.context)
    d.source_admission=f.admission;d.pin_held=False
 def test_hold_cancel_independently_closes_every_acquired_role(self):
  f=self.fixture();self.authority(f);d=f.driver;primary=KeyboardInterrupt('private');calls=[]
  d.validate_manifest=lambda row,context:None
  def inspect(table,version):
   calls.append(table)
   raise OSError('inert closing failure')
  d.inspect_snapshot=inspect
  with self.assertRaises(KeyboardInterrupt)as error:
   with d.query_read_interval(d.context):
    with d.hold(f.vector,context=d.context):raise primary
  self.assertIs(error.exception,primary);self.assertEqual(set(calls),set(f.vector.targets));self.assertEqual(len(calls),4);self.assertFalse(d.held);self.assertFalse(d.pin_held)
 def test_medical_closing_native_failure_or_drift_attempts_all_owners(self):
  for change in ('first-detail','factory','admission'):
   with self.subTest(change=change):
    f=self.fixture();self.authority(f);d=f.driver;d.validate_manifest=lambda row,context:None
    primary=KeyboardInterrupt('private');native=[];original=d.transport.query
    def query(sql,args):
     native.append(sql)
     if change=='first-detail'and len(native)==1:raise OSError('First closing detail failed')
     return original(sql,args)
    d.transport.query=query
    with self.assertRaises(KeyboardInterrupt)as error:
     with d.query_read_interval(d.context):
      with d.hold(f.vector,context=d.context):
       if change=='factory':d.query_source_factory=None
       if change=='admission':d.source_admission=object()
       raise primary
    self.assertIs(error.exception,primary);self.assertEqual(len(native),6)
    self.assertIn('close:ordinary_reader',f.events);self.assertIn('close:ordinary_ack',f.events)
    self.assertEqual(sum(x.startswith('close:')for x in f.events),sum(x.startswith('rollback:')for x in f.events))
    self.assertFalse(d.pin_held);self.assertFalse(d.held)
 def test_factory_replacement_cannot_suppress_medical_held_closing(self):
  f=self.fixture();self.authority(f);d=f.driver;d.validate_manifest=lambda row,context:None
  with self.assertRaises(PermissionError):
   with d.query_read_interval(d.context):
    with d.hold(f.vector,context=d.context):d.query_source_factory=None
  self.assertEqual(len(f.snapshots),4);self.assertFalse(d.pin_held);self.assertFalse(d.held)
 def test_acquired_gate_cancellation_closes_descriptor_and_preserves_primary(self):
  import os
  f=self.fixture();d=f.driver;primary=GeneratorExit('private');closed=[];gate=[];original=os.close;opening=os.open
  def open_gate(path,*args):
   fd=opening(path,*args)
   if str(path).endswith('.pipeline-writer.lock'):gate.append(fd)
   return fd
  def close(fd):closed.append(fd);original(fd);raise OSError('closing secondary')
  with patch('ashlar_host.driver.os.open',side_effect=open_gate),patch('ashlar_host.driver.os.fstat',side_effect=primary),patch('ashlar_host.driver.os.close',side_effect=close),self.assertRaises(GeneratorExit)as error:
   with d.query_read_interval(d.context):self.fail('No yielded interval')
  self.assertIs(error.exception,primary);self.assertEqual(len(gate),1);self.assertEqual(closed.count(gate[0]),1);self.assertFalse(d.held)
 def test_original_medical_file_drift_refuses_current_use(self):
  f=self.fixture();self.authority(f);d=f.driver
  with self.assertRaises((ValueError,PermissionError)):
   with d.query_read_interval(d.context):
    d.pin_held=True;f.admission.files[0].path.write_bytes(b'changed')
    d.renew_original_graph_read_source(f.vector,context=d.context)

class Connection:
 autocommit=False
 def __init__(self,events,role):self.events=events;self.role=role
 def rollback(self):self.events.append('rollback:'+self.role)
 def close(self):self.events.append('close:'+self.role)
