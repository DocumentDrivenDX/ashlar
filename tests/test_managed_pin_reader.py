# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual synthetic private files/inert driver only; no PostgreSQL or credentials."""
import copy,json,os,tempfile
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.managed_pin_reader import (ManagedPinReader,ManagedPinReaderConfig,ManagedPinReaderError,parse_managed_pin_reader)

class ReaderTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup)
        self.path=Path(self.temp.name).resolve()/'synthetic.pgpass';self.path.write_bytes(b'not-an-operator-credential\n');self.path.chmod(0o600)
        self.value=dict(profile='ashlar-managed-pin-reader/0.1',host='127.0.0.1',hostaddr='127.0.0.1',port=25440,expected_server_address='172.18.0.2',expected_server_port=5432,dbname='pins',user='ordinary_reader',sslmode='disable',passfile=str(self.path),connect_timeout_seconds=5,statement_timeout_ms=5000,lock_timeout_ms=1000)
    def config(self):return parse_managed_pin_reader(json.dumps(self.value).encode())
    def driver(self):
        row=dict(current_user='ordinary_reader',session_user='ordinary_reader',database='pins',server_address='172.18.0.2',server_port='5432',statement_timeout='5000',lock_timeout='1000')
        row.update({k:True for k in ('login','schema_usage','inventory_read','assert_active','assert_available')})
        row.update({k:False for k in ('superuser','create_role','create_db','replication','bypass_rls','schema_create','table_write','register','release','maintenance','table_maintenance','quarantine_cleanup','close_quarantine','quarantine_write','writer_member','maintenance_member','recovery_member')})
        events=[];inventory=[];conn=SimpleNamespace(autocommit=False,info=SimpleNamespace(transaction_status=0),add_notice_handler=lambda cb:events.append('notice-suppressed'))
        class Cursor:
            def execute(self,sql,args):
                conn.info.transaction_status=2
                if sql.startswith('SELECT current_user'):
                    events.append('observe');self.rows=[dict(row)]
                elif sql.startswith('SELECT table_name'):
                    events.append('inventory');self.rows=[dict(r) for r in inventory]
                else:
                    events.append('assert-active' if 'assert_active' in sql else 'available')
                    self.rows=[{'guard':None}]
                names=list(self.rows[0]) if self.rows else ['table_name','table_uuid','version','digest','released']
                self.description=[SimpleNamespace(name=k) for k in names]
                self.names=names
            def fetchmany(self,n):return [tuple(r[k] for k in self.names) for r in self.rows]
            def __enter__(self):return self
            def __exit__(self,*args):events.append('cursor-close')
        conn.cursor=Cursor;conn.inventory=inventory
        def rollback():events.append('rollback');conn.info.transaction_status=0
        def commit():events.append('commit');conn.info.transaction_status=0
        conn.rollback=rollback;conn.commit=commit;conn.close=lambda:events.append('close')
        selected=[]
        def connect(**kwargs):selected.append(kwargs);return conn
        return SimpleNamespace(connect=connect),conn,row,events,selected
    def run_owner(self,body):
        driver,conn,row,events,selected=self.driver();context=object();owner=ManagedPinReader(self.config(),context)
        with patch.dict(os.environ,{},clear=True),patch('ashlar_host.managed_pin_reader.importlib.import_module',return_value=driver):
            body(owner,context,conn,row,events,selected)
        return events,selected
    def test_real_private_file_forwarded_server_identity_and_transaction_lifetime(self):
        leaked=[]
        def body(owner,ctx,conn,row,events,selected):
            with owner.transaction(ctx) as session:
                leaked.append(session);self.assertNotIn('commit',events);self.assertNotIn('close',events)
                self.assertNotIn('rollback',events);self.assertEqual(conn.info.transaction_status,2)
                with self.assertRaises(ManagedPinReaderError):session.query('COMMIT',{})
            self.assertFalse(leaked[0].active);self.assertFalse(owner.active)
        events,selected=self.run_owner(body)
        self.assertEqual(events.count('observe'),2);self.assertEqual(events[-2:],['commit','close'])
        self.assertEqual(selected[0]['port'],25440);self.assertEqual(selected[0]['passfile'],str(self.path));self.assertNotIn('password',selected[0]);self.assertNotIn('service',selected[0])
        self.assertEqual(selected[0]['options'],'-c statement_timeout=5000 -c lock_timeout=1000')
    def test_wrong_identity_or_writer_privileges_refuse_and_close_without_yield(self):
        for key,value in [('server_address','127.0.0.1'),('server_port','25440'),('session_user','postgres'),('writer_member',True),('table_maintenance',True),('quarantine_cleanup',True),('close_quarantine',True),('quarantine_write',True),('recovery_member',True)]:
            with self.subTest(key=key):
                def body(owner,ctx,conn,row,events,selected):
                    row[key]=value
                    with self.assertRaises(ManagedPinReaderError):
                        with owner.transaction(ctx):self.fail('Unadmitted connection yielded')
                    self.assertIn('rollback',events);self.assertIn('close',events)
                self.run_owner(body)
    def test_body_primary_and_all_cleanup_with_first_cancellation(self):
        primary=KeyboardInterrupt('owner')
        def body(owner,ctx,conn,row,events,selected):
            def rollback():events.append('rollback-failed');raise OSError('private-connection-detail')
            with self.assertRaises(KeyboardInterrupt) as got:
                with owner.transaction(ctx):
                    conn.rollback=rollback
                    raise primary
            self.assertIs(got.exception,primary);self.assertIn('close',events);self.assertTrue(primary.cleanup_failed)
        self.run_owner(body)
        primary=ValueError('business')
        def normal(owner,ctx,conn,row,events,selected):
            with self.assertRaises(ValueError) as got:
                with owner.transaction(ctx):raise primary
            self.assertIs(got.exception,primary);self.assertIn('close',events)
        self.run_owner(normal)
    def test_config_environment_and_private_file_refuse_before_driver(self):
        for change in ({'port':True},{'service':'ambient'},{'expected_server_address':'host.example'},{'lock_timeout_ms':5001}):
            value=copy.deepcopy(self.value);value.update(change)
            with self.assertRaises(ManagedPinReaderError):parse_managed_pin_reader(json.dumps(value).encode())
        for env,mode in [({'PGSERVICE':'operator'},0o600),({},0o644)]:
            self.path.chmod(mode);ctx=object()
            with patch.dict(os.environ,env,clear=True),patch('ashlar_host.managed_pin_reader.importlib.import_module') as driver:
                with self.assertRaises(ManagedPinReaderError):
                    with ManagedPinReader(self.config(),ctx).transaction(ctx):self.fail('No acquisition')
                driver.assert_not_called()
    def test_driver_error_is_fixed_and_cleanup_only_cancellation_is_exact(self):
        ctx=object();failure=RuntimeError('secret-server-path')
        with patch.dict(os.environ,{},clear=True),patch('ashlar_host.managed_pin_reader.importlib.import_module',return_value=SimpleNamespace(connect=lambda **kw:(_ for _ in ()).throw(failure))):
            with self.assertRaises(ManagedPinReaderError) as got:
                with ManagedPinReader(self.config(),ctx).transaction(ctx):pass
            self.assertNotIn('secret',str(got.exception))
        cancellation=GeneratorExit('first cancellation')
        def body(owner,ctx,conn,row,events,selected):
            def close():events.append('close');raise cancellation
            with self.assertRaises(GeneratorExit) as got:
                with owner.transaction(ctx):conn.close=close
            self.assertIs(got.exception,cancellation)
        self.run_owner(body)

    def test_public_postgres_pins_complete_vector_hold_and_closing_inventory(self):
        from ashlar.pins import PostgresPins,PinVector,PinError
        vector=PinVector('native-owner','manifest','original-publication','a'*64,
            {'client_dev.original.object_current':('uuid-object',1),'client_dev.original.edge_current':('uuid-edge',2)})
        for closing_drift in (False,True):
            with self.subTest(closing_drift=closing_drift):
                def body(owner,ctx,conn,row,events,selected):
                    conn.inventory.extend(dict(table_name=t,table_uuid=u,version=str(v),digest=vector.custody_digest,released=False) for t,(u,v) in vector.targets.items())
                    class Policy:
                        def authorize_read(self,actual,context):
                            if actual is not vector or context is not ctx:raise AssertionError('Original vector/context required')
                    pins=PostgresPins(owner,Policy())
                    def consume():
                        with pins.hold(vector,context=ctx) as actual:
                            self.assertIs(actual,vector);self.assertTrue(owner.active)
                            self.assertEqual(events.count('assert-active'),2);self.assertEqual(events.count('inventory'),1)
                            self.assertNotIn('commit',events);self.assertNotIn('rollback',events);self.assertNotIn('close',events)
                            if closing_drift:conn.inventory.pop()
                    if closing_drift:
                        with self.assertRaises(PinError):consume()
                        self.assertNotIn('commit',events);self.assertIn('rollback',events)
                    else:
                        consume();self.assertEqual(events.count('inventory'),2);self.assertIn('commit',events)
                    self.assertIn('close',events);self.assertFalse(owner.active)
                self.run_owner(body)
