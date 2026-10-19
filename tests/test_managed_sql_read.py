# SPDX-License-Identifier: MIT OR Apache-2.0
"""Inert SDK and synthetic private selections; no network/auth/native calls."""
import copy,json,os,sys,tempfile
from contextlib import contextmanager
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
import unittest
from ashlar_host import managed_sql_read as m

class ReadTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory();self.addCleanup(self.temp.cleanup);root=Path(self.temp.name).resolve()
        self.credentials=root/'synthetic.cfg';self.credentials.write_text('synthetic-selection-only');self.credentials.chmod(0o600)
        self.cli=root/'inert-cli';self.cli.write_text('inert-never-executed');self.cli.chmod(0o700)
    def config(self):return m.ManagedSQLReadConfig('https://adb-123.1.azuredatabricks.net','selected',self.credentials,self.cli,'a'*16,'dedicated','owner@example','actor1','owner@example',1,1,4,4096)
    def result(self):
        return dict(statement_id='owned-1',status=dict(state='SUCCEEDED'),manifest=dict(format='JSON_ARRAY',truncated=False,total_row_count=2,total_chunk_count=1,schema=dict(column_count=2,columns=[dict(position=0,name='x',type_text='STRING'),dict(position=1,name='y',type_text='STRING')]),chunks=[dict(chunk_index=0,row_offset=0,row_count=2)]),result=dict(chunk_index=0,row_offset=0,row_count=2,data_array=[['λ',None],['λ',None]]))
    @contextmanager
    def fixture(self,responses=None):
        c=self.config();calls=[];pending=list(responses or [self.result()]);metadata=[]
        actor=SimpleNamespace(id=c.actor_id,user_name=c.actor_name)
        warehouse=SimpleNamespace(id=c.warehouse_id,name=c.warehouse_name,creator_name=c.warehouse_creator,state=SimpleNamespace(value='RUNNING'),enable_serverless_compute=True)
        def request(method,path,body=None):
            calls.append((method,path,body))
            if path.endswith('/cancel'):return {}
            response=pending.pop(0)
            if isinstance(response,BaseException):raise response
            return response
        def user():metadata.append('actor');return actor
        w=SimpleNamespace(config=SimpleNamespace(host=c.host,profile=c.profile,config_file=str(c.config_file),databricks_cli_path=str(c.cli),auth_type='databricks-cli'),current_user=SimpleNamespace(me=user),warehouses=SimpleNamespace(get=lambda _:warehouse),api_client=SimpleNamespace(do=request))
        arguments=[]
        def construct(**kwargs):arguments.append(kwargs);return w
        with patch.dict(os.environ,{},clear=True),patch.object(m.os,'getsid',return_value=os.getppid()),patch.object(m.os,'getpgrp',return_value=os.getppid()),patch.object(m.importlib.metadata,'version',return_value='0.117.0'),patch.object(m.importlib,'import_module',return_value=SimpleNamespace(WorkspaceClient=construct)):
            context=m.managed_sql_child_context()
            yield c,context,w,warehouse,calls,arguments,metadata
    def test_actual_retained_supervisor_child_matches_capture_contract(self):
        from ashlar_host.evolution_producer import capture
        source=Path(__file__).resolve().parents[1]/'src'
        code='import sys,json;sys.path.insert(0,'+repr(str(source))+');from ashlar_host.managed_sql_read import managed_sql_child_context;print(json.dumps(vars(managed_sql_child_context())))'
        out,err=capture([sys.executable,'-I','-B','-c',code],cwd=Path(self.temp.name).resolve(),
            environment={'PATH':'/usr/bin:/bin','LANG':'C','LC_ALL':'C'},timeout_seconds=5,maximum_output_bytes=4096)
        facts=json.loads(out);self.assertEqual(err,b'')
        self.assertEqual(facts['session'],facts['group']);self.assertNotEqual(facts['pid'],facts['session'])
    def test_wrong_supervisor_parent_refuses_before_sdk_acquisition(self):
        with self.fixture() as (c,ctx,*_):
            with patch.object(m.os,'getppid',return_value=ctx.session+1),patch.object(m.importlib,'import_module') as acquire:
                with self.assertRaises(m.ManagedSQLReadError):m.ManagedSQLReader(c,ctx)
                acquire.assert_not_called()
    def test_concrete_sdk_identity_complete_null_duplicate_bag_and_lifetime(self):
        with self.fixture() as (c,ctx,w,warehouse,calls,args,metadata):
            with m.open_managed_sql_read(c,context=ctx) as reader:
                result=reader.query('SELECT x,y FROM original',{})
                self.assertEqual(result.rows,[{'x':'λ','y':None},{'x':'λ','y':None}]);self.assertEqual(result.columns,(('x','STRING'),('y','STRING')))
                self.assertEqual(reader.statement_observations,(('owned-1','SUCCEEDED'),))
            self.assertFalse(reader.active);self.assertEqual(args[0]['retry_timeout_seconds'],1)
            self.assertEqual(calls[0][2]['row_limit'],5);self.assertEqual(calls[0][2]['byte_limit'],4096)
            self.assertGreaterEqual(metadata.count('actor'),4)
            with self.assertRaises(m.ManagedSQLReadError):reader.query('SELECT x',{})
    def test_incomplete_or_untyped_carriers_never_release_prefix(self):
        for mutate in (lambda r:r['manifest'].update(truncated=True),lambda r:r['manifest'].update(total_row_count=1),lambda r:r['result'].update(next_chunk_index=1),lambda r:r['result']['data_array'][0].__setitem__(0,1),lambda r:r['manifest']['schema'].update(column_count=True)):
            r=self.result();mutate(r)
            with self.assertRaises(m.ManagedSQLReadError):m.decode_managed_sql_result(r,maximum_rows=4,maximum_bytes=4096)
    def test_boolean_metadata_cannot_alias_integer_counts_or_offsets(self):
        r=self.result();r['manifest']['schema']['columns']=r['manifest']['schema']['columns'][:1]
        r['manifest']['schema']['column_count']=1;r['result']['data_array']=[[v[0]] for v in r['result']['data_array']]
        self.assertEqual(len(m.decode_managed_sql_result(r,maximum_rows=4,maximum_bytes=4096).rows),2)
        r['manifest']['schema']['column_count']=True
        with self.assertRaises(m.ManagedSQLReadError):m.decode_managed_sql_result(r,maximum_rows=4,maximum_bytes=4096)
        for key,value in (('chunk_index',False),('row_offset',False),('row_count',True)):
            r=self.result();r['manifest']['chunks'][0][key]=value
            with self.subTest(key=key),self.assertRaises(m.ManagedSQLReadError):
                m.decode_managed_sql_result(r,maximum_rows=4,maximum_bytes=4096)
    def test_owned_pending_cancellation_and_closing_preserve_primary(self):
        primary=KeyboardInterrupt('operation')
        responses=[dict(statement_id='owned-1',status=dict(state='PENDING')),primary,dict(statement_id='owned-1',status=dict(state='CANCELED'))]
        with self.fixture(responses) as (c,ctx,w,warehouse,calls,args,metadata):
            with self.assertRaises(KeyboardInterrupt) as got:
                with m.open_managed_sql_read(c,context=ctx) as reader:reader.query('SELECT x',{})
            self.assertIs(got.exception,primary);self.assertFalse(reader.active);self.assertFalse(reader.pending)
            self.assertEqual([path for method,path,_ in calls if method=='POST'],['/api/2.0/sql/statements','/api/2.0/sql/statements/owned-1/cancel'])
    def test_stopped_or_drifted_native_warehouse_refuses_without_submission(self):
        with self.fixture() as (c,ctx,w,warehouse,calls,args,metadata):
            warehouse.state.value='STOPPED'
            with self.assertRaises(m.ManagedSQLReadError):
                with m.open_managed_sql_read(c,context=ctx):self.fail('Not admitted')
            self.assertEqual(calls,[])
        with self.fixture() as (c,ctx,w,warehouse,calls,args,metadata):
            with self.assertRaises(m.ManagedSQLReadError):
                with m.open_managed_sql_read(c,context=ctx):warehouse.name='another'
    def test_shared_pair_and_ambient_credentials_refuse_without_sdk_construction(self):
        c=self.config()
        with self.assertRaises(m.ManagedSQLReadError):m.ManagedSQLReadConfig(m.SHARED_ENDPOINT[0],c.profile,c.config_file,c.cli,m.SHARED_ENDPOINT[1],c.warehouse_name,c.warehouse_creator,c.actor_id,c.actor_name,1,1,4,4096)
        for alias in (m.SHARED_ENDPOINT[0].upper().replace('HTTPS','https'),m.SHARED_ENDPOINT[0]+':443'):
            with self.subTest(alias=alias),self.assertRaises(m.ManagedSQLReadError):m.ManagedSQLReadConfig(alias,c.profile,c.config_file,c.cli,m.SHARED_ENDPOINT[1],c.warehouse_name,c.warehouse_creator,c.actor_id,c.actor_name,1,1,4,4096)
        with self.fixture() as (c,ctx,w,warehouse,calls,args,metadata):
            with patch.dict(os.environ,{'DATABRICKS_TOKEN':'synthetic-never-used'}):
                with self.assertRaises(m.ManagedSQLReadError):m.ManagedSQLReader(c,ctx)
            self.assertEqual(args,[])

    def test_late_terminal_response_refuses_result_without_canceling_finished_statement(self):
        with self.fixture() as (c,ctx,w,warehouse,calls,args,metadata):
            with self.assertRaises(m.ManagedSQLReadError):
                with m.open_managed_sql_read(c,context=ctx) as reader:
                    with patch.object(m.time,'monotonic',side_effect=[0,2]):reader.query('SELECT x',{})
            self.assertFalse(reader.pending);self.assertEqual(reader.statement_observations,(('owned-1','SUCCEEDED'),))
            self.assertFalse(any(path.endswith('/cancel') for _,path,_ in calls))
