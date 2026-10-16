"""Inert local custody mechanics; no native authority qualification."""
import hashlib
import json
import types
import unittest
from unittest.mock import patch
from ashlar_host.local_current_development import LocalCurrentDevelopmentOwner, local_current_development_context


def sha(text):return hashlib.sha256(text.encode()).hexdigest()
class Parent:
    def source_request_json(self):return '{}'
class Child:
    def __init__(self):self.parent=Parent()
    def source_preparation(self):return self.parent
    def designation_json(self):return '{"designation":"original"}'
    def native_request_json(self):return json.dumps({'binding':{'sha256':'b'*64},'scans':[{'scan':'s0','record':{'element':'R'}}]})
class Frame:
    def __init__(self,rows,names):
        self.rows=rows;self.schema=types.SimpleNamespace(fields=[types.SimpleNamespace(name=x,nullable=False,dataType=types.SimpleNamespace(simpleString=lambda:'string'))for x in names])
    def selectExpr(self,*args):return self
    def toLocalIterator(self,prefetchPartitions):return iter(self.rows)
class Spark:
    def __init__(self):
        self.sparkContext=types.SimpleNamespace(master='local[1]',applicationId='app-original')
        self.version='0';self.uuid='native-delta';self.location='file:/private/tmp/local-original-delta';self.raw_schema='{"type":"struct","fields":[]}'
        self.read=self
    def sql(self,statement):
        if statement.startswith('DESCRIBE DETAIL'):return Frame([[self.uuid,'delta',self.location]],['id','format','location'])
        if statement.startswith('DESCRIBE HISTORY'):return Frame([[self.version]],['version'])
        raise AssertionError('Unexpected SQL')
    def format(self,*args):return self
    def option(self,*args):return self
    def table(self,*args):return types.SimpleNamespace(schema=types.SimpleNamespace(json=lambda:self.raw_schema))


class LocalOwnerTests(unittest.TestCase):
    def create(self):
        self.spark=Spark();self.child=Child();self.context=local_current_development_context()
        self.binding=json.dumps({'version':'weft.databricks.current-values/0.1-candidate','sourceRequestSha256':sha('{}'),
            'designationSha256':sha(self.child.designation_json()),'sourceBindingSha256':'b'*64,
            'scans':[{'scan':'s0','target':{'element':'R'},'relation':{'catalog':'spark_catalog','schema':'private','table':'values','uuid':'native-delta','version':'0','schemaSha256':sha(self.spark.raw_schema)}}],'values':[]})
        native=types.SimpleNamespace(CurrentQueryPreparation=Child,ApplicationLineagePreparation=Parent)
        with patch('ashlar_host.local_current_development.importlib.import_module',return_value=native):
            return LocalCurrentDevelopmentOwner(self.spark,self.child,self.binding,self.context)
    def test_actual_original_interval_and_context(self):
        owner=self.create()
        with owner.open_current_query(self.child,self.context)as held:
            self.assertIs(held,owner);self.assertIs(held.binding(),self.binding)
            self.assertIsNone(held.admit(self.child,self.binding,self.context))
            with self.assertRaises(PermissionError):held.renew(self.child,self.binding,local_current_development_context())
        with self.assertRaises(PermissionError):owner.binding()
    def test_native_drift_and_primary_failure_close(self):
        for target in ['version','uuid','location','raw_schema']:
            owner=self.create()
            with self.subTest(target=target),self.assertRaises((PermissionError,ValueError)):
                with owner.open_current_query(self.child,self.context):setattr(self.spark,target,'changed')
            self.assertFalse(owner.active)
        owner=self.create()
        with self.assertRaises(LookupError):
            with owner.open_current_query(self.child,self.context):
                self.spark.version='changed';raise LookupError('primary')
        self.assertFalse(owner.active)
    def test_session_or_child_replacement_refuses(self):
        owner=self.create();self.spark.sparkContext.applicationId='other'
        with self.assertRaises(PermissionError):
            with owner.open_current_query(self.child,self.context):pass
        owner=self.create()
        with self.assertRaises(PermissionError):
            with owner.open_current_query(Child(),self.context):pass

    def test_owner_factory_uses_exact_original_session(self):
        from ashlar_host.path_capture import PathCaptureConfig
        owner=self.create();limits=PathCaptureConfig(10,256,1024)
        backend=owner.backend(capture_limits=limits,source_limits=limits)
        self.assertIs(backend.spark,self.spark);self.assertIs(backend.authority_owner,owner)
        owner.spark=Spark()
        with self.assertRaises(PermissionError):owner.backend(capture_limits=limits,source_limits=limits)
