# SPDX-License-Identifier: MIT OR Apache-2.0
"""Actual minted read scopes with inert Spark catalog state; no engine execution."""
import json,re,unittest
from contextlib import contextmanager
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.publication import ResolvedPublication,Snapshot
from ashlar_host.configured_query_aliases import open_configured_query_aliases
import test_configured_local_read as read_fixture

class Spark:
    def __init__(self,transport,columns):
        self.version='4.0.1';self.sparkContext=SimpleNamespace(master='local[1]',applicationId='inert-local');self.calls=[];self.tables={};self.databases=set();self.transport=transport;self.columns=columns
        self.catalog=SimpleNamespace(currentCatalog=lambda:'spark_catalog',currentDatabase=lambda:'default',getTable=lambda name:SimpleNamespace(tableType=self.tables[name]['type']))
        self.read=self
    def format(self,value):assert value=='delta';return self
    def option(self,name,value):assert name=='versionAsOf';self.snapshot_version=value;return self
    def load(self,path):
        role=next(t.table.rsplit('.',1)[1]for t in self.transport.targets.values()if str(t.path)==path);columns=self.columns[role]
        fields=[SimpleNamespace(name=n,dataType=SimpleNamespace(simpleString=lambda k=k:k.lower()))for n,k in columns]
        return SimpleNamespace(schema=SimpleNamespace(fields=fields,json=lambda:json.dumps({'type':'struct','fields':[{'name':n,'type':k.lower(),'nullable':True,'metadata':{}}for n,k in columns]})))
    def sql(self,sql):
        self.calls.append(sql);names=re.findall(r'`([^`]+)`',sql);name='.'.join(names)
        result=[]
        if sql.startswith('CREATE DATABASE '):
            assert name not in self.databases;self.databases.add(name)
        elif sql.startswith('CREATE TABLE '):
            assert name not in self.tables;path=sql.split(" LOCATION '")[1][:-1];target=next(t for t in self.transport.targets.values()if str(t.path)==path)
            self.tables[name]={'id':target.uuid,'location':'file:'+path,'type':'EXTERNAL','format':'delta'}
        elif sql.startswith('DESCRIBE DETAIL '):result=[SimpleNamespace(asDict=lambda:dict(self.tables[name]))]
        elif sql.startswith('DROP TABLE '):del self.tables[name]
        elif sql.startswith('DROP DATABASE '):
            if any(t.startswith(name+'.')for t in self.tables):raise PermissionError('Nonempty owned database')
            self.databases.remove(name)
        else:raise AssertionError(sql)
        return SimpleNamespace(collect=lambda:result,first=lambda:result[0])

class Tests(unittest.TestCase):
    def scope(self,action):
        fixture=read_fixture.Tests();original=read_fixture.read_configured_local_singleton;ran=[]
        def selected(transport,source,progress,**kwargs):
            if not ran:
                ran.append(True);options={k:v for k,v in kwargs.items()if k not in ('type_id','entity_id')}
                with read_fixture.open_configured_local_read(transport,source,progress,**options)as scope:
                    spark=Spark(transport,scope.columns);transport.spark=spark;action(scope,transport,spark)
            return original(transport,source,progress,**kwargs)
        try:
            with patch.object(read_fixture,'read_configured_local_singleton',side_effect=selected):fixture.test_old_and_deleted_singleton_full_core_read_and_no_writes()
            self.assertTrue(ran)
        finally:fixture.doCleanups()
    def test_creation_schema_identity_and_owned_metadata_cleanup(self):
        def action(scope,transport,spark):
            descriptor=scope.descriptor;vector=scope.vector
            with open_configured_query_aliases(scope,transport)as owner:
                self.assertEqual(len(owner),4);self.assertTrue(all(a.startswith('spark_catalog.ashlar_configured_query_')for a in owner.values()));owner.renew()
                with self.assertRaises(TypeError):owner['foreign']='bad'
                original=next(iter(owner));target=transport.targets[original]
                resolved=ResolvedPublication(descriptor,{t:Snapshot(t,u,v)for t,(u,v)in vector.targets.items()})
                schema=owner.native_table_schema({'name':owner[original].split('.'),'uuid':target.uuid,'version':descriptor.versions[original]},resolved,scope.context)
                self.assertEqual(schema['nativeTypes'],[list(c)for c in scope.columns[original.rsplit('.',1)[1]]])
                spoofed=ResolvedPublication(descriptor,{t:Snapshot('foreign.table.label',u,v)for t,(u,v)in vector.targets.items()})
                with self.assertRaises(PermissionError):owner.native_table_schema({'name':owner[original].split('.'),'uuid':target.uuid,'version':descriptor.versions[original]},spoofed,scope.context)
                self.assertEqual(spark.snapshot_version,descriptor.versions[original]);self.assertIs(scope.descriptor,descriptor);self.assertIs(scope.vector,vector)
            self.assertFalse(spark.tables);self.assertFalse(spark.databases)
            with self.assertRaises(PermissionError):owner.renew()
            self.assertTrue(all('CASCADE'not in c and 'IF NOT EXISTS'not in c for c in spark.calls))
        self.scope(action)
    def test_foreign_replacement_and_cancellation_never_drop_foreign_alias(self):
        def action(scope,transport,spark):
            for cancelled in (False,True):
                primary=KeyboardInterrupt();release=[];foreign=None
                try:
                    with open_configured_query_aliases(scope,transport)as owner:
                        foreign=next(iter(owner.values()));spark.tables[foreign]['id']='foreign-replacement'
                        if cancelled:raise primary
                    release.append(True)
                except KeyboardInterrupt as caught:self.assertTrue(cancelled);self.assertIs(caught,primary)
                except PermissionError:self.assertFalse(cancelled)
                self.assertEqual(release,[]);self.assertIn(foreign,spark.tables)
                self.assertNotIn('DROP TABLE `'+foreign.replace('.','`.`')+'`',spark.calls)
                # Inert fixture reset only; real owner deliberately leaves unknown metadata.
                spark.tables.clear();spark.databases.clear()
            spark.sparkContext.master='spark://shared'
            with self.assertRaises(PermissionError):
                with open_configured_query_aliases(scope,transport):pass
        self.scope(action)

def run_with_alias_scope(action):
    """Real minted source/read/alias owners with inert native SQL/catalog only."""
    def held(scope,transport,spark):
        with open_configured_query_aliases(scope,transport)as aliases:action(scope,transport,aliases,spark)
    Tests().scope(held)

if __name__=='__main__':unittest.main()
