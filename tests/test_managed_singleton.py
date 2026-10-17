"""Inert lower-port composition controls; no SDK, producer or native calls."""
from contextlib import contextmanager
import json,os,tempfile,unittest,hashlib
from ashlar.publication import Descriptor
from ashlar.retention_policy import RetentionGate as ActualGate
from types import MappingProxyType
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar_host.managed_singleton import read_managed_singleton,ManagedSingletonSelector
from ashlar_host.managed_publication_selection import ManagedPublicationSelection,ROLES
from ashlar_host.managed_sql_read import ManagedSQLReader
from ashlar_host.managed_pin_reader import ManagedPinReader
from ashlar.native import SQLResult
from ashlar.pins import PinVector,PostgresPins,INVENTORY_SQL
from ashlar.effect_validation import validate_effect_snapshot as ActualParity
from ashlar.protocol import inspect_protocol as ActualProtocol

class Tests(unittest.TestCase):
    def run_case(self,*,cancel=False,missing=False,real_core=False,wrong_selector=False,distinct_reader=False):
        context=object();seen=[];namespace='test.original';uuids={namespace+'.'+r:r for r in ROLES}
        row={'source_system':'original','type_id':'1','id':'2','entity_version':'1','published_at':'1','lookup_hash':hashlib.sha256(b'{"source_system":"original","type_id":1,"id":2}').hexdigest()}
        expected={r:[]for r in ROLES};expected['object_current']=[]if missing else [row]
        selection=object.__new__(ManagedPublicationSelection)
        selection.configuration=SimpleNamespace(schema_alias='source',intake=SimpleNamespace(document_id='doc',document_revision='1',row=lambda:{'document_id':'doc','document_revision':'1','complete_interpretation':True}),policy=SimpleNamespace(source_system='original',types={1:{}}))
        selection.batches=(object(),);selection.spec=SimpleNamespace(completed_ordinal=1);selection.namespace=namespace
        selection.original_actor_id='actor-id';selection.original_warehouse_id='warehouse';selection.original_materialization_clock_us='1';selection.table_uuids=tuple(uuids.items())
        tables={t:{'uuid':u}for t,u in uuids.items()};tables[namespace+'.publication_manifest']={'uuid':'manifest'}
        selection.installation_json=json.dumps({'authenticated_owner':'owner','tables':tables});selection.intake_proof_json=json.dumps({'table':namespace+'.schema_intake','table_uuid':'intake'})
        selection.targets_json=json.dumps({t:{'columns':[['x','STRING']],'rows':expected[r]}for r,t in ((r,namespace+'.'+r)for r in ROLES)})
        versions={t:1 for t in uuids};report={'retention':{'profile':'ashlar-retention-window/0.1','margin_us':'600000000','targets':{t:{'uuid':u,'version':1,'committed_at':'1','readable_until':'604200000001'}for t,u in uuids.items()}}}
        raw={'publication_id':'pub','profile_version':'ashlar-delta/0.3','table_versions_json':json.dumps(versions),'schema_revisions_json':json.dumps({'source':'1'}),'source_progress_json':'{}','validation_report_json':json.dumps(report),'recorded_at':'1'}
        descriptor=Descriptor('pub','ashlar-delta/0.3',MappingProxyType(versions),MappingProxyType({'source':'1'}),MappingProxyType({}),report,raw);selection.descriptor=descriptor
        reader=object.__new__(ManagedSQLReader);reader.config=SimpleNamespace(actor_name='owner',actor_id='actor-id',warehouse_id='dedicated-reader'if distinct_reader else 'warehouse');reader.workspace=object()
        pins=object.__new__(ManagedPinReader);pins.context=context
        vector=PinVector('owner','manifest','pub','a'*64,{t:(u,1)for t,u in uuids.items()})
        calls=[]
        object_columns=tuple((k,'BIGINT'if k in ('id','type_id','entity_version')else 'TIMESTAMP'if k=='published_at'else 'STRING')for k in row)
        def table_columns(role):return object_columns if real_core and role=='object_current'else (('x','STRING'),)
        def query(sql,params):
            calls.append(sql)
            if sql.startswith('DESCRIBE'):
                table=sql[len('DESCRIBE DETAIL '):].replace('`','')
                return SQLResult([{'id':'intake'if table.endswith('schema_intake')else 'manifest'if table.endswith('publication_manifest')else uuids[table],'format':'delta','minReaderVersion':'3','minWriterVersion':'7','tableFeatures':'[]'}])
            if sql.startswith('SHOW'):return SQLResult([])
            if 'AS now_us'in sql:return SQLResult([{'now_us':'2'}])
            if 'FROM `test`.`original`.`publication_manifest`'in sql:return SQLResult([raw])
            if 'LIMIT 0'in sql:return SQLResult([],table_columns('object_current'if '`object_current`'in sql else 'other'))
            if 'LIMIT 1001'in sql:return SQLResult([row]if '`object_current`'in sql and not missing else [])
            if 'lookup_hash=:hash'in sql:return SQLResult([]if missing else [row])
            return SQLResult([{'document_id':'doc','document_revision':'1','complete_interpretation':'true'}])
        @contextmanager
        def transaction(self,supplied):
            seen.append('transaction-open')
            def pin_query(sql,p):
                if sql==INVENTORY_SQL:return SQLResult([{'table_name':t,'table_uuid':u,'version':'1','digest':vector.custody_digest,'released':False}for t,u in uuids.items()])
                return SQLResult([])
            try:yield SimpleNamespace(query=pin_query)
            except BaseException:seen.append('rollback');raise
            else:seen.append('commit')
        original_hold=PostgresPins.hold
        @contextmanager
        def hold(self,value,*,context):
            seen.append('pin-open')
            if real_core or cancel:
                with original_hold(self,value,context=context):yield value
            else:yield value
            seen.append('pin-close')
        error=KeyboardInterrupt()
        def lookup(executor,backend,held,vector,policy,**kw):
            with held.hold(vector,context=context):
                if cancel:raise error
                result=None if missing else row
                policy.authorize_row(descriptor,namespace+'.object_current',result,context)
                return result
        if real_core:
            selection.targets_json=json.dumps({t:{'columns':[list(c)for c in table_columns(r)],'rows':expected[r]}for r,t in ((r,namespace+'.'+r)for r in ROLES)})
        original_selection=(selection.original_warehouse_id,selection.targets_json,selection.installation_json,selection.descriptor)
        with tempfile.TemporaryDirectory()as directory:
            path=Path(directory).resolve();path.chmod(0o700)
            with patch.object(ManagedPublicationSelection,'renew',side_effect=lambda:seen.append('source')),patch.object(ManagedSQLReader,'renew',side_effect=lambda:seen.append('sql')),patch.object(ManagedSQLReader,'query',side_effect=query),patch('ashlar_host.managed_singleton.admit_managed_records',return_value=(b'{}',)),patch('ashlar_host.managed_singleton.load_managed_carriers',return_value=SimpleNamespace(ordered_columns=table_columns)),patch('ashlar_host.managed_singleton.validate_managed_installation',return_value=namespace),patch('ashlar_host.managed_singleton.configured_prefix_carrier_rows',return_value=SimpleNamespace(rows=lambda:expected)),patch('ashlar_host.managed_singleton.manifest_pin_vector',return_value=vector),patch('ashlar_host.managed_singleton.bind_manifest_pins'),patch('ashlar_host.managed_singleton.NativeOwnerLane'),patch('ashlar_host.managed_singleton.inspect_protocol',side_effect=ActualProtocol if real_core else None),patch('ashlar_host.managed_singleton.validate_effect_snapshot',side_effect=lambda ex,t,*a:(seen.append(t),ActualParity(ex,t,*a)if real_core else None)[1]),patch('ashlar_host.managed_singleton.RetentionGate',side_effect=ActualGate if real_core else None)as gate,patch('ashlar_host.managed_singleton.PostgresPins.hold',hold),patch.object(ManagedPinReader,'transaction',transaction),patch('ashlar_host.managed_singleton.read_singleton',side_effect=__import__('ashlar.singleton',fromlist=['read_singleton']).read_singleton if real_core else lookup):
                if wrong_selector:
                    for selected in (ManagedSingletonSelector('foreign',1,2),ManagedSingletonSelector('original',99,2)):
                        with self.assertRaises(PermissionError):read_managed_singleton(selection,reader,pins,producer=object(),selector=selected,context=context,capture_directory=path)
                    self.assertEqual(calls,[]);self.assertEqual(seen,[]);self.assertEqual(list(path.iterdir()),[])
                    return
                if cancel:
                    with self.assertRaises(KeyboardInterrupt)as raised:read_managed_singleton(selection,reader,pins,producer=object(),selector=ManagedSingletonSelector('original',1,2),context=context,capture_directory=path)
                    self.assertIs(raised.exception,error)
                else:self.assertEqual(read_managed_singleton(selection,reader,pins,producer=object(),selector=ManagedSingletonSelector('original',1,2),context=context,capture_directory=path),None if missing else row)
        if cancel:
            self.assertIn('rollback',seen);self.assertNotIn('commit',seen)
        else:self.assertIn('pin-close',seen)
        for table in uuids:self.assertEqual(seen.count(table),1)
        self.assertEqual(seen[-2:],['source','sql'])
        self.assertEqual((selection.original_warehouse_id,selection.targets_json,selection.installation_json,selection.descriptor),original_selection)
        self.assertTrue(all(sql.startswith(('SELECT ','DESCRIBE ','SHOW '))for sql in calls))
        if real_core:
            self.assertLessEqual(len(calls),128)
            self.assertTrue(any('LIMIT 2'in sql for sql in calls))
            self.assertEqual(sum('AS now_us'in sql for sql in calls),3)
            self.actual_statement_count=len(calls)
        return seen
    def test_present_and_missing_source_derived_singleton(self):
        self.run_case();self.run_case(missing=True)
    def test_cancellation_still_closes_four_roles_and_pins(self):self.run_case(cancel=True)
    def test_actual_core_resolution_and_statement_budget(self):
        self.run_case(real_core=True)
        self.assertGreater(self.actual_statement_count,80)
    def test_distinct_dedicated_reader_preserves_original_writer_selection(self):
        self.run_case(real_core=True,distinct_reader=True)
        self.assertEqual(self.actual_statement_count,110)
    def test_foreign_source_or_type_refused_before_producer_or_native(self):self.run_case(wrong_selector=True)
    def test_selector_refuses_bool_and_implicit_source(self):
        for args in [('',1,2),('s',True,2),('s',1,2**63)]:
            with self.assertRaises(PermissionError):ManagedSingletonSelector(*args)
