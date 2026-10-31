# SPDX-License-Identifier: MIT OR Apache-2.0
import json
import re
import tempfile
import unittest
import uuid
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch

from ashlar.apply import empty_state
from ashlar_host.graph_sql import graph_sql_plan, TABLES
from ashlar_host.publication_planning import local_effect_plan
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.schema_rows import local_current_columns
from ashlar_host.truss_graph_admission import TrussGraphAdmission
from ashlar_host.driver import NativeDriver
from ashlar_host.delta_custody import DeltaTarget
from ashlar_host.local_current_carrier_profile import (
    _observed_role, _protocol, SDK058_PROFILE, SDK010_PROFILE, apply_local_writer_settings,
    create_local_current_xxhash64_tables, create_local_current_xxhash64_sdk058_tables,
    create_local_current_xxhash64_sdk010_tables, verify_local_current_profile,
    verify_local_writer_settings,
)
from ashlar_host.truss_note_publication import native_xxhash64,graph_expected_rows

import test_truss_typed_admission


class LocalCurrentXxhash64ProfileTests(unittest.TestCase):
    def test_protocol_uses_shared_configured_local_reader_profile(self):
        detail={'id':'6212b33d-e246-40d0-8c01-46e45dd2fd6b','format':'delta',
                'minReaderVersion':1,'minWriterVersion':7,
                'tableFeatures':['domainMetadata','clustering','invariants','appendOnly']}
        observed=_protocol(detail,detail['id'])
        self.assertEqual(observed,{
            'uuid':detail['id'],'profile':'ashlar-configured-local-delta/0.1',
            'reader_version':1,'writer_version':7,
            'features':['appendOnly','clustering','domainMetadata','invariants']})

    def test_native_long_type_uses_umf_delta_type_name(self):
        try:
            from pyspark.sql.types import LongType, StringType, TimestampType
        except ImportError:
            self.skipTest('pinned PySpark optional extra is not installed')
        data_types=(('source_system',StringType(),False),('type_id',LongType(),False),
                    ('published_at',TimestampType(),False))
        fields=[SimpleNamespace(name=name,dataType=data_type,nullable=nullable,metadata={})
                for name,data_type,nullable in data_types]
        spark = SimpleNamespace(read=SimpleNamespace(format=lambda _: SimpleNamespace(
            load=lambda _: SimpleNamespace(schema=SimpleNamespace(fields=fields)))))
        target_uuid=str(uuid.uuid4())
        detail = {'id':target_uuid,'clusteringColumns':[],'partitionColumns':[],
                  'properties':{},'minReaderVersion':'1','minWriterVersion':'7',
                  'tableFeatures':[]}
        with tempfile.TemporaryDirectory() as folder:
            path=Path(folder)/'object_current';path.mkdir()
            with patch('ashlar_host.local_current_carrier_profile._detail',return_value=detail), \
                 patch('ashlar_host.local_current_carrier_profile._protocol',return_value={}):
                target, observed = _observed_role(spark,'object_current','local.graph.object_current',
                    path,(('source_system','STRING',False),('type_id','BIGINT',False),
                    ('published_at','TIMESTAMP',False)),
                    {'clusterBy':[],'partitionBy':[],'properties':{}})
        self.assertEqual(target.uuid,target_uuid)
        self.assertEqual(observed['fields'],[['source_system','STRING',False],
            ['type_id','BIGINT',False],['published_at','TIMESTAMP',False]])
        self.assertEqual(LongType().simpleString(),'bigint')
        self.assertEqual(LongType().typeName(),'long')

    def test_explicit_profile_changes_only_selected_physical_hash_types(self):
        legacy = local_current_columns(RESOURCE_ROOT)
        selected = local_current_columns(RESOURCE_ROOT,
                                         'ashlar-delta/0.4-xxhash64')
        self.assertEqual(legacy['object_current'][0], ('source_system', 'STRING'))
        self.assertEqual(dict(legacy['object_current'])['lookup_hash'], 'STRING')
        self.assertEqual(dict(selected['object_current'])['lookup_hash'], 'BIGINT')
        self.assertEqual(dict(selected['edge_current'])['lookup_hash'], 'BIGINT')
        self.assertIn('phase', dict(selected['attempts']))
        with self.assertRaises(ValueError):
            local_current_columns(RESOURCE_ROOT, 'ashlar-delta/0.5')

    def test_typed_graph_plan_uses_native_xxhash_and_typed_identity_keys(self):
        batch, catalogs = test_truss_typed_admission.Tests().fixture(
            'decimal', 'required', {'kind': 'decimal', 'text': '1.2300'})
        admission = TrussGraphAdmission(batch, catalogs)
        tables = {role: 'local.trussgraph.' + role for role in TABLES}
        stamp='2026-10-22T00:00:00+00:00'
        after, steps = graph_sql_plan(empty_state(), batch, tables,
            materialized_at=stamp, schema_policy=admission.admit,
            native_scalar_profile='typed',
            lookup_hash_profile='spark-xxhash64-typed-key/0.1')
        merge = next(step for step in steps if step['statement'].startswith('MERGE INTO'))
        insert = next(step for step in steps if 'xxhash64(' in step['statement'])
        self.assertIn('type_id:BIGINT,id:BIGINT', merge['statement'])
        self.assertIn('t.type_id=s.type_id', merge['statement'])
        self.assertIn('xxhash64(CAST(r.source_system AS STRING),CAST(r.type_id AS BIGINT),CAST(r.id AS BIGINT)) AS lookup_hash', insert['statement'])
        self.assertTrue(all(type(row['type_id']) is int and type(row['id']) is int
                            for row in json.loads(merge['parameters']['keys'])))
        typed_tables={role:'local.trussgraph.'+role for role in
                      ('object_current','edge_current','tombstone','whole_source_history')}
        selected,elisions=local_effect_plan(steps,
            {'object_current':[],'edge_current':[]},typed_tables,
            lookup_hash_profile='spark-xxhash64-typed-key/0.1')
        self.assertEqual(sum(step['statement'].startswith('MERGE INTO') for step in selected),0)
        self.assertTrue(elisions)
        self.assertTrue(all(type(key['type_id'])is int and type(key['id'])is int
                            for item in elisions for key in item['source_keys']))
        columns=local_current_columns(RESOURCE_ROOT,'ashlar-delta/0.4-xxhash64')
        class Result:
            def __init__(self,rows):self.rows=rows
            def collect(self):return self.rows
        class Row:
            def __init__(self,value):self.value=value
            def asDict(self,recursive=True):return self.value
        class Spark:
            statements=[]
            def sql(self,statement,args):
                self.statements.append(statement)
                inputs=json.loads(args['keys'])
                hash_rows=[Row({**row,'lookup_hash':-17}) for row in inputs]
                return Result(hash_rows)
        native=Spark()
        hashes=native_xxhash64(native,admission.parsed.changes,after)
        self.assertTrue(native.statements)
        self.assertTrue(all('xxhash64(CAST(k.source_system AS STRING),CAST(k.' in statement
                            and ' AS BIGINT),CAST(k.id AS BIGINT)) AS lookup_hash' in statement
                            for statement in native.statements))
        expected=graph_expected_rows(
            {'object_current':[],'edge_current':[],'tombstone':[],'whole_source_history':[]},
            after,admission.parsed.changes,batch,columns,stamp,admission.parsed.event_bytes,
            admission.parsed.edge_order_keys,lookup_hashes=hashes)
        for role in ('object_current','edge_current'):
            for row in expected[role]:
                self.assertIs(type(row['id']),str)
                self.assertIs(type(row['lookup_hash']),str)
                self.assertIs(type(row['entity_version']),str)
                self.assertEqual(str(int(row['lookup_hash'])),row['lookup_hash'])

    def test_typed_zero_match_elision_requires_exact_typed_key_shape(self):
        table = 'local.trussgraph.object_current'
        quoted = '`local`.`trussgraph`.`object_current`'
        statement = ('MERGE INTO ' + quoted + " t USING (SELECT k.* FROM (SELECT explode(from_json(:keys,'"
            + 'ARRAY<STRUCT<source_system:STRING,type_id:BIGINT,id:BIGINT>>'
            + "')) k)) s ON t.source_system=s.source_system AND t.type_id=s.type_id AND t.id=s.id WHEN MATCHED THEN DELETE")
        step = {'statement': statement,
                'parameters': {'keys': '[{"source_system":"s","type_id":7,"id":11}]'}}
        selected, elisions = local_effect_plan([step],
            {'object_current': [], 'edge_current': []},
            {'object_current': table, 'edge_current': 'local.trussgraph.edge_current',
             'tombstone': 'local.trussgraph.tombstone',
             'whole_source_history': 'local.trussgraph.whole_source_history'},
            lookup_hash_profile='spark-xxhash64-typed-key/0.1')
        self.assertEqual(selected, [])
        self.assertEqual(elisions[0]['source_keys'], [{'source_system': 's', 'type_id': 7, 'id': 11}])
        malformed = dict(step, parameters={'keys': '[{"source_system":"s","type_id":"7","id":11}]'})
        with self.assertRaises(ValueError):
            local_effect_plan([malformed], {'object_current': [], 'edge_current': []},
                {'object_current': table, 'edge_current': 'local.trussgraph.edge_current',
                 'tombstone': 'local.trussgraph.tombstone',
                 'whole_source_history': 'local.trussgraph.whole_source_history'},
                lookup_hash_profile='spark-xxhash64-typed-key/0.1')

    def test_xxhash64_driver_refuses_other_current_truss_versions(self):
        transport=SimpleNamespace(installation_profile={
            'physical_profile':'ashlar-delta/0.4-xxhash64',
            'lookup_hash_profile':'spark-xxhash64-typed-key/0.1'})
        columns={role:(('lookup_hash','BIGINT'),) for role in
                 ('object_current','edge_current','tombstone','whole_source_history')}
        with self.assertRaises(PermissionError):
            NativeDriver(transport,None,None,{}, {}, (), columns,
                source_admission=SimpleNamespace(current_truss_version='0.0.45'),
                lookup_hash_profile='spark-xxhash64-typed-key/0.1')

    def test_umf_six_role_ddl_loop_is_closed_with_inert_spark(self):
        class Spark:
            def __init__(self):
                self.statements=[]
                self.conf=self.Conf()
            class Conf:
                value=None
                def set(self,key,value):self.value=(key,value)
                def get(self,key):return self.value[1] if self.value and self.value[0]==key else None
            def sql(self,statement):
                self.statements.append(statement)

        with tempfile.TemporaryDirectory() as temporary:
            output=Path(temporary).resolve()/'publication'
            output.mkdir()
            roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
            tables={role:'local.trussgraph.'+role for role in roles}
            observed=[]
            def inert_observation(spark,carrier_role,table,path,fields,layout,field_metadata=None):
                self.assertEqual(path.parent,output)
                self.assertTrue(fields)
                self.assertEqual(len(field_metadata),len(fields))
                path.mkdir()
                observed.append((carrier_role,table,layout))
                return DeltaTarget(table,path,'uuid-'+carrier_role),{
                    'carrier_role':carrier_role,'table':table,'path':str(path),
                    'uuid':'uuid-'+carrier_role,'fields':[list(field) for field in fields],
                    'field_metadata':[dict(item) for item in field_metadata],
                    'protocol':{},'clustering_columns':layout.get('clusterBy',[]),
                    'partition_columns':layout.get('partitionBy',[]),
                    'layout_properties':layout.get('properties',{})}
            spark=Spark()
            with patch('ashlar_host.local_current_carrier_profile._observed_role',
                       side_effect=inert_observation):
                targets,profile,columns=create_local_current_xxhash64_tables(spark,output,tables)
            self.assertEqual(len(targets),6)
            self.assertEqual(profile['profile'],'ashlar-local-current-installation-profile/0.1')
            self.assertEqual(profile['umf_revision'],'2ede4c879bea5db13794a6fa30e67c821306c17f')
            self.assertEqual(len(observed),6)
            self.assertEqual(len(spark.statements),6)
            self.assertEqual(set(profile['roles']),set(roles))
            self.assertEqual(set(columns),set(roles))
            self.assertLessEqual(len(json.dumps(profile,sort_keys=True,separators=(',',':')).encode()),65536)
            self.assertEqual(spark.conf.get('spark.sql.parquet.compression.codec'),'zstd')
            self.assertTrue(all(statement.startswith('CREATE TABLE delta.`')
                                and 'USING DELTA' in statement and ' LOCATION ' not in statement
                                for statement in spark.statements))

    def test_sdk058_creator_is_explicit_and_keeps_six_role_identity_closed(self):
        class Conf:
            value=None
            def set(self,key,value):self.value=(key,value)
            def get(self,key):return self.value[1] if self.value and self.value[0]==key else None
        class Spark:
            def __init__(self):self.statements=[];self.conf=Conf()
            def sql(self,statement):self.statements.append(statement)
        with tempfile.TemporaryDirectory() as temporary:
            output=Path(temporary).resolve()/'sdk058';output.mkdir()
            roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
            tables={role:'local.configured.'+role for role in roles}
            def inert_observation(spark,carrier_role,table,path,fields,layout,field_metadata=None):
                path.mkdir()
                return DeltaTarget(table,path,'uuid-'+carrier_role),{
                    'carrier_role':carrier_role,'table':table,'path':str(path),
                    'uuid':'uuid-'+carrier_role,'fields':[list(field) for field in fields],
                    'field_metadata':[dict(item) for item in field_metadata],
                    'protocol':{},'clustering_columns':layout.get('clusterBy',[]),
                    'partition_columns':layout.get('partitionBy',[]),
                    'layout_properties':layout.get('properties',{})}
            with patch('ashlar_host.local_current_carrier_profile._observed_role',
                       side_effect=inert_observation):
                targets,profile,columns=create_local_current_xxhash64_sdk058_tables(
                    Spark(),output,tables,table_prefix='local.configured')
            self.assertEqual(profile['profile'],SDK058_PROFILE)
            self.assertEqual(profile['umf_revision'],'0.5.8')
            self.assertEqual(profile['umf_candidate_commit'],'855f6c48649496fc60b3303798790d0ddb55d38f')
            self.assertEqual(profile['umf_selected_tag_commit'],'efb4042719866888ad80c01cb2da694972eb2738')
            self.assertEqual(set(profile['roles']),set(roles))
            self.assertEqual(len({target.uuid for target in targets}),6)
            self.assertEqual(set(columns),set(roles))

    def test_sdk010_creator_is_explicit_and_profile_reopens_against_same_bridge(self):
        from ashlar_host.configured_local_delta4_publication import _valid_installation_profile_identity
        from ashlar_host.local_current_delta4_umf_sdk010 import (
            BRIDGE_SHA256, BUILD_INPUTS_SHA256, MANIFEST_SHA256, SOURCE_POLICY_SHA256)

        class Conf:
            value=None
            def set(self,key,value):self.value=(key,value)
            def get(self,key):return self.value[1] if self.value and self.value[0]==key else None
        class Spark:
            def __init__(self):self.statements=[];self.conf=Conf()
            def sql(self,statement):self.statements.append(statement)
        with tempfile.TemporaryDirectory() as temporary:
            output=Path(temporary).resolve()/'sdk010';output.mkdir()
            roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
            tables={role:'local.trussgraph.'+role for role in roles}
            spark=Spark()
            def inert_observation(spark,carrier_role,table,path,fields,layout,field_metadata=None):
                path.mkdir()
                return DeltaTarget(table,path,'uuid-'+carrier_role),{
                    'carrier_role':carrier_role,'table':table,'path':str(path),
                    'uuid':'uuid-'+carrier_role,'fields':[list(field) for field in fields],
                    'field_metadata':[dict(item) for item in field_metadata],
                    'protocol':{},'clustering_columns':layout.get('clusterBy',[]),
                    'partition_columns':layout.get('partitionBy',[]),
                    'layout_properties':layout.get('properties',{})}
            with patch('ashlar_host.local_current_carrier_profile._observed_role',side_effect=inert_observation):
                targets,profile,columns=create_local_current_xxhash64_sdk010_tables(spark,output,tables)
            self.assertEqual(profile['profile'],SDK010_PROFILE)
            self.assertEqual(profile['umf_revision'],'0.10.0')
            self.assertEqual(profile['umf_realization_profile'],
                             'ashlar-local-current-delta4-umf-source-sdk010/0.1')
            self.assertEqual(profile['umf_realization_manifest_sha256'],MANIFEST_SHA256)
            self.assertEqual(profile['umf_bridge_sha256'],BRIDGE_SHA256)
            self.assertEqual(profile['umf_build_inputs_sha256'],BUILD_INPUTS_SHA256)
            self.assertEqual(profile['umf_source_policy_sha256'],SOURCE_POLICY_SHA256)
            self.assertTrue(_valid_installation_profile_identity(profile))
            self.assertEqual(set(columns),set(roles));self.assertEqual(len(targets),6)
            self.assertEqual(len(spark.statements),6)

            base_role_keys={'field_metadata','umf_source_bytes_sha256','umf_source_sha256',
                'umf_schema_sha256','umf_target_sha256','umf_proposal_sha256','umf_ddl_sha256',
                'umf_dispositions','umf_writer_settings','umf_proposal'}
            def retained_observation(spark,carrier_role,table,path,fields,layout,field_metadata=None):
                role=next(name for name,binding in profile['roles'].items()
                          if binding['carrier_role']==carrier_role)
                binding=profile['roles'][role]
                observed={key:value for key,value in binding.items() if key not in base_role_keys}
                return DeltaTarget(table,path,binding['uuid']),observed
            with patch('ashlar_host.local_current_carrier_profile._observed_role',side_effect=retained_observation):
                verify_local_current_profile(spark,profile,targets,'ashlar-delta/0.4-xxhash64')

    def test_umf_sdk_selection_dispatch_preserves_default_and_selects_sdk010(self):
        from ashlar_host.truss_note_publication import _create_current_graph_tables
        sentinel=object();legacy_candidate=object()
        with patch('ashlar_host.local_current_carrier_profile.create_local_current_xxhash64_tables',return_value='historical') as old, \
             patch('ashlar_host.local_current_carrier_profile.create_local_current_xxhash64_sdk058_tables',return_value=legacy_candidate) as sdk058, \
             patch('ashlar_host.local_current_carrier_profile.create_local_current_xxhash64_sdk010_tables',return_value=sentinel) as latest:
            self.assertEqual(_create_current_graph_tables('spark','out',{},'ashlar-delta/0.4-xxhash64',None),'historical')
            self.assertIs(_create_current_graph_tables('spark','out',{},'ashlar-delta/0.4-xxhash64','0.5.8'),legacy_candidate)
            self.assertIs(_create_current_graph_tables('spark','out',{},'ashlar-delta/0.4-xxhash64','0.10.0'),sentinel)
        old.assert_called_once_with('spark','out',{})
        sdk058.assert_called_once_with('spark','out',{})
        latest.assert_called_once_with('spark','out',{})
        with self.assertRaises(ValueError):
            _create_current_graph_tables('spark','out',{},'ashlar-delta/0.4-xxhash64','0.9.0')
        with self.assertRaises(ValueError):
            _create_current_graph_tables('spark','out',{},'ashlar-delta/0.3','0.10.0')
        from ashlar_host.truss_note_publication import publish_graph_pages
        with patch('ashlar_host.truss_note_publication.publish_note_pages',return_value={'ok':True}) as publish:
            result=publish_graph_pages('spark','output',connection_factory=lambda:None,
                consumer_id='consumer',feed='feed',carrier_profile='ashlar-delta/0.4-xxhash64',
                umf_sdk_version='0.10.0')
        self.assertEqual(result,{'ok':True})
        self.assertEqual(publish.call_args.kwargs['umf_sdk_version'],'0.10.0')

    def test_writer_settings_apply_before_work_and_reopen_verification_never_repairs(self):
        class Conf:
            def __init__(self, value): self.value=value
            def set(self, key, value): self.value=value
            def get(self, key): return self.value
        spark=SimpleNamespace(conf=Conf('zstd'))
        verify_local_writer_settings(spark)
        apply_local_writer_settings(spark)
        spark.conf.value='snappy'
        with self.assertRaises(ValueError): verify_local_writer_settings(spark)
        self.assertEqual(spark.conf.value,'snappy')

    def test_publication_version_follows_selected_installation_profile(self):
        driver=object.__new__(NativeDriver)
        driver.lookup_hash_profile='spark-xxhash64-typed-key/0.1'
        driver.transport=SimpleNamespace(installation_profile={
            'physical_profile':'ashlar-delta/0.4-xxhash64',
            'lookup_hash_profile':'spark-xxhash64-typed-key/0.1'})
        self.assertEqual(driver._selected_publication_profile(),'ashlar-delta/0.4-xxhash64')
        driver.lookup_hash_profile='sha256-json-v1'
        driver.transport=SimpleNamespace(installation_profile=None)
        self.assertEqual(driver._selected_publication_profile(),'ashlar-delta/0.3')
        driver.transport=SimpleNamespace(installation_profile={
            'physical_profile':'ashlar-delta/0.4-xxhash64'})
        with self.assertRaises(PermissionError):driver._selected_publication_profile()


if __name__ == '__main__':
    unittest.main()
