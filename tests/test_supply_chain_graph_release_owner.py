"""Inert full original-carrier/core wiring controls; no native/lease qualification."""
from collections import Counter
from contextlib import contextmanager
import copy
import json
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock

from ashlar.graph_release import (release_columns,validate_release_carrier)
from ashlar.native import SQLResult
from ashlar.publication import Snapshot
from ashlar.supply_chain_source import build_transaction
from ashlar_host import supply_chain_graph_release as owner
from ashlar_host.supply_chain_common_read import OpenedSupplyChainCommonRead
from ashlar_host.supply_chain_projection import original_supply_chain_oracle
from ashlar_host.supply_chain_request import SOURCE_SYSTEM,SOURCE_SHA
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.schema_rows import fixture_columns

ROOT=Path(__file__).resolve().parents[1]

class Controls(unittest.TestCase):
    def setUp(self):
        base=ROOT/'examples/domain-packs/supply-chain/upstream'
        model=(base/'ontology.json').read_bytes();graph=(base/'graph/fixture.json').read_bytes()
        batch,bindings=build_transaction(model,graph,source_system=SOURCE_SYSTEM)
        columns=fixture_columns(RESOURCE_ROOT)
        tables={role:'local.original.'+role for role in (*columns,'attempts','manifest')}
        targets={table:SimpleNamespace(uuid='uuid-'+role)for role,table in tables.items()}
        versions={tables[role]:1 for role in columns}
        manifest={'publication_id':'original-publication','profile_version':'ashlar-delta/0.3',
            'table_versions_json':json.dumps(versions),'schema_revisions_json':json.dumps({SOURCE_SYSTEM:SOURCE_SHA}),
            'source_progress_json':json.dumps({SOURCE_SYSTEM:{'position':1}}),
            'validation_report_json':'{"complete":true}','recorded_at':'1'}
        self.expected=original_supply_chain_oracle(model,graph,bindings,batch,columns)
        self.rows={role:copy.deepcopy(self.expected[role])for role in ('object_current','edge_current')}
        self.active=False;self.live=True;self.source_current=True;self.calls=[];self.reads=0
        self.context=object();self.fail_open=None;self.closing_mutation=None;self.closing_failure=None
        self.suppress=False
        self.opened=object.__new__(OpenedSupplyChainCommonRead)
        self.original_manifest=copy.deepcopy(manifest)
        self.driver=SimpleNamespace(tables=tables,columns=columns,transport=SimpleNamespace(targets=targets,query=self.query))
        self.source=SimpleNamespace(batch=batch)
        self.provider=SimpleNamespace(authorize=self.authorize,descriptors=self.descriptors,
            validate_descriptor=self.descriptor,inspect_snapshot=self.snapshot,
            closed_interval_custody=self.closed)
        fields={'driver':self.driver,'provider':self.provider,'context':self.context,'source':self.source,
            'manifest':manifest,'vector':{table:(targets[table].uuid,version)for table,version in versions.items()},
            'aliases':{table:'spark_catalog.original.'+table.split('.')[-1]for table in versions},
            'model':model,'graph':graph,'bindings':bindings,'columns':columns}
        for key,value in fields.items():object.__setattr__(self.opened,key,value)
        # Only synthetic owner ports are patched. Production still requires the
        # actual internally minted common-owner registry and current leases.
        self.patch_require=mock.patch.object(OpenedSupplyChainCommonRead,'require_current',self.require)
        self.patch_interval=mock.patch.object(OpenedSupplyChainCommonRead,'read_interval',self.interval)
        self.patch_held=mock.patch.object(OpenedSupplyChainCommonRead,'require_held_correspondence',self.held_correspondence)
        self.patch_held.start();self.addCleanup(self.patch_held.stop)
        self.patch_require.start();self.patch_interval.start()
        self.addCleanup(self.patch_require.stop);self.addCleanup(self.patch_interval.stop)
    def held_correspondence(self,context):
        if context is not self.context or not self.active:
            raise PermissionError('Original held correspondence required')
    def require(self,context):
        self.calls.append('current')
        if context is not self.context or not self.live or not self.source_current:
            raise PermissionError('Inert current original source refused')
    @contextmanager
    def interval(self,context):
        self.require(context)
        if self.active:raise PermissionError('nested interval')
        self.active=True;self.calls.append('interval-open')
        try:
            try:yield
            except BaseException:
                if not self.suppress:raise
            if self.closing_failure:raise self.closing_failure
        finally:self.calls.append('interval-close');self.active=False
    def authorize(self,context,publication,tables):
        self.require(context)
        if not self.active or publication!=self.original_manifest['publication_id']or set(tables)!=set(self.opened.vector):
            raise PermissionError('Inert whole active vector required')
    def descriptors(self,publication):return [copy.deepcopy(self.original_manifest)]
    def descriptor(self,descriptor,context):
        self.authorize(context,descriptor.publication_id,descriptor.versions)
        if dict(descriptor.raw)!=self.original_manifest:raise PermissionError('Original descriptor differs')
    def snapshot(self,table,version):
        if not self.active or version!=1:raise PermissionError('Original snapshot unavailable')
        return Snapshot(table,self.driver.transport.targets[table].uuid,version)
    def closed(self,context):
        if self.active or context is not self.context:raise PermissionError('Interval not closed')
        self.calls.append('closed-custody');return {'fixture-only':True}
    def query(self,sql,parameters):
        if not self.active:raise PermissionError('Unheld query')
        self.calls.append(sql)
        if sql.startswith('DESCRIBE DETAIL'):
            table=sql[len('DESCRIBE DETAIL '):].replace('`','')
            return SQLResult([{'id':self.driver.transport.targets[table].uuid}])
        role='object_current'if 'object_current'in sql else'edge_current'
        if role=='object_current':
            self.reads+=1
            if self.reads==1 and self.fail_open is not None:raise self.fail_open
            if self.reads==2 and self.closing_mutation:self.closing_mutation(self.rows)
        kind='node'if role=='object_current'else'edge'
        names=release_columns(kind)[:17 if kind=='node' else 20]
        return SQLResult(copy.deepcopy(self.rows[role]),tuple((name,'STRING')for name in names))
    def read(self):return owner.read_original_supply_chain_graph_release(self.opened)
    def test_core_full_original18_23_columns_values_nulls_endpoints_identity_and_closing(self):
        release=self.read();value=json.loads(release.payload)
        self.assertEqual((len(value['nodes']),len(value['edges'])),(20,23))
        for kind,key,role in (('node','nodes','object_current'),('edge','edges','edge_current')):
            expected=[validate_release_carrier(row,kind)for row in self.expected[role]]
            self.assertEqual(Counter(json.dumps(row,sort_keys=True)for row in value[key]),Counter(json.dumps(row,sort_keys=True)for row in expected))
            self.assertTrue(all(set(row)==set(release_columns(kind))for row in value[key]))
        keys={row['graph_id']for row in value['nodes']}
        self.assertTrue(all(row['src']in keys and row['dst']in keys for row in value['edges']))
        self.assertEqual(len({row['graph_id']for row in value['edges']}),23)
        self.assertEqual(self.reads,2);self.assertFalse(self.active);self.assertTrue(self.live)
        self.assertIn('closed-custody',self.calls)
        self.assertTrue(any('VERSION AS OF 1'in sql for sql in self.calls))
    def test_missing_duplicate_foreign_or_null_mutated_native_carrier_refuses(self):
        for alteration in ('missing','duplicate','foreign','null'):
            with self.subTest(alteration=alteration):
                self.rows={role:copy.deepcopy(self.expected[role])for role in ('object_current','edge_current')}
                if alteration=='missing':self.rows['edge_current'].pop()
                if alteration=='duplicate':self.rows['edge_current'][-1]=copy.deepcopy(self.rows['edge_current'][0])
                if alteration=='foreign':self.rows['edge_current'][0]['retained_json']='foreign'
                if alteration=='null':self.rows['object_current'][0]['root_id']='0'
                with self.assertRaises((ValueError,PermissionError)):self.read()
                self.assertFalse(self.active)
    def test_closing_bag_change_withholds_complete_result(self):
        self.closing_mutation=lambda rows:rows['edge_current'].pop()
        with self.assertRaises(PermissionError):self.read()
        self.assertEqual(self.reads,2);self.assertFalse(self.active)
    def test_body_cancellation_independently_reads_original_closing_bag(self):
        cancellation=KeyboardInterrupt('inert body');self.fail_open=cancellation
        with self.assertRaises(KeyboardInterrupt)as failure:self.read()
        self.assertIs(failure.exception,cancellation);self.assertEqual(self.reads,2);self.assertFalse(self.active)
    def test_suppressed_body_failure_and_outer_interval_close_failure_withhold(self):
        self.fail_open=ValueError('body');self.suppress=True
        with self.assertRaisesRegex(ValueError,'body'):self.read()
        self.fail_open=None;self.closing_failure=PermissionError('outer interval closing')
        with self.assertRaisesRegex(PermissionError,'outer interval closing'):self.read()
        self.assertFalse(self.active)
    def test_foreign_owner_and_unminted_nominal_have_no_current_read_authority(self):
        with self.assertRaises(ValueError):owner.read_original_supply_chain_graph_release(SimpleNamespace())
        self.patch_require.stop();self.patch_interval.stop()
        with self.assertRaises(PermissionError):self.read()
    def test_bool_version_and_partial_original_vector_refuse_before_carrier_sql(self):
        for alteration in ('bool','partial'):
            original=dict(self.opened.vector)
            if alteration=='bool':
                table=next(iter(original));self.opened.vector[table]=(original[table][0],True)
            else:self.opened.vector.pop(next(iter(original)))
            with self.assertRaises(PermissionError):self.read()
            self.opened.vector.clear();self.opened.vector.update(original)
        self.assertFalse(any('SELECT'in call for call in self.calls))
    def test_current_source_denial_precedes_carrier_read_and_no_release(self):
        self.source_current=False
        with self.assertRaises(PermissionError):self.read()
        self.assertEqual(self.reads,0);self.assertFalse(self.active)
    def test_unrelated_opened_drift_still_attempts_original_graph_closing(self):
        query=self.query
        def drift(sql,parameters):
            result=query(sql,parameters)
            if sql.startswith('SELECT')and'edge_current'in sql:
                self.opened.aliases[next(iter(self.opened.aliases))]='spark_catalog.foreign.table'
            return result
        self.driver.transport.query=drift
        with mock.patch.object(owner,'_read',wraps=owner._read)as reads:
            with self.assertRaises(PermissionError):self.read()
        self.assertEqual(reads.call_count,2);self.assertFalse(self.active)

    def test_rows_do_not_renew_source_or_native_authorization(self):
        policy=owner._OriginalGraphPolicy(self.opened)
        descriptor=SimpleNamespace(raw=self.opened.manifest)
        with self.interval(self.context):
            policy.require(self.context)
            before=list(self.calls)
            for role in ('object_current','edge_current'):
                for row in self.rows[role]:
                    policy.authorize_graph_row(descriptor,self.driver.tables[role],row,self.context)
            self.assertEqual(self.calls,before)
            self.source_current=False
            with self.assertRaises(PermissionError):
                policy.authorize_graph_result(descriptor,(),(),self.context)
        with self.assertRaises(PermissionError):
            policy.authorize_graph_row(descriptor,self.driver.tables['object_current'],self.rows['object_current'][0],self.context)

    def test_original_held_guard_rejects_ports_and_inactive_owner(self):
        from ashlar_host import supply_chain_common_read as common
        self.patch_held.stop()
        provider=SimpleNamespace(active=True)
        self.driver.held=True
        guard=mock.Mock(return_value=None)
        record={'context':self.context,'provider':provider,'guard':guard}
        with mock.patch.dict(common._OPENED,{self.opened:record}):
            self.opened.require_held_correspondence(self.context)
            guard.assert_called_once_with(self.opened,self.context)
            guard.side_effect=ValueError('original ports changed')
            with self.assertRaises(ValueError):self.opened.require_held_correspondence(self.context)
            guard.side_effect=None
            for provider_active,driver_held,candidate in [(False,True,None),(True,False,None),(True,True,object())]:
                provider.active=provider_active;self.driver.held=driver_held;record['candidate']=candidate
                with self.assertRaises(PermissionError):self.opened.require_held_correspondence(self.context)

    def test_descriptor_callbacks_reuse_only_held_pure_correspondence(self):
        policy=owner._OriginalGraphPolicy(self.opened)
        descriptor=SimpleNamespace(raw=copy.deepcopy(self.opened.manifest))
        with self.interval(self.context):
            with owner._OriginalHeldPins(policy).hold(policy.vector,context=self.context):
                before=list(self.calls)
                policy.bind_descriptor(descriptor,policy.vector,self.context)
                policy.bind_descriptor(descriptor,policy.vector,self.context)
                self.assertEqual(self.calls,before)
                with self.assertRaises(PermissionError):policy.bind_descriptor(descriptor,object(),self.context)
                changed=SimpleNamespace(raw={**descriptor.raw,'publication_id':'foreign'})
                with self.assertRaises(PermissionError):policy.bind_descriptor(changed,policy.vector,self.context)
                original=self.driver.transport
                self.driver.transport=object()
                with self.assertRaises(PermissionError):policy.bind_descriptor(descriptor,policy.vector,self.context)
                self.driver.transport=original
            self.assertGreater(len(self.calls),len(before))
        with self.interval(self.context):
            with self.assertRaises(PermissionError):
                with owner._OriginalHeldPins(policy).hold(policy.vector,context=self.context):
                    self.source_current=False
                    policy.bind_descriptor(descriptor,policy.vector,self.context)
