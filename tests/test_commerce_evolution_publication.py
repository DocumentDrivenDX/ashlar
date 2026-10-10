import gzip
import json
from pathlib import Path
import unittest
import tempfile
from dataclasses import replace
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.apply import empty_state
from ashlar.whole_entity import changes_from_batch
from commerce_evolution_transactions import prepare
from commerce_evolution_batch_scope import qualify,scoped_oracle
from fixture_oracle import fixture_columns
from run_commerce_evolution_publication import (ORDER,CombinedAdmissions,prefix_union,admit_batch_inventory,NativeResponseLoss,LostNativeCommitResponse,close_resources,assert_recovery_inventory,run,ROOT,CLOCK,JARS,VERSIONS)
from whole_graph_sql import graph_sql_plan

class PreparationTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        evidence=ROOT/'docs/helix/04-build/evidence/commerce-present-field-preparation-20261009'
        cls.originals=((evidence/'candidate.json').read_bytes(),gzip.decompress((evidence/'public-presence.json.gz').read_bytes()),(ROOT/'docs/helix/04-build/evidence/commerce-evolution-preparation-20261009/original-model.json').read_bytes());cls.columns=fixture_columns(ROOT)
    def fixtures(self,qualified=True):
        result={}
        for label in ('a','b'):
            original=prepare(*self.originals,source_system='commerce-'+label,epoch='original-'+label);prepared=qualify(original)if qualified else original
            result[label]={'originals':self.originals,'admission':SimpleNamespace(prepared=prepared),'batches':[(b.begin+b''.join(r.raw for r in b.records)+b.commit,b)for b in prepared.batches]}
        return result

    def test_batch_scope_no_global_or_epoch_collision_original_events_unchanged(self):
        with self.assertRaises(PermissionError):admit_batch_inventory(self.fixtures(False))
        fixtures=self.fixtures();admit_batch_inventory(fixtures)
        for label,fixture in fixtures.items():
            original=prepare(*self.originals,source_system='commerce-'+label,epoch='original-'+label)
            for before,after in zip(original.batches,fixture['admission'].prepared.batches):
                self.assertEqual(tuple(r.raw for r in before.records),tuple(r.raw for r in after.records))
                self.assertEqual(changes_from_batch(before),changes_from_batch(after))
                self.assertEqual(json.loads(after.batch_id),[original.source_system,original.epoch,before.batch_id])
            replacement=qualify(prepare(*self.originals,source_system=original.source_system,epoch='replacement'))
            self.assertTrue(set(b.batch_id for b in replacement.batches).isdisjoint(b.batch_id for b in fixture['admission'].prepared.batches))

    def test_exact_interleave_sql_state_and_independent_full_rows(self):
        fixtures=self.fixtures();prefixes={'a':0,'b':0};state=empty_state();tables={r:'local.fixture.'+r for r in self.columns}
        allowed=tuple(c for f in fixtures.values()for b in f['admission'].prepared.batches for c in changes_from_batch(b))
        def admit(change):
            if change not in allowed:raise PermissionError()
        pairs=set()
        def transition(previous,change):
            prepared=fixtures['a'if change.feed=='commerce-a'else'b']['admission'].prepared
            self.assertEqual(previous.schema_revision,prepared.schema_revisions[2]);self.assertEqual(change.state.schema_revision,prepared.schema_revisions[3]);pairs.add(change.state.key)
        for label,index in ORDER:
            batch=fixtures[label]['admission'].prepared.batches[index];state,steps=graph_sql_plan(state,batch,tables,materialized_at=CLOCK,schema_policy=admit,schema_transition_policy=transition);prefixes[label]=index+1
            expected=prefix_union(fixtures,prefixes,self.columns)
            self.assertEqual(len(state.current),len(expected['object_current'])+len(expected['edge_current']))
            self.assertEqual(len(state.history),len(expected['whole_source_history']))
            for entity in state.current.values():
                role='object_current'if entity.key.kind=='object'else'edge_current';type_column='type_id'if entity.key.kind=='object'else'rel_type_id'
                row=next(r for r in expected[role]if (r['source_system'],r[type_column],r['id'])==(entity.key.source,str(entity.key.type_id),str(entity.key.id)))
                self.assertEqual(entity.retained_json,row['retained_json']);self.assertEqual(entity.props_json,row['props_json'])
            for step in steps:
                if 'rows'in step['parameters']and step['statement'].startswith('INSERT INTO `local`.`fixture`.`object_current`'):
                    for native in json.loads(step['parameters']['rows']):
                        row=next(r for r in expected['object_current']if (r['source_system'],r['type_id'],r['id'])==(native['source_system'],native['type_id'],native['id']))
                        self.assertEqual(native['source_cursor_json'],row['source_cursor_json']);self.assertEqual(native['apply_batch_id'],row['apply_batch_id'])
        self.assertEqual({r:len(v)for r,v in expected.items()},{'object_current':22,'edge_current':20,'tombstone':4,'whole_source_history':90});self.assertEqual(len(pairs),38)

    def test_wrong_envelope_and_record_cursors_refuse(self):
        prepared=self.fixtures()['a']['admission'].prepared;b=prepared.batches[0]
        for changed in (replace(b,batch_id='R1'),replace(b,cursor_after='0'),replace(b,records=(replace(b.records[0],cursor='0'),*b.records[1:]))):
            with self.assertRaises(ValueError):scoped_oracle(*self.originals,replace(prepared,batches=(changed,*prepared.batches[1:])),self.columns,prefix=1,materialized_at=CLOCK)

    def test_real_commit_response_loss_wrapper_only_selected_command(self):
        events=[]
        class Frame:
            def collect(self):events.append('real collect');return []
        class Spark:
            def sql(self,*args,**kwargs):events.append(args[0]);return Frame()
        wrapper=NativeResponseLoss(Spark());wrapper.arm('ORIGINAL INSERT')
        wrapper.sql('UNRELATED READ').collect()
        with self.assertRaises(LostNativeCommitResponse):wrapper.sql('ORIGINAL INSERT',args={'rows':'exact'}).collect()
        self.assertEqual(events,['UNRELATED READ','real collect','ORIGINAL INSERT','real collect']);self.assertEqual(wrapper.events[0]['parameters'],{'rows':'exact'})
        wrapper.sql('ORIGINAL INSERT').collect();self.assertEqual(len(wrapper.events),1)

    def test_both_cleanup_resources_attempted_and_failure_propagates(self):
        for failed in ('transport','spark'):
            closed=[]
            def close(name):
                closed.append(name)
                if name==failed:raise OSError(name)
            with self.assertRaises(OSError):close_resources(SimpleNamespace(close=lambda:close('transport')),SimpleNamespace(stop=lambda:close('spark')))
            self.assertEqual(closed,['transport','spark'])

    def test_recovery_inventory_requires_exact_actual_injections(self):
        recovery=[{'ordinal':1,'boundary':'LostAfterManifest'},{'ordinal':3,'boundary':'LostNativeCommitResponse'}]
        native=SimpleNamespace(statement=None,events=[{'boundary':'after real native collect; original journal remains submitted'}]);acks=[{'publication_id':'commerce-evolution-publication-2','uncertain_commit_fresh_reconciled':True}]
        assert_recovery_inventory(recovery,native,acks)
        for wrong in ([],recovery[:1],[*recovery,*recovery]):
            with self.assertRaises(ValueError):assert_recovery_inventory(wrong,native,acks)
        for wrong in ([],[{'publication_id':'wrong','uncertain_commit_fresh_reconciled':True}],[*acks,*acks]):
            with self.assertRaises(ValueError):assert_recovery_inventory(recovery,native,wrong)
        with self.assertRaises(ValueError):assert_recovery_inventory(recovery,SimpleNamespace(statement='unconsumed',events=native.events),acks)
        with self.assertRaises(ValueError):assert_recovery_inventory(recovery,SimpleNamespace(statement=None,events=[]),acks)

    def test_oversized_compressed_input_refuses_before_public_or_spark(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary);archive=root/'docs/helix/04-build/evidence';presence=archive/'commerce-present-field-preparation-20261009';presence.mkdir(parents=True)
            (presence/'candidate.json').write_bytes(self.originals[0]);(presence/'public-presence.json.gz').write_bytes(gzip.compress(b' ' * 8_000_000,mtime=0))
            original=archive/'commerce-evolution-preparation-20261009';original.mkdir();(original/'original-model.json').write_bytes(self.originals[2])
            jars=root/'jars';jars.mkdir()
            for name in JARS:(jars/name).touch()
            with patch('run_commerce_evolution_publication.ROOT',root),patch('run_commerce_evolution_publication.importlib.metadata.version',side_effect=lambda name:VERSIONS[name]),patch('run_commerce_evolution_publication.ScopedCommerceEvolutionAdmission',side_effect=AssertionError('Public call before bound')):
                with self.assertRaises(PermissionError):run(root/'output',jars,root/'producer')
            self.assertFalse((root/'output/report.json').exists());self.assertFalse((root/'output/object_current').exists())

if __name__=='__main__':unittest.main()
