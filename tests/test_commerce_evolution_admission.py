import gzip
import json
from pathlib import Path
import tempfile
import unittest
from dataclasses import replace
from unittest.mock import patch
from types import SimpleNamespace

from ashlar.apply import empty_state
from ashlar.whole_entity import changes_from_batch
from commerce_evolution_admission import CommerceEvolutionAdmission,PIN,ROOT
from run_local_outbox_publication import NativeDriver
from local_delta_custody import encoded

class AdmissionTests(unittest.TestCase):
    def setUp(self):
        self.temporary=tempfile.TemporaryDirectory();self.addCleanup(self.temporary.cleanup)
        self.directory=Path(self.temporary.name)
        evidence=ROOT/'docs/helix/04-build/evidence/commerce-present-field-preparation-20261009'
        self.paths=[self.directory/name for name in ('candidate.json','public.json','model.json')]
        self.paths[0].write_bytes((evidence/'candidate.json').read_bytes())
        self.paths[1].write_bytes(gzip.decompress((evidence/'public-presence.json.gz').read_bytes()))
        self.paths[2].write_bytes((ROOT/'docs/helix/04-build/evidence/commerce-evolution-preparation-20261009/original-model.json').read_bytes())
        self.calls=[]

    def command(self,args,**kwargs):
        self.calls.append(args)
        if args[0]=='git':return SimpleNamespace(returncode=0,stdout=PIN+'\n'if 'rev-parse'in args else '')
        Path(args[-1]).write_bytes(self.paths[1].read_bytes())
        return SimpleNamespace(returncode=0)

    def admission(self):
        # Pure host wiring test: actual public execution has separate native-free proof.
        return CommerceEvolutionAdmission(*self.paths,umf_repo=self.directory,source_system='A',epoch='original')

    def driver(self,admission):
        owner=object.__new__(NativeDriver);owner.source_admission=admission;owner.original_admission=encoded(admission.metadata());owner.allowed_changes=admission.changes
        return owner

    @patch('commerce_evolution_admission.subprocess.run')
    def test_forwarding_exact_prior_policy_and_replay(self,run):
        run.side_effect=self.command;admission=self.admission();driver=self.driver(admission)
        tables={r:'local.fixture.'+r for r in ('object_current','edge_current','tombstone','whole_source_history')};state=empty_state()
        for batch in admission.prepared.batches:
            state,steps=driver.plan_graph(state,batch,tables,materialized_at='2026-10-09T00:00:00+00:00');self.assertTrue(steps)
        self.assertEqual((len(state.current),len(state.history),len(state.tombstones)),(21,45,2))
        self.assertEqual(len([c for c in self.calls if c[0]=='bun']),1)
        prior,change=admission.transitions[0]
        for wrong in (replace(prior,props_json='{}'),replace(prior,key=replace(prior.key,source='B')),replace(prior,schema_revision='unrelated')):
            with self.assertRaises(PermissionError):driver.schema_transition_admit(wrong,change)
        replay,steps=driver.plan_graph(state,admission.prepared.batches[-1],tables,materialized_at='2026-10-09T00:00:00+00:00')
        self.assertEqual(replay,state);self.assertEqual(steps,[])

    @patch('commerce_evolution_admission.subprocess.run')
    def test_missing_incomplete_and_changed_source_policy_refuse(self,run):
        run.side_effect=self.command;admission=self.admission();driver=self.driver(admission);prior,change=admission.transitions[0]
        for callback in (None,lambda p,c:True):
            with patch.object(admission,'admit_transition',callback):
                with self.assertRaises(PermissionError):driver.schema_transition_admit(prior,change)
        def changed_metadata(previous,change):admission.facts['epoch']='replacement'
        with patch.object(admission,'admit_transition',changed_metadata):
            with self.assertRaises(PermissionError):driver.schema_transition_admit(prior,change)
        admission.facts['epoch']='original'
        with self.assertRaises(PermissionError):driver.schema_transition_admit(prior,replace(change,state=replace(change.state,props_json='{}')))
        before=len(self.calls);self.paths[0].write_bytes(self.paths[0].read_bytes()+b' ')
        with self.assertRaises(PermissionError):driver.schema_transition_admit(prior,change)
        self.assertEqual(len(self.calls),before)

    @patch('commerce_evolution_admission.subprocess.run')
    def test_fresh_public_failure_and_wrong_original_hash_refuse(self,run):
        run.side_effect=lambda args,**kw:SimpleNamespace(returncode=0,stdout=PIN if 'rev-parse'in args else '')if args[0]=='git'else SimpleNamespace(returncode=1)
        with self.assertRaises(PermissionError):self.admission()
        self.paths[0].write_bytes(b'{}');run.reset_mock()
        with self.assertRaises(ValueError):self.admission()
        run.assert_not_called()

    @patch('commerce_evolution_admission.subprocess.run')
    def test_compressed_oversize_refuses_before_public_calls(self,run):
        compressed=self.directory/'public.json.gz';compressed.write_bytes(gzip.compress(b' ' * 4_000_001,mtime=0))
        self.paths[1]=compressed
        with self.assertRaises(PermissionError):self.admission()
        run.assert_not_called()

    @patch('commerce_evolution_admission.subprocess.run')
    def test_giant_file_is_bounded_before_full_read_or_public_calls(self,run):
        with self.paths[0].open('wb')as stream:stream.truncate(8_000_000)
        with patch.object(Path,'read_bytes',side_effect=AssertionError('Unbounded whole-file read')):
            with self.assertRaises(PermissionError):self.admission()
        run.assert_not_called()

if __name__=='__main__':unittest.main()
