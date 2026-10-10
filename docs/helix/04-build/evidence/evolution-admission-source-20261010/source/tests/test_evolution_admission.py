from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
from ashlar_host.config import EvolutionAdmissionConfig, HostError
from ashlar_host.evolution_admission import admit_commerce_evolution, packaged_inputs, expand_receipt


class EvolutionAdmissionTests(unittest.TestCase):
    def config(self):
        return EvolutionAdmissionConfig(Path('/selected/source'), Path('/selected/bun'), Path('/selected/git'), 2, 100, 4*1024*1024)

    def test_config_exact_bounds_and_types(self):
        for value in (0, True, 61):
            with self.assertRaises(HostError): EvolutionAdmissionConfig(Path('/source'),Path('/bun'),Path('/git'),value,100,100)
        with self.assertRaises(HostError): EvolutionAdmissionConfig(Path('relative'),Path('/bun'),Path('/git'),2,100,100)

    def test_package_resources_core_four_batches_and_exact_fresh_proof_gate(self):
        raw = dict(packaged_inputs()); proof = expand_receipt(raw['public-presence.json.gz'],4*1024*1024)
        def fake_capture(argv, **kwargs):
            Path(argv[-1]).write_bytes(proof)
            return b'', b''
        with patch('ashlar_host.evolution_admission.custody', return_value=b'{}'), patch('ashlar_host.evolution_admission.capture', side_effect=fake_capture):
            admitted=admit_commerce_evolution(self.config(),source_system='separate-a',epoch='epoch-a')
        self.assertEqual(len(admitted.prepared.batches),4)
        self.assertEqual(admitted.receipt_bytes,proof)
        self.assertTrue(all('separate-a' in b.batch_id and 'epoch-a' in b.batch_id for b in admitted.prepared.batches))
        admitted.admit(admitted.changes[0])
        self.assertEqual(len(admitted.transitions),19)
        admitted.admit_transition(*admitted.transitions[0])
        from dataclasses import replace
        previous,change=admitted.transitions[0]
        with self.assertRaises(HostError): admitted.admit_transition(replace(previous,version=previous.version+1),change)
        import json
        self.assertLess(len(json.dumps(admitted.metadata())),1048576)
        self.assertEqual(admitted.metadata()['profile'], 'ashlar-commerce-evolution-private-source-admission/0.2')
        self.assertEqual(admitted.metadata()['batch_identity_profile'], 'source-epoch-qualified/0.1')
        self.assertEqual(admitted.metadata()['batch_ids'], [batch.batch_id for batch in admitted.prepared.batches])
        with self.assertRaises(HostError): admitted.admit(object())

    def test_bad_receipt_refuses_and_cancel_identity_preserved(self):
        def bad(argv, **kwargs): Path(argv[-1]).write_bytes(b'{}'); return b'',b''
        with patch('ashlar_host.evolution_admission.custody', return_value=b'inert'), patch('ashlar_host.evolution_admission.capture', side_effect=bad):
            with self.assertRaises(HostError): admit_commerce_evolution(self.config(),source_system='a',epoch='e')
        primary=KeyboardInterrupt()
        with patch('ashlar_host.evolution_admission.custody', side_effect=primary):
            with self.assertRaises(KeyboardInterrupt) as caught: admit_commerce_evolution(self.config(),source_system='a',epoch='e')
        self.assertIs(caught.exception,primary)

    def test_gzip_limits_trailing_data_and_invalid_config_before_capture(self):
        raw=dict(packaged_inputs())['public-presence.json.gz']
        for data,maximum in ((raw,10),(raw+b'x',4*1024*1024)):
            with self.assertRaises(HostError): expand_receipt(data,maximum)
        with patch('ashlar_host.evolution_admission.capture') as transport:
            with self.assertRaises(HostError): admit_commerce_evolution(object(),source_system='a',epoch='e')
            transport.assert_not_called()

    def test_closing_drift_withholds_return_and_failure_still_checks_closing(self):
        raw = dict(packaged_inputs()); proof=expand_receipt(raw['public-presence.json.gz'],4*1024*1024)
        def receipt(argv, **kwargs): Path(argv[-1]).write_bytes(proof); return b'',b''
        with patch('ashlar_host.evolution_admission.custody',side_effect=(b'opening',b'drift')) as check, patch('ashlar_host.evolution_admission.capture',side_effect=receipt):
            with self.assertRaises(HostError): admit_commerce_evolution(self.config(),source_system='a',epoch='e')
            self.assertEqual(check.call_count,2)
        primary=KeyboardInterrupt()
        with patch('ashlar_host.evolution_admission.custody',return_value=b'fixed') as check, patch('ashlar_host.evolution_admission.capture',side_effect=primary):
            with self.assertRaises(KeyboardInterrupt) as caught: admit_commerce_evolution(self.config(),source_system='a',epoch='e')
            self.assertEqual(check.call_count,2)
            self.assertIs(caught.exception,primary)

    def test_cleanup_failure_preserves_cancel_and_removes_owned_directory(self):
        primary=KeyboardInterrupt()
        real_cleanup=tempfile.TemporaryDirectory.cleanup
        directories=[]
        def fail_cleanup(instance):
            directories.append(instance.name)
            real_cleanup(instance)
            raise OSError('private-path')
        with patch('ashlar_host.evolution_admission.custody',return_value=b'fixed'), patch('ashlar_host.evolution_admission.capture',side_effect=primary), patch('ashlar_host.evolution_admission.tempfile.TemporaryDirectory.cleanup',fail_cleanup):
            with self.assertRaises(KeyboardInterrupt) as caught: admit_commerce_evolution(self.config(),source_system='a',epoch='e')
        self.assertIs(caught.exception,primary)
        self.assertTrue(primary.cleanup_failed)
        self.assertTrue(all(not Path(p).exists() for p in directories))

    def test_cleanup_cancellations_win_over_ordinary_body_failures(self):
        actual_cleanup = tempfile.TemporaryDirectory.cleanup
        for cancellation in (KeyboardInterrupt(), SystemExit(), GeneratorExit()):
            def cleanup(instance):
                actual_cleanup(instance)
                raise cancellation
            with patch('ashlar_host.evolution_admission.custody', return_value=b'fixed'), patch('ashlar_host.evolution_admission.capture', side_effect=OSError('private')), patch('ashlar_host.evolution_admission.tempfile.TemporaryDirectory.cleanup', cleanup):
                with self.assertRaises(type(cancellation)) as caught:
                    admit_commerce_evolution(self.config(), source_system='a', epoch='e')
            self.assertIs(caught.exception, cancellation)

    def test_closing_cancellation_wins_over_ordinary_body_failure(self):
        for cancellation in (KeyboardInterrupt(), SystemExit(), GeneratorExit()):
            with patch('ashlar_host.evolution_admission.custody', side_effect=(b'fixed', cancellation)), patch('ashlar_host.evolution_admission.capture', side_effect=OSError('private')):
                with self.assertRaises(type(cancellation)) as caught:
                    admit_commerce_evolution(self.config(), source_system='a', epoch='e')
            self.assertIs(caught.exception, cancellation)

    def test_writer_cleanup_cancellation_wins_over_ordinary_write_failure(self):
        import os
        actual_close = os.close
        for cancellation in (KeyboardInterrupt(), SystemExit(), GeneratorExit()):
            def close(fd):
                actual_close(fd)
                raise cancellation
            with patch('ashlar_host.evolution_admission.custody', return_value=b'fixed'), patch('ashlar_host.evolution_admission.os.write', side_effect=OSError('private')), patch('ashlar_host.evolution_admission.os.close', side_effect=close):
                with self.assertRaises(type(cancellation)) as caught:
                    admit_commerce_evolution(self.config(), source_system='a', epoch='e')
            self.assertIs(caught.exception, cancellation)
