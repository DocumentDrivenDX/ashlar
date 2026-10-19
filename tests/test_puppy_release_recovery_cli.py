# SPDX-License-Identifier: MIT OR Apache-2.0
"""Explicit original-intent reconciliation with inert ports, no native claim."""
from contextlib import contextmanager
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
import test_puppy_release_adapter as fixtures
from ashlar_host.puppy_native import PuppyNativeReleaseAdapter, encoded
from ashlar_host.puppy_release_cli import (PuppyRecoveryInvocation, PuppySession,
    operation_lock, run_puppy_command, run_puppy_recovery)
from ashlar_host.puppy_release import PuppyReleaseError
from ashlar_host.config import HostError

fixture = None
lose_reply = False
closing = None
opening = None
opened = []


@contextmanager
def provider(invocation, release):
    opened.append(invocation)
    directory = invocation.output_directory
    original = invocation.original if type(invocation) is PuppyRecoveryInvocation else invocation
    if release.payload != fixture.releases[0].payload or original.release_sha256 != release.sha256:
        raise PuppyReleaseError('Independent original source refused')
    def http(path, data, context):
        raw = fixture.http(path,data,context)
        if data is not None and lose_reply:
            raise ConnectionError('Complete upstream effect; client reply lost (inert)')
        return raw
    adapter = PuppyNativeReleaseAdapter(prepared=fixture.prepared,
        prior_model=encoded({'catalog':[],'node':[],'edge':[]}),http=http,
        cypher=fixture.query,runtime=fixture.runtime,evidence=directory/'native')
    fixture.adapter = adapter
    if opening is not None: opening(invocation)
    primary = None
    try: yield PuppySession(adapter,fixtures.Policy(),None)
    except BaseException as error: primary = error
    from ashlar_host.lifecycle import finish
    finish(primary,[lambda:closing(invocation) if closing else None])


class Tests(unittest.TestCase):
    def setUp(self):
        global fixture,lose_reply,closing,opening
        fixture = fixtures.Tests(methodName='runTest');fixture.setUp()
        self.addCleanup(fixture.doCleanups)
        lose_reply = False;closing = None;opening = None;opened.clear()
        self.base = fixture.directory
        self.root = self.base/'operation'
        self.release = self.base/'release.json';self.release.write_bytes(fixture.releases[0].payload)
        self.config = self.base/'configuration.json'
        self.config.write_bytes(encoded({'profile':'ashlar-puppy-invocation/0.1',
            'release_path':str(self.release),'release_sha256':fixture.releases[0].sha256,
            'output_directory':str(self.root)}))
        self.provider = self.base/'provider.py'
        self.provider.write_text('from test_puppy_release_recovery_cli import provider\n'
            'open_puppy_release=provider\nopen_puppy_recovery=provider\n')
        self.pin = hashlib.sha256(self.provider.read_bytes()).hexdigest()

    def activate(self):
        return run_puppy_command(str(self.config),str(self.provider),self.pin,'activate')

    def interrupted(self):
        global lose_reply
        lose_reply = True
        with self.assertRaises(ConnectionError): self.activate()
        lose_reply = False
        self.intent = self.root/'original-intent.json'
        self.intent_bytes = self.intent.read_bytes()
        self.intent_pin = hashlib.sha256(self.intent_bytes).hexdigest()

    def recover(self,ordinal=0):
        return run_puppy_recovery(str(self.config),str(self.provider),self.pin,
            str(self.intent),self.intent_pin,str(self.root/'attempts'/('attempt-%02d'%ordinal)))

    def test_complete_unknown_outcome_reopens_original_intent_and_zero_posts(self):
        self.interrupted()
        posts = len([data for _,data in fixture.calls if data is not None])
        result = self.recover()
        self.assertEqual(len([data for _,data in fixture.calls if data is not None]),posts)
        self.assertEqual(self.intent.read_bytes(),self.intent_bytes)
        self.assertEqual(opened[-1].original.output_directory,self.root)
        self.assertEqual(opened[-1].original_intent_sha256,self.intent_pin)
        self.assertTrue(Path(result['activation_path']).is_file())
        count = len(opened)
        with self.assertRaises(PuppyReleaseError): self.recover(1)
        self.assertEqual(len(opened),count)

    def test_original_drift_refuses_before_provider_execution(self):
        self.interrupted();count = len(opened)
        self.config.write_bytes(self.config.read_bytes()+b' ')
        with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertEqual(len(opened),count)
        self.assertFalse((self.root/'attempts').exists())

    def test_initial_activation_and_recovery_share_lock_and_preserve_foreign(self):
        self.interrupted();count = len(opened)
        with operation_lock(self.root):
            with self.assertRaises(FileExistsError):self.recover()
            with self.assertRaises(FileExistsError):self.activate()
        self.assertEqual(len(opened),count)
        with self.assertRaises(PuppyReleaseError):
            with operation_lock(self.root):
                lock = next(self.base.glob('.ashlar-puppy-*.lock'))
                lock.rename(self.base/'retained-lock');lock.write_bytes(b'foreign')
        self.assertEqual(lock.read_bytes(),b'foreign')

    def test_closing_intent_drift_withholds_completed_handle(self):
        global closing
        self.interrupted()
        def drift(invocation):
            self.intent.chmod(0o600);self.intent.write_bytes(self.intent_bytes+b' ')
        closing = drift
        with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertFalse((self.root/'attempts/attempt-00/activation.json').exists())

    def test_cancelled_closing_preserves_identity_and_no_handle(self):
        global closing
        self.interrupted();cancel = KeyboardInterrupt()
        def fail(invocation):
            self.config.write_bytes(self.config.read_bytes()+b' ')
            raise cancel
        closing = fail
        try:self.recover()
        except BaseException as error:self.assertIs(error,cancel)
        else:self.fail('Cancellation required')
        self.assertFalse((self.root/'attempts/attempt-00/activation.json').exists())

    def test_bounded_attempts_unknown_entry_and_completed_original_refuse(self):
        self.interrupted();attempts = self.root/'attempts';attempts.mkdir(mode=0o700)
        (attempts/'foreign').mkdir(mode=0o700)
        with self.assertRaises(PuppyReleaseError):self.recover()
        (attempts/'foreign').rmdir()
        for ordinal in range(8):(attempts/('attempt-%02d'%ordinal)).mkdir(mode=0o700)
        with self.assertRaises(PuppyReleaseError):self.recover(9)
        self.assertEqual(len(opened),1)

    def test_legacy_provider_not_silently_used_for_recovery(self):
        self.provider.write_text('from test_puppy_release_recovery_cli import provider\nopen_puppy_release=provider\n')
        self.pin = hashlib.sha256(self.provider.read_bytes()).hexdigest()
        self.interrupted();count = len(opened)
        with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertEqual(len(opened),count)
        self.assertTrue((self.root/'attempts/attempt-00/recovery-intent.json').is_file())

    def test_completed_original_and_wrong_intent_selection_refuse(self):
        result = self.activate()
        self.intent = self.root/'original-intent.json';self.intent_bytes = self.intent.read_bytes()
        self.intent_pin = hashlib.sha256(self.intent_bytes).hexdigest()
        count = len(opened)
        with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertEqual(Path(result['activation_path']).read_bytes(),(self.root/'activation.json').read_bytes())
        self.assertEqual(len(opened),count)
        self.intent_pin = '0'*64
        with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertEqual(len(opened),count)

    def test_unknown_schema_refuses_without_replacement_post(self):
        self.interrupted();posts = len([d for _,d in fixture.calls if d is not None])
        fixture.schema['node'].append({'label':'unrelated-unknown'})
        with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertEqual(len([d for _,d in fixture.calls if d is not None]),posts)
        self.assertFalse((self.root/'attempts/attempt-00/activation.json').exists())

    def test_post_provider_current_policy_refusal_withholds_receipt(self):
        global closing
        self.interrupted();closed = []
        closing = lambda invocation:closed.append(True)
        def admit(policy,handle,context):
            if closed:raise PuppyReleaseError('Current independent authority revoked after SDK close')
        with patch.object(fixtures.Policy,'admit',admit):
            with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertTrue(closed)
        self.assertFalse((self.root/'attempts/attempt-00/activation.json').exists())

    def test_provider_startup_drift_refuses_before_any_native_stage(self):
        global opening
        for selected in ('intent','configuration','provider','release','attempt'):
            with self.subTest(selected=selected):
                self.setUp();self.interrupted()
                before = list(fixture.calls)
                def drift(invocation):
                    if selected == 'attempt':
                        invocation.output_directory.rename(invocation.output_directory.with_name('retained'))
                        invocation.output_directory.mkdir(mode=0o700)
                    else:
                        path = {'intent':self.intent,'configuration':self.config,
                            'provider':self.provider,'release':self.release}[selected]
                        path.chmod(0o600);path.write_bytes(path.read_bytes()+b' ')
                opening = drift
                with self.assertRaises((PuppyReleaseError,HostError)):self.recover()
                self.assertEqual(fixture.calls,before)
                self.assertFalse((self.root/'attempts/attempt-00/activation.json').exists())

    def test_forged_returned_session_never_dispatches_policy_cleanup(self):
        self.interrupted();dispatched = []
        class Forged:
            @property
            def policy(self):
                dispatched.append(True)
                raise AssertionError('Forged policy must never be consulted')
        @contextmanager
        def forged(invocation,release):yield Forged()
        with patch(__name__+'.provider',forged):
            with self.assertRaises(PuppyReleaseError):self.recover()
        self.assertEqual(dispatched,[])

    def read_configuration(self,selected):
        path = self.base/'read-configuration.json'
        path.write_bytes(encoded({'profile':'ashlar-puppy-invocation/0.1',
            'release_path':str(self.release),'release_sha256':fixture.releases[0].sha256,
            'output_directory':str(self.base/'read-output'),
            'activation_path':selected['activation_path'],'activation_sha256':selected['activation_sha256']}))
        return path

    def test_initial_activate_and_read_startup_drift_prevents_native_effects(self):
        global opening
        for mode in ('activate','read'):
            with self.subTest(mode=mode):
                self.setUp()
                config = self.config if mode == 'activate' else self.read_configuration(self.activate())
                before = list(fixture.calls)
                def drift(invocation):config.write_bytes(config.read_bytes()+b' ')
                opening = drift
                with self.assertRaises(HostError):
                    run_puppy_command(str(config),str(self.provider),self.pin,mode)
                self.assertEqual(fixture.calls,before)

    def test_read_selected_receipt_startup_drift_prevents_native_observation(self):
        global opening
        selected = self.activate();config = self.read_configuration(selected)
        before = list(fixture.calls)
        def drift(invocation):
            path = Path(selected['activation_path']);path.chmod(0o600);path.write_bytes(path.read_bytes()+b' ')
        opening = drift
        with self.assertRaises(PuppyReleaseError):
            run_puppy_command(str(config),str(self.provider),self.pin,'read')
        self.assertEqual(fixture.calls,before)

    def test_initial_closing_intent_drift_and_revocation_withhold_receipts(self):
        global closing
        for mode in ('activate','read'):
            for reason in ('intent','policy'):
                with self.subTest(mode=mode,reason=reason):
                    self.setUp()
                    config = self.config if mode == 'activate' else self.read_configuration(self.activate())
                    closed = []
                    def close(invocation):
                        closed.append(True)
                        if reason == 'intent':
                            path = invocation.output_directory/'original-intent.json'
                            path.chmod(0o600);path.write_bytes(path.read_bytes()+b' ')
                    closing = close
                    def admit(policy,handle,context):
                        if reason == 'policy' and closed:raise PuppyReleaseError('Current authority revoked')
                    with patch.object(fixtures.Policy,'admit',admit):
                        with self.assertRaises(PuppyReleaseError):
                            run_puppy_command(str(config),str(self.provider),self.pin,mode)
                    output = self.root if mode == 'activate' else self.base/'read-output'
                    self.assertFalse((output/'activation.json').exists())


class DispatchTests(unittest.TestCase):
    def argv(self):
        return ['ashlar','resume-puppy-release','--configuration','/owned/config.json',
            '--provider','/owned/provider.py','--provider-sha256','a'*64,
            '--original-intent','/owned/output/original-intent.json',
            '--original-intent-sha256','b'*64,'--attempt-directory','/owned/output/attempts/attempt-00']

    def test_actual_parser_forwards_exact_six_arguments_to_public_owner(self):
        from ashlar.cli import main
        output = io.StringIO();answer = {'profile':'selected','activation_path':'/owned/receipt.json'}
        with patch.object(sys,'argv',self.argv()),patch('sys.stdout',output),\
                patch('ashlar_host.puppy_release_cli.run_puppy_recovery',return_value=answer) as run,\
                patch('ashlar_host.puppy_release_cli.run_puppy_command') as legacy:
            main()
        run.assert_called_once_with('/owned/config.json','/owned/provider.py','a'*64,
            '/owned/output/original-intent.json','b'*64,'/owned/output/attempts/attempt-00')
        legacy.assert_not_called();self.assertEqual(json.loads(output.getvalue()),answer)

    def test_missing_original_intent_refuses_before_dispatch(self):
        from ashlar.cli import main
        argv = self.argv();index = argv.index('--original-intent');del argv[index:index+2]
        with patch.object(sys,'argv',argv),patch('sys.stderr',io.StringIO()),\
                patch('ashlar_host.puppy_release_cli.run_puppy_recovery') as run:
            with self.assertRaises(SystemExit) as caught:main()
        self.assertEqual(caught.exception.code,2);run.assert_not_called()

    def test_refusal_is_payload_free_and_cancellation_preserves_identity(self):
        from ashlar.cli import main
        for error in (ValueError('private fixture payload'),KeyboardInterrupt()):
            stderr = io.StringIO();stdout = io.StringIO()
            with patch.object(sys,'argv',self.argv()),patch('sys.stderr',stderr),patch('sys.stdout',stdout),\
                    patch('ashlar_host.puppy_release_cli.run_puppy_recovery',side_effect=error):
                try:main()
                except BaseException as actual:
                    if isinstance(error,Exception):
                        self.assertIsInstance(actual,SystemExit);self.assertEqual(actual.code,2)
                        self.assertEqual(stderr.getvalue(),'ashlar selected PuppyGraph release refused\n')
                    else:self.assertIs(actual,error)
                else:self.fail('Failure required')
            self.assertEqual(stdout.getvalue(),'');self.assertNotIn('private fixture payload',stderr.getvalue())


if __name__ == '__main__': unittest.main()
