"""Packaged public command controls with inert native ports, no native claim."""
from contextlib import contextmanager
import hashlib
import json
from pathlib import Path
import unittest
from unittest.mock import patch
import test_puppy_release_adapter as fixtures
from ashlar.graph_release import release_columns
from ashlar_host.puppy_native import PuppyNativeReleaseAdapter, encoded
from ashlar_host.puppy_gremlin import GremlinReleaseProjection
from ashlar_host.puppy_release import PuppyReleaseError
from ashlar_host.puppy_release_cli import PuppySession, run_puppy_command

selected_fixture = None
closing = None


@contextmanager
def open_test_provider(invocation, release):
    fixture = selected_fixture
    if release.payload != fixture.releases[0].payload:
        raise PuppyReleaseError('Original source policy refused')
    adapter = PuppyNativeReleaseAdapter(prepared=fixture.prepared,
        prior_model=encoded(fixture.schema), http=fixture.http, cypher=fixture.query,
        runtime=fixture.runtime, evidence=invocation.output_directory/'native')
    fixture.adapter = adapter
    try: yield PuppySession(adapter,fixtures.Policy(),None)
    finally:
        if closing is not None: closing(invocation)


class Tests(unittest.TestCase):
    def setUp(self):
        global selected_fixture, closing
        self.fixture=fixtures.Tests(methodName='runTest');self.fixture.setUp()
        self.addCleanup(self.fixture.doCleanups)
        selected_fixture=self.fixture;closing=None;self.ordinal=0
        self.base=self.fixture.directory
        self.release=self.base/'original.graph.json';self.release.write_bytes(self.fixture.releases[0].payload)
        self.marker=self.base/'provider-executed'

    def configuration(self, mode='activate', selected=None):
        self.ordinal+=1
        output=self.base/('output-'+str(self.ordinal))
        value={'profile':'ashlar-puppy-invocation/0.1','release_path':str(self.release),
            'release_sha256':self.fixture.releases[0].sha256,'output_directory':str(output)}
        if selected is not None:
            value.update(activation_path=selected['activation_path'],activation_sha256=selected['activation_sha256'])
        config=self.base/('config-'+str(self.ordinal)+'.json');config.write_bytes(encoded(value))
        provider=self.base/('provider-'+str(self.ordinal)+'.py')
        provider.write_text('from pathlib import Path\n'+
            'assert Path('+repr(str(output/'original-intent.json'))+').is_file()\n'+
            'Path('+repr(str(self.marker))+').write_text("executed")\n'+
            'from test_puppy_release_cli import open_test_provider\n'+
            'open_puppy_release=open_test_provider\n')
        return config,provider,hashlib.sha256(provider.read_bytes()).hexdigest(),mode,output

    def run_case(self, case):
        return run_puppy_command(*(str(p) if isinstance(p,Path) else p for p in case[:4]))

    def test_original_intent_precedes_provider_and_explicit_read_reopens(self):
        selected=self.run_case(self.configuration())
        self.assertTrue(self.marker.is_file())
        first=Path(selected['observation_path']).read_bytes()
        reopened=self.run_case(self.configuration('read',selected))
        self.assertEqual(Path(reopened['observation_path']).read_bytes(),first)
        self.assertEqual(selected['activation_sha256'],reopened['activation_sha256'])
        self.assertEqual(release_columns('node'),fixtures.columns('node'))
        self.assertEqual(GremlinReleaseProjection.__module__, 'ashlar_host.puppy_gremlin')

    def test_wrong_provider_or_selected_hash_refuses_before_operator_execution(self):
        case=self.configuration()
        with self.assertRaises(PuppyReleaseError):
            self.run_case((*case[:2],'0'*64,*case[3:]))
        self.assertFalse(self.marker.exists());self.assertFalse(case[4].exists())
        selected=self.run_case(self.configuration());self.marker.unlink()
        case=self.configuration('read',{**selected,'activation_sha256':'0'*64})
        with self.assertRaises(PuppyReleaseError):self.run_case(case)
        self.assertFalse(self.marker.exists());self.assertFalse(case[4].exists())

    def test_closing_original_mutation_and_output_replacement_refuse_receipt(self):
        global closing
        case=self.configuration()
        closing=lambda invocation:self.release.write_bytes(self.release.read_bytes()+b' ')
        with self.assertRaises(PuppyReleaseError):self.run_case(case)
        self.assertFalse((case[4]/'activation.json').exists())
        self.release.write_bytes(self.fixture.releases[0].payload)
        def replace_output(invocation):
            invocation.output_directory.rename(invocation.output_directory.with_name('retained-original-output'))
            invocation.output_directory.mkdir()
        closing=replace_output;case=self.configuration()
        with self.assertRaises(PuppyReleaseError):self.run_case(case)
        self.assertFalse((case[4]/'activation.json').exists())

    def test_cancelled_provider_closing_preserves_identity_and_no_success(self):
        global closing
        for cancellation in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            case=self.configuration()
            def cancel(invocation):raise cancellation
            closing=cancel
            try:self.run_case(case)
            except BaseException as error:self.assertIs(error,cancellation)
            else:self.fail('Closing cancellation must propagate')
            self.assertFalse((case[4]/'activation.json').exists())

    def test_intent_fsync_failure_prevents_provider_and_fd_wrap_failure_closes(self):
        case=self.configuration()
        with patch('ashlar_host.puppy_release_cli.os.fsync',side_effect=OSError('fsync failed')):
            with self.assertRaises(OSError):self.run_case(case)
        self.assertFalse(self.marker.exists())
        case=self.configuration()
        captured=[]
        def fail_wrap(fd,mode):
            captured.append(fd);raise OSError('wrap failed')
        with patch('ashlar_host.puppy_release_cli.os.fdopen',side_effect=fail_wrap):
            with self.assertRaises(OSError):self.run_case(case)
        self.assertFalse(self.marker.exists())
        import os
        self.assertEqual(len(captured),1)
        with self.assertRaises(OSError):os.fstat(captured[0])

if __name__=='__main__':unittest.main()
