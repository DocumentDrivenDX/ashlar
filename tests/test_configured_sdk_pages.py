# SPDX-License-Identifier: MIT OR Apache-2.0
"""SDK-free selection controls; no compiler/native authority is simulated."""
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.configured_sdk_pages import ConfiguredSdkPagesConfig,ConfiguredSdk053LiveSession,_require_frozen_page_response,_require_frozen_grouped_artifact,_supported_page_compiler,run_configured_sdk_pages
from ashlar_host.configured_local_runtime import page_configured_local,run_configured_local_r02_weft053
from ashlar_host.path_capture import PathCaptureConfig

class Parameters:
    def model_dump(self,**kwargs):return {}
class Result:
    model_fields={'person_key':str,'person_name':str}

class Tests(unittest.TestCase):
    def fixture(self):
        return ConfiguredSdkPagesConfig(SimpleNamespace(**{n:lambda *a:None for n in ('compile_engine_page_json','check_engine_page_json','check_engine_read_json')}),
            'SELECT p.person_key,p.person_name FROM Person p',Parameters,Result,Parameters(),2,10,90,PathCaptureConfig(1000,65536,1048576))
    def test_explicit_typed_capacity_and_parameter_selection(self):
        query=self.fixture()
        for changed in ({'page_size':0},{'page_size':True},{'maximum_rows':1001},{'timeout_seconds':121},
                        {'parameters':object()},{'capture':PathCaptureConfig(2,65536,1048576)},{'sql':''}):
            with self.subTest(changed=changed),self.assertRaises(ValueError):replace(query,**changed)
    def test_sdk_pages_cannot_fall_through_to_publish(self):
        with patch('ashlar_host.configured_local_runtime._run')as run:
            for invalid in (None,{},object()):
                with self.assertRaises(ValueError):page_configured_local(object(),ordinal=1,query=invalid)
            run.assert_not_called();query=self.fixture();page_configured_local(object(),ordinal=1,query=query)
            self.assertEqual(run.call_args.kwargs,{'query':query});self.assertEqual(run.call_args.args[1],1)
    def test_public_sdk_entrypoint_rejects_053_without_live_gate(self):
        from ashlar.weft_053_distribution import Weft053Distribution
        compiler=object.__new__(Weft053Distribution)
        for name in ('compile_engine_page_json','check_engine_page_json','check_engine_read_json'):
            setattr(compiler,name,lambda *args:None)
        self.assertFalse(_supported_page_compiler(compiler))
        query=replace(self.fixture(),compiler=compiler)
        with self.assertRaises(ValueError):run_configured_sdk_pages(None,None,query,runtime_guard=lambda:())
    def test_053_page1_refuses_direct_execution_outside_ordered_gate(self):
        session=object.__new__(ConfiguredSdk053LiveSession)
        session.review_gate=SimpleNamespace(stage='pre-sql',consumed=False,approved=False,executing_name=None)
        with self.assertRaises(ValueError):session.page()
    def test_page1_checked_response_must_match_frozen_bytes(self):
        _require_frozen_page_response(b'{"status":"compiled"}',b'{"status":"compiled"}')
        with self.assertRaises(ValueError):
            _require_frozen_page_response(b'{"status":"compiled"}',b'{"status":"compiled","extra":true}')

    def test_grouped_execution_bytes_must_match_review_record(self):
        import hashlib
        request=b'{"request":1}\n';response=b'{"status":"compiled"}\n'
        compiled={'requestBytes':request,'responseBytes':response,'recompiledBytes':response}
        record={'requestSha256':hashlib.sha256(request).hexdigest(),
            'responseSha256':hashlib.sha256(response).hexdigest()}
        _require_frozen_grouped_artifact(compiled,record)
        with self.assertRaises(ValueError):
            _require_frozen_grouped_artifact({**compiled,'requestBytes':b'{"request":2}\n'},record)
        with self.assertRaises(ValueError):
            _require_frozen_grouped_artifact({**compiled,'recompiledBytes':b'{}'},record)

    def test_r02_selection_is_idempotent_and_wrapper_checks_ordinal_before_runtime(self):
        from pathlib import Path
        from ashlar.weft_053_distribution import Weft053Distribution
        from ashlar_host.configured_sdk_pages import ConfiguredR02Weft053Selection
        compiler=object.__new__(Weft053Distribution)
        for name in ('compile_engine_page_json','check_engine_page_json','check_engine_read_json'):
            setattr(compiler,name,lambda *args:None)
        query=replace(self.fixture(),compiler=compiler)
        selection=ConfiguredR02Weft053Selection(Path('/private/tmp/r02'), 'a'*32,
            'b'*40,'c'*40,'d'*64,'e'*64,{},
            'f04f74668ac9adc46ac9928b01a4155ccad3aab6421cd297d018b7e45348aab0')
        selection.__post_init__();selection.__post_init__()
        with patch('ashlar_host.configured_local_runtime._run') as run:
            self.assertEqual(run_configured_local_r02_weft053(object(),ordinal=1,query=query,selection=selection),run.return_value)
            self.assertIs(run.call_args.kwargs['r02'],selection)
            run.reset_mock()
            with self.assertRaises(ValueError):
                run_configured_local_r02_weft053(object(),ordinal=2,query=query,selection=selection)
            run.assert_not_called()
