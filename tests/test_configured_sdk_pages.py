# SPDX-License-Identifier: MIT OR Apache-2.0
"""SDK-free selection controls; no compiler/native authority is simulated."""
from dataclasses import replace
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from ashlar_host.configured_sdk_pages import ConfiguredSdkPagesConfig
from ashlar_host.configured_local_runtime import page_configured_local
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
