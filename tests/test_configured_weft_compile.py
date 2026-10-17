"""Sequencing only: actual minted scope/builder, inert compiler/schema boundaries."""
from pathlib import Path
import unittest
from unittest.mock import patch
import test_configured_weft_binding as binding_fixture
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
from ashlar_host.configured_weft_binding import configured_count_star_request
from ashlar_host.configured_weft_compile import ConfiguredWeftCompileConfig,compile_configured_count_star
from ashlar_host.count_star_admission import SCHEMA_SHA256

class Tests(unittest.TestCase):
    def test_raw_observation_recompile_and_closing_on_success_and_failure(self):
        tested=[];response=b'{"status":"compiled"}\n';config=ConfiguredWeftCompileConfig(CountStarDistributionPaths(Path('/original/index'),Path('/original/installation')),1048576)
        schemas={name:b'inert schema boundary'for name in SCHEMA_SHA256}
        def inside(scope,aliases,sql,*,layout_sha256):
            if tested:return configured_count_star_request(scope,aliases,sql,layout_sha256=layout_sha256)
            tested.append(True)
            for mode in ('success','different','malformed','cancel'):
                observed=[];admissions=[];cancel=KeyboardInterrupt()
                outputs=[response,response if mode!='different'else b'{"status":"different"}']
                if mode=='malformed':outputs=[b'{not-json',b'{not-json']
                def observe(iteration,request,raw):
                    observed.append((iteration,request,raw))
                    if mode=='cancel':raise cancel
                def admit(*a,**kw):
                    self.assertEqual(len(observed),2);admissions.append(True);return 'inert admission'
                with patch('ashlar_host.configured_weft_compile.installed_count_star_schema_bundle',return_value=schemas)as bundle,patch('ashlar_host.configured_weft_compile.make_offline_count_star_schema_validation',return_value='inert schema port'),patch('ashlar_host.configured_weft_compile.CountStarAdmissionConfig',return_value='inert admission configuration'),patch('ashlar_host.configured_weft_compile.admit_count_star_artifact',side_effect=admit),patch('ashlar_host.configured_weft_compile.compile_count_star_distribution',side_effect=outputs)as compiler:
                    if mode=='success':
                        result=compile_configured_count_star(scope,aliases,sql,config,observe=observe)
                        self.assertEqual(result['responseBytes'],response);self.assertEqual(result['recompiledBytes'],response)
                        self.assertEqual(result['requestBytes'],observed[0][1]);self.assertEqual(result['admitted'],'inert admission')
                        self.assertEqual(compiler.call_count,2);self.assertEqual(admissions,[True])
                    else:
                        with self.assertRaises(KeyboardInterrupt if mode=='cancel'else ValueError)as raised:compile_configured_count_star(scope,aliases,sql,config,observe=observe)
                        if mode=='cancel':self.assertIs(raised.exception,cancel)
                        self.assertEqual(admissions,[])
                    self.assertEqual(bundle.call_count,2) #opening and closing, including parse/cancel failure
                    self.assertEqual([item[0]for item in observed],list(range(len(observed))))
                    scope.renew()
            return configured_count_star_request(scope,aliases,sql,layout_sha256=layout_sha256)
        # Reuse the existing real scope fixture and its original-vector assertions;
        # only compiler/schema/admission lower boundaries above are controlled.
        helper=binding_fixture.Tests();self.addCleanup(helper.doCleanups)
        with patch.object(binding_fixture,'configured_count_star_request',side_effect=inside):helper.test_original_model_vector_and_homes_survive_alias_projection()
        self.assertEqual(tested,[True])
