import json,copy,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths
from ashlar_host import commerce_implicit_arithmetic as owner
from ashlar_host.publication_reader import PublicationProvider,original_commerce_reader_bindings
from ashlar_host.source_identity import build_transaction
from ashlar_host.implicit_arithmetic_profile import ImplicitArithmeticSchemaValidation,ImplicitArithmeticAdmissionConfig,ImplicitArithmeticExecutionConfig
from ashlar_host.path_capture import PathCaptureConfig
from ashlar.weft_path_decode import PathDecodeConfig
from test_implicit_arithmetic import SCHEMAS

class Tests(unittest.TestCase):
    def setUp(self):
        root=Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream'
        self.model=(root/'ontology.json').read_bytes();self.graph=(root/'graph/fixture.json').read_bytes();self.pack=(root/'pack.json').read_bytes()
        batch,bindings=build_transaction(self.model,self.graph,source_system='private-original-commerce-fixture')
        roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
        registry=[{'table':'local.commerce.'+role,'uuid':role+'-fixture'}for role in roles]
        versions={r['table']:2 for r in registry[:4]};aliases={t:t.replace('local.','spark_catalog.')for t in versions}
        manifest={'publication_id':'fixture-only','table_versions_json':json.dumps(versions),'source_progress_json':json.dumps({batch.feed:{}})}
        context=object()
        provider=PublicationProvider(None,aliases,None,b'original',b'original',{},context=context,
            original_bindings=original_commerce_reader_bindings(self.model,bindings,manifest,registry,aliases))
        self.opened=SimpleNamespace(model=self.model,graph=self.graph,bindings=bindings,manifest=manifest,aliases=aliases,
            original_report={'table_registry':registry},provider=provider,context=context)
        self.paths=ImplicitArithmeticDistributionPaths(Path('/index'),Path('/installation'))
        self.config=ImplicitArithmeticExecutionConfig(ImplicitArithmeticAdmissionConfig(1048576,
            ImplicitArithmeticSchemaValidation(SCHEMAS,lambda *args:None)),PathCaptureConfig(100,10000,100000),PathDecodeConfig(10000),None,None)

    def test_all_four_original_requests_select_complete_binding_and_compile_twice(self):
        requests=[];executed=[]
        def compile(paths,raw):
            request=json.loads(raw);requests.append(raw)
            self.assertEqual(request['interfaceVersion'],'weft-compile/0.4.2')
            return b'{"original":"compiler"}\n'
        def execute(opened,request,artifact,recompiled,**kwargs):
            binding=json.loads(opened.provider.expected_binding)
            self.assertEqual(len(binding['records']),10);self.assertEqual(len(binding['relationships']),9)
            self.assertEqual(sum(len(r['properties'])for r in binding['records']),34)
            self.assertIs(kwargs['original_oracle'],owner.original_commerce_scenario_oracle)
            executed.append(request['sql']);return {'provisional':True}
        with patch.object(owner,'compile_implicit_arithmetic_distribution',side_effect=compile),patch.object(owner,'execute_commerce_implicit_arithmetic',side_effect=execute):
            result=owner.execute_original_commerce_implicit_arithmetic_cases(self.opened,self.paths,config=self.config,pack_bytes=self.pack)
        self.assertEqual(tuple(c['case']for c in result),tuple(n for n,_ in owner.commerce_original_cases(self.pack)))
        self.assertEqual(executed,[sql for _,sql in owner.commerce_original_cases(self.pack)])
        self.assertEqual(len(requests),8)
        for i in range(0,8,2):self.assertEqual(requests[i],requests[i+1])
        with self.assertRaises(PermissionError):self.opened.provider.expected_binding='substitute'

    def test_forged_full_binding_refused_and_legacy_builder_bytes_unchanged(self):
        from ashlar_host.commerce_path_request import commerce_path_request
        from ashlar_host.commerce_binding import compiler_request
        from ashlar_host.publication_reader import compiler_request as compatibility
        opened=self.opened;sql=owner.commerce_original_cases(self.pack)[0][1]
        original=commerce_path_request(sql,self.model,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases,profile='paths-keys')
        request=owner.commerce_implicit_arithmetic_request(sql,self.model,opened.bindings,opened.manifest,opened.original_report['table_registry'],opened.aliases,pack_bytes=self.pack)
        self.assertEqual(request['target']['bindingJson'],original['target']['bindingJson'])
        self.assertIs(compiler_request,compatibility)
        changed=json.loads(request['target']['bindingJson']);changed['records'][0]['typeId']='forged'
        import hashlib
        raw=json.dumps(changed,sort_keys=True,separators=(',',':'));request['target'].update(bindingJson=raw,bindingSha256=hashlib.sha256(raw.encode()).hexdigest())
        with self.assertRaises(ValueError):opened.provider.select_original_binding(request,context=opened.context)

    def test_borrowed_metadata_is_frozen_before_compiler_and_bounds_precede_transport(self):
        seen=[]
        def compile(paths,raw):
            seen.append(json.loads(raw))
            self.opened.bindings.clear();self.opened.aliases.clear()
            return b'{}\n'
        with patch.object(owner,'compile_implicit_arithmetic_distribution',side_effect=compile),patch.object(owner,'execute_commerce_implicit_arithmetic',return_value={}):
            result=owner.execute_original_commerce_implicit_arithmetic_cases(self.opened,self.paths,config=self.config,pack_bytes=self.pack)
        self.assertEqual(len(result),4)
        bindings=[q['target']['bindingJson']for q in seen]
        self.assertEqual(len(set(bindings)),1)
        self.setUp()
        from dataclasses import replace
        from ashlar_host.config import HostError
        bounded=replace(self.config,admission=replace(self.config.admission,maximum_artifact_bytes=16))
        with patch.object(owner,'compile_implicit_arithmetic_distribution')as compile:
            with self.assertRaises(HostError):owner.execute_original_commerce_implicit_arithmetic_cases(self.opened,self.paths,config=bounded,pack_bytes=self.pack)
            compile.assert_not_called()

    def test_failure_and_cancellation_withhold_all_case_evidence(self):
        for failure in (ValueError('compiler'),KeyboardInterrupt()):
            with patch.object(owner,'compile_implicit_arithmetic_distribution',side_effect=failure),patch.object(owner,'execute_commerce_implicit_arithmetic')as execute:
                with self.assertRaises(type(failure))as caught:owner.execute_original_commerce_implicit_arithmetic_cases(self.opened,self.paths,config=self.config,pack_bytes=self.pack)
                self.assertIs(caught.exception,failure);execute.assert_not_called()
        with patch.object(owner,'compile_implicit_arithmetic_distribution')as compile:
            with self.assertRaises(ValueError):owner.execute_original_commerce_implicit_arithmetic_cases(self.opened,self.paths,config=self.config,pack_bytes=b'{}')
            compile.assert_not_called()

if __name__=='__main__':unittest.main()
