"""Real configured source composition over an inert public graph producer port."""
import json,tempfile,unittest
from dataclasses import replace
from pathlib import Path
from unittest.mock import patch
from ashlar.attempt_store import verify_request_digest
from ashlar.source_checkpoint import jsonl_checkpoint
from ashlar_host.config import ProducerConfig
from ashlar_host.configured_source_admission_config import ConfiguredSourceAdmissionConfig
from ashlar_host.configured_source_local import ConfiguredLocalSource,open_configured_local_source
from test_typed_graph_source import fixture,inert_receipt

PORT='ashlar_host.configured_typed_graph_source_admission.admit_typed_graph'

class Tests(unittest.TestCase):
    def selected(self,root):
        original=fixture(root)
        producer=ProducerConfig(root/'producer',root/'bun',root/'git',30,1048576,1048576)
        return original,ConfiguredSourceAdmissionConfig(original.path,producer,root/'receipts',1048576)

    def test_minted_graph_owner_prefixes_exact_requests_restart_and_retention(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();root.chmod(0o700);original,config=self.selected(root);context=object()
            def producer(selection,model,request):
                self.assertEqual(model,original.model_source);return inert_receipt(original,request)
            with patch(PORT,side_effect=producer)as public:
                with open_configured_local_source(config,stream='graph-雪',context=context)as source:
                    self.assertIs(type(source),ConfiguredLocalSource);self.assertTrue(source.graph);self.assertTrue(source.typed)
                    self.assertEqual(source.type_ids,(17,18));self.assertNotIn(19,source.type_ids)
                    states=[source.state_at(n,context)for n in range(4)]
                    self.assertEqual([len(s.current)for s in states],[0,3,3,2])
                    self.assertEqual([len(s.history)for s in states],[0,3,4,5])
                    self.assertEqual([len(s.tombstones)for s in states],[0,0,0,1])
                    edge=next(v for k,v in states[1].current.items()if k.kind=='edge')
                    self.assertEqual([(e.type_id,e.id)for e in edge.endpoints],[(17,1),(18,1)])
                    self.assertEqual(edge.retained_json,'{"unknown":{"keep":true}}')
                    self.assertEqual({k.kind for k in states[1].current if k.id==1},{'object','edge'})
                    updated=next(v for k,v in states[2].current.items()if k.kind=='object'and k.type_id==17)
                    self.assertEqual(updated.props_json,'{"21":3}');self.assertEqual(updated.version,2)
                    specs=[source.spec(n,context)for n in (1,2,3)]
                    for n,spec in enumerate(specs):
                        request=spec.request();verify_request_digest(request);source.admit_request(request,context)
                        self.assertEqual(request['source_checkpoint_json'],jsonl_checkpoint(source.batches[n]))
                        self.assertEqual(request['predecessor'],specs[n-1].publication_id if n else'empty-local-configured-jsonl-stream')
                    metadata=source.metadata(context);self.assertEqual(metadata['profile'],'ashlar-configured-local-typed-graph-source/0.1')
                    report=json.loads((config.output/'report.json').read_bytes());self.assertFalse(report['published']);self.assertFalse(report['acknowledged'])
                    self.assertEqual(report['records'],5);self.assertEqual(len(source.receipt_files),1)
                with self.assertRaises(ValueError):source.metadata(context)
                with open_configured_local_source(replace(config,output=root/'reopened'),stream='graph-雪',context=object())as reopened:
                    self.assertEqual([reopened.spec(n,reopened.context)for n in (1,2,3)],specs)
                    self.assertEqual(reopened.state_at(3,reopened.context),states[3])
                self.assertEqual(public.call_count,2)

    def test_graph_query_invalid_registry_refuses_before_spark_and_releases_actual_lease(self):
        import hashlib
        from ashlar_host.configured_local_runtime import ConfiguredLocalRuntimeConfig,query_configured_local
        from ashlar_host.configured_local_query import ConfiguredLocalQueryConfig
        from ashlar_host.configured_weft_compile import ConfiguredWeftCompileConfig
        from ashlar.weft_count_star_distribution import CountStarDistributionPaths
        from ashlar.weft_path_decode import PathDecodeConfig
        from ashlar_host.path_capture import PathCaptureConfig
        from ashlar_host.supply_chain_native import PrivateFiniteLease
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();root.chmod(0o700);original,selection=self.selected(root)
            installation=root/'native';installation.mkdir(mode=0o700)
            lock=installation/'.finite-installation.lock';lock.write_bytes(b'');lock.chmod(0o600)
            registry=installation/'installation.json';registry.write_bytes(b'{}')
            output=root/'query';selection=replace(selection,output=output/'admission')
            runtime=ConfiguredLocalRuntimeConfig(installation,output,root/'jars',root/'java',selection,'graph','1791547200000000',None,'graph',hashlib.sha256(b'{}').hexdigest())
            query=ConfiguredLocalQueryConfig('SELECT COUNT(*) FROM A a',ConfiguredWeftCompileConfig(CountStarDistributionPaths(root/'index',root/'compiler'),1048576),PathCaptureConfig(1000,65536,1048576),PathDecodeConfig(65536))
            with patch(PORT,side_effect=lambda chosen,model,request:inert_receipt(original,request)),patch('ashlar_host.configured_local_runtime._runtime',return_value=()),patch('ashlar_host.configured_local_runtime.open_finite_spark')as spark:
                with self.assertRaisesRegex(ValueError,'Configured local runtime refused')as raised:query_configured_local(runtime,ordinal=1,query=query)
                self.assertFalse(getattr(raised.exception,'cleanup_failed',False));spark.assert_not_called()
            reopened=PrivateFiniteLease(installation,create=False);reopened.close()
            self.assertFalse((output/'runtime-report.json').exists())

    def test_original_source_drift_and_primary_cancellation_close(self):
        with tempfile.TemporaryDirectory()as temporary:
            root=Path(temporary).resolve();root.chmod(0o700);original,config=self.selected(root);context=object();cancel=KeyboardInterrupt()
            with patch(PORT,side_effect=lambda selection,model,request:inert_receipt(original,request)):
                with self.assertRaises(KeyboardInterrupt)as raised:
                    with open_configured_local_source(config,stream='graph',context=context)as source:
                        original.source.write_bytes(b'changed')
                        with self.assertRaises(ValueError):source.state_at(1,context)
                        raise cancel
                self.assertIs(raised.exception,cancel);self.assertTrue(cancel.cleanup_failed)
                with self.assertRaises(ValueError):source.spec(1,context)

if __name__=='__main__':unittest.main()
