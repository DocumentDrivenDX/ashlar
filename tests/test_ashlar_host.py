"""Source-only installed composition controls; no native workflow qualification."""
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
from unittest.mock import patch
from ashlar_host.config import HostError, PrivatePostgresConfig, ProducerConfig
from ashlar_host.lifecycle import owned_context, owned_connection, finish
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.schema_rows import fixture_columns


class HostTests(unittest.TestCase):
    def test_required_typed_configuration(self):
        with self.assertRaises(HostError): ProducerConfig(Path('relative'),Path('/bun'),Path('/git'),1,1,1)
        with self.assertRaises(HostError): ProducerConfig(Path('/source'),Path('/bun'),Path('/git'),True,1,1)
        with self.assertRaises(HostError): PrivatePostgresConfig('other','127.0.0.1',15432,'truss_e2e')

    def test_packaged_resources_have_original_schema(self):
        columns=fixture_columns(RESOURCE_ROOT)
        self.assertIn(('id','BIGINT'),columns['edge_current'])
        self.assertTrue((RESOURCE_ROOT/'ontology.json').is_file())
        self.assertIn('validateCoreDatasetValues',(RESOURCE_ROOT/'check_commerce_dataset.ts').read_text())

    def test_context_replacement_preserves_primary(self):
        primary=KeyboardInterrupt()
        @contextmanager
        def context():
            try: yield None
            finally: raise OSError('private cleanup payload')
        with self.assertRaises(KeyboardInterrupt) as seen:
            with owned_context(context()): raise primary
        self.assertIs(seen.exception,primary)
        self.assertTrue(primary.cleanup_failed)

    def test_context_suppression_preserves_primary(self):
        primary=KeyboardInterrupt()
        @contextmanager
        def context():
            try: yield None
            except BaseException: pass
        with self.assertRaises(KeyboardInterrupt) as seen:
            with owned_context(context()): raise primary
        self.assertIs(seen.exception,primary)

    def test_connection_and_stop_both_attempted(self):
        primary=KeyboardInterrupt(); calls=[]
        def broken(): calls.append('close'); raise OSError()
        def stop(): calls.append('stop'); raise OSError()
        with self.assertRaises(KeyboardInterrupt) as seen: finish(primary,[broken,stop])
        self.assertIs(seen.exception,primary)
        self.assertEqual(calls,['close','stop'])

    def test_package_without_checkout_or_native_import(self):
        import shutil, subprocess, sys
        root=Path(__file__).resolve().parents[1]
        with tempfile.TemporaryDirectory() as temporary:
            package=Path(temporary).resolve()
            # Isolate the checked-out package without requiring Git history.
            # Independent build/export custody selects the reviewed production bytes.
            shutil.copytree(root/'src/ashlar',package/'ashlar',ignore=shutil.ignore_patterns('__pycache__'))
            shutil.copytree(root/'src/ashlar_host',package/'ashlar_host',ignore=shutil.ignore_patterns('__pycache__'))
            code="""import sys
sys.path.insert(0,sys.argv[1])
import ashlar_host.commerce, ashlar_host.delta_publication, ashlar_host.delta_query
from ashlar_host.resources import RESOURCE_ROOT
from ashlar_host.schema_rows import fixture_columns
assert ('id','BIGINT') in fixture_columns(RESOURCE_ROOT)['edge_current']
assert not any(n.split('.')[0] in {'pyspark','psycopg','graphframes','tools'} for n in sys.modules)
"""
            subprocess.run([sys.executable,'-S','-c',code,str(package)],cwd=package,check=True,capture_output=True,timeout=10)

    def test_query_reader_close_failure_withholds_report_and_stops(self):
        import json, sys
        from types import SimpleNamespace
        from ashlar_host import delta_query
        class Builder:
            def __getattr__(self, name):
                return lambda *a,**k: spark if name=='getOrCreate' else self
        calls=[]
        spark=SimpleNamespace(stop=lambda:calls.append('stop'))
        with tempfile.TemporaryDirectory() as temporary:
            root=Path(temporary).resolve(); publication=root/'publication';publication.mkdir()
            (publication/'report.json').write_text('{}')
            (publication/'public-dataset.json').write_bytes(b'receipt')
            output=root/'output'
            config=SimpleNamespace(index=root/'index',installation=root/'installed',publication=publication,producer=SimpleNamespace(source=root/'source',maximum_receipt_bytes=1024),jars=root/'jars',output=output,model=root/'model',graph=root/'graph')
            @contextmanager
            def interval(context): yield
            provider=SimpleNamespace(interval=interval,closed_interval_custody=lambda c:{'closed':True})
            opened=SimpleNamespace(original_report={},provider=provider,context=object(),aliases={},native_files=lambda:{},original_native_files={},manifest={})
            @contextmanager
            def reader(*args):
                yield opened
                raise OSError('reader close')
            def producer(*args): args[-1].write_bytes(b'receipt')
            with patch.dict(sys.modules,{'pyspark':SimpleNamespace(),'pyspark.sql':SimpleNamespace(SparkSession=SimpleNamespace(builder=Builder()))}), patch.object(delta_query,'runtime_paths',return_value=[]),patch.object(delta_query,'recompute_dataset',side_effect=producer),patch.object(delta_query,'open_commerce_reader',side_effect=reader),patch.object(delta_query,'run_indexed_queries',return_value={'native':'passed'}),patch.object(delta_query,'run_indexed_relationship',return_value={'native':'passed'}):
                with self.assertRaises(OSError): delta_query.query(config)
            self.assertEqual(calls,['stop'])
            self.assertFalse((output/'report.json').exists())

    def test_actual_provider_interval_preserves_query_cancellation(self):
        from contextlib import nullcontext
        from types import SimpleNamespace
        from ashlar_host import publication_reader, connection, postgres
        import ashlar.manifest
        calls=[];primary=KeyboardInterrupt()
        def rollback(): calls.append('rollback');raise OSError('private cleanup')
        def close(): calls.append('close')
        native=SimpleNamespace(rollback=rollback,close=close)
        driver=SimpleNamespace(policy=SimpleNamespace(active={'manifest':{'table_versions_json':'{}'}}),transport=SimpleNamespace(targets={}),writer=lambda *a:nullcontext(),hold=lambda *a,**k:nullcontext())
        context=object()
        provider=publication_reader.PublicationProvider(driver,{}, {'role':'ashlar_ack_test'},b'{}',b'{}',{},context=context)
        provider._ack=lambda session:{'mock':'ack'}
        with patch.object(connection,'connect',return_value=native),patch.object(postgres,'Session',side_effect=lambda c:c),patch.object(ashlar.manifest,'manifest_pin_vector',return_value={}):
            with self.assertRaises(KeyboardInterrupt) as observed:
                with provider.interval(context): raise primary
        self.assertIs(observed.exception,primary)
        self.assertTrue(primary.cleanup_failed)
        self.assertEqual(calls,['rollback','close'])
        self.assertFalse(provider.active)
        self.assertIsNone(provider._closed_custody)

    def test_publication_qualification_present(self):
        from ashlar_host import delta_publication
        self.assertIn('Selected original commerce',delta_publication.__doc__)

    def test_cleanup_marker_cannot_replace_readonly_primary(self):
        class ReadonlyCancellation(KeyboardInterrupt):
            def __setattr__(self,name,value): raise RuntimeError('marker refused')
        def broken(): raise OSError('cleanup')
        primary=ReadonlyCancellation()
        with self.assertRaises(ReadonlyCancellation) as observed: finish(primary,[broken])
        self.assertIs(observed.exception,primary)
        @contextmanager
        def context():
            try: yield
            finally: broken()
        primary=ReadonlyCancellation()
        with self.assertRaises(ReadonlyCancellation) as observed:
            with owned_context(context()): raise primary
        self.assertIs(observed.exception,primary)

    def test_public_api_refuses_before_phase_import(self):
        from ashlar_host.commerce import publish_commerce,query_commerce
        for api in (publish_commerce,query_commerce):
            with self.assertRaises(HostError): api(None)


if __name__=='__main__': unittest.main()
