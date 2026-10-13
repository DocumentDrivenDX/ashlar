"""Inert lifecycle provenance controls, never public producer/native qualification."""
from pathlib import Path
import copy
import json
import os
import tempfile
import unittest
from unittest import mock
from ashlar_host import supply_chain_producer as owner
from ashlar_host import supply_chain_finite_source as source

ROOT=Path(__file__).resolve().parents[1]

class Controls(unittest.TestCase):
    def setUp(self):
        self.tmp=tempfile.TemporaryDirectory();self.addCleanup(self.tmp.cleanup)
        self.root=Path(self.tmp.name).resolve()
        original=ROOT/'examples/domain-packs/supply-chain/upstream'
        self.model=(original/'ontology.json').read_bytes();self.graph=(original/'graph/fixture.json').read_bytes()
        self.model_path=self.root/'model';self.model_path.write_bytes(self.model)
        self.graph_path=self.root/'graph';self.graph_path.write_bytes(self.graph)
        self.checkout=self.root/'source';self.checkout.mkdir()
        self.bun=self.root/'bun';self.bun.write_bytes(b'inert bun never executed')
        self.git=self.root/'git';self.git.write_bytes(b'inert git never executed')
        self.script=self.root/'check_supply_chain_dataset.ts';self.script.write_bytes(b'inert producer never executed')
        self.output=self.root/'receipt'
        self.config=owner.SupplyChainProducerConfig(self.checkout,self.bun,self.git,1,1048576,8388608)
        graph=json.loads(self.graph)
        self.receipt={'profile':'ashlar-supply-chain-public-dataset/0.1','umfRevision':source.UMF_REVISION,
            'sourceSha256':source.SOURCE_SHA,'graphSha256':source.GRAPH_SHA,'receipt':{
                'scope':'supplied-dataset-only','input':{'scope':{'id':'ashlar-original-supply-chain-fixture','closure':'supplied-dataset-only'}},
                'datasetValidation':{'valid':True,'complete':True,'diagnostics':[]},
                'records':[{'instanceId':obj['key'],'result':{'validation':{'valid':True}}}for obj in graph['objects']],
                'keys':[{'instanceId':str(i)}for i in range(20)],
                'relationships':[{'instanceId':e['key'],'sourceInstanceId':e['source'],'targetInstanceId':e['target']}for e in graph['edges']]}}
        self.raw=json.dumps(self.receipt,separators=(',',':')).encode();self.calls=[]
        self.patch_resource=mock.patch.object(owner,'RESOURCE_ROOT',self.root);self.patch_resource.start();self.addCleanup(self.patch_resource.stop)
        self.patch_capture=mock.patch.object(owner,'capture',side_effect=self.capture);self.capture_mock=self.patch_capture.start();self.addCleanup(self.patch_capture.stop)
    def capture(self,argv,**kwargs):
        self.calls.append((tuple(argv),kwargs))
        if argv[0]==str(self.bun):self.output.write_bytes(self.raw);return b'inert success',b''
        return (source.UMF_REVISION.encode()+b'\n',b'')if argv[-1]=='HEAD'else(b'',b'')
    def producer(self):return owner.HeldSupplyChainDataset(self.config,self.model_path,self.graph_path,self.output)
    def test_explicit_output_parent_tmpdir_ignores_ambient(self):
        with mock.patch.dict(os.environ,{'TMPDIR':'/foreign/ambient'}):producer=self.producer()
        self.assertEqual(producer.environment['TMPDIR'],str(self.output.parent))
        self.assertTrue(all(call[1]['environment']['TMPDIR']==str(self.output.parent)for call in self.calls))
    def test_git_warning_still_refuses_before_production(self):
        def warning(argv,**kwargs):
            out,err=self.capture(argv,**kwargs)
            return out,b'warning'if argv[0]==str(self.git)else err
        with mock.patch.object(owner,'capture',side_effect=warning):
            with self.assertRaisesRegex(ValueError,'Clean original public UMF'):self.producer()
        self.assertFalse(self.output.exists())

    def test_actual_lifecycle_mints_nominal_origin_and_reopens_without_production(self):
        producer=self.producer();invocation=producer.original_invocation()
        self.assertIs(type(invocation),owner.OriginalSupplyChainDatasetInvocation)
        retained=owner.RetainedOriginalSupplyChainDataset(invocation)
        finite=source.FiniteSupplyChainSource(self.model_path,self.graph_path,current_dataset=retained)
        finite.renew();retained.renew();self.assertEqual(retained(self.model,self.graph),self.raw)
        self.assertEqual(sum(argv[0]==str(self.bun)for argv,_ in self.calls),1)
        self.assertGreater(sum(argv[0]==str(self.git)for argv,_ in self.calls),2)
        retained.close()
    def test_forged_hash_json_flag_callback_and_unminted_nominal_refuse(self):
        for value in (self.receipt,{'sha256':'0'*64,'valid':True},lambda *a:self.raw,object.__new__(owner.OriginalSupplyChainDatasetInvocation)):
            with self.assertRaises(ValueError):owner.RetainedOriginalSupplyChainDataset(value)
        with self.assertRaises(TypeError):owner.OriginalSupplyChainDatasetInvocation()
        self.assertEqual(self.calls,[])
    def test_copied_nominal_origin_not_authentic_and_existing_output_refuses(self):
        producer=self.producer();invocation=producer.original_invocation()
        with self.assertRaises(ValueError):owner.RetainedOriginalSupplyChainDataset(copy.copy(invocation))
        with self.assertRaises(ValueError):self.producer()
    def test_incomplete_semantics_refuses_before_mint(self):
        self.receipt['receipt']['relationships'].pop();self.raw=json.dumps(self.receipt).encode()
        with self.assertRaises(ValueError):self.producer()
    def test_runtime_script_receipt_and_source_mutation_refuse(self):
        for role in ('bun','git','script','receipt','source'):
            with self.subTest(role=role):
                # Each mutation is tested against its own genuine inert lifecycle.
                if self.output.exists():self.output.unlink()
                producer=self.producer();retained=owner.RetainedOriginalSupplyChainDataset(producer.original_invocation())
                path={'bun':self.bun,'git':self.git,'script':self.script,'receipt':self.output,'source':self.checkout}[role]
                if role=='source':
                    old=self.root/'saved-source';path.rename(old);path.mkdir()
                    with self.assertRaises(ValueError):retained.renew()
                    path.rmdir();old.rename(path)
                else:
                    raw=path.read_bytes();path.write_bytes(raw+b'!')
                    with self.assertRaises(ValueError):retained.renew()
                    path.write_bytes(raw)
    def test_config_drift_cannot_redirect_original_git_or_skip_file_checks(self):
        producer=self.producer();retained=owner.RetainedOriginalSupplyChainDataset(producer.original_invocation())
        previous=self.config.git;object.__setattr__(self.config,'git',self.root/'foreign')
        self.calls.clear()
        with mock.patch.object(owner,'_file',wraps=owner._file)as files:
            with self.assertRaises(ValueError):retained.renew()
        self.assertTrue(self.calls);self.assertTrue(all(argv[0]==str(previous)for argv,_ in self.calls))
        self.assertGreaterEqual(files.call_count,6)
    def test_body_input_failure_preserves_primary_and_original_closing(self):
        producer=self.producer();retained=owner.RetainedOriginalSupplyChainDataset(producer.original_invocation())
        primary=ValueError('closing failure')
        with mock.patch.object(owner,'_pin_inputs',side_effect=primary),mock.patch.object(owner,'_file',wraps=owner._file)as files:
            with self.assertRaisesRegex(ValueError,'retained producer inputs')as failure:retained(b'foreign',self.graph)
        self.assertTrue(failure.exception.cleanup_failed);self.assertGreaterEqual(files.call_count,6)
    def test_capture_cancellation_closes_all_originals_and_never_mints(self):
        cancellation=KeyboardInterrupt('inert cancellation')
        def interrupted(argv,**kwargs):
            if argv[0]==str(self.bun):raise cancellation
            return self.capture(argv,**kwargs)
        with mock.patch.object(owner,'capture',side_effect=interrupted),mock.patch.object(owner,'_file',wraps=owner._file)as files:
            with self.assertRaises(KeyboardInterrupt)as failure:self.producer()
        self.assertIs(failure.exception,cancellation);self.assertGreaterEqual(files.call_count,10)
        self.assertFalse(self.output.exists())
    def test_closing_clean_source_failure_no_origin(self):
        produced=False
        def dirty(argv,**kwargs):
            nonlocal produced
            if argv[0]==str(self.bun):produced=True;return self.capture(argv,**kwargs)
            if produced and argv[-1]=='--porcelain':return b' M changed',b''
            return self.capture(argv,**kwargs)
        with mock.patch.object(owner,'capture',side_effect=dirty):
            with self.assertRaises(ValueError):self.producer()
    def test_original_owner_drift_still_checks_original_resources(self):
        producer=self.producer();producer.environment={'PATH':'foreign'}
        with mock.patch.object(owner,'_file',wraps=owner._file)as files:
            with self.assertRaises(ValueError):producer.original_invocation()
        self.assertGreaterEqual(files.call_count,6)
    def test_closed_retained_owner_refuses_and_still_checks_originals(self):
        producer=self.producer();retained=owner.RetainedOriginalSupplyChainDataset(producer.original_invocation());retained.close()
        with mock.patch.object(owner,'_file',wraps=owner._file)as files:
            with self.assertRaises(ValueError):retained.renew()
        self.assertGreaterEqual(files.call_count,6)
        with self.assertRaises(AttributeError):retained.closed=False
    def test_public_reopening_factory_nominal_source_and_callback_drift(self):
        producer=self.producer();finite=owner.reopen_original_supply_chain_source(producer.original_invocation())
        self.assertIs(type(finite.current_dataset),owner.RetainedOriginalSupplyChainDataset)
        self.assertEqual(sum(argv[0]==str(self.bun)for argv,_ in self.calls),1)
        original=finite.current_dataset;finite.current_dataset=lambda *args:self.raw
        with mock.patch.object(owner,'_pin_inputs',wraps=owner._pin_inputs)as pins:
            with self.assertRaises(ValueError):finite.renew()
        # Source closing still invokes its actual original owner, not the forged replacement.
        self.assertGreater(pins.call_count,0)
        with self.assertRaises(AttributeError):finite._original_dataset=lambda *args:self.raw
    def test_receipt_replacement_between_snapshot_and_identity_capture_refuses(self):
        snapshot=owner.snapshot
        def replaced(path,maximum):
            raw=snapshot(path,maximum)
            if path==self.output:self.output.write_bytes(raw+b'\n')
            return raw
        with mock.patch.object(owner,'snapshot',side_effect=replaced):
            with self.assertRaisesRegex(ValueError,'receipt snapshot'):self.producer()
    def test_head_cancellation_still_attempts_original_clean_status_and_files(self):
        producer=self.producer();retained=owner.RetainedOriginalSupplyChainDataset(producer.original_invocation())
        cancellation=KeyboardInterrupt('HEAD cancellation');calls=[]
        def cancelled(argv,**kwargs):
            calls.append(tuple(argv))
            if argv[-1]=='HEAD':raise cancellation
            return self.capture(argv,**kwargs)
        with mock.patch.object(owner,'capture',side_effect=cancelled),mock.patch.object(owner,'_file',wraps=owner._file)as files:
            with self.assertRaises(KeyboardInterrupt)as failure:retained.renew()
        self.assertIs(failure.exception,cancellation)
        self.assertTrue(any(argv[-1]=='--porcelain'for argv in calls));self.assertGreaterEqual(files.call_count,6)
    def test_original_multilink_tool_admitted_but_changed_link_count_refuses(self):
        alias=self.root/'git-hardlink';os.link(self.git,alias)
        producer=self.producer();retained=owner.RetainedOriginalSupplyChainDataset(producer.original_invocation())
        retained.renew();self.assertEqual(self.git.stat().st_nlink,2)
        alias.unlink();self.capture_mock.reset_mock()
        with self.assertRaises(ValueError):retained.renew()
        self.capture_mock.assert_not_called()
    def test_changed_git_wholebytes_refuses_before_any_capture(self):
        producer=self.producer();retained=owner.RetainedOriginalSupplyChainDataset(producer.original_invocation())
        self.git.write_bytes(b'replaced git must never execute');self.capture_mock.reset_mock()
        with mock.patch.object(owner,'_file',wraps=owner._file)as files:
            with self.assertRaises(ValueError):retained.renew()
        self.capture_mock.assert_not_called();self.assertGreaterEqual(files.call_count,8)
    def test_data_hardlink_refuses_before_capture(self):
        os.link(self.model_path,self.root/'model-hardlink')
        with self.assertRaises(ValueError):self.producer()
        self.capture_mock.assert_not_called()
    def test_script_changed_during_initial_git_pin_never_runs_producer(self):
        bun_calls=[];changed=False
        def mutating(argv,**kwargs):
            nonlocal changed
            if argv[0]==str(self.bun):bun_calls.append(tuple(argv))
            result=self.capture(argv,**kwargs)
            if argv[-1]=='--porcelain'and not changed:
                self.script.write_bytes(b'changed during initial Git status');changed=True
            return result
        with mock.patch.object(owner,'capture',side_effect=mutating):
            with self.assertRaises(ValueError):self.producer()
        self.assertTrue(changed);self.assertEqual(bun_calls,[]);self.assertFalse(self.output.exists())
    def test_closed_environment_keeps_real_dirty_status_index_read_only(self):
        from ashlar_host.evolution_producer import capture as actual_capture
        self.producer()
        environments=[kwargs['environment']for _,kwargs in self.calls]
        self.assertTrue(environments)
        self.assertTrue(all(env.get('GIT_OPTIONAL_LOCKS')=='0'for env in environments))
        # The very same environment is forwarded to Bun, so its inner Git pins
        # receive the read-only flag too. No Bun is executed in this control.
        environment=dict(environments[0]);repo=self.root/'throwaway-git';repo.mkdir()
        def git(*args,env=environment):
            out,err=actual_capture(['/usr/bin/git','-C',str(repo),*args],cwd=repo,
                environment=env,timeout_seconds=5,maximum_output_bytes=4096)
            self.assertEqual(err,b'');return out
        git('init','--quiet')
        changed=repo/'changed';unchanged=repo/'unchanged'
        changed.write_text('original\n');unchanged.write_text('original\n')
        git('add','changed','unchanged')
        git('-c','user.name=Inert Control','-c','user.email=inert@example.invalid','commit','--quiet','-m','inert')
        # A clean file with stale index stat metadata triggers optional refresh;
        # a separately modified tracked file makes status nonvacuous.
        info=unchanged.stat();os.utime(unchanged,ns=(info.st_atime_ns,info.st_mtime_ns+2000000000))
        changed.write_text('actually changed\n');index=repo/'.git/index'
        def snapshot_index():
            info=index.stat()
            return (index.read_bytes(),info.st_dev,info.st_ino,info.st_uid,info.st_mode,
                    info.st_nlink,info.st_size,info.st_mtime_ns,info.st_ctime_ns)
        before=snapshot_index();status=git('status','--porcelain')
        self.assertEqual(status,b' M changed\n');self.assertEqual(snapshot_index(),before)
        # Negative control: identical local operation with optional refresh
        # enabled actually changes index bytes/identity; this is not a vacuous
        # metadata-preservation assertion. Only the throwaway repo is modified.
        enabled=dict(environment);enabled['GIT_OPTIONAL_LOCKS']='1'
        self.assertEqual(git('status','--porcelain',env=enabled),status)
        self.assertNotEqual(snapshot_index(),before)
