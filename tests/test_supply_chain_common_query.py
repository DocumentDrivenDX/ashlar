"""SDK-free composition controls; inert ports confer no native/source grant."""
from contextlib import ExitStack
from dataclasses import replace
import copy
import json
import hashlib
from pathlib import Path
from types import SimpleNamespace
import unittest
from unittest import mock
from ashlar.graph_release import GraphRelease
from ashlar.weft_count_star_distribution import CountStarDistributionPaths
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host.supply_chain_common_read import OpenedSupplyChainCommonRead
from ashlar_host import supply_chain_common_query as owner
from ashlar_host.supply_chain_common_count_requests import prepare_supply_chain_common_count_requests
import test_supply_chain_common_count_requests as fixtures

class Controls(unittest.TestCase):
    def setUp(self):
        fixture=fixtures.Controls();fixture.setUp();self.fixture=fixture
        self.config=owner.SupplyChainCommonQueryConfig(CountStarDistributionPaths(Path('/inert/index'),Path('/inert/installation')),
            1024*1024,PathCaptureConfig(128,1024*1024,16*1024*1024),PathDecodeConfig(1024*1024))
        self.context=object();self.current=True;self.events=[];self.active=False
        source=SimpleNamespace(model=fixture.raw[1],graph=fixture.raw[2],bindings=fixture.bindings,
            receipt=b'inert',raw=b'inert',metadata=lambda:{'inert':True})
        driver=SimpleNamespace(transport=SimpleNamespace(targets={row['table']:SimpleNamespace(**row)for row in fixture.registry}))
        self.opened=object.__new__(OpenedSupplyChainCommonRead)
        for name,value in {'source':source,'driver':driver,'context':self.context,'model':source.model,'graph':source.graph,
            'pack':fixture.raw[0],'bindings':fixture.bindings,'manifest':fixture.manifest,'aliases':fixture.aliases}.items():object.__setattr__(self.opened,name,value)
        self.schemas={name:b'inert schema only'for name in ('compile-request-v0.4.1.schema.json','compile-response-v0.4.1.schema.json','logical-plan-v0.4.1.schema.json')}
        self.stack=ExitStack();self.addCleanup(self.stack.close)
        self.stack.enter_context(mock.patch.object(OpenedSupplyChainCommonRead,'require_current',self.require))
        self.stack.enter_context(mock.patch.object(OpenedSupplyChainCommonRead,'native_files',lambda opened:{'original':'inert'}))
        self.stack.enter_context(mock.patch.object(OpenedSupplyChainCommonRead,'query_provider',self.provider,create=True))
        self.graph_mock=self.stack.enter_context(mock.patch.object(owner,'read_original_supply_chain_graph_release',return_value=GraphRelease(b'{"publication":{},"snapshots":{},"nodes":[],"edges":[]}',hashlib.sha256(b'{"publication":{},"snapshots":{},"nodes":[],"edges":[]}').hexdigest())))
        self.schema_mock=self.stack.enter_context(mock.patch.object(owner,'installed_count_star_schema_bundle',return_value=tuple(self.schemas.items())))
        # These construction and execution ports are isolated test doubles. The
        # production API constructs the actual pinned validation/typed decoder
        # itself and never accepts these caller-supplied ports.
        self.stack.enter_context(mock.patch.object(owner,'make_offline_count_star_schema_validation'))
        self.stack.enter_context(mock.patch.object(owner,'CountStarAdmissionConfig'))
        self.stack.enter_context(mock.patch.object(owner,'CountStarExecutionConfig'))
        self.compiler=self.stack.enter_context(mock.patch.object(owner,'compile_count_star_distribution',return_value=b'{}'))
        self.executor=self.stack.enter_context(mock.patch.object(owner,'execute_commerce_count_star',side_effect=self.execute))
        self.observed=[]
    def require(self,context):
        self.events.append('current')
        if context is not self.context or not self.current:raise PermissionError('inert current custody refused')
    def provider(self,context,*,family,part,ordinal):
        self.require(context)
        batch=prepare_supply_chain_common_count_requests(*self.fixture.raw,self.fixture.bindings,self.fixture.manifest,
            self.fixture.registry,self.fixture.aliases,family=family,part=part)
        query=next(q for q in batch['queries']if q['ordinal']==ordinal)
        self.events.append((family,part,ordinal))
        return SimpleNamespace(context=context,original_fixed=True),copy.deepcopy(query['request'])
    def execute(self,actual,request,artifact,recompiled,*,config,original_oracle):
        self.assertIs(actual.opened,self.opened);self.assertIs(actual.context,self.context)
        self.assertEqual(actual.native_files(),actual.original_native_files)
        self.assertTrue(actual.provider.original_fixed)
        oracle=original_oracle(actual.model,actual.graph,copy.deepcopy(request))
        return {'fixture_only':True,'oracle':oracle}
    def run_query(self,observe=None):
        return owner.query_supply_chain_common(self.opened,self.config,observe=observe or(lambda *args:self.observed.append(args)))
    def test_fixed16_queries32_protocols_explicit_partitions_and_outer_provisional_custody(self):
        result=self.run_query()
        self.assertEqual((len(result['queries']),len(result['protocol_index'])),(16,32))
        self.assertEqual((self.compiler.call_count,self.executor.call_count,len(self.observed)),(32,16,32))
        self.assertEqual([q['query_ordinal']for q in result['queries'][:13]],list(range(13)))
        self.assertTrue(all(q['part']=='relationships-0-6'for q in result['queries'][:7]))
        self.assertTrue(all(q['part']=='relationships-7-12'for q in result['queries'][7:13]))
        self.assertEqual(self.graph_mock.call_count,2);self.assertEqual(result['required_pending'],['grouped-count'])
        raw=sum(len(request)+len(response)for _,_,request,response in self.observed)
        index=sum(len((owner.encoded(entry)+'\n').encode())for entry in result['protocol_index'])
        self.assertEqual(result['protocol_total_bytes'],raw+index);self.assertLessEqual(raw+index,8*1024*1024)
        self.assertEqual(result['queries'][13]['evidence']['oracle']['rows'][0][1],'{"state":"null"}')
        self.assertIn('outer',result['qualification']);self.assertTrue(self.current)
    def test_oversized_nonbytes_or_subclass_response_not_observed_or_executed(self):
        class Bytes(bytes):pass
        for response in (b'x'*(1024*1024+1),'not bytes',Bytes(b'{}')):
            self.compiler.return_value=response;self.compiler.reset_mock();self.executor.reset_mock();self.observed.clear()
            with self.subTest(response=type(response)),self.assertRaises(ValueError):self.run_query()
            self.assertEqual(self.observed,[]);self.executor.assert_not_called()
    def test_recompile_difference_refuses_before_executor_with_full_closing(self):
        self.compiler.side_effect=[b'{}',b'{ }']
        with self.assertRaises(ValueError):self.run_query()
        self.executor.assert_not_called();self.assertEqual(self.graph_mock.call_count,2)
    def test_actual_protocol_index_budget_overflow_withholds_combined_result(self):
        self.compiler.return_value=b' '*(1024*1024-2)+b'{}'
        with self.assertRaisesRegex(ValueError,'eight MiB'):self.run_query()
        self.assertLess(len(self.observed),32);self.assertLess(self.executor.call_count,16)
        self.assertEqual(self.graph_mock.call_count,2)
    def test_callback_cancellation_preserved_with_original_graph_schema_closing(self):
        cancellation=KeyboardInterrupt('inert observer')
        def observer(*args):raise cancellation
        with self.assertRaises(KeyboardInterrupt)as failure:self.run_query(observer)
        self.assertIs(failure.exception,cancellation);self.executor.assert_not_called()
        self.assertEqual(self.graph_mock.call_count,2);self.assertEqual(self.schema_mock.call_count,2)
    def test_config_mutation_cannot_redirect_original_compiler_closing(self):
        def mutate(*args):object.__setattr__(self.config.distribution,'index',Path('/foreign/index'))
        with self.assertRaises(ValueError):self.run_query(mutate)
        self.assertEqual(self.compiler.call_count,1)
        self.assertEqual(self.schema_mock.call_args_list[-1].args[0].index,Path('/inert/index'))
        self.assertEqual(self.graph_mock.call_count,2)
    def test_fixed_actual_owner_request_drift_refuses_and_closing_native_drift_withholds(self):
        provider=self.provider
        def foreign(context,**kwargs):
            actual,request=provider(context,**kwargs);request['sql']='SELECT foreign';return actual,request
        with mock.patch.object(OpenedSupplyChainCommonRead,'query_provider',mock.Mock(side_effect=foreign)):
            with self.assertRaises(ValueError):self.run_query()
        self.executor.assert_not_called()
        self.graph_mock.side_effect=[GraphRelease(b'{}',hashlib.sha256(b'{}').hexdigest()),GraphRelease(b'{ }',hashlib.sha256(b'{ }').hexdigest())]
        with self.assertRaises(ValueError):self.run_query()
    def test_wrong_owner_and_finite_configuration_refuse_before_effects(self):
        with self.assertRaises(ValueError):owner.query_supply_chain_common(object(),self.config,observe=lambda *a:None)
        self.compiler.assert_not_called();self.graph_mock.assert_not_called()
        for changed in ({'maximum_artifact_bytes':True},{'maximum_artifact_bytes':1024*1024+1},
            {'capture':PathCaptureConfig(129,1,1)},{'decoder':PathDecodeConfig(1024*1024+1)}):
            with self.assertRaises(ValueError):replace(self.config,**changed)
    def test_actual64MiB_combined_overflow_withholds_without_reducing_evidence(self):
        # Shared string references keep this control small in memory; the exact
        # report still contains >64MiB across otherwise individually bounded
        # per-query evidence. No native data/compiler/producer is executed.
        payload='x'*(1024*1024)
        def bounded_query_evidence(*args,**kwargs):
            return {'native_result':{'rows':[[payload]for _ in range(16)]}}
        self.executor.side_effect=bounded_query_evidence
        with self.assertRaises(ValueError):self.run_query()
        self.assertEqual(self.executor.call_count,4)
        self.assertEqual(self.graph_mock.call_count,2);self.assertEqual(self.schema_mock.call_count,2)
    def test_named_graph_and_genuine_typed_decoder_report_values_preserved(self):
        from ashlar.weft_decode import ExactScalar
        from ashlar.weft_related_keys_decode import DecodedRelatedKeys
        graph={'publication':{'original':'exact'},'snapshots':{'native':{'uuid':'original','version':9007199254740993}},
               'nodes':[{'id':'9007199254740993','root_id':None,'props_json':'{ "retained": true }'}],
               'edges':[{'id':'23','src':'source','dst':'target','order_key':None}]}
        payload=(json.dumps(graph,ensure_ascii=False)+'\n').encode()
        self.graph_mock.return_value=GraphRelease(payload,hashlib.sha256(payload).hexdigest())
        exact=ExactScalar(9007199254740993,'9007199254740993')
        decoded=DecodedRelatedKeys(b'{"items":[["key"]],"truncated":false}',((ExactScalar('key','key'),),),False)
        self.executor.side_effect=lambda *a,**kw:{'decoded':[exact,decoded]}
        result=self.run_query()
        self.assertEqual(result['graph_release']['original_payload_text'],payload.decode())
        self.assertEqual(result['graph_release']['document'],graph)
        reported=result['queries'][0]['evidence']['decoded']
        self.assertEqual(reported[0]['value'],{'$type':'integer','text':'9007199254740993'})
        self.assertEqual(reported[1]['$type'],'DecodedRelatedKeys');self.assertFalse(reported[1]['truncated'])
        self.assertEqual(bytes.fromhex(reported[1]['original']['hex']),decoded.original)
    def test_whole_archived_public_receipt_above1MiB_uses_existing16MiB_source_bound(self):
        from ashlar_host.supply_chain_query import finite_public_source
        from ashlar_host.supply_chain_finite_source import validate_supply_chain_dataset_receipt
        from pathlib import Path
        raw=(Path(__file__).resolve().parents[1]/'docs/helix/04-build/evidence/supply-chain-public-dataset-20261009.json').read_bytes()
        self.assertGreater(len(raw),1024*1024)
        validate_supply_chain_dataset_receipt(*self.fixture.raw[1:],raw)
        self.opened.source.receipt=raw
        batch=prepare_supply_chain_common_count_requests(*self.fixture.raw,self.fixture.bindings,self.fixture.manifest,
            self.fixture.registry,self.fixture.aliases,family='singleton',part='complete')
        request=batch['queries'][0]['request'];binding=json.loads(request['target']['bindingJson'])
        artifact={'modelPins':binding['modelPins'],'bindingSha256':request['target']['bindingSha256']}
        # This is payload/semantic correspondence of archived actual bytes only,
        # never an admission of archive origin or current source/native authority.
        port=finite_public_source(self.opened.source,16*1024*1024)
        receipt=port(request,artifact,[])
        self.assertEqual(json.loads(receipt['originalReceiptText'])['originalDatasetReceiptText'],raw.decode())
        self.assertGreater(len(receipt['originalReceiptText'].encode()),1024*1024)
        with self.assertRaises(ValueError):finite_public_source(self.opened.source,1024*1024)(request,artifact,[])
        with mock.patch.object(owner,'CountStarExecutionConfig')as execution:
            self.run_query()
        self.assertEqual(execution.call_args.kwargs['maximum_public_source_bytes'],16*1024*1024)
