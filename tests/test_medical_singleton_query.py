"""Small pure correspondence/nominal seam and mandatory closing controls."""
import hashlib,json,unittest,uuid
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.medical_source import build_transaction,SOURCE_SHA
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths
from ashlar_host.medical_publication_plan import FEED
from ashlar_host.medical_singleton_request import SQL,medical_singleton_request,medical_singleton_oracle
from ashlar_host.medical_singleton_query import MedicalSingletonQuery,execute_medical_singleton,_MedicalQueryProvider,medical_query_closing_report,MedicalSingletonBuffer
from ashlar_host.medical_singleton_local import query_original_medical_singleton

ROOT=Path(__file__).resolve().parents[1]
MODEL=(ROOT/'examples/domain-packs/medical/historical/archive/schemas/ontology.json').read_bytes()
GRAPH=(ROOT/'examples/domain-packs/medical/upstream/graph/fixture.json').read_bytes()
BASELINE=(ROOT/'docs/helix/04-build/evidence/medical-historical-admission-20261009/public-receipt.json').read_bytes()

class MedicalSingletonTests(unittest.TestCase):
    def fixture(self):
        _,bindings=build_transaction(MODEL,GRAPH,BASELINE,source_system=FEED)
        names=['private.medical.'+r for r in ('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')]
        registry=[{'table':t,'uuid':str(uuid.UUID(int=i+1))}for i,t in enumerate(names)]
        manifest={'publication_id':'inert-test-only','table_versions_json':json.dumps({t:2 for t in names[:4]}),'source_progress_json':json.dumps({FEED:'1'}),'schema_revisions_json':json.dumps({FEED:SOURCE_SHA})}
        aliases={t:'spark_catalog.test_medical.'+t.rsplit('.',1)[-1]for t in names[:4]}
        return bindings,manifest,registry,aliases
    def test_original_model_binding_and_independent_singleton_bag(self):
        bindings,manifest,registry,aliases=self.fixture()
        request=medical_singleton_request(MODEL,GRAPH,BASELINE,bindings,manifest,registry,aliases)
        self.assertEqual(request['modules'][0]['documentJson'].encode(),MODEL)
        self.assertEqual(request['modules'][0]['pin']['umfVersion'],'0.8.0')
        self.assertEqual(request['sql'],SQL)
        binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(request['target']['bindingSha256'],hashlib.sha256(request['target']['bindingJson'].encode()).hexdigest())
        self.assertEqual(len(binding['publication']['tables']),4)
        self.assertEqual(medical_singleton_oracle(MODEL,GRAPH,request)['rows'],[['Encounter/example']])
        self.assertEqual(medical_singleton_oracle(MODEL,GRAPH,request)['witnesses']['complete_objects'],51)
        for alteration in ('binding','vector','revision'):
            b,m,r,a=self.fixture()
            if alteration=='binding':b['properties'][0]['property_id']='changed'
            elif alteration=='vector':m['table_versions_json']=json.dumps({next(iter(a)):2})
            else:m['schema_revisions_json']=json.dumps({FEED:'changed'})
            with self.subTest(alteration=alteration),self.assertRaises((PermissionError,ValueError)):
                medical_singleton_request(MODEL,GRAPH,BASELINE,b,m,r,a)
    def test_nominal_source_refuses_before_compiler_and_public_seam_uses_selection(self):
        paths=ImplicitArithmeticDistributionPaths(Path('/private/tmp/index'),Path('/private/tmp/installation'))
        selection=MedicalSingletonQuery(paths)
        with patch('ashlar_host.medical_singleton_query.compile_implicit_arithmetic_distribution')as compile_call:
            with self.assertRaises(PermissionError):execute_medical_singleton(selection,SimpleNamespace(),object(),None,MODEL,GRAPH,BASELINE,{},output=Path('/private/tmp/test'),bounds=None)
            compile_call.assert_not_called()
        with patch('ashlar_host.medical_singleton_local.read_original_medical_local',return_value={'closed':True})as read:
            result=query_original_medical_singleton('inert-config',paths)
            self.assertEqual(result,{'closed':True});self.assertIs(type(read.call_args.kwargs['singleton_query']),MedicalSingletonQuery)
    def test_large_valid_ack_report_references_preserve_outer_bytes_and_budget(self):
        ack={'position':'1','request_hex':(b'r'*382101).hex(),
            'manifest_hex':(b'm'*27613).hex(),'receipt_hex':(b'a'*623).hex()}
        observation={'native':{},'protected_ack':ack,'qualification':'inert valid hex codec fixture'}
        compact=medical_query_closing_report(observation)
        self.assertEqual(compact['protected_ack']['original_bytes']['request_hex'],
            {'bytes':382101,'sha256':hashlib.sha256(b'r'*382101).hexdigest()})
        self.assertEqual(observation['protected_ack'],ack)
        buffer=MedicalSingletonBuffer(b'{\"sql\":\"original\"}',b'{}',b'{}','{}')
        self.assertEqual(buffer.report()['original_protocol']['recompiled_response'],buffer.report()['original_protocol']['response'])
        self.assertEqual(buffer.protocol_files()[0][1],b'{\"sql\":\"original\"}')
        self.assertEqual(buffer.report()['original_protocol']['request']['sha256'],hashlib.sha256(buffer.request).hexdigest())
        with self.assertRaises(PermissionError):MedicalSingletonBuffer(b'{}',b'{}',b'{ }','{}')
        from ashlar_host.delta_custody import encoded
        answer={'observation':observation,'singleton_query':{'interval':compact}}
        self.assertLess(len((encoded(answer)+'\n').encode()),1048576)
        self.assertGreater(len((encoded({'outer':observation,'duplicate':observation})+'\n').encode()),1048576)
        with self.assertRaises(ValueError):medical_query_closing_report(dict(observation,protected_ack=dict(ack,receipt_hex='gg')))
    def test_private_byte_write_preserves_primary_over_close_and_refuses_short_write(self):
        import ashlar_host.original_medical_local_read as reader
        for primary in (OSError('write'),KeyboardInterrupt()):
            with self.subTest(primary=type(primary).__name__),patch.object(reader.os,'open',return_value=71),patch.object(reader.os,'write',side_effect=primary),patch.object(reader.os,'fsync')as sync,patch.object(reader.os,'close',side_effect=OSError('close'))as close:
                with self.assertRaises(type(primary))as raised:reader._write_private_file(Path('/private/tmp/inert'),b'original')
                self.assertIs(raised.exception,primary);close.assert_called_once_with(71);sync.assert_not_called()
        with patch.object(reader.os,'open',return_value=71),patch.object(reader.os,'write',return_value=1),patch.object(reader.os,'fsync')as sync,patch.object(reader.os,'close')as close:
            with self.assertRaises(PermissionError):reader._write_private_file(Path('/private/tmp/inert'),b'original')
            close.assert_called_once_with(71);sync.assert_not_called()
    def test_interval_closing_attempted_on_capture_failure_and_preserves_primary(self):
        provider=object.__new__(_MedicalQueryProvider);context=object();primary=KeyboardInterrupt();calls=[]
        resolved=SimpleNamespace(descriptor=SimpleNamespace(raw={'publication_id':'inert'}))
        provider.context=context;provider.active=False;provider.closed=None
        provider.resolve=lambda c:(calls.append('resolve')or resolved)
        def observe(descriptor,*,context):
            calls.append('observe')
            if calls.count('observe')==2:raise ValueError('closing refused')
            return {}
        provider.driver=SimpleNamespace(observe_original_medical_read=observe)
        with self.assertRaises(KeyboardInterrupt)as raised:
            with provider.interval(context):raise primary
        self.assertIs(raised.exception,primary);self.assertEqual(calls,['resolve','observe','resolve','observe']);self.assertFalse(provider.active);self.assertIsNone(provider.closed)

if __name__=='__main__':unittest.main()
