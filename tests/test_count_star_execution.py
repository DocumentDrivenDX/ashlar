# SPDX-License-Identifier: MIT OR Apache-2.0
"""Held provider controls; fixture frames never claim native SQL qualification."""
import copy,unittest
from types import SimpleNamespace
from test_count_star_admission import PAIRS,SCHEMAS
import test_run_commerce_path_weft as ports
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.count_star_admission import CountStarAdmissionConfig,CountStarSchemaValidation
from ashlar_host.count_star_execution import CountStarExecutionConfig,CountStarExecutionError,execute_commerce_count_star,_scalar
from ashlar_host.path_capture import PathCaptureConfig
class CountStarExecutionTests(unittest.TestCase):
    def setup(self,case='count-star:original-replay',rows=None):
        rows=[]if rows is None else rows;q,r=copy.deepcopy(PAIRS[case]);provider=ports.Provider(r,rows)
        opened=SimpleNamespace(provider=provider,context=provider.context,model=q['modules'][0]['documentJson'].encode(),graph=b'fixture',original_native_files={'fixture':'fixed'},native_files=lambda:{'fixture':'fixed'})
        config=CountStarExecutionConfig(CountStarAdmissionConfig(16777216,CountStarSchemaValidation(SCHEMAS,lambda *args:None)),PathCaptureConfig(100,10000,100000),PathDecodeConfig(10000),None,None)
        oracle=lambda *args:{'rows':rows,'witnesses':{'fixture':True},'scope':'inert provider control'}
        return q,r,opened,config,oracle
    def execute(self,s):return execute_commerce_count_star(s[2],s[0],s[1],copy.deepcopy(s[1]),config=s[3],original_oracle=s[4])
    def test_empty_having_result_still_executes_complete_aggregate_guard(self):
        s=self.setup();result=self.execute(s)
        original=next(o for o in s[1]['obligations']if o['id']=='ashlar.arithmetic.exact')['parameters']['checks']
        self.assertEqual([g['check']for g in result['guards']if g['obligation']=='ashlar.arithmetic.exact'],original)
        self.assertEqual(result['native_result']['rows'],[]);self.assertFalse(s[2].provider.active)
    def test_nonzero_full_bag_guard_withholds_having_query(self):
        for case,owner in (('count-star:original-replay','ashlar.arithmetic.exact'),('count-star:expansion-count-owner','ashlar.path.countCapacity')):
            s=self.setup(case);provider=s[2].provider;query=s[1]['sql'];check=next(o for o in s[1]['obligations']if o['id']==owner)['parameters']['checks'][0]['sql'];old=provider.sql;calls=[]
            def sql(statement,args):
                calls.append(statement);return ports.frame(['violations'],[['1']])if statement==check else old(statement,args)
            provider.driver.transport.spark.sql=sql
            with self.assertRaises(CountStarExecutionError):self.execute(s)
            self.assertNotIn(query,calls);self.assertFalse(provider.active)
    def test_closing_publication_drift_withholds_count_result(self):
        s=self.setup();values=iter(['opening','changed']);s[2].provider.resolve=lambda context:next(values)
        with self.assertRaises(CountStarExecutionError):self.execute(s)
        self.assertFalse(s[2].provider.active)
    def test_closing_cancellation_outranks_ordinary_native_failure(self):
        from contextlib import contextmanager
        for cancellation in (KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            s=self.setup();provider=s[2].provider;body=ValueError('ordinary native failure')
            def sql(*args,**kwargs):raise body
            provider.driver.transport.spark.sql=sql
            @contextmanager
            def interval(context):
                provider.active=True
                try:yield
                finally:
                    provider.active=False
                    raise cancellation
            provider.interval=interval
            with self.assertRaises(type(cancellation))as caught:self.execute(s)
            self.assertIs(caught.exception,cancellation);self.assertFalse(provider.active)
    def test_original_body_cancellation_identity_survives_cleanup(self):
        from contextlib import contextmanager
        for closing in (ValueError('cleanup'),KeyboardInterrupt()):
            s=self.setup();provider=s[2].provider;body=SystemExit()
            def sql(*args,**kwargs):raise body
            provider.driver.transport.spark.sql=sql
            @contextmanager
            def interval(context):
                provider.active=True
                try:yield
                finally:
                    provider.active=False
                    raise closing
            provider.interval=interval
            with self.assertRaises(SystemExit)as caught:self.execute(s)
            self.assertIs(caught.exception,body);self.assertFalse(provider.active)
    def test_observer_snapshot_mutation_cannot_repair_or_corrupt_capture(self):
        from dataclasses import replace
        s=list(self.setup());seen=[]
        def observe(captured):
            seen.append(copy.deepcopy(captured));captured['rows'].append(['forged']);captured['schema'].clear()
        s[3]=replace(s[3],native_observer=observe)
        result=self.execute(s)
        self.assertEqual(seen[0],result['native_result']);self.assertEqual(result['native_result']['rows'],[])
        self.assertTrue(seen[0]['schema']);self.assertFalse(s[2].provider.active)
    def test_observer_sees_raw_mismatch_before_refusal(self):
        from dataclasses import replace
        s=list(self.setup());seen=[]
        s[3]=replace(s[3],native_observer=lambda captured:seen.append(captured))
        s[4]=lambda *args:{'rows':[['independent mismatch']],'witnesses':{},'scope':'independent control'}
        with self.assertRaises(CountStarExecutionError):self.execute(s)
        self.assertEqual(seen[0]['rows'],[]);self.assertFalse(s[2].provider.active)
    def test_observer_failure_and_cancellation_withhold_success(self):
        from dataclasses import replace
        from contextlib import contextmanager
        for body in (ValueError('observer refusal'),KeyboardInterrupt(),SystemExit(),GeneratorExit()):
            s=list(self.setup());provider=s[2].provider
            def observe(captured):raise body
            s[3]=replace(s[3],native_observer=observe)
            @contextmanager
            def interval(context):
                provider.active=True
                try:yield
                finally:
                    provider.active=False
                    if not isinstance(body,Exception):raise SystemExit('closing cancellation')
            provider.interval=interval
            with self.assertRaises(type(body))as caught:self.execute(s)
            self.assertIs(caught.exception,body);self.assertFalse(provider.active)
    def presence(self,raw,*,capability=True,flag=True,availability='absent-allowed',nullable=False,family='string'):
        identity={'documentId':'original','module':'domain','element':'parent','revision':'fixed'}
        descriptor={'identity':identity,'kind':'scalar','availability':availability,'type':{'family':family,'facets':{},'nullable':nullable}}
        column={'representation':{'kind':'value','descriptor':identity,'nativeNull':flag}}
        return _scalar(column,{'op':'field'},raw,False,[descriptor],capability)
    def test_selected_optional_native_null_retains_value_and_present_null(self):
        value=self.presence('{"state":"value","value":"opaque"}')
        self.assertEqual(value['state'],'value');self.assertEqual(value['value'].value,'opaque');self.assertEqual(value['value'].original,'opaque')
        self.assertEqual(self.presence('{"state":"null"}'),{'state':'null'})
        with self.assertRaises(CountStarExecutionError):self.presence('{"state":"absent"}')
        with self.assertRaises(CountStarExecutionError):self.presence(None)
    def test_native_null_requires_capability_optional_descriptor_and_exact_flag(self):
        for options in ({'capability':False},{'capability':1},{'availability':'required'},{'flag':1},{'flag':False}):
            with self.assertRaises(CountStarExecutionError):self.presence('{"state":"null"}',**options)
        self.assertEqual(self.presence('{"state":"null"}',flag=False,nullable=True),{'state':'null'})
    def test_optional_carrier_does_not_accept_malformed_numeric_or_duplicate_values(self):
        from ashlar.publication import ResolutionError
        for raw in ('{"state":"null","extra":true}','{"state":"value","value":3}','{"state":"value","value":null}','{"state":"value","value":"x","value":"y"}','[]','{"state":"absent","value":"x"}'):
            with self.assertRaises((CountStarExecutionError,ResolutionError)):self.presence(raw)
    def test_tagged_boolean_uses_exact_json_boolean_carrier(self):
        for flag in (False,True):
            for value in (True,False):
                raw='{"state":"value","value":'+('true'if value else 'false')+'}'
                decoded=self.presence(raw,family='boolean',flag=flag)
                self.assertIs(decoded['value'].value,value);self.assertIs(decoded['value'].original,value)
            for atom in ('"true"','"false"','1','0','null'):
                from ashlar.publication import ResolutionError
                with self.assertRaises((CountStarExecutionError,ResolutionError)):
                    self.presence('{"state":"value","value":'+atom+'}',family='boolean',flag=flag)
if __name__=='__main__':unittest.main()

class PublicSourceBudgetTests(unittest.TestCase):
    def test_whole_original_receipt_uses_independent_bound_without_compiler_budget_change(self):
        import hashlib,json,tarfile
        from pathlib import Path
        from ashlar_host.count_star_execution import _source_receipt
        root=Path(__file__).resolve().parents[1]
        with tarfile.open(root/'docs/helix/04-build/evidence/pack-installed-compiler-20261010/actual.tar.gz')as archive:
            request=json.loads(archive.extractfile('actual/compile-output/archaeology-dating-0.request').read())
            artifact=json.loads(archive.extractfile('actual/compile-output/archaeology-dating-0.response').read())
        with tarfile.open(root/'docs/helix/04-build/evidence/finite-pack-native-20261010-b/evidence.tar.gz')as archive:
            dataset=archive.extractfile('actual/archaeology/public-dataset.json').read()
        checks=[c for o in artifact['obligations']for c in o['parameters'].get('checks',[])if c.get('publicSourceOnly')is True]
        original={'sourceText':request['modules'][0]['documentJson'],'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':checks}
        original_text=json.dumps(original,sort_keys=True,separators=(',',':'),ensure_ascii=True)
        revision='07357eadbda9cf8299b9d72d004cbb77f8091aee'
        raw=json.dumps({'originalRequestText':original_text,'umfRevision':revision,'admitted':True,'originalDatasetReceiptText':dataset.decode()},sort_keys=True,separators=(',',':'),ensure_ascii=True)
        receipt={'originalRequestText':original_text,'originalReceiptText':raw,'receiptSha256':hashlib.sha256(raw.encode()).hexdigest()}
        self.assertGreater(len(raw.encode()),1048576);self.assertLess(len(raw.encode()),2097152)
        base=(CountStarAdmissionConfig(1048576,CountStarSchemaValidation(SCHEMAS,lambda *a:None)),PathCaptureConfig(100,10000,100000),PathDecodeConfig(10000),lambda *a:dict(receipt),revision)
        legacy=CountStarExecutionConfig(*base);selected=CountStarExecutionConfig(*base,maximum_public_source_bytes=2097152)
        self.assertEqual(selected.admission.maximum_artifact_bytes,legacy.admission.maximum_artifact_bytes)
        with self.assertRaises(CountStarExecutionError):_source_receipt(legacy.public_source,revision,request,artifact,checks,legacy.admission.maximum_artifact_bytes)
        accepted=_source_receipt(selected.public_source,revision,request,artifact,checks,selected.maximum_public_source_bytes)
        self.assertEqual(accepted['originalReceiptText'].encode(),raw.encode());self.assertEqual(json.loads(accepted['originalReceiptText'])['originalDatasetReceiptText'].encode(),dataset)
        for invalid in (True,False,0,-1,16777217,2.0,'2097152'):
            with self.assertRaises(CountStarExecutionError):CountStarExecutionConfig(*base,maximum_public_source_bytes=invalid)
    def test_new_field_preserves_legacy_positional_observer_and_none_default(self):
        observer=lambda *a:None
        configuration=CountStarExecutionConfig(CountStarAdmissionConfig(1048576,CountStarSchemaValidation(SCHEMAS,lambda *a:None)),PathCaptureConfig(100,10000,100000),PathDecodeConfig(10000),None,None,observer)
        self.assertIs(configuration.native_observer,observer);self.assertIsNone(configuration.maximum_public_source_bytes)
