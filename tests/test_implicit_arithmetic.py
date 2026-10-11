"""Closed 0.4.2 host controls; no compiler installation or native qualification.

Schema bytes are original owning reviewed artifacts. Inert callback controls
exercise host selection only; actual offline validator and public compiler pairs
have separate tests/evidence.
"""
import copy, hashlib, json, unittest, zlib
from dataclasses import FrozenInstanceError
from pathlib import Path
from unittest.mock import patch
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.path_capture import PathCaptureConfig
from ashlar_host.implicit_arithmetic_profile import (PROFILE, ImplicitArithmeticProfileError,
    ImplicitArithmeticSchemaValidation, ImplicitArithmeticAdmissionConfig,
    ImplicitArithmeticExecutionConfig)
from ashlar_host.count_star_admission import (CountStarPlanError, admit_implicit_arithmetic_artifact,
    admit_count_star_artifact, CountStarAdmissionConfig, CountStarSchemaValidation)
from ashlar_host.count_star_execution import (execute_commerce_implicit_arithmetic,
    execute_commerce_count_star, CountStarExecutionConfig, CountStarExecutionError)
from test_count_star_admission import SCHEMAS as OLD_SCHEMAS

compressed=(Path(__file__).parent/'fixtures/weft_implicit_arithmetic_host.json.zlib').read_bytes()
if len(compressed)>262144:raise ValueError('Compressed original fixture bound')
expand=zlib.decompressobj();raw=expand.decompress(compressed,2097153)
if len(raw)>2097152 or not expand.eof or expand.unused_data or expand.unconsumed_tail:raise ValueError('Whole original fixture bound')
if hashlib.sha256(raw).hexdigest()!='f04dca9af0c14daf3609f07a27848c0342eef9677270be369cb9b3de6408fd87':raise ValueError('Original fixture pin')
fixture=json.loads(raw);SCHEMAS={name:text.encode()for name,text in fixture['schemas'].items()}
PAIRS={pair['id']:(json.loads(pair['requestText']),json.loads(pair['responseText']))for pair in fixture['pairs']}

class ConfigurationTests(unittest.TestCase):
    def config(self):
        return ImplicitArithmeticAdmissionConfig(16777216,ImplicitArithmeticSchemaValidation(SCHEMAS,lambda *args:None))
    def execution(self):
        return ImplicitArithmeticExecutionConfig(self.config(),PathCaptureConfig(100,10000,100000),PathDecodeConfig(10000),None,None)
    def test_schema_port_copies_original_inventory_and_refuses_changed_bytes(self):
        borrowed=dict(SCHEMAS);port=ImplicitArithmeticSchemaValidation(borrowed,lambda *args:None)
        borrowed.clear();self.assertEqual(dict(port.schemas),SCHEMAS)
        with self.assertRaises(TypeError):port.schemas['new']=b'x'
        bad=dict(SCHEMAS);key=next(iter(bad));bad[key]+=b' '
        with self.assertRaises(ImplicitArithmeticProfileError):ImplicitArithmeticSchemaValidation(bad,lambda *args:None)
        with self.assertRaises(ImplicitArithmeticProfileError):ImplicitArithmeticSchemaValidation(OLD_SCHEMAS,lambda *args:None)
    def test_exact_primitive_carriers_refuse_without_executable_equality(self):
        touched=[]
        class Text(str):
            def __eq__(self,other):touched.append('equality');return True
            __hash__=str.__hash__
        class Mapping(dict):
            def items(self):touched.append('items');return super().items()
        for schemas in (Mapping(SCHEMAS),{Text(k):v for k,v in SCHEMAS.items()}):
            with self.assertRaises(ImplicitArithmeticProfileError):ImplicitArithmeticSchemaValidation(schemas,lambda *args:None)
        with self.assertRaises(ImplicitArithmeticProfileError):ImplicitArithmeticAdmissionConfig(1000,self.config().schema_validation,Text('implicit-arithmetic'))
        self.assertEqual(touched,[])
    def test_finite_separate_typed_configuration_and_frozen_profile(self):
        for bound in (True,0,16777217,1.0):
            with self.assertRaises(ImplicitArithmeticProfileError):ImplicitArithmeticAdmissionConfig(bound,self.config().schema_validation)
        for profile in ('count-star','0.4.2',PROFILE):
            with self.assertRaises(ImplicitArithmeticProfileError):ImplicitArithmeticAdmissionConfig(1000,self.config().schema_validation,profile)
        with self.assertRaises(FrozenInstanceError):PROFILE.name='changed'
        config=self.execution()
        for bound in (True,0,16777217):
            with self.assertRaises(ImplicitArithmeticProfileError):ImplicitArithmeticExecutionConfig(config.admission,config.capture,config.decoder,None,None,maximum_public_source_bytes=bound)
    def test_entrypoint_profile_mismatch_precedes_admission_or_native_callbacks(self):
        old=CountStarAdmissionConfig(1000,CountStarSchemaValidation(OLD_SCHEMAS,lambda *args:None))
        for call,config in ((admit_count_star_artifact,self.config()),(admit_implicit_arithmetic_artifact,old)):
            with self.assertRaises(CountStarPlanError):call({}, {}, {},config=config)
        old_execution=CountStarExecutionConfig(old,PathCaptureConfig(1,1,1),PathDecodeConfig(1),None,None)
        for call,config in ((execute_commerce_count_star,self.execution()),(execute_commerce_implicit_arithmetic,old_execution)):
            with self.assertRaises(CountStarExecutionError):call(None,{}, {}, {},config=config,original_oracle=lambda *args:None)
    def test_legacy_default_configuration_remains_separate(self):
        old=CountStarAdmissionConfig(1000,CountStarSchemaValidation(OLD_SCHEMAS,lambda *args:None))
        old_execution=CountStarExecutionConfig(old,PathCaptureConfig(1,1,1),PathDecodeConfig(1),None,None)
        self.assertEqual(old.profile,'count-star');self.assertIsNone(old_execution.native_observer)
        self.assertIsNone(old_execution.maximum_public_source_bytes)
        self.assertIsNot(type(old_execution),type(self.execution()))

    def test_real_public_four_original_compiles_and_manifest_correspond(self):
        manifest=json.loads(fixture['manifestText'])
        self.assertEqual({c['id'] for c in manifest['capabilities']},PROFILE.capabilities)
        self.assertEqual(len(PAIRS),4)
        for q,r in PAIRS.values():
            admitted=admit_implicit_arithmetic_artifact(q,r,copy.deepcopy(r),config=self.config())
            self.assertEqual([c['outputName'] for c in admitted.artifact['columns']],
                             [o['name'] for o in r['logicalPlan']['outputs']])
            with self.assertRaises(TypeError):admitted.artifact['sql']='changed'
        q,r=PAIRS['partial-return']
        self.assertEqual([c['outputName'] for c in r['columns']][:2],['quantity','quantity'])
        self.assertEqual(len({c['carrierName'] for c in r['columns']}),3)

    def test_original_pair_markers_and_recompile_type_correspondence_refuse(self):
        q,r=PAIRS['partial-return']
        for member,value in (('interfaceVersion','weft-compile/0.4.1'),('dialect','weft-sql/0.4.1')):
            changed=copy.deepcopy(r);changed[member]=value
            with self.assertRaises(CountStarPlanError):admit_implicit_arithmetic_artifact(q,changed,copy.deepcopy(changed),config=self.config())
        other=copy.deepcopy(r);other['columns'][0]['position']=1.0
        with self.assertRaises(CountStarPlanError):admit_implicit_arithmetic_artifact(q,r,other,config=self.config())

    def test_original_arithmetic_span_bounds_and_computed_collision_refuse(self):
        q,r=PAIRS['partial-return']
        for span in ({'start':True,'end':61},{'start':61,'end':29},{'start':29,'end':len(q['sql'].encode())+1}):
            changed=copy.deepcopy(r)
            changed['logicalPlan']['outputs'][2]['expression']['expression']['span']=span
            with self.assertRaises(CountStarPlanError):admit_implicit_arithmetic_artifact(q,changed,copy.deepcopy(changed),config=self.config())
        changed=copy.deepcopy(r);changed['logicalPlan']['outputs'][2]['name']='quantity'
        changed['columns'][2]['outputName']='quantity'
        with self.assertRaises(CountStarPlanError):admit_implicit_arithmetic_artifact(q,changed,copy.deepcopy(changed),config=self.config())

    def test_utf8_span_boundary_and_unknown_capability_refuse(self):
        from ashlar_host.count_star_admission import _admit_implicit_labels
        q,r=copy.deepcopy(PAIRS['partial-return'])
        # Literal metadata test only: the compiler owns SQL interpretation.
        plan=copy.deepcopy(r['logicalPlan']);expression=plan['outputs'][2]['expression']['expression']
        text=q['sql']+' é';start=len(q['sql'].encode())+1
        expression['span']={'start':start,'end':start+2};plan['outputs'][2]['name']='é'
        _admit_implicit_labels({'sql':text},plan)
        expression['span']['start']=start+1
        with self.assertRaises(UnicodeError):_admit_implicit_labels({'sql':text},plan)
        r['logicalPlan']['requiredCapabilities'].append('invented.capability')
        with self.assertRaises(CountStarPlanError):admit_implicit_arithmetic_artifact(q,r,copy.deepcopy(r),config=self.config())

    def test_new_execution_requires_actual_source_before_native_or_oracle_callbacks(self):
        from types import SimpleNamespace
        q,r=copy.deepcopy(PAIRS['partial-return']);calls=[]
        opened=SimpleNamespace()
        with self.assertRaises(CountStarExecutionError):
            execute_commerce_implicit_arithmetic(opened,q,r,copy.deepcopy(r),
                config=self.execution(),original_oracle=lambda *args:calls.append('oracle'))
        self.assertEqual(calls,[])

    def test_real_pairs_execute_source_port_before_arithmetic_and_close_fence(self):
        from types import SimpleNamespace
        from dataclasses import replace
        import test_run_commerce_path_weft as ports
        for original_q,original_r in PAIRS.values():
            q,r=copy.deepcopy((original_q,original_r));provider=ports.Provider(r,[])
            opened=SimpleNamespace(provider=provider,context=provider.context,
                model=q['modules'][0]['documentJson'].encode(),graph=b'fixture',
                original_native_files={'fixture':'fixed'},native_files=lambda:{'fixture':'fixed'})
            # Inert protocol control, never public UMF or native qualification.
            def source(request,artifact,checks):
                numeric=next(o for o in r['obligations'] if o['id']=='ashlar.arithmetic.exact')
                called=[c[1] for c in provider.calls if isinstance(c,tuple)]
                self.assertFalse(any(c['sql'] in called for c in numeric['parameters']['checks']))
                raw=json.dumps({'sourceText':request['modules'][0]['documentJson'],
                    'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':checks})
                request['sql']='borrowed mutation';artifact['sql']='borrowed mutation';checks.clear()
                receipt=json.dumps({'originalRequestText':raw,'umfRevision':'inert-source','admitted':True})
                return {'originalRequestText':raw,'originalReceiptText':receipt,
                    'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
            config=replace(self.execution(),public_source=source,public_source_revision='inert-source')
            oracle=lambda *args:{'rows':[],'witnesses':{},'scope':'inert owning provider only'}
            result=execute_commerce_implicit_arithmetic(opened,q,r,copy.deepcopy(r),config=config,original_oracle=oracle)
            needs_source=any(c.get('publicSourceOnly') is True for o in r['obligations'] for c in o['parameters'].get('checks',[]))
            self.assertEqual(result['public_source'] is not None,needs_source);self.assertFalse(provider.active)
            self.assertEqual([c for c in provider.calls if isinstance(c,tuple)][-1][1],r['sql'])
            provider.calls.clear()
            values=iter(['opening','changed']);provider.resolve=lambda context:next(values)
            with self.assertRaises(CountStarExecutionError):
                execute_commerce_implicit_arithmetic(opened,q,r,copy.deepcopy(r),config=config,original_oracle=oracle)
            self.assertFalse(provider.active)

if __name__=='__main__':unittest.main()
