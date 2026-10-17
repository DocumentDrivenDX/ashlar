"""Finite original object bags and inert owning-port lifecycle controls, not native evidence."""
import copy,json,hashlib,unittest
from types import SimpleNamespace
from dataclasses import replace
from ashlar_host.delta_custody import encoded
from ashlar_host.medical_query_requests import prepare_medical_queries,require_medical_case_bag
from ashlar_host.medical_query_source import MedicalQuerySource,medical_integer_projection
from ashlar_host.medical_query_batch import MedicalQueryBatchBuffer
import test_medical_singleton_query as fixtures
import test_implicit_arithmetic as arithmetic
import test_run_commerce_path_weft as ports
from ashlar_host.count_star_execution import execute_commerce_implicit_arithmetic

class Tests(unittest.TestCase):
    def prepared(self):
        b,m,r,a=fixtures.MedicalSingletonTests().fixture()
        return b,prepare_medical_queries(fixtures.MODEL,fixtures.GRAPH,fixtures.BASELINE,b,m,r,a,('all-objects',))
    def test_all_fifty_one_original_rows_null_boolean_and_complete_inventory(self):
        bindings,cases=self.prepared();actual=[]
        for case in cases:
            evidence={'native_result':{'rows':case['oracle']['rows']}}
            actual.extend(require_medical_case_bag(fixtures.MODEL,fixtures.GRAPH,bindings,case,{},evidence))
            changed=copy.deepcopy(evidence);changed['native_result']['rows']*=2
            with self.assertRaises(ValueError):require_medical_case_bag(fixtures.MODEL,fixtures.GRAPH,bindings,case,{},changed)
        self.assertEqual(len(actual),51);self.assertEqual(sum(v is None for o in actual for v in o['values'].values()),8)
        case=next(c for c in cases if c['id']=='all-objects-patients')
        for cell in ('{"state":"absent"}','{"state":"value","value":"False"}'):
            changed=copy.deepcopy(case['oracle']['rows']);changed[0][1]=cell
            with self.assertRaises(ValueError):require_medical_case_bag(fixtures.MODEL,fixtures.GRAPH,bindings,case,{}, {'native_result':{'rows':changed}})
        from ashlar_host.medical_singleton_query import MedicalSingletonBuffer
        buffers=tuple((c['id'],MedicalSingletonBuffer(b'{}',b'{}',b'{}','{}'),'[]')for c in cases)
        MedicalQueryBatchBuffer(buffers,'{}')
        with self.assertRaises(ValueError):MedicalQueryBatchBuffer(buffers[:-1],'{}')
    def test_complete_source_population_and_property_pair_refusals(self):
        bindings,cases=self.prepared();case=cases[-1];request=case['request'];binding=json.loads(request['target']['bindingJson']);record=next(r for r in binding['records']if r['logical']['element']=='resource_references');field=record['properties'][0]['logical']
        home=record['properties'][0]['home'];source={'publicSourceOnly':True,'field':field,'record':record['logical'],'propertyId':home['propertyId'],'logicalType':{'family':'integer','facets':{},'nullable':False},'sql':'source','failureCode':'WFT-NUMERIC-DOMAIN'}
        rep={'representabilityOnly':True,'field':field,'record':record['logical'],'sql':'representation','failureCode':'WFT-CAPABILITY'}
        artifact={'obligations':[{'parameters':{'checks':[source,rep]}}],'modelPins':binding['modelPins'],'bindingSha256':request['target']['bindingSha256'],'logicalPlan':{'source':{'record':record['logical']},'outputs':[{'expression':{'op':'field','identity':field}}]},'columns':[{'representation':{'logicalType':source['logicalType']}}]}
        owner=object.__new__(MedicalQuerySource);owner.renew=lambda:None;owner.originals=(fixtures.MODEL,fixtures.GRAPH,fixtures.BASELINE);owner.bindings=encoded(bindings).encode();owner.expected=encoded(binding);owner.maximum=1048576;owner.admission=SimpleNamespace(batch=SimpleNamespace(feed=record['sourceSystem']),revalidation=b'{}')
        rows=medical_integer_projection(*owner.originals,bindings,record['sourceSystem'],source);self.assertEqual(len(rows),17)
        owner(request,artifact,[{'check':source,'rows':rows}])
        for bad in (rows[:-1],rows+[rows[0]],[dict(rows[0],id='9999')]+rows[1:]):
            with self.assertRaises(ValueError):owner(request,artifact,[{'check':source,'rows':bad}])
        for key in ('property','pair'):
            changed=copy.deepcopy(artifact)
            if key=='property':changed['obligations'][0]['parameters']['checks'][0]['propertyId']='9999'
            else:changed['obligations'][0]['parameters']['checks'].pop()
            with self.assertRaises(ValueError):owner(request,changed,[{'check':source,'rows':rows}])
        with self.assertRaises(PermissionError):MedicalQuerySource(object(),object(),object(),100)
    def test_replaced_owner_selection_refuses_before_delegate(self):
        from ashlar_host.driver import NativeDriver
        from ashlar_host.original_medical_publication import MedicalAdmission
        from ashlar_host.medical_producer import HeldMedicalDataset
        from ashlar_host.medical_read_sources import MedicalReadSourceFactory
        owner=object.__new__(MedicalQuerySource);driver=object.__new__(NativeDriver);calls=[]
        driver.require=lambda context:calls.append('delegate')
        owner.driver=driver;owner.context=object();owner.admission=object.__new__(MedicalAdmission)
        owner.producer=object.__new__(HeldMedicalDataset);owner.factory=object.__new__(MedicalReadSourceFactory)
        from ashlar.pins import PinVector
        owner.vector=PinVector('local','manifest','original','0'*64,{'a.b.c':('uuid',1)});owner.originals=(b'm',b'g',b'b');owner.bindings=b'{}';owner.expected='{}';owner.maximum=100
        owner._selection=(driver,owner.context,owner.admission,owner.producer,owner.factory,owner.vector,owner.originals,owner.bindings,owner.expected,owner.maximum,owner)
        owner._require_selection()
        for name,replacement in (('driver',SimpleNamespace(require=lambda c:calls.append('duck'))),('context',object()),('expected','changed')):
            original=getattr(owner,name);setattr(owner,name,replacement)
            with self.subTest(name=name),self.assertRaises(PermissionError):owner.renew()
            self.assertEqual(calls,[]);setattr(owner,name,original)
        with self.assertRaises(AttributeError):owner._selection=()

    def test_public_source_refusal_prevents_representation_and_user_sql(self):
        controls=arithmetic.ConfigurationTests()
        q,r=copy.deepcopy(arithmetic.PAIRS['partial-return']);provider=ports.Provider(r,[])
        opened=SimpleNamespace(provider=provider,context=provider.context,model=q['modules'][0]['documentJson'].encode(),graph=b'fixture',original_native_files={'fixture':'fixed'},native_files=lambda:{'fixture':'fixed'})
        representations=[c['sql']for o in r['obligations']for c in o['parameters'].get('checks',[])if c.get('representabilityOnly')is True];self.assertTrue(representations)
        def source(*args):
            called=[c[1]for c in provider.calls if isinstance(c,tuple)]
            self.assertFalse(any(sql in called for sql in representations));raise PermissionError('source refusal')
        config=replace(controls.execution(),public_source=source,public_source_revision='inert')
        with self.assertRaises(PermissionError):execute_commerce_implicit_arithmetic(opened,q,r,copy.deepcopy(r),config=config,original_oracle=lambda *args:{'rows':[],'witnesses':{},'scope':'inert'})
        called=[c[1]for c in provider.calls if isinstance(c,tuple)]
        self.assertNotIn(r['sql'],called);self.assertFalse(provider.active)
        provider.calls.clear()
        def accepted(request,artifact,checks):
            called=[c[1]for c in provider.calls if isinstance(c,tuple)]
            self.assertFalse(any(sql in called for sql in representations));provider.calls.append('source-owned')
            raw=json.dumps({'sourceText':request['modules'][0]['documentJson'],'modelPins':artifact['modelPins'],'bindingSha256':artifact['bindingSha256'],'checks':checks})
            receipt=json.dumps({'originalRequestText':raw,'umfRevision':'inert','admitted':True})
            return {'originalRequestText':raw,'originalReceiptText':receipt,'receiptSha256':hashlib.sha256(receipt.encode()).hexdigest()}
        config=replace(config,public_source=accepted)
        execute_commerce_implicit_arithmetic(opened,q,r,copy.deepcopy(r),config=config,original_oracle=lambda *args:{'rows':[],'witnesses':{},'scope':'inert'})
        position=provider.calls.index('source-owned')
        self.assertTrue(all(i>position for i,c in enumerate(provider.calls)if isinstance(c,tuple)and c[1]in representations))

if __name__=='__main__':unittest.main()
