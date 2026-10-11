"""Public commerce receipt correspondence controls; no producer/native grant."""
from copy import deepcopy
from types import SimpleNamespace
import hashlib,json,unittest
from unittest.mock import patch
import test_host_paths_query as original
from ashlar_host import commerce_source_receipt as owner
from ashlar_host import count_star_query as legacy
from ashlar_host.config import HostError

class SourceReceiptTests(unittest.TestCase):
    def setUp(self):
        f=original.PublicSourceTests();f.setUp()
        self.original=f
    def port(self,bindings=None,maximum=16777216):
        return owner.commerce_public_source_port(model=self.original.opened.model,
            graph=self.original.opened.graph,
            bindings=self.original.opened.bindings if bindings is None else bindings,
            dataset_receipt=self.original.dataset,maximum_bytes=maximum)
    def args(self):
        f=self.original
        return deepcopy((f.request,f.artifact,[{'check':f.check,'rows':[f.row]}]))
    def test_exact_legacy_receipt_bytes_and_whole_unknown_dataset_content(self):
        f=self.original
        expected=original.module._source_port(f.opened,f.dataset,16777216)(*self.args())
        self.assertEqual(self.port()(*self.args()),expected)
        self.assertEqual(legacy._source_port(f.opened,f.dataset,16777216)(*self.args()),expected)
        value=json.loads(f.dataset);value['unknownExtension']={'opaque':['original',None,False]}
        f.dataset=json.dumps(value).encode()
        result=self.port()(*self.args())
        self.assertEqual(json.loads(result['originalReceiptText'])['originalDatasetReceiptText'],f.dataset.decode())
        self.assertEqual(hashlib.sha256(result['originalReceiptText'].encode()).hexdigest(),result['receiptSha256'])
    def test_borrowed_bindings_and_opened_mutation_cannot_change_captured_originals(self):
        f=self.original;bindings=deepcopy(f.opened.bindings)
        public=self.port(bindings);delegated=legacy._source_port(f.opened,f.dataset,16777216)
        expected=public(*self.args());bindings.clear()
        f.opened.model=b'changed';f.opened.graph=b'changed';f.opened.bindings={}
        self.assertEqual(public(*self.args()),expected)
        self.assertEqual(delegated(*self.args()),expected)
    def test_complete_native_projection_and_original_scalar_refuse_drift(self):
        public=self.port();args=self.args();expected=public(*args)
        for target,key,value in (('check','propertyId','14'),('row','extracted_token','11'),('row','id','6'),('row','props_json','{}')):
            changed=deepcopy(args)
            if target=='check':changed[2][0]['check'][key]=value
            else:changed[2][0]['rows'][0][key]=value
            with self.assertRaises(HostError):public(*changed)
        for rows in ([],[self.original.row,self.original.row]):
            changed=self.args();changed[2][0]['rows']=rows
            with self.assertRaises(HostError):public(*changed)
        args[2][0]['rows'][0]['id']='later'
        self.assertNotIn('later',expected['originalReceiptText'])
    def test_bounds_and_exact_carriers_precede_callbacks(self):
        for maximum in (True,0,16777217,1.0):
            with self.assertRaises(HostError):self.port(maximum=maximum)
        with self.assertRaises(HostError):self.port(maximum=10)
        touched=[]
        class Forged(dict):
            def items(self):touched.append('items');return super().items()
        with self.assertRaises(HostError):self.port(Forged(self.original.opened.bindings))
        args=list(self.args());args[0]=Forged(args[0])
        with self.assertRaises(HostError):self.port()(*args)
        self.assertEqual(touched,[])
        with patch.object(owner.json,'dumps',side_effect=AssertionError('encoder reached')):
            with self.assertRaises(HostError):owner.encode_bounded_commerce_json({'x':'\x7f'*1000},1100)
    def test_changed_whole_receipt_and_invalid_public_validity_refuse(self):
        f=self.original
        for key,value in (('sourceSha256','changed'),('graphSha256','changed'),('umfRevision','changed')):
            d=json.loads(f.dataset);d[key]=value
            with self.assertRaises(HostError):owner.commerce_public_source_port(model=f.opened.model,graph=f.opened.graph,bindings=f.opened.bindings,dataset_receipt=json.dumps(d).encode(),maximum_bytes=16777216)
        d=json.loads(f.dataset);d['receipt']['datasetValidation']['complete']=False
        f.dataset=json.dumps(d).encode()
        with self.assertRaises(HostError):self.port()
    def test_numeric_validation_flags_and_malformed_original_shapes_refuse(self):
        f=self.original
        for validation in ({'valid':1,'complete':1},{'valid':True,'complete':1},[],None):
            d=json.loads(f.dataset);d['receipt']['datasetValidation']=validation
            with self.assertRaises(HostError):owner.commerce_public_source_port(model=f.opened.model,graph=f.opened.graph,bindings=f.opened.bindings,dataset_receipt=json.dumps(d).encode(),maximum_bytes=16777216)
        for dataset in (b'[]',b'{"receipt":[]}'):
            with self.assertRaises(HostError):owner.commerce_public_source_port(model=f.opened.model,graph=f.opened.graph,bindings=f.opened.bindings,dataset_receipt=dataset,maximum_bytes=16777216)
        for binding in ({}, {'properties':[None],'entities':[]}):
            with self.assertRaises(HostError):self.port(binding)
        for args in (({}, {}, []), (f.request, f.artifact, [None])):
            with self.assertRaises(HostError):self.port()(*args)

if __name__=='__main__':unittest.main()
