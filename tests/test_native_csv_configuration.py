import copy
from pathlib import Path
import unittest
from uuid import UUID
from generated_carriers import load_generated_carriers
from native_csv_configuration import installation_namespace
from ashlar.publisher import PublicationError

ROOT=Path(__file__).resolve().parents[1]
class ConfigurationTests(unittest.TestCase):
    def proof(self,namespace='customer.graph'):
        generated,digest=load_generated_carriers(ROOT)
        types={'string':'STRING','long':'BIGINT','boolean':'BOOLEAN','timestamp':'TIMESTAMP'}
        return {'state':'installed','namespace':namespace,'authenticated_owner':'owner','generated_carriers_sha256':digest,
            'umf_generator':generated['generator'],'model_inputs':generated['inputs'],
            'tables':{namespace+'.'+role:{'uuid':str(UUID(int=i+1)),
                'columns':[[f['name'],types[f['deltaType']]] for f in carrier['columns']],
                'detail':{'originalUnknownObservation':{'opaque':'18446744073709551615'}}}
                for i,(role,carrier) in enumerate(generated['carriers'].items())}}
    def test_caller_namespace_has_complete_original_model_custody_without_mutation(self):
        p=self.proof();before=copy.deepcopy(p)
        self.assertEqual(installation_namespace(p,ROOT),'customer.graph');self.assertEqual(p,before)
    def test_cross_namespace_missing_carriers_wrong_columns_and_stale_model_refuse(self):
        for mode in ('unsafe','missing','foreign','columns','duplicate_uuid','generator','digest','model','owner'):
            p=self.proof();names=list(p['tables'])
            if mode=='unsafe':p['namespace']='customer.graph; DROP TABLE x'
            if mode=='missing':p['tables'].pop(names[0])
            if mode=='foreign':p['tables']['other.graph.object_current']=p['tables'].pop(names[0])
            if mode=='columns':p['tables'][names[0]]['columns'][0][1]='DOUBLE'
            if mode=='duplicate_uuid':p['tables'][names[1]]['uuid']=p['tables'][names[0]]['uuid']
            if mode=='generator':p['umf_generator']['revision']='f'*40
            if mode=='digest':p['generated_carriers_sha256']='f'*64
            if mode=='model':p['model_inputs'].pop(next(iter(p['model_inputs'])))
            if mode=='owner':p['authenticated_owner']=''
            with self.subTest(mode=mode),self.assertRaises(PublicationError):installation_namespace(p,ROOT)

if __name__=='__main__':unittest.main()
