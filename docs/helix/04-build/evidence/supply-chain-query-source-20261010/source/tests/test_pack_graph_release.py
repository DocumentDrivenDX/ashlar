import copy,json,tempfile,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from run_pack_publication_weft import admit_pack_inventory,open_pack_reader
from run_pack_graph_release import read_after_stop

class Tests(unittest.TestCase):
    def inventory(self,pack):
        native=SimpleNamespace(SOURCE_SYSTEM='private-original-'+pack+'-fixture',SOURCE_SHA='original-model-sha')
        roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
        namespace='supplychain' if pack=='supply-chain' else pack
        registry=[{'table':'local.'+namespace+'.'+role,'uuid':'00000000-0000-0000-0000-'+str(i+1).zfill(12)}for i,role in enumerate(roles)]
        manifest={'table_versions_json':json.dumps({r['table']:2 for r in registry[:4]}),'schema_revisions_json':json.dumps({native.SOURCE_SYSTEM:native.SOURCE_SHA}),'source_progress_json':json.dumps({native.SOURCE_SYSTEM:{}})}
        return native,manifest,registry
    def test_graph_inventory_requires_complete_source_qualified_native_vector(self):
        for pack in ('archaeology','ecology','medical','supply-chain'):
            n,m,r=self.inventory(pack);a=admit_pack_inventory(pack,n,m,r);self.assertEqual(len(a),4)
            for change in ('role','missing','uuid','version','source','schema'):
                mm=copy.deepcopy(m);rr=copy.deepcopy(r)
                if change=='role':rr[0]['table']='local.other.object_current'
                elif change=='missing':rr.pop()
                elif change=='uuid':rr[1]['uuid']=rr[0]['uuid']
                elif change=='version':v=json.loads(mm['table_versions_json']);v[next(iter(v))]=True;mm['table_versions_json']=json.dumps(v)
                elif change=='source':mm['source_progress_json']='{"other":{}}'
                else:mm['schema_revisions_json']='{"other":"wrong"}'
                with self.subTest(pack=pack,change=change),self.assertRaises(ValueError):admit_pack_inventory(pack,n,mm,rr)
    def test_medical_cannot_be_used_as_unqualified_compiler_reader(self):
        with self.assertRaises(ValueError):
            with open_pack_reader(None,'not-read','medical'):pass
        with self.assertRaises(ValueError):
            with open_pack_reader(None,'not-read','medical',graph_only=1):pass
    def test_cleanup_failure_withholds_provisional_export(self):
        spark=SimpleNamespace(stop=lambda:(_ for _ in ()).throw(RuntimeError('stop failed')))
        with patch('run_pack_graph_release.read_commerce_export',return_value=('release','custody','files')):
            with self.assertRaises(RuntimeError):read_after_stop(spark,lambda:None)
    def test_source_failure_still_closes_spark_and_returns_no_export(self):
        calls=[];spark=SimpleNamespace(stop=lambda:calls.append('stopped'))
        with patch('run_pack_graph_release.read_commerce_export',side_effect=PermissionError('closing source failed')):
            with self.assertRaises(PermissionError):read_after_stop(spark,lambda:None)
        self.assertEqual(calls,['stopped'])
