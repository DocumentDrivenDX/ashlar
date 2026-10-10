"""Held provider controls; fixture frames never claim native SQL qualification."""
import copy,unittest
from types import SimpleNamespace
from test_count_star_admission import PAIRS,SCHEMAS
import test_run_commerce_path_weft as ports
from ashlar.weft_path_decode import PathDecodeConfig
from ashlar_host.count_star_admission import CountStarAdmissionConfig,CountStarSchemaValidation
from ashlar_host.count_star_execution import CountStarExecutionConfig,CountStarExecutionError,execute_commerce_count_star
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
if __name__=='__main__':unittest.main()
