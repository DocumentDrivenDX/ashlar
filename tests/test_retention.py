import unittest
from ashlar.retention import RetentionError,validate_predictive_optimization_disabled
class RetentionTests(unittest.TestCase):
    def test_explicit_or_effective_inherited_disabled_observations(self):
        self.assertIsNone(validate_predictive_optimization_disabled('DISABLE',{'value':'DISABLE'}))
        self.assertIsNone(validate_predictive_optimization_disabled('INHERIT',{'value':'DISABLE','inherited_from_name':'original-schema'}))
    def test_enabled_absent_unknown_or_contradictory_state_refuses(self):
        for setting,flag in [('ENABLE',{'value':'DISABLE'}),('DISABLE',{'value':'ENABLE'}),('INHERIT',{'value':'ENABLE','inherited_from_name':'metastore'}),('DISABLE',None),(None,{'value':'DISABLE'}),('FUTURE',{'value':'DISABLE'}),('INHERIT',{'value':'DISABLE'}),('DISABLE',{'value':'FUTURE'}),('DISABLE',{})]:
            with self.assertRaises(RetentionError):validate_predictive_optimization_disabled(setting,flag)
if __name__=='__main__':unittest.main()
