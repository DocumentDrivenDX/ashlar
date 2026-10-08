import copy,json,unittest
from ashlar.native import SQLResult
from ashlar.publication import ResolutionError
from weft_native_profile import EXPECTED_PROFILE,EXPECTED_ENGINE,verify_observations

class ProfileTests(unittest.TestCase):
    def test_exact_observations_and_no_profile_fallback(self):
        engine=SQLResult([{'__weft_warehouse':json.dumps(EXPECTED_ENGINE)}],(('__weft_warehouse','STRING'),))
        ansi=SQLResult([{'key':'ANSI_MODE','value':'true'}],(('key','STRING'),('value','STRING')))
        self.assertEqual(verify_observations(EXPECTED_PROFILE,engine,ansi),EXPECTED_ENGINE)
        for member in ('dbsql_version','u_build_hash','r_build_hash'):
            drift=dict(EXPECTED_ENGINE);drift[member]='different'
            with self.assertRaises(ResolutionError):verify_observations(EXPECTED_PROFILE,SQLResult([{'__weft_warehouse':json.dumps(drift)}],engine.columns),ansi)
        for wrong in [SQLResult([],ansi.columns),SQLResult([{'key':'ANSI_MODE','value':'false'}],ansi.columns),SQLResult([{'key':'ANSI_MODE','value':True}],ansi.columns)]:
            with self.assertRaises(ResolutionError):verify_observations(EXPECTED_PROFILE,engine,wrong)
        duplicate=SQLResult([{'__weft_warehouse':json.dumps(EXPECTED_ENGINE)[:-1]+',"dbsql_version":"2026.39"}'}],engine.columns)
        with self.assertRaises(ResolutionError):verify_observations(EXPECTED_PROFILE,duplicate,ansi)
        drift=copy.deepcopy(EXPECTED_PROFILE);drift['settings']['ansi_mode']=False
        with self.assertRaises(ResolutionError):verify_observations(drift,engine,ansi)
