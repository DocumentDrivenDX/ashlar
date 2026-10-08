import unittest
from types import SimpleNamespace
from ashlar.publication import ResolutionError
from databricks_transport import CompiledWeftTransport
from durable_sql import SQLCustodyError
from weft_execution_guard import guarded_statement
from weft_native_profile import EXPECTED_PROFILE


class GuardTests(unittest.TestCase):
    def test_exact_compiler_query_and_parameters_use_guarded_execution(self):
        calls=[]
        client=SimpleNamespace(records=[])
        def execute(label,sql,parameters):
            calls.append((label,sql,parameters))
            client.records.append({'response':{'status':{'state':'SUCCEEDED'},'manifest':{'total_row_count':0,'schema':{'columns':[{'position':0,'name':'value','type_text':'STRING'}]}},'result':{'data_array':[]}}})
        client.sql=execute
        query='WITH x AS (SELECT :p1 AS value) SELECT value FROM x WHERE false;'
        artifact={'sql':query,'obligations':[{'id':'ashlar.nativeProfile','parameters':EXPECTED_PROFILE}]}
        executor=CompiledWeftTransport(SimpleNamespace(read_client=client),artifact)
        result=executor.query(query,{'p1':"雪'; DROP TABLE x;"})
        self.assertEqual(result.columns,(('value','STRING'),))
        self.assertEqual(list(result.rows),[])
        self.assertEqual(calls,[('weft-compiled-read',guarded_statement(query,EXPECTED_PROFILE),[{'name':'p1','type':'STRING','value':"雪'; DROP TABLE x;"}])])
        self.assertNotIn("雪'; DROP",calls[0][1])

    def test_missing_duplicate_or_changed_profile_refuses_before_sql(self):
        for profiles in [[],[EXPECTED_PROFILE,EXPECTED_PROFILE],[dict(EXPECTED_PROFILE,engineVersion='new')]]:
            with self.assertRaises(SQLCustodyError):
                CompiledWeftTransport(None,{'sql':'SELECT 1','obligations':[{'id':'ashlar.nativeProfile','parameters':p} for p in profiles]})
        with self.assertRaises(ResolutionError):guarded_statement('SELECT 1',{})


if __name__=='__main__':unittest.main()
