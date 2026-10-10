import json
from types import SimpleNamespace
import unittest
from unittest.mock import patch
from pathlib import Path
from ashlar.weft_distribution import DistributionPaths
import indexed_weft_commerce as tool


class IndexedCommerceTests(unittest.TestCase):
    def setUp(self):
        self.paths = DistributionPaths(Path('index'), Path('installation'))
        self.opened = SimpleNamespace(graph=json.dumps({'objects':[{'type':{'element':'products'},'values':{'products.id':'p1'}}]}).encode(), model=b'model', bindings={}, manifest={}, original_report={'table_registry':[]}, aliases={}, context=object(), original_native_files={'file':'digest'}, native_files=lambda:{'file':'digest'}, provider=SimpleNamespace(closed_interval_custody=lambda context:{'closed':True}))
        self.request = {'target':{'bindingJson':'{}'}}
        self.response = json.dumps({'status':'compiled','backend':{'backendId':'ashlar.databricks','backendVersion':'0.1.0-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql-candidate'}}).encode()+b'\n'

    def test_public_indexed_compile_exact_sql_and_all_guards(self):
        calls=[]
        def compile(paths, raw): calls.append(json.loads(raw)); return json.dumps({'status':'compiled','backend':{'backendId':'ashlar.databricks','backendVersion':'0.1.0-candidate','interfaceVersion':'weft-backend/0.2.0','targetProfile':'dbsql-candidate'}}).encode()+b'\n'
        def execute(provider, request, artifact, context):
            return {'rows':[{'id':'p1'}] if request['sql']==tool.CASES[0][1] else [{'n':'1'}], 'integrity':[{'verified':True}]}
        def request(sql,*args,**kwargs): return {**self.request,'sql':sql}
        with patch.object(tool,'compiler_request',side_effect=request),patch.object(tool,'compile_distribution',side_effect=compile),patch.object(tool,'execute_guarded',side_effect=execute) as host:
            result=tool.run_indexed_queries(self.opened,self.paths)
        self.assertEqual([r['sql'] for r in calls],[c[1] for c in tool.CASES]);self.assertEqual(host.call_count,2)
        self.assertTrue(all(q['closed_interval']['closed'] for q in result['queries']))

    def test_unknown_obligation_and_closing_failure_never_return(self):
        for error in ('Unknown obligation','Closing ACK failed'):
            with patch.object(tool,'compiler_request',return_value=self.request),patch.object(tool,'compile_distribution',return_value=self.response),patch.object(tool,'execute_guarded',side_effect=ValueError(error)):
                with self.assertRaises(ValueError):tool.run_indexed_queries(self.opened,self.paths)

    def test_oracle_bag_and_native_closure_refuse(self):
        with patch.object(tool,'compiler_request',return_value=self.request),patch.object(tool,'compile_distribution',return_value=self.response),patch.object(tool,'execute_guarded',return_value={'rows':[{'id':'wrong'}]}):
            with self.assertRaises(ValueError):tool.run_indexed_queries(self.opened,self.paths)
        self.opened.native_files=lambda:{'file':'changed'}
        with patch.object(tool,'compile_distribution',side_effect=AssertionError('compile before custody')):
            with self.assertRaises(ValueError):tool.run_indexed_queries(self.opened,self.paths)

    def test_response_framing_and_duplicate_keys_refuse(self):
        for raw in (b'{}',b'{}\n{}\n',b'{"a":1,"a":2}\n',b'{"a":NaN}\n'):
            with self.assertRaises(ValueError):tool.decode_compile_artifact(raw)


if __name__=='__main__':unittest.main()
