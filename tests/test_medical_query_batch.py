# SPDX-License-Identifier: MIT OR Apache-2.0
"""Finite original medical bags; inert decoder/native seams are not engine evidence."""
from collections import Counter
import json,unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from ashlar.weft_path_decode import DecodedPaths,PathItem
from ashlar.manifest import manifest_pin_vector
from ashlar_host.delta_custody import encoded
from ashlar_host.medical_query_requests import prepare_medical_queries,require_medical_case_bag
from ashlar_host.medical_query_batch import MedicalQueryBatch,MedicalQueryBatchBuffer,MedicalBatchQueryProvider
from ashlar_host.medical_singleton_query import MedicalSingletonBuffer
from ashlar.weft_implicit_arithmetic_distribution import ImplicitArithmeticDistributionPaths
import test_medical_singleton_query as singleton_controls
MODEL,GRAPH,BASELINE=singleton_controls.MODEL,singleton_controls.GRAPH,singleton_controls.BASELINE

class Tests(unittest.TestCase):
    def prepared(self):
        bindings,manifest,registry,aliases=singleton_controls.MedicalSingletonTests().fixture()
        return bindings,prepare_medical_queries(MODEL,GRAPH,BASELINE,bindings,manifest,registry,aliases,('singleton','two-hop'))
    def test_complete_original_full_row_and_twenty_edge_occurrences_with_refusals(self):
        bindings,cases=self.prepared();self.assertEqual(len(cases),6)
        singleton=cases[0];evidence={'native_result':{'rows':singleton['oracle']['rows']}}
        self.assertEqual(require_medical_case_bag(MODEL,GRAPH,bindings,singleton,{},evidence),singleton['oracle']['witnesses']['original_rows'])
        wrong=json.loads(encoded(evidence));wrong['native_result']['rows'][0][-1]='changed'
        with self.assertRaises(ValueError):require_medical_case_bag(MODEL,GRAPH,bindings,singleton,{},wrong)
        def decode(rep,cell,**kwargs):
            value=json.loads(cell)
            return DecodedPaths(cell.encode(),'value',tuple(PathItem(tuple(i['intermediate']),tuple(i['terminal']),tuple(i['edges']))for i in value['items']),value['truncated'])
        actual=[]
        with patch('ashlar.weft_path_decode.decode_related_paths',side_effect=decode):
            for case in cases[1:]:
                artifact={'columns':[{}, {'representation':{}}],'modelPins':[]};evidence={'native_result':{'rows':case['oracle']['rows']}}
                actual.extend(require_medical_case_bag(MODEL,GRAPH,bindings,case,artifact,evidence))
            case=cases[1]
            for change in ('missing','duplicate','foreign-edge','truncated'):
                evidence={'native_result':{'rows':json.loads(encoded(case['oracle']['rows']))}};cell=json.loads(evidence['native_result']['rows'][0][1])
                if change=='missing':cell['items']=[]
                elif change=='duplicate':cell['items']*=2
                elif change=='foreign-edge':cell['items'][0]['edges'][0]='9999'
                else:cell['truncated']=True
                evidence['native_result']['rows'][0][1]=encoded(cell)
                with self.subTest(change=change),self.assertRaises(ValueError):require_medical_case_bag(MODEL,GRAPH,bindings,case,artifact,evidence)
        graph=json.loads(GRAPH);expected=[[a['source'],a['key'],a['target'],b['key'],b['target']]for a in graph['edges']for b in graph['edges']if a['target']==b['source']]
        self.assertEqual(len(actual),20);self.assertEqual(Counter(map(encoded,actual)),Counter(map(encoded,expected)))
    def test_closed_selection_and_exact_bounded_file_references(self):
        paths=ImplicitArithmeticDistributionPaths(Path('/private/tmp/index'),Path('/private/tmp/compiler'))
        for cases in (('all-edges',),('singleton','singleton'),['singleton']):
            with self.assertRaises(ValueError):MedicalQueryBatch(paths,cases)
        buffer=MedicalSingletonBuffer(b'{"original":true}',b'{}',b'{}','{}')
        batch=MedicalQueryBatchBuffer((('singleton',buffer,'[]'),),'{}')
        self.assertEqual(len(batch.protocol_files()),3)
        self.assertEqual(batch.report()['cases'][0]['original_protocol']['response'],batch.report()['cases'][0]['original_protocol']['recompiled_response'])
        with self.assertRaises(ValueError):MedicalQueryBatchBuffer((('all-edges',buffer,'[]'),),'{}')
        with self.assertRaises(ValueError):MedicalQueryBatchBuffer((('two-hop-1',buffer,'[]'),),'{}')
    def test_one_native_boundary_per_batch_and_failed_closing_prevents_release(self):
        _,manifest,_,_=singleton_controls.MedicalSingletonTests().fixture();manifest.update(profile_version='ashlar-delta/0.3',validation_report_json='{"complete":true}',recorded_at='1')
        vector=manifest_pin_vector(manifest,{t:'uuid-'+str(i)for i,t in enumerate(json.loads(manifest['table_versions_json']))},authority='private-local-process')
        for failure in ('success','closing-only','cancel-and-closing'):
            calls=[];context=object();provider=object.__new__(MedicalBatchQueryProvider)
            provider.context=context;provider.active=False;provider.batch_context=None;provider.batch_closed=None;provider.closed=None;provider.vector=vector
            provider.resolved=SimpleNamespace(descriptor=SimpleNamespace(raw=manifest,publication_id=manifest['publication_id']))
            primary=KeyboardInterrupt();count=[0]
            def observe(*args,**kwargs):
                count[0]+=1;calls.append('full-observation')
                if failure!='success' and count[0]==2:raise OSError('closing')
                return {'native':{},'protected_ack':{'position':'1','request_hex':'72','manifest_hex':'6d','receipt_hex':'61'},'qualification':'inert'}
            driver=SimpleNamespace(_held_original_vector=vector,pin_held=True,require=lambda c:None,
                renew_original_graph_read_source=lambda *args,**kwargs:calls.append('renew'),observe_original_medical_read=observe,
                policy=SimpleNamespace(active={'manifest':manifest},renew=lambda:None),transport=SimpleNamespace(spark=SimpleNamespace(version='4.0.1')))
            provider.driver=driver;released=[]
            try:
                with provider.batch_interval(context):
                    for _ in range(2):
                        with provider.interval(context):provider.resolve(context)
                    if failure=='cancel-and-closing':raise primary
                released.append(True)
            except KeyboardInterrupt as caught:
                self.assertEqual(failure,'cancel-and-closing');self.assertIs(caught,primary)
            except OSError:
                self.assertEqual(failure,'closing-only')
            self.assertEqual(count[0],2);self.assertIsNone(provider.batch_context);self.assertFalse(provider.active)
            self.assertEqual(released,[True]if failure=='success' else[]);self.assertGreater(calls.count('renew'),2)

if __name__=='__main__':unittest.main()
