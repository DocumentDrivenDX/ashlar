# SPDX-License-Identifier: MIT OR Apache-2.0
"""Small original-core/held composition controls; no native engine qualification."""
import copy,json,unittest
from unittest import mock
from ashlar.graph_release import GraphRelease
from ashlar_host import supply_chain_graphframes as engine
from ashlar_host.graphframes_release import GraphFramesRuntimeConfig,execute_supply_chain_graphframes_release
from ashlar_host.supply_chain_graph_release import hold_original_supply_chain_graph_release
from ashlar_host.graphframes_pack_scenarios import common_oracle
import test_supply_chain_graph_release_owner as original_fixture


class SupplyChainGraphFramesTests(unittest.TestCase):
    def setUp(self):
        # Reuse the existing original-model/core-query fixture, not an engine or
        # source-admission substitute. Production nominal owner guards stay live.
        self.fixture=original_fixture.Controls('test_core_full_original18_23_columns_values_nulls_endpoints_identity_and_closing')
        self.fixture.setUp();self.addCleanup(self.fixture.doCleanups)

    def test_full_original_common_profile_and_held_consumer_closing(self):
        f=self.fixture
        with hold_original_supply_chain_graph_release(f.opened)as(release,renew):
            profile=engine.prepare_supply_chain_graphframes_profile(f.opened,release)
            self.assertTrue(f.active);self.assertEqual(profile['profile'],engine.PROFILE)
            self.assertEqual((len(profile['nodes']),len(profile['edges'])),(20,23))
            self.assertEqual(profile['cases'],[]);self.assertEqual(profile['fields'],[])
            expected=common_oracle(json.loads(f.opened.graph))
            self.assertEqual(len(expected['one-hop']),23)
            self.assertGreater(len(expected['two-hop']),0)
            self.assertGreaterEqual(expected['filtered-total'],2)
            self.assertEqual(len({row['graph_id']for row in profile['edges']}),23)
            # The new common-only result crosses the real existing derived
            # snapshot validator; it cannot impersonate authored scenario passes.
            from ashlar_host.graphframes_release import validate_derived_evidence
            from pathlib import Path
            work=Path('/operator/supply-chain-engine');payload=json.loads(release.payload)
            derived={role:{'uuid':'00000000-0000-0000-0000-00000000000'+str(i),
                'version':0,'location':(work/role).as_uri(),'minReaderVersion':1,
                'minWriterVersion':2,'tableFeatures':['appendOnly','invariants']}
                for role,i in [('nodes',2),('edges',3)]}
            result={'format':'ashlar-common-pack-graphframes/0.1','pack':'supply-chain',
                'cases':[],'common':{},'common_cases_passed':7,'full_carrier_parity':True,
                'authority_scope':'Inert protocol control only','derived_snapshots':derived,
                'original_snapshots':payload['snapshots']}
            witnesses=[{'kind':'derived-snapshot','phase':phase,'role':role,
                'detail':copy.deepcopy(detail),'history':[[0]]}
                for role,detail in derived.items()for phase in ('opening','before-query','closing')]
            validate_derived_evidence(result,payload,witnesses,work)
            for changes in ({'common_cases_passed':True},{'pack':'ecology'},
                    {'cases':[{'original_scenario':{'id':'replay'}}]}):
                with self.assertRaises(ValueError):
                    validate_derived_evidence(dict(result,**changes),payload,witnesses,work)
            renew()
        self.assertFalse(f.active);self.assertIn('closed-custody',f.calls)
        with self.assertRaises(PermissionError):renew()
        with self.assertRaises(PermissionError):engine.prepare_supply_chain_graphframes_profile(f.opened,release)

    def test_missing_duplicate_endpoint_and_original_property_carriers_refuse(self):
        f=self.fixture
        with hold_original_supply_chain_graph_release(f.opened)as(release,_):
            for mutation in ('missing','duplicate','endpoint','property'):
                with self.subTest(mutation=mutation):
                    value=json.loads(release.payload)
                    if mutation=='missing':value['edges'].pop()
                    elif mutation=='duplicate':value['edges'][-1]=copy.deepcopy(value['edges'][0])
                    elif mutation=='endpoint':value['edges'][0]['dst']=value['edges'][0]['src']
                    else:value['nodes'][0]['props_json']='{}'
                    import hashlib
                    raw=json.dumps(value).encode();bad=GraphRelease(raw,hashlib.sha256(raw).hexdigest())
                    with self.assertRaises((PermissionError,ValueError)):
                        engine.prepare_supply_chain_graphframes_profile(f.opened,bad)

    def test_shared_worker_seam_is_inside_actual_interval_and_cancel_closes(self):
        f=self.fixture;runtime=object.__new__(GraphFramesRuntimeConfig);failure=KeyboardInterrupt()
        def child(*args):
            self.assertTrue(f.active);self.assertEqual(args[4]['pack'],'supply-chain')
            args[5]();raise failure
        with mock.patch('ashlar_host.graphframes_release._execute_prepared_graphframes',side_effect=child):
            with self.assertRaises(KeyboardInterrupt)as caught:
                execute_supply_chain_graphframes_release(f.opened,runtime,object(),object(),lambda _:None)
        self.assertIs(caught.exception,failure);self.assertFalse(f.active)
        self.assertGreaterEqual(f.reads,2)

    def test_changed_closing_graph_withholds_worker_result(self):
        f=self.fixture;runtime=object.__new__(GraphFramesRuntimeConfig)
        def child(*args):
            f.rows['edge_current'].pop()
            return {'not_released':True}
        with mock.patch('ashlar_host.graphframes_release._execute_prepared_graphframes',side_effect=child):
            with self.assertRaises(PermissionError):
                execute_supply_chain_graphframes_release(f.opened,runtime,object(),object(),lambda _:None)
        self.assertFalse(f.active)
