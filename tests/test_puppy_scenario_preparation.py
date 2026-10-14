"""Original17 pure preparation and inert ownership controls; no native claims."""
import unittest
import json
import tempfile
from pathlib import Path
from dataclasses import replace
from unittest.mock import patch
from ashlar_host.puppy_scenario_preparation import (PuppyScenarioPreparationConfig,
    project_puppy_scenario_pack, prepare_puppy_scenarios)
ROOT=Path(__file__).resolve().parents[1]

class Tests(unittest.TestCase):
    def selected(self,name):
        folder=ROOT/'examples/domain-packs'/name/'upstream'
        release=ROOT/'docs/helix/04-build/evidence/original17-graphframes-native-20261009/original'/('1-release.graph.json' if name=='archaeology' else '9-release.graph.json')
        return [(folder/n).read_bytes() for n in ('ontology.json','graph/fixture.json','pack.json')]+[release.read_bytes()]
    def test_original_both_profiles_and_all_seventeen_oracles(self):
        total=0;observations=0
        for name,nodes,edges in [('archaeology',39,46),('ecology',40,54)]:
            item=project_puppy_scenario_pack(name,*self.selected(name))
            self.assertEqual((len(item['nodes']),len(item['edges'])),(nodes,edges))
            total+=len(item['profile']['cases'])
            for row in item['nodes']:
                for column in item['long_columns']:
                    if row[column] is not None:observations+=1;self.assertIs(type(row[column]),int)
            self.assertNotIn('original_bindings_base64',item['profile']['source_admission'])
        self.assertEqual(total,17);self.assertEqual(observations,7)
    def test_missing_population_and_original_bytes_refuse(self):
        selected=self.selected('archaeology');value=json.loads(selected[-1]);value['nodes'].pop();selected[-1]=json.dumps(value).encode()
        with self.assertRaises(ValueError):project_puppy_scenario_pack('archaeology',*selected)
        selected=self.selected('ecology');selected[0]+=b' '
        with self.assertRaises(ValueError):project_puppy_scenario_pack('ecology',*selected)
    def test_missing_nominal_owners_refuse_before_output_or_sdk(self):
        with tempfile.TemporaryDirectory() as tmp:
            output=Path(tmp).resolve()/'fresh';config=PuppyScenarioPreparationConfig(tuple(self.selected(n)[2]for n in ('archaeology','ecology')),output)
            with patch.dict('sys.modules',{'duckdb':None}):
                with self.assertRaises(ValueError):prepare_puppy_scenarios(config,sources=(object(),object()),exports=(object(),object()))
            self.assertFalse(output.exists())

    def test_four_real_duckdb_tables_under_inert_nominal_owner_ports(self):
        from contextlib import contextmanager
        from ashlar.graph_release import GraphRelease
        from ashlar_host.finite_pack import FinitePackDefinition
        from ashlar_host.finite_dataset import FinitePackSource
        from ashlar_host.local_graph_export import LocalGraphExportUse
        import hashlib
        try:import duckdb
        except ImportError:self.skipTest('Optional DuckDB not selected')
        # Inert lower ports test orchestration; no producer/source authority is minted.
        @contextmanager
        def interval(owner,context):yield
        with tempfile.TemporaryDirectory() as tmp:
            root=Path(tmp).resolve();root.chmod(0o700);sources=[];exports=[];packs=[];checks=[]
            for name in ('archaeology','ecology'):
                model,graph,pack,raw=self.selected(name);context=object()
                source=object.__new__(FinitePackSource);source.definition=FinitePackDefinition(name);source.model=model;source.graph=graph;source.context=context
                export=object.__new__(LocalGraphExportUse);export._context=context;export._release=GraphRelease(raw,hashlib.sha256(raw).hexdigest());export._custody_bytes=b'inert';export._custody_sha256='0'*64;export._source=object()
                sources.append(source);exports.append(export);packs.append(pack)
            def metadata(source):return source.definition.inputs(source.model,source.graph)
            def admit(owner,*args):checks.append(owner.context)
            output=root/'prepared'
            with patch.object(FinitePackSource,'metadata',metadata),patch.object(LocalGraphExportUse,'writer',interval),patch.object(LocalGraphExportUse,'admit_original_export',admit):
                result=prepare_puppy_scenarios(PuppyScenarioPreparationConfig(tuple(packs),output),sources=tuple(sources),exports=tuple(exports))
            self.assertEqual([o['rows']for o in result['observations']],[39,46,40,54]);self.assertEqual(len(result['reports']),2)
            with duckdb.connect(str(output/'scenarios.duckdb'),read_only=True) as connection:
                self.assertEqual(connection.execute("SELECT count(*) FROM information_schema.tables WHERE table_schema='carrier'").fetchone()[0],4)
            self.assertEqual((output/'scenarios.duckdb').stat().st_mode&0o777,0o444);self.assertGreaterEqual(len(checks),6)
            raw=(output/'model.json').read_bytes()
            self.assertEqual(result['model_bytes'],len(raw));self.assertEqual(result['model_sha256'],hashlib.sha256(raw).hexdigest())
            # First source cancellation must not skip the other acquired owner renewals.
            calls={id(source):0 for source in sources}; checks.clear();cancel=KeyboardInterrupt()
            def first_fails(source):
                calls[id(source)]+=1
                if source is sources[0] and calls[id(source)]>=2:raise cancel
                return metadata(source)
            with patch.object(FinitePackSource,'metadata',first_fails),patch.object(LocalGraphExportUse,'writer',interval),patch.object(LocalGraphExportUse,'admit_original_export',admit):
                with self.assertRaises(KeyboardInterrupt) as caught:
                    prepare_puppy_scenarios(PuppyScenarioPreparationConfig(tuple(packs),root/'refused'),sources=tuple(sources),exports=tuple(exports))
            self.assertIs(caught.exception,cancel)
            self.assertGreaterEqual(calls[id(sources[1])],3)
            self.assertGreaterEqual(checks.count(exports[0].context),2)
            self.assertGreaterEqual(checks.count(exports[1].context),2)
            self.assertFalse((root/'refused').exists())

