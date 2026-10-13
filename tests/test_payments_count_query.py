"""Inert request correspondence; these tests do not qualify native execution."""
import copy
import json
import unittest
from pathlib import Path
from ashlar_host.finite_pack import FinitePackDefinition
from ashlar_host.payments_count_query import payments_count_request,payments_count_oracle

class Tests(unittest.TestCase):
    def inputs(self):
        root=Path(__file__).resolve().parents[1]/'examples/domain-packs/payments/upstream'
        model=(root/'ontology.json').read_bytes();graph=(root/'graph/fixture.json').read_bytes()
        definition=FinitePackDefinition('payments');_,bindings=definition.build(model,graph)
        roles=('object_current','edge_current','tombstone','whole_source_history','attempts','manifest')
        registry=[{'table':'c.s.'+role,'uuid':'00000000-0000-0000-0000-'+str(i+1).zfill(12)}for i,role in enumerate(roles)]
        versions={'c.s.'+role:1 for role in roles[:4]}
        manifest={'publication_id':'inert-payments','table_versions_json':json.dumps(versions),
            'source_progress_json':json.dumps({definition.facts()['source_system']:{}})}
        aliases={table:table for table in versions}
        return model,graph,bindings,manifest,registry,aliases

    def test_original_base_count_request_and_independent_population(self):
        values=self.inputs();request=payments_count_request('ledger_entries',*values)
        self.assertEqual(request['sql'],'SELECT COUNT(*) AS n FROM ledger_entries a')
        self.assertEqual(request['modules'][0]['documentJson'].encode(),values[0])
        self.assertEqual(request['interfaceVersion'],'weft-compile/0.4.1')
        binding=json.loads(request['target']['bindingJson'])
        self.assertEqual(len(binding['publication']['tables']),4)
        self.assertEqual(payments_count_oracle('ledger_entries',*values[:2])['rows'],[['4']])
        self.assertEqual(payments_count_oracle('returns',*values[:2])['rows'],[['1']])

    def test_unknown_record_and_changed_binding_refused(self):
        values=self.inputs()
        with self.assertRaises(ValueError):payments_count_request('ledger_entries WHERE true',*values)
        bad=list(copy.deepcopy(values));bad[2]['properties'][0]['property_id']='9999'
        with self.assertRaises(ValueError):payments_count_request('ledger_entries',*bad)

if __name__=='__main__':unittest.main()
