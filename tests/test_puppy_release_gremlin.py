"""Inert full-column/GraphSON controls; native Gremlin remains separately qualified."""
import copy
import json
import unittest
import test_puppy_release_adapter as fixtures
from puppy_release_adapter import PuppyNativeReleaseAdapter, encoded
from puppy_release_gremlin import GremlinReleaseProjection
from ashlar_host.puppy_release import PuppyReleaseOwner, PuppyReleaseError


class Tests(unittest.TestCase):
    def setUp(self):
        self.fixture = fixtures.Tests(methodName='runTest')
        self.fixture.setUp(); self.addCleanup(self.fixture.doCleanups)
        self.queries = []; self.mutate = None
        self.adapter = PuppyNativeReleaseAdapter(prepared=self.fixture.prepared,
            prior_model=encoded(self.fixture.schema), http=self.fixture.http,
            runtime=self.fixture.runtime, projection=GremlinReleaseProjection(self.submit),
            evidence=self.fixture.directory/'gremlin-evidence')
        self.fixture.adapter = self.adapter
        self.owner = PuppyReleaseOwner(self.adapter, fixtures.Policy())

    def submit(self, query, context):
        self.queries.append(query)
        label = next(label for label in self.fixture.values if "hasLabel('"+label+"')" in query)
        rows = self.fixture.query(':'+label, context)
        # GraphSON 3 may preserve string wrappers for native identity. Exact
        # strings/nulls are the only supported decoded property carriers.
        for row in rows:
            for name, value in list(row.items()):
                if name.startswith('native_'):
                    row[name] = {'@type':'g:String','@value':value}
        if self.mutate is not None: self.mutate(rows)
        return rows

    def test_full_columns_nulls_and_original_identity_across_releases(self):
        old = self.owner.refresh(self.fixture.releases[0], context=None)
        before = self.owner.read(old, context=None)
        new = self.owner.refresh(self.fixture.releases[1], context=None)
        self.assertEqual(self.owner.read(old, context=None), before)
        self.owner.read(new, context=None)
        self.assertTrue(all('.coalesce(__.values(' in query for query in self.queries))
        self.assertTrue(any('.by(__.outV().id()).by(__.inV().id())' in query for query in self.queries))
        self.assertTrue(any("'carrier_id'" in query and "'carrier_key'" in query for query in self.queries))
        native = sorted(self.adapter.evidence.glob('*-gremlin-native-query.json'))
        self.assertTrue(native)
        self.assertEqual(json.loads(native[0].read_bytes())['rows'][0]['native_id']['@type'], 'g:String')

    def test_unknown_graphson_and_numeric_promotion_refuse(self):
        handle = self.owner.refresh(self.fixture.releases[0], context=None)
        for value in [True, 9007199254740993, ['original'],
                      {'@type':'g:Int64','@value':'9007199254740993'},
                      {'@type':'g:String','@value':1},
                      {'@type':'g:String','@value':'original','extra':True}]:
            with self.subTest(value=value):
                self.mutate = lambda rows: rows[0].update(id=copy.deepcopy(value))
                with self.assertRaises(PuppyReleaseError): self.owner.read(handle, context=None)

    def test_full_oracle_refuses_missing_values_and_changed_endpoints(self):
        handle = self.owner.refresh(self.fixture.releases[0], context=None)
        for mutation in [lambda rows:rows[0].pop('props_json'),
                         lambda rows:rows.append(copy.deepcopy(rows[0])),
                         lambda rows:rows[0].update(native_id='other-release[other]'),
                         lambda rows:rows[0].update(props_json='{}'),
                         lambda rows:rows[0].update(native_source='other[endpoint]') if 'native_source' in rows[0] else None]:
            with self.subTest(mutation=mutation):
                self.mutate = mutation
                with self.assertRaises(PuppyReleaseError): self.owner.read(handle, context=None)

    def test_native_closing_gate_and_label_injection_refuse(self):
        handle = self.owner.refresh(self.fixture.releases[0], context=None)
        self.fixture.drift = True
        with self.assertRaises(PuppyReleaseError): self.owner.read(handle, context=None)
        projection = GremlinReleaseProjection(lambda query, context:self.fail('Must refuse before transport'))
        for kind, label in [('node',"label');g.V().drop();//"), ('node',handle.edge_label), ('edge',handle.node_label)]:
            with self.assertRaises(PuppyReleaseError): projection(kind,label,None,self.adapter.retain)

if __name__ == '__main__': unittest.main()
