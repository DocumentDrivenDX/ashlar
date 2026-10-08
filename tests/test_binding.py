import json
from pathlib import Path
import unittest
from ashlar.binding import BindingError,plan_string_record_binding
from ashlar.catalog import Identity,plan_catalog_ids

ROOT=Path(__file__).resolve().parents[1]
PIN='16c35e8d943769ccfa7bb57d16785aa7159abe65'
class BindingTests(unittest.TestCase):
    def report(self,v):return (ROOT/f'examples/end-to-end/schema-v{v}.interpretation.json').read_bytes()
    def plan(self,raw):return plan_string_record_binding(raw,trusted_validator_revision=PIN)
    def test_unknown_optional_not_translated(self):
        plan=self.plan(self.report(2))
        self.assertEqual(plan['status'],'blocked')
        self.assertEqual([b['sourcePointer'] for b in plan['blocked']],['/modules/0/elements/2/nullability'])
        self.assertEqual(plan['properties'][1]['nullability'],{'state':'unknown','value':'optional'})
    def test_explicit_absent_allowed_is_candidate_only(self):
        plan=self.plan(self.report(3))
        self.assertEqual(plan['status'],'candidate')
        self.assertEqual(len(plan['types']),1)
        self.assertEqual(len(plan['properties']),2)
        self.assertTrue(all(x['enforcement']=='engine-unimplemented' for x in plan['properties']))
        desired=[Identity('type',tuple(x['identity'])) for x in plan['types']]+[Identity('property',tuple(x['identity'])) for x in plan['properties']]
        ids=plan_catalog_ids([],desired,highwater={'type':0,'property':0,'relationship':0},expected_head='0')
        self.assertEqual({e.identity.parts[-1]:e.catalog_id for e in ids.entries},{'item':1,'caption':1,'label':2})
    def test_incomplete_foreign_duplicate_correspondence_refuses(self):
        for change in ['missing','duplicate','foreign','source']:
            value=json.loads(self.report(3))
            if change=='missing':value['elements'].pop()
            if change=='duplicate':value['elements'].append(value['elements'][0])
            if change=='foreign':value['elements'][0]['identity'][0]='other'
            if change=='source':value['elements'][0]['kind']['source']['id']='other'
            with self.subTest(change=change):
                with self.assertRaises(BindingError):self.plan(json.dumps(value).encode())
    def test_wrong_source_pin_refuses(self):
        with self.assertRaises(BindingError):plan_string_record_binding(self.report(3),trusted_validator_revision='0'*40)
if __name__=='__main__':unittest.main()
