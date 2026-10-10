import copy,json,unittest
from pathlib import Path
from prepare_commerce_evolution import prepare
from ashlar.commerce_source import build_transaction
BASE=Path(__file__).resolve().parents[1]/'examples/domain-packs/commerce/upstream'
class CommerceEvolutionTests(unittest.TestCase):
    def original(self):return (BASE/'ontology.json').read_bytes(),(BASE/'graph/fixture.json').read_bytes()
    def test_original_bindings_remain_exact_and_new_id_appends(self):
        s,g=self.original();_,old=build_transaction(s,g,source_system='commerce-evolution-preparation');r=prepare(s,g)
        for k in ('types','entities'):self.assertEqual(r['registry'][k],old[k])
        self.assertEqual(r['registry']['properties'][:-1],old['properties'])
        self.assertEqual(int(r['registry']['properties'][-1]['property_id']),max(int(p['property_id'])for p in old['properties'])+1)
        self.assertEqual(len({p['property_id']for p in r['registry']['properties']}),35)
    def test_exact_tokens_delete_and_absence_are_preserved(self):
        s,g=self.original();r=prepare(s,g);a,b,c,d,e=r['revisions']
        self.assertEqual(a['source'],json.loads(s));self.assertEqual(a['graph'],json.loads(g))
        self.assertEqual(next(o for o in b['graph']['objects']if o['type']['element']=='products')['values']['products.unit_price'],'12.75')
        self.assertEqual((len(c['graph']['objects']),len(c['graph']['edges'])),(10,9))
        self.assertFalse(any(c['deletedOriginalKey']in (x['source'],x['target'])for x in c['graph']['edges']))
        self.assertTrue(all('products.evolution_note'not in o['values']for o in d['graph']['objects']))
        self.assertEqual(d['evolutionCompatibility'],'not-admitted');self.assertEqual(e['evolutionCompatibility'],'not-admitted')
    def test_addition_preserves_all_existing_content_order_and_unknowns(self):
        source,graph=self.original();result=prepare(source,graph)
        original=result['revisions'][0]['source'];added=copy.deepcopy(result['revisions'][3]['source'])
        module=next(m for m in added['modules']if m['id']=='domain')
        field=module['elements'].pop();self.assertEqual(field['nullability'],'absent-allowed')
        record=next(e for e in module['elements']if e['id']=='products')
        self.assertEqual(record['members'].pop(),{'module':'domain','element':'products.evolution_note'})
        self.assertEqual(added,original)
        breaking=copy.deepcopy(result['revisions'][4]['source'])
        field=next(e for m in breaking['modules']for e in m['elements']if e['id']=='order_lines.quantity')
        self.assertEqual(field['scalarType'],'string');field['scalarType']='integer'
        self.assertEqual(breaking,result['revisions'][3]['source'])
    def test_wrong_original_bytes_refuse(self):
        s,g=self.original()
        for a,b in [(s+b' ',g),(s,g+b' ')]:
            with self.assertRaises(ValueError):prepare(a,b)
