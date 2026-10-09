import base64
import copy
import json
from pathlib import Path
import shutil
import tempfile
import unittest

from domain_pack_inventory import InventoryError, sha256
from prepare_commerce_values import literal_request, prepare_values

ROOT=Path(__file__).resolve().parents[1]


class CommerceValueRequestTests(unittest.TestCase):
    def setUp(self):
        self.temp=tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.source=Path(self.temp.name)/'commerce'
        shutil.copytree(ROOT/'examples/domain-packs/commerce/upstream',self.source)
        self.inventory=json.loads((ROOT/'examples/domain-packs/inventory.json').read_bytes())
        self.inspection=json.loads((ROOT/'docs/helix/04-build/evidence/commerce-domain-interpretation-20261009.json').read_bytes())

    def prepare(self):
        return prepare_values(self.source,self.inventory,self.inspection)

    def test_original_rows_and_exact_literals_remain_unadmitted(self):
        result=self.prepare()
        self.assertEqual(len(result['rows']),11)
        self.assertEqual(len({r['sourceRowKey'] for r in result['rows']}),11)
        product=next(r for r in result['rows'] if r['record']['element']=='products')
        values={f['column']:f for f in product['fields']}
        self.assertEqual(values['unit_price']['value'],{'decimalToken':'12.50'})
        self.assertEqual(values['id']['value'],{'string':'P1'})
        order=next(r for r in result['rows'] if r['record']['element']=='orders')
        self.assertEqual(next(f['value'] for f in order['fields'] if f['column']=='ordered_at'),{'string':'2026-01-01T12:00:00Z'})
        self.assertTrue(all(f['public_umf_validation']=='unexecuted' for r in result['rows'] for f in r['fields']))
        for row in result['rows']:
            raw=base64.b64decode(row['source']['rawRowBase64'])
            self.assertEqual(sha256(raw),row['source']['rawRowSha256'])
            self.assertIn(raw,(self.source/row['source']['path']).read_bytes())
        for source in result['sources']:
            self.assertEqual(base64.b64decode(source['sourceBase64']),(self.source/source['path']).read_bytes())

    def test_no_numeric_conversion_and_no_null_sentinel(self):
        decimal={'kind':'field','cardinality':'one','scalarType':'decimal'}
        integer={'kind':'field','cardinality':'one','scalarType':'integer'}
        string={'kind':'field','cardinality':'one','scalarType':'string'}
        self.assertEqual(literal_request(decimal,'-0.1000e+3'),{'decimalToken':'-0.1000e+3'})
        self.assertEqual(literal_request(integer,'9007199254740993'),{'integerToken':'9007199254740993'})
        for text in ['', 'null', '\\N', '  quoted  ', '雪']:
            self.assertEqual(literal_request(string,text),{'string':text})
        # Even malformed lexical numeric requests are unadmitted inputs for UMF;
        # this constructor does not duplicate UMF's numeric grammar validator.
        self.assertEqual(literal_request(decimal,'not-a-number'),{'decimalToken':'not-a-number'})

    def test_unknown_family_collection_and_record_refuse(self):
        for field in [{'kind':'field','cardinality':'one','scalarType':'float'},
                      {'kind':'field','cardinality':'array','scalarType':'string'},
                      {'kind':'record','cardinality':'one','scalarType':'string'}]:
            with self.assertRaises(InventoryError):
                literal_request(field,'x')

    def test_changed_original_csv_and_unknown_extra_file_refuse(self):
        path=self.source/'data/products.csv';raw=path.read_bytes()
        path.write_bytes(raw.replace(b'12.50',b'12.51'))
        with self.assertRaises(InventoryError):self.prepare()
        path.write_bytes(raw)
        (self.source/'extra').write_bytes(b'unknown addition')
        with self.assertRaises(InventoryError):self.prepare()

    def test_symlink_copy_refuses(self):
        p=self.source/'data/products.csv';raw=p.read_bytes();p.unlink()
        target=Path(self.temp.name)/'target';target.write_bytes(raw);p.symlink_to(target)
        with self.assertRaises(InventoryError):self.prepare()

    def test_stale_partial_or_forged_receipt_refuses(self):
        original=copy.deepcopy(self.inspection)
        mutations=[lambda i:i.update(validatorRevision='0'*40),
                   lambda i:i.update(sourceSha256='0'*64),
                   lambda i:i['validation'].update(complete=False),
                   lambda i:i['selection']['selection'].pop(),
                   lambda i:i['selection']['source'].update(umf='0.7.0'),
                   lambda i:i['resultSchema'].update(id='urn:umf:core:element-selection:0.7.0')]
        for mutate in mutations:
            self.inspection=copy.deepcopy(original);mutate(self.inspection)
            with self.subTest(mutate=mutate),self.assertRaises(InventoryError):self.prepare()

    def test_source_pin_and_path_inventory_refuse(self):
        self.inventory['commit']='0'*40
        with self.assertRaises(InventoryError):self.prepare()
        self.inventory['commit']=self.inspection['validatorRevision']
        self.inventory['packs'][next(i for i,p in enumerate(self.inventory['packs']) if p['directory']=='commerce')]['files'][0]['path']='../escape'
        with self.assertRaises(InventoryError):self.prepare()


if __name__=='__main__':unittest.main()
